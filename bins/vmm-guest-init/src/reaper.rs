//! Reaps every child of this process and reports the exit of the tracked ones.
//!
//! PID 1 inherits each orphan of the VM. One thread waits for `SIGCHLD` through
//! a pipe and collects all exited children, so no zombie remains and no thread polls.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::{AsFd, IntoRawFd};
use std::process::{ChildStderr, ChildStdin, ChildStdout, Command};
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use vmm_guest_proto::rpc::ExitStatus;

use crate::{lock, sys, unix_nanos};

static WAKE_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_sigchld(_signal: libc::c_int) {
    // SAFETY: only async-signal-safe calls, and `errno` is restored for the interrupted code.
    unsafe {
        let errno = *libc::__errno_location();
        libc::write(WAKE_FD.load(Ordering::Relaxed), [1u8].as_ptr().cast(), 1);
        *libc::__errno_location() = errno;
    }
}

/// Exit of one tracked child.
#[derive(Default)]
pub struct ExitCell {
    exit: Mutex<Option<(ExitStatus, i64)>>,
    changed: Condvar,
}

impl ExitCell {
    /// The exit status and its Unix time in nanoseconds.
    pub fn get(&self) -> Option<(ExitStatus, i64)> {
        *lock(&self.exit)
    }

    /// Waits for the exit. Returns `None` at the timeout.
    pub fn wait(&self, timeout: Option<Duration>) -> Option<(ExitStatus, i64)> {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut exit = lock(&self.exit);
        while exit.is_none() {
            match deadline {
                None => exit = self.changed.wait(exit).unwrap_or_else(|e| e.into_inner()),
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    exit = self
                        .changed
                        .wait_timeout(exit, remaining)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                }
            }
        }
        *exit
    }
}

pub struct Spawned {
    pub pid: u32,
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
    pub exit: Arc<ExitCell>,
}

type ExitHook = Box<dyn FnOnce() + Send>;

struct Tracked {
    exit: Arc<ExitCell>,
    on_exit: Option<ExitHook>,
}

pub struct Reaper {
    children: Mutex<HashMap<u32, Tracked>>,
    orphans: AtomicU64,
}

impl Reaper {
    /// The reaper of this process. The first call installs the `SIGCHLD`
    /// handler and starts the thread.
    pub fn global() -> &'static Reaper {
        static REAPER: OnceLock<Reaper> = OnceLock::new();
        REAPER.get_or_init(|| {
            let (wake_read, wake_write) = sys::pipe().expect("create the reaper pipe");
            sys::set_nonblocking(wake_write.as_fd()).expect("configure the reaper pipe");
            WAKE_FD.store(wake_write.into_raw_fd(), Ordering::Relaxed);
            // SAFETY: the handler is async-signal-safe and `action` is fully initialized.
            unsafe {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = on_sigchld as extern "C" fn(libc::c_int) as usize;
                action.sa_flags = libc::SA_RESTART | libc::SA_NOCLDSTOP;
                libc::sigemptyset(&mut action.sa_mask);
                sys::cvt(libc::sigaction(
                    libc::SIGCHLD,
                    &action,
                    std::ptr::null_mut(),
                ))
                .expect("install the SIGCHLD handler");
            }
            std::thread::Builder::new()
                .name("reaper".into())
                .spawn(move || {
                    let reaper = Reaper::global();
                    let mut wake = File::from(wake_read);
                    let mut drained = [0u8; 64];
                    loop {
                        reaper.reap();
                        if wake.read(&mut drained).is_err() {
                            return;
                        }
                    }
                })
                .expect("start the reaper thread");
            Reaper {
                children: Mutex::new(HashMap::new()),
                orphans: AtomicU64::new(0),
            }
        })
    }

    /// Starts a child and tracks its exit. `on_exit` runs on the reaper thread.
    ///
    /// The lock spans the start, so the reaper cannot collect the child before
    /// it is tracked. The command is dropped here to close its pipe ends.
    pub fn spawn(&self, mut command: Command, on_exit: Option<ExitHook>) -> io::Result<Spawned> {
        let mut children = lock(&self.children);
        let mut child = command.spawn()?;
        let exit = Arc::new(ExitCell::default());
        children.insert(
            child.id(),
            Tracked {
                exit: exit.clone(),
                on_exit,
            },
        );
        Ok(Spawned {
            pid: child.id(),
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            exit,
        })
    }

    /// Signals a tracked child that has not been reaped. Returns false after its exit.
    ///
    /// The lock excludes the reaper, so the PID cannot name another process.
    pub fn signal(&self, pid: u32, signal: libc::c_int, group: bool) -> io::Result<bool> {
        let children = lock(&self.children);
        if !children.contains_key(&pid) {
            return Ok(false);
        }
        match sys::kill(pid, signal, group) {
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(false),
            result => result.map(|()| true),
        }
    }

    pub fn orphans_reaped(&self) -> u64 {
        self.orphans.load(Ordering::Relaxed)
    }

    fn reap(&self) {
        loop {
            let mut children = lock(&self.children);
            let mut status = 0;
            // SAFETY: `status` is a valid `int` for the kernel to fill.
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid <= 0 {
                return;
            }
            let Some(tracked) = children.remove(&(pid as u32)) else {
                self.orphans.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            drop(children);
            let exit = if libc::WIFSIGNALED(status) {
                let signal = libc::WTERMSIG(status);
                ExitStatus {
                    code: 128 + signal,
                    signal: Some(signal),
                }
            } else {
                ExitStatus {
                    code: libc::WEXITSTATUS(status),
                    signal: None,
                }
            };
            *lock(&tracked.exit.exit) = Some((exit, unix_nanos()));
            tracked.exit.changed.notify_all();
            if let Some(on_exit) = tracked.on_exit {
                on_exit();
            }
        }
    }
}
