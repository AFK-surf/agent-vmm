//! The main process of the workload, and its output log.

use std::fs::File;
use std::io::Read;
use std::os::fd::OwnedFd;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use vmm_guest_proto::rpc::{CommandSpec, ErrorCode, LogStream, MainState, RpcError};

use crate::log_ring::LogRing;
use crate::reaper::{ExitCell, Reaper};
use crate::spawn::{self, SpawnContext};
use crate::{lock, unix_nanos};

enum State {
    /// Shell-idle mode, or before the start.
    Idle,
    Started {
        pid: u32,
        started_unix_nanos: i64,
        exit: Arc<ExitCell>,
    },
    FailedToStart(String),
}

pub struct MainProcess {
    state: Mutex<State>,
    pub logs: Arc<LogRing>,
}

impl MainProcess {
    pub(super) fn new(log_limit_bytes: usize) -> Self {
        Self {
            state: Mutex::new(State::Idle),
            logs: Arc::new(LogRing::new(log_limit_bytes)),
        }
    }

    pub(super) fn start(&self, reaper: &Reaper, context: &SpawnContext, argv: Vec<String>) {
        let spec = CommandSpec {
            argv,
            env: Vec::new(),
            workdir: None,
            user: None,
        };
        let spawned = spawn::command(context, &spec, false).and_then(|mut command| {
            command
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            reaper.spawn(command, None).map_err(|error| {
                RpcError::new(
                    ErrorCode::Internal,
                    format!("start {:?}: {error}", spec.argv[0]),
                )
            })
        });
        let spawned = match spawned {
            Ok(spawned) => spawned,
            Err(error) => {
                *lock(&self.state) = State::FailedToStart(error.message);
                return;
            }
        };
        let sources = [
            (LogStream::Stdout, spawned.stdout.map(OwnedFd::from)),
            (LogStream::Stderr, spawned.stderr.map(OwnedFd::from)),
        ];
        for (stream, source) in sources {
            let Some(mut source) = source.map(File::from) else {
                continue;
            };
            let logs = self.logs.clone();
            logs.open_writer();
            // The thread blocks in `read` and ends with the last writer of the pipe.
            std::thread::spawn(move || {
                let mut buffer = vec![0u8; 64 * 1024];
                loop {
                    match source.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(count) => logs.push(stream, &buffer[..count]),
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(_) => break,
                    }
                }
                logs.close_writer();
            });
        }
        *lock(&self.state) = State::Started {
            pid: spawned.pid,
            started_unix_nanos: unix_nanos(),
            exit: spawned.exit,
        };
    }

    pub fn state(&self) -> MainState {
        match &*lock(&self.state) {
            State::Idle => MainState::Idle,
            State::FailedToStart(message) => MainState::FailedToStart {
                message: message.clone(),
            },
            State::Started {
                pid,
                started_unix_nanos,
                exit,
            } => match exit.get() {
                Some((exit, exited_unix_nanos)) => MainState::Exited {
                    exit,
                    exited_unix_nanos,
                },
                None => MainState::Running {
                    pid: *pid,
                    started_unix_nanos: *started_unix_nanos,
                },
            },
        }
    }

    /// Waits for the exit of a running main process, then returns the state.
    pub fn wait_exit(&self, timeout: Duration) -> MainState {
        if let Some((_, exit)) = self.running() {
            exit.wait(Some(timeout));
        }
        self.state()
    }

    pub(super) fn running(&self) -> Option<(u32, Arc<ExitCell>)> {
        match &*lock(&self.state) {
            State::Started { pid, exit, .. } if exit.get().is_none() => Some((*pid, exit.clone())),
            _ => None,
        }
    }

    pub fn signal(&self, reaper: &Reaper, signal: libc::c_int) -> Result<(), RpcError> {
        let not_running = || {
            RpcError::new(
                ErrorCode::FailedPrecondition,
                "the main process is not running",
            )
        };
        let (pid, _) = self.running().ok_or_else(not_running)?;
        match reaper.signal(pid, signal, false) {
            Ok(true) => Ok(()),
            Ok(false) => Err(not_running()),
            Err(error) => Err(crate::io_error(error)),
        }
    }
}
