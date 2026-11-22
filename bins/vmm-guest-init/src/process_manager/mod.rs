//! Supervises the main process and the processes that RPC callers start.

mod cgroup;
mod exec;
mod main_process;
mod table;

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use vmm_guest_proto::rpc::{
    ErrorCode, ProcessStartReply, ProcessStartRequest, QuiesceReply, QuiesceRequest, RpcError,
    Signal,
};

pub use self::exec::run as run_exec;
pub use self::main_process::MainProcess;
pub use self::table::ProcessTable;
use crate::lock;
use crate::reaper::Reaper;
use crate::spawn::SpawnContext;

/// Time that a killed process group gets to exit before a caller stops waiting.
const KILL_WAIT: Duration = Duration::from_secs(5);
/// Time to collect the last output of an exited process. A descendant that
/// keeps the output open cannot hold a caller for longer.
const OUTPUT_DRAIN: Duration = Duration::from_millis(250);

pub fn signal_number(signal: Signal) -> libc::c_int {
    match signal {
        Signal::Term => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
        Signal::Int => libc::SIGINT,
        Signal::Hup => libc::SIGHUP,
        Signal::Quit => libc::SIGQUIT,
        Signal::Usr1 => libc::SIGUSR1,
        Signal::Usr2 => libc::SIGUSR2,
    }
}

struct Admission {
    closed: bool,
    active_execs: u32,
    idle_since: Instant,
}

pub struct ProcessManager {
    reaper: &'static Reaper,
    context: SpawnContext,
    admission: Mutex<Admission>,
    exec_seq: AtomicU64,
    pub main: MainProcess,
    pub table: ProcessTable,
}

/// One admitted `Exec`. `Quiesce` succeeds only when none is held.
pub struct ExecPermit<'a>(&'a ProcessManager);

impl Drop for ExecPermit<'_> {
    fn drop(&mut self) {
        let mut admission = lock(&self.0.admission);
        admission.active_execs -= 1;
        admission.idle_since = Instant::now();
    }
}

impl ProcessManager {
    pub fn new(context: SpawnContext, log_limit_bytes: usize) -> Self {
        Self {
            reaper: Reaper::global(),
            context,
            admission: Mutex::new(Admission {
                closed: false,
                active_execs: 0,
                idle_since: Instant::now(),
            }),
            exec_seq: AtomicU64::new(0),
            main: MainProcess::new(log_limit_bytes),
            table: ProcessTable::default(),
        }
    }

    pub(super) fn next_exec_seq(&self) -> u64 {
        self.exec_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn reaper(&self) -> &'static Reaper {
        self.reaper
    }

    /// Starts the main process of the boot configuration.
    pub fn start_main(&self, argv: Vec<String>) {
        self.main.start(self.reaper, &self.context, argv);
    }

    fn closed() -> RpcError {
        RpcError::new(ErrorCode::Unavailable, "the guest admits no new process")
    }

    fn admit_exec(&self) -> Result<ExecPermit<'_>, RpcError> {
        let mut admission = lock(&self.admission);
        if admission.closed {
            return Err(Self::closed());
        }
        admission.active_execs += 1;
        Ok(ExecPermit(self))
    }

    pub fn start_process(
        &self,
        request: ProcessStartRequest,
    ) -> Result<ProcessStartReply, RpcError> {
        if lock(&self.admission).closed {
            return Err(Self::closed());
        }
        self.table.start(self.reaper, &self.context, request)
    }

    /// Closes admission when no `Exec` ran for the requested time, or reopens it.
    pub fn quiesce(&self, request: &QuiesceRequest) -> QuiesceReply {
        let mut admission = lock(&self.admission);
        let idle = match admission.active_execs {
            0 => admission.idle_since.elapsed(),
            _ => Duration::ZERO,
        };
        if request.cancel {
            admission.closed = false;
        } else if admission.active_execs == 0
            && idle >= Duration::from_millis(request.minimum_idle_ms)
        {
            admission.closed = true;
        }
        QuiesceReply {
            quiesced: admission.closed,
            active_execs: admission.active_execs,
            running_processes: self.table.running().len() as u32,
            idle_ms: idle.as_millis() as u64,
        }
    }

    pub fn is_quiesced(&self) -> bool {
        lock(&self.admission).closed
    }

    /// Closes admission, then stops the main process and each started process:
    /// `SIGTERM`, and `SIGKILL` for the process groups that outlive `grace`.
    pub fn stop_all(&self, grace: Duration) {
        lock(&self.admission).closed = true;
        let mut targets = self.table.running();
        targets.extend(self.main.running());
        for (pid, _) in &targets {
            let _ = self.reaper.signal(*pid, libc::SIGTERM, true);
        }
        let deadline = Instant::now() + grace;
        for (pid, exit) in &targets {
            if exit
                .wait(Some(deadline.saturating_duration_since(Instant::now())))
                .is_none()
            {
                let _ = self.reaper.signal(*pid, libc::SIGKILL, true);
            }
        }
        let deadline = Instant::now() + KILL_WAIT;
        for (_, exit) in &targets {
            exit.wait(Some(deadline.saturating_duration_since(Instant::now())));
        }
    }
}
