//! The effects of the managed commands: the lifecycle helper, the Host operator
//! socket, the clock, and the update preparation. Tests replace them.

use crate::envelope::LogRecord;
use crate::facts::Facts;
use crate::options::Options;
use agent_vmm_proto::host::v1 as hostv1;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use vmm_lifecycle::{Deadline, RpcCode};

/// One command observes for this long. A lifecycle owner that needs longer continues without the command.
pub const OBSERVATION_BUDGET: Duration = Duration::from_secs(30);
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// A failed call to the Host operator socket.
#[derive(Clone, Debug, Default)]
pub struct Failure {
    /// `None` when the call did not reach the Host.
    pub code: Option<RpcCode>,
    /// The `host.v1.ErrorReason` name without `ERROR_REASON_`. Empty when the Host named none.
    pub reason: String,
    pub metadata: HashMap<String, String>,
    pub message: String,
}

pub type Call<T> = Result<T, Failure>;

/// What a lifecycle helper process printed. `error` is set when it did not exit with success.
#[derive(Clone, Debug, Default)]
pub struct HelperOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct LogPage {
    pub records: Vec<LogRecord>,
    pub cursor: String,
    pub gap: bool,
}

/// Why an update artifact was not prepared. Lifecycle records it under the request.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparationError {
    pub stage: &'static str,
    pub code: String,
    pub retryable: bool,
    /// The download reached its time bound.
    pub cancelled: bool,
    pub message: String,
}

impl PreparationError {
    pub fn at(stage: &'static str, message: impl Into<String>) -> Self {
        Self { stage, code: format!("update_{stage}_failed"), retryable: false, cancelled: false, message: message.into() }
    }

    pub fn download_unavailable(message: impl Into<String>) -> Self {
        Self { stage: "download", code: "update_download_unavailable".into(), retryable: true, cancelled: false, message: message.into() }
    }
}

pub trait Runtime {
    fn now(&self) -> SystemTime;
    fn euid(&self) -> u32;
    /// Returns false when the budget ends before the interval does.
    fn sleep(&self, interval: Duration, budget: Deadline) -> bool;
    fn helper(&self, path: &str, arguments: &[String], input: Option<&[u8]>, budget: Deadline) -> HelperOutput;

    fn operation(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::Operation>>;
    fn environment(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::AgentEnvironment>>;
    fn registration(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::RemoteRegistration>>;
    fn impact_summary(&self, socket: &str, budget: Deadline) -> Call<hostv1::GetVmImpactSummaryResponse>;
    fn vm(&self, socket: &str, budget: Deadline) -> Call<Option<hostv1::Vm>>;
    fn start_vm(&self, socket: &str, request: hostv1::StartVmRequest, budget: Deadline) -> Call<Option<hostv1::Operation>>;
    fn stop_vm(&self, socket: &str, request: hostv1::StopVmRequest, budget: Deadline) -> Call<Option<hostv1::Operation>>;
    fn plan_cleanup(&self, socket: &str, environment_id: &str, kind: hostv1::CleanupKind, budget: Deadline) -> Call<Option<hostv1::CleanupPlan>>;
    fn execute_cleanup(&self, socket: &str, request: hostv1::ExecuteCleanupRequest, budget: Deadline) -> Call<Option<hostv1::Operation>>;
    fn set_host_socket(&self, socket: &str, request: hostv1::SetHostSocketRequest, budget: Deadline) -> Call<Option<hostv1::HostSocket>>;
    fn remove_host_socket(&self, socket: &str, request: hostv1::RemoveHostSocketRequest, budget: Deadline) -> Call<()>;
    fn host_sockets(&self, socket: &str, environment_id: Option<String>, budget: Deadline) -> Call<Vec<hostv1::HostSocket>>;
    /// Reads at most `options.limit` records of one container after `options.after_cursor`.
    fn container_logs(&self, socket: &str, options: &Options, budget: Deadline) -> Result<LogPage, String>;
    fn local_logs(&self, status: &Facts, options: &Options) -> Result<LogPage, String>;

    /// Downloads and unpacks the selected release next to the installed Host.
    /// Returns the prepared path, or an empty path when the installed Host is the target.
    fn prepare_update(&self, options: &Options, status: &Facts) -> Result<String, PreparationError>;
    fn remove_prepared(&self, path: &str) -> std::io::Result<()>;
}

/// One command: its effects and the end of its observation budget.
#[derive(Clone, Copy)]
pub struct Session<'a> {
    pub runtime: &'a dyn Runtime,
    pub budget: Deadline,
}
