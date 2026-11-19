//! Host installation, maintenance and local operation for the lifecycle helper
//! (`agent-vmm-lifecycle`, and `agent-vmm lifecycle` on Linux).
//!
//! Standard output carries exactly one JSON value. Comma Electron validates it
//! against a schema; an optional field that has no value is omitted.

mod account;
mod args;
mod cli;
mod cutover;
pub mod error;
pub mod grpc;
mod journal;
mod legacy;
mod local;
mod operation;
pub mod operator;
mod platform;
mod preflight;
mod registration;
mod registration_plan;
mod report;
mod schema;
mod service;
mod startup;
mod statefs;
mod system;
#[cfg(test)]
mod testing;
mod status;
pub mod timefmt;
mod update;

pub use cli::{main, run};
pub use error::{Deadline, Error, Kind, Result, RpcCode};
pub use local::protojson;
pub use service::{ServiceConfig, ServiceMode, ServiceRequest};

/// The release identity of an installed or prepared Host: an application bundle on macOS, a release directory on Linux.
pub use platform::release_identity;

use operator::Operator;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub const HOST_APP_NAME: &str = "Agent VMM Host.app";
pub const HOST_BUNDLE_IDENTIFIER: &str = "com.afk-surf.agent-vmm-host";
pub const CARRIER_BUNDLE_IDENTIFIER: &str = "com.afk-surf.agent-vmm-carrier";
/// The output contract that Comma Electron accepts. Comma must change first.
pub const CONTRACT_VERSION: u32 = 1;
/// The `PRAGMA user_version` range of the Host database that this release opens.
pub const CURRENT_SCHEMA: u32 = 2;
pub const MINIMUM_SCHEMA: u32 = 1;
/// The Host application identifier in the database header.
pub const DATABASE_APPLICATION_ID: u32 = 0x564d_4d43;
pub const START_FAILURE_FILE: &str = "host-start-failure.json";

/// The service manager stops the Host after 90 s, so a Host stop has the same bound.
pub(crate) const HOST_STOP_TIMEOUT: Duration = Duration::from_secs(90);
pub(crate) const INIT_TIMEOUT: Duration = Duration::from_secs(600);

/// The release identity that the package build injects.
pub fn release_id() -> &'static str {
    option_env!("AGENT_VMM_RELEASE_ID").unwrap_or("0.1.0")
}

#[derive(Clone, Debug, Default)]
pub struct CommandOutput {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl CommandOutput {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }

    /// Both streams, as a caller that reports the failure of a tool needs them.
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr).trim().to_string()
    }
}

/// One HTTP response of the registration service.
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

/// The effects outside the lifecycle state directory. Tests replace them.
pub trait System: Send + Sync {
    fn operator(&self, socket: &Path) -> Result<Box<dyn Operator>>;
    fn run(&self, program: &str, arguments: &[&str], deadline: Deadline) -> Result<CommandOutput>;
    fn process_running(&self, pid: u32) -> bool;
    /// Posts JSON with one `Authorization` value and follows no redirect.
    fn post(&self, url: &str, authorization: &str, body: &[u8], limit: u64) -> Result<HttpResponse>;
    fn euid(&self) -> u32;
    /// True after SIGTERM or SIGINT. A step sequence stops at its next boundary.
    fn interrupted(&self) -> bool;
}

#[derive(Clone)]
pub struct Context {
    pub system: Arc<dyn System>,
}

impl Context {
    pub fn operator(&self, paths: &Paths) -> Result<Box<dyn Operator>> {
        self.system.operator(&paths.operator_socket())
    }
}

/// What the service manager knows about the Host job.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostServiceFacts {
    pub loaded: bool,
    pub running: bool,
    pub pid: u32,
    pub last_exit_status: Option<i32>,
}

/// The fixed layout of one Host installation. macOS derives it from the
/// service user's home; Linux uses the system directories.
#[derive(Clone, Debug, Default)]
pub struct Paths {
    /// The request that asked for a recovery of an older request.
    pub correlation_request_id: Option<String>,
    pub home: PathBuf,
    /// The installed Host: the application bundle, or the current release link.
    pub app: PathBuf,
    /// The bundle or release directory that contains the running helper.
    pub source: PathBuf,
    pub application_data: PathBuf,
    pub logs: PathBuf,
    pub service_logs: PathBuf,
    pub service_config_file: PathBuf,
    pub service: ServiceConfig,
    pub agent_plist: PathBuf,
    pub daemon_plist: PathBuf,
    pub host_plist: PathBuf,
    pub install_root: PathBuf,
    pub unit: PathBuf,
}

impl Paths {
    pub fn operator_socket(&self) -> PathBuf {
        self.application_data.join("operator.sock")
    }

    /// The identity that state files receive when root writes for a daemon service.
    pub(crate) fn file_owner(&self, euid: u32) -> Option<statefs::FileOwner> {
        (self.service.mode == ServiceMode::Daemon && euid == 0)
            .then_some(statefs::FileOwner { uid: self.service.uid, gid: self.service.gid })
    }
}

pub(crate) fn valid_id(value: &str) -> bool {
    (1..=128).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as i64)
}

pub(crate) fn new_operation_id(prefix: &str) -> String {
    format!("{prefix}-{}", statefs::random_hex(16))
}
