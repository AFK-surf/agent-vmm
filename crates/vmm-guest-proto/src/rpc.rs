//! Calls that dom0 makes on `vmm-guest-init`, and their replies.
//!
//! Messages use a positional encoding. Change a layout only with
//! [`crate::PROTOCOL_VERSION`].

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::Secret;
use crate::auth::{NONCE_LEN, PROOF_LEN};

/// Largest `Data`, `Stdout`, or `Stderr` payload in one frame.
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
pub const MAX_WORKSPACE_PATH_BYTES: usize = 1024;
pub const MAX_LIST_ENTRIES: u32 = 4096;
pub const MAX_ARCHIVE_ENTRIES: u64 = 100_000;
pub const MAX_ARCHIVE_BYTES: u64 = 4 << 30;
pub const MAX_REQUEST_ID_BYTES: usize = 128;
/// Longest server-side wait of one long-poll call. Callers repeat the call.
pub const MAX_WAIT_MS: u32 = 300_000;

pub type Sha256Digest = [u8; 32];

/// First frame of each connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub version: u32,
    pub token: Secret,
    pub call: Call,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Call {
    Health(HealthRequest),
    /// The client then sends `Data` (stdin), `End`, `Signal`, and `Resize` frames.
    /// The server sends `Stdout` and `Stderr` frames, then [`Reply::Exec`].
    Exec(ExecRequest),
    ProcessStart(ProcessStartRequest),
    ProcessList,
    ProcessWrite(ProcessWriteRequest),
    ProcessTail(ProcessTailRequest),
    ProcessStop(ProcessStopRequest),
    WorkspaceStat(WorkspaceTarget),
    WorkspaceList(WorkspaceListRequest),
    /// The server sends `Data` frames, then [`Reply::WorkspaceRead`].
    WorkspaceRead(WorkspaceReadRequest),
    /// The client sends `Data` frames, then `End`.
    WorkspaceWrite(WorkspaceWriteRequest),
    /// The client sends a tar stream in `Data` frames, then `End`.
    WorkspaceImportArchive(WorkspaceImportRequest),
    /// The server sends a tar stream in `Data` frames, then [`Reply::WorkspaceExport`].
    WorkspaceExportArchive(WorkspaceTarget),
    /// The server sends `Log` frames, then [`Reply::Logs`].
    ReadLogs(ReadLogsRequest),
    SendSignal(Signal),
    WaitMainExit(WaitMainExitRequest),
    Quiesce(QuiesceRequest),
    PowerOff(PowerOffRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Empty,
    Health(Box<HealthReply>),
    Exec(ExecResult),
    ProcessStart(ProcessStartReply),
    ProcessList(Vec<ProcessInfo>),
    ProcessWrite(ProcessWriteReply),
    ProcessTail(ProcessTailReply),
    Process(ProcessInfo),
    WorkspaceEntry(WorkspaceEntry),
    WorkspaceList(WorkspaceListReply),
    WorkspaceRead(WorkspaceReadReply),
    WorkspaceWrite(WorkspaceWriteReply),
    WorkspaceImport(WorkspaceImportReply),
    WorkspaceExport(WorkspaceExportReply),
    Logs(LogsEnd),
    Main(MainState),
    Quiesce(QuiesceReply),
    PowerOff(PowerOffReply),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    Unauthenticated,
    UnsupportedVersion,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    /// The path leaves its workspace root or crosses a symbolic link.
    PathEscape,
    PermissionDenied,
    FailedPrecondition,
    /// Admission is closed by `Quiesce` or `PowerOff`.
    Unavailable,
    ResourceExhausted,
    DeadlineExceeded,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    pub code: ErrorCode,
    pub message: String,
}

impl RpcError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signal {
    Term,
    Kill,
    Int,
    Hup,
    Quit,
    Usr1,
    Usr2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitStatus {
    /// The exit code, or `128 + signal` for a process that a signal ended.
    pub code: i32,
    pub signal: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub argv: Vec<String>,
    /// `KEY=VALUE` entries that override the workload environment.
    pub env: Vec<String>,
    pub workdir: Option<String>,
    /// `name`, `uid`, `name:group`, or `uid:gid`. Absent selects the workload user.
    pub user: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthRequest {
    pub nonce: [u8; NONCE_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReply {
    pub instance_id: String,
    /// See [`crate::auth::boot_token_proof`].
    pub boot_token_proof: [u8; PROOF_LEN],
    pub main: MainState,
    pub quiesced: bool,
    pub uptime_ms: u64,
    /// Orphan processes that PID 1 reaped since boot.
    pub orphans_reaped: u64,
    pub usage: ResourceUsage,
}

/// Sampled usage of the workload. `None` marks a source that the Guest cannot read.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub sampled_unix_nanos: i64,
    pub cpu_nanos: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub pids: Option<u64>,
    pub disk_read_bytes: Option<u64>,
    pub disk_write_bytes: Option<u64>,
    /// Sum of `overlay_used_bytes` and `volume_used_bytes`. `None` when a part is missing.
    pub disk_used_bytes: Option<u64>,
    /// Bytes in use on the filesystem of the writable root layer.
    pub overlay_used_bytes: Option<u64>,
    /// Bytes in use on the filesystem of each volume, by volume name.
    /// A volume that the Guest cannot read is absent.
    pub volume_used_bytes: Vec<(String, u64)>,
    pub network_rx_bytes: Option<u64>,
    pub network_tx_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MainState {
    /// Shell-idle mode: the boot configuration names no main process.
    Idle,
    Running {
        pid: u32,
        started_unix_nanos: i64,
    },
    Exited {
        exit: ExitStatus,
        exited_unix_nanos: i64,
    },
    FailedToStart {
        message: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtySize {
    pub rows: u16,
    pub columns: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRequest {
    pub command: CommandSpec,
    /// When false the process reads `/dev/null`.
    pub stdin: bool,
    /// Runs the process on a new terminal, which is also its stdin.
    /// Its output arrives as `Stdout` only.
    pub tty: Option<TtySize>,
    /// Kills the process group after this time. Zero sets no limit.
    pub timeout_ms: u64,
    /// Forwards at most this many bytes of each output stream. Zero sets no limit.
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResult {
    pub exit: ExitStatus,
    pub timed_out: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub oom_killed: bool,
    pub memory_peak_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStartRequest {
    /// A repeated identifier with the same command returns the first process.
    pub request_id: String,
    pub command: CommandSpec,
    /// Size of the retained output window. Zero selects the default.
    pub output_limit_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessState {
    Running,
    Exited(ExitStatus),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub process_id: String,
    pub request_id: String,
    pub pid: u32,
    /// The leading arguments, cut at 2 kB.
    pub argv: Vec<String>,
    pub state: ProcessState,
    pub started_unix_nanos: i64,
    /// Offset after the last output byte that the process wrote.
    pub output_end_offset: u64,
    pub oom_killed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStartReply {
    pub process: ProcessInfo,
    /// True when `request_id` matched an earlier start.
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessWriteRequest {
    pub process_id: String,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    pub close_stdin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessWriteReply {
    pub bytes_written: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessTailRequest {
    pub process_id: String,
    pub since_offset: u64,
    /// Zero selects [`MAX_CHUNK_BYTES`].
    pub max_bytes: u32,
    /// Waits this long for output after `since_offset` or for process exit.
    pub wait_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessTailReply {
    /// Merged stdout and stderr.
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// Offset of `data`. It exceeds `since_offset` when older output left the window.
    pub offset: u64,
    pub next_offset: u64,
    pub state: ProcessState,
    pub oom_killed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStopRequest {
    pub process_id: String,
    pub signal: Signal,
    /// Sends `SIGKILL` to the process group after this time.
    pub grace_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceTarget {
    /// A volume name of the boot configuration. Absent selects `workspace_path`.
    pub volume: Option<String>,
    /// Relative to the root. No component can be a symbolic link.
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceEntry {
    pub path: String,
    pub kind: EntryKind,
    pub size: u64,
    pub mode: u32,
    pub modified_unix_nanos: i64,
    pub sha256: Option<Sha256Digest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListRequest {
    pub target: WorkspaceTarget,
    /// Zero selects [`MAX_LIST_ENTRIES`].
    pub limit: u32,
    /// Returns entries strictly after this direct child name.
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListReply {
    /// Files and directories in name order. Other entry types are omitted.
    pub entries: Vec<WorkspaceEntry>,
    /// The directory holds more entries than the limit or than one reply carries.
    pub truncated: bool,
    /// Direct child name of the last returned entry when `truncated` is true.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReadRequest {
    pub target: WorkspaceTarget,
    pub offset: u64,
    /// Zero reads to the end of the file.
    pub max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceReadReply {
    pub end_offset: u64,
    pub file_size: u64,
    /// Digest of the bytes that this call sent.
    pub sha256: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteRequest {
    /// Names the staging file. A retry with the same identifier resumes it.
    pub request_id: String,
    pub target: WorkspaceTarget,
    /// Must equal the size of the staging file. Zero starts a new upload.
    pub offset: u64,
    /// Digest of the staged bytes before `offset`. Required when `offset` is not zero.
    pub expected_prefix_sha256: Option<Sha256Digest>,
    /// Permission bits of the file. Zero selects `0600`.
    pub mode: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceWriteReply {
    pub entry: WorkspaceEntry,
    pub sha256: Sha256Digest,
    /// Bytes in use on the filesystem of the workspace root after the write.
    pub root_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceImportRequest {
    pub request_id: String,
    /// A directory path that does not exist. It appears only after a complete extraction.
    pub target: WorkspaceTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceImportReply {
    pub entry: WorkspaceEntry,
    /// Digest of the tar stream.
    pub sha256: Sha256Digest,
    pub entries: u64,
    pub bytes: u64,
    /// Bytes in use on the filesystem of the workspace root after the import.
    pub root_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceExportReply {
    pub sha256: Sha256Digest,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLogsRequest {
    /// Returns records after this cursor.
    pub after_cursor: Option<u64>,
    pub since_unix_nanos: Option<i64>,
    /// Without `after_cursor`, returns only this many of the newest records. Zero returns all.
    pub tail_records: u64,
    /// Keeps the call open until the main process output ends.
    pub follow: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRecord {
    pub cursor: u64,
    pub timestamp_unix_nanos: i64,
    pub stream: LogStream,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// Records between the previous position and this record that left the ring.
    pub dropped_records: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogsEnd {
    /// Position after the last record that the call saw. Pass it as `after_cursor` to continue.
    pub next_cursor: u64,
    /// True when the main process output ended and all of it was sent.
    pub ended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitMainExitRequest {
    /// Returns the current state after this time. Capped at [`MAX_WAIT_MS`].
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiesceRequest {
    /// Reopens admission.
    pub cancel: bool,
    /// Closes admission only when no `Exec` ran during this time.
    pub minimum_idle_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuiesceReply {
    /// True when admission of `Exec` and `ProcessStart` is closed.
    pub quiesced: bool,
    pub active_execs: u32,
    /// Reported only. A background process does not block `Quiesce`.
    pub running_processes: u32,
    pub idle_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerOffRequest {
    /// Time that processes get between `SIGTERM` and `SIGKILL`.
    pub grace_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PowerOffReply {
    pub trims: Vec<TrimReport>,
    /// The Guest unmounted each volume, removed each mapping, and made the root
    /// read-only. When false, a volume can need its journal at the next mount.
    pub clean_unmount: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrimReport {
    pub mount_path: String,
    pub trimmed_bytes: Option<u64>,
    pub error: Option<String>,
}
