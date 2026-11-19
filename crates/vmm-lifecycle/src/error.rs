use std::fmt;
use std::time::{Duration, Instant};

/// The gRPC codes that lifecycle decisions and the `local-*` error envelope name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcCode {
    Canceled,
    Unknown,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
}

impl RpcCode {
    /// The name that Go `codes.Code.String` prints. Electron matches `Unimplemented`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Canceled => "Canceled",
            Self::Unknown => "Unknown",
            Self::InvalidArgument => "InvalidArgument",
            Self::DeadlineExceeded => "DeadlineExceeded",
            Self::NotFound => "NotFound",
            Self::AlreadyExists => "AlreadyExists",
            Self::PermissionDenied => "PermissionDenied",
            Self::ResourceExhausted => "ResourceExhausted",
            Self::FailedPrecondition => "FailedPrecondition",
            Self::Aborted => "Aborted",
            Self::OutOfRange => "OutOfRange",
            Self::Unimplemented => "Unimplemented",
            Self::Internal => "Internal",
            Self::Unavailable => "Unavailable",
            Self::DataLoss => "DataLoss",
            Self::Unauthenticated => "Unauthenticated",
        }
    }
}

/// The failure descriptor that the managed CLI gives `update --preparation-failure-stdin`.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationFailure {
    pub version: u32,
    pub stage: String,
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Other,
    /// A state file or directory does not exist.
    NotFound,
    /// Another process holds `owner.lock`.
    OwnerBusy,
    /// An exclusive transition of another request is unfinished.
    ExclusiveBusy(String),
    HostUninstalled,
    /// The local owner fenced this request.
    RequestBlocked(String),
    Timeout,
    Http {
        status: u16,
        code: String,
    },
    /// `reason` is the `host.v1.ErrorReason` name without `ERROR_REASON_`.
    Rpc {
        code: RpcCode,
        reason: Option<String>,
    },
    /// A terminal maintenance failure with its receipt code.
    Maintenance(String),
    Preparation(PreparationFailure),
    ServiceNotFound,
    PlanUnavailable,
    /// The restore window of a maintenance request is used up.
    RestoreWindowExhausted,
    Interrupted,
}

#[derive(Clone, Debug)]
pub struct Error {
    pub kind: Kind,
    pub message: String,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(kind: Kind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    pub fn context(mut self, prefix: impl fmt::Display) -> Self {
        self.message = format!("{prefix}: {}", self.message);
        self
    }

    pub fn is(&self, kind: &Kind) -> bool {
        &self.kind == kind
    }

    pub fn not_found(&self) -> bool {
        self.kind == Kind::NotFound
    }

    pub fn rpc_code(&self) -> Option<RpcCode> {
        match self.kind {
            Kind::Rpc { code, .. } => Some(code),
            _ => None,
        }
    }

    pub fn rpc_reason(&self) -> Option<&str> {
        match &self.kind {
            Kind::Rpc { reason, .. } => reason.as_deref(),
            _ => None,
        }
    }
}

pub fn err(message: impl Into<String>) -> Error {
    Error::new(Kind::Other, message)
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        let kind = if error.kind() == std::io::ErrorKind::NotFound { Kind::NotFound } else { Kind::Other };
        Self::new(kind, error.to_string())
    }
}

impl From<rustix::io::Errno> for Error {
    fn from(error: rustix::io::Errno) -> Self {
        std::io::Error::from(error).into()
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        err(error.to_string())
    }
}

/// The bound of one blocking step, as `context.WithTimeout` gives it in the Go Host.
#[derive(Clone, Copy, Debug)]
pub struct Deadline(Instant);

impl Deadline {
    pub fn after(duration: Duration) -> Self {
        Self(Instant::now() + duration)
    }

    /// The earlier of this deadline and `duration` from now.
    pub fn within(self, duration: Duration) -> Self {
        Self(self.0.min(Instant::now() + duration))
    }

    pub fn remaining(self) -> Duration {
        self.0.saturating_duration_since(Instant::now())
    }

    pub fn expired(self) -> bool {
        self.remaining().is_zero()
    }

    pub fn check(self) -> Result<()> {
        if self.expired() {
            return Err(Error::new(Kind::Timeout, "context deadline exceeded"));
        }
        Ok(())
    }

    /// Sleeps for `interval`, or returns the timeout when the deadline passes first.
    pub fn sleep(self, interval: Duration) -> Result<()> {
        let remaining = self.remaining();
        std::thread::sleep(interval.min(remaining));
        if remaining <= interval {
            return Err(Error::new(Kind::Timeout, "context deadline exceeded"));
        }
        Ok(())
    }
}
