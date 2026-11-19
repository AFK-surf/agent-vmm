use bytes::Bytes;

macro_rules! codes {
    ($($name:ident = $value:literal => $constructor:ident),+ $(,)?) => {
        #[repr(u8)]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum Code { $($name = $value),+ }

        impl TryFrom<u8> for Code {
            type Error = Status;
            fn try_from(value: u8) -> Result<Self, Status> {
                match value { $($value => Ok(Self::$name),)+ _ => Err(Status::data_loss("invalid status code")) }
            }
        }
        impl Status {
            $(pub fn $constructor(message: impl Into<String>) -> Self {
                Self::new(Code::$name, message)
            })+
        }
    }
}

codes! {
    Ok = 0 => ok, Cancelled = 1 => cancelled, Unknown = 2 => unknown,
    InvalidArgument = 3 => invalid_argument, DeadlineExceeded = 4 => deadline_exceeded,
    NotFound = 5 => not_found, AlreadyExists = 6 => already_exists, PermissionDenied = 7 => permission_denied,
    ResourceExhausted = 8 => resource_exhausted, FailedPrecondition = 9 => failed_precondition,
    Aborted = 10 => aborted, OutOfRange = 11 => out_of_range, Unimplemented = 12 => unimplemented,
    Internal = 13 => internal, Unavailable = 14 => unavailable, DataLoss = 15 => data_loss,
    Unauthenticated = 16 => unauthenticated,
}

#[derive(Debug, Clone)]
pub struct Status {
    pub code: Code,
    pub message: String,
    pub details: Bytes,
}

impl Status {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self::with_details(code, message, Bytes::new())
    }
    pub fn with_details(code: Code, message: impl Into<String>, details: Bytes) -> Self {
        Self {
            code,
            message: message.into(),
            details,
        }
    }
    pub fn code(&self) -> Code {
        self.code
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub fn details(&self) -> &[u8] {
        &self.details
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}
impl std::error::Error for Status {}
impl From<std::io::Error> for Status {
    fn from(error: std::io::Error) -> Self {
        use std::io::ErrorKind;
        match error.kind() {
            ErrorKind::TimedOut | ErrorKind::WouldBlock => {
                Self::deadline_exceeded(error.to_string())
            }
            ErrorKind::UnexpectedEof => Self::data_loss("truncated RPC stream"),
            _ => Self::unavailable(error.to_string()),
        }
    }
}

impl Code {
    pub fn from_i32(code: i32) -> Self {
        u8::try_from(code)
            .ok()
            .and_then(|c| c.try_into().ok())
            .unwrap_or(Self::Unknown)
    }
}
