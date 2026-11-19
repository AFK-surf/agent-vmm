//! `vmm-guest-init`: PID 1 of an l4h workload VM.
//!
//! [`pid1`] assembles the machine from its boot configuration. The other
//! modules serve the RPC of `vmm-guest-proto` and run without privileges,
//! so tests exercise them on a temporary directory.
#![cfg(target_os = "linux")]

pub mod health;
pub mod log_ring;
pub mod pid1;
pub mod process_manager;
pub mod reaper;
pub mod rpc_server;
pub mod runtime;
pub mod spawn;
pub mod sys;
pub mod users;
pub mod workspace_server;

use std::io;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use vmm_guest_proto::rpc::{ErrorCode, MAX_REQUEST_ID_BYTES, RpcError};

/// Locks a mutex whose data stays consistent when a holder panics.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn unix_nanos() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    elapsed.as_nanos() as i64
}

fn io_error(error: io::Error) -> RpcError {
    let code = match error.raw_os_error() {
        Some(libc::ELOOP | libc::EXDEV) => {
            return RpcError::new(
                ErrorCode::PathEscape,
                "the path leaves its root or crosses a symbolic link",
            );
        }
        Some(libc::ENOENT) => ErrorCode::NotFound,
        Some(libc::EEXIST) => ErrorCode::AlreadyExists,
        Some(libc::EACCES | libc::EPERM) => ErrorCode::PermissionDenied,
        Some(libc::ENOSPC | libc::EDQUOT) => ErrorCode::ResourceExhausted,
        Some(libc::ENOTDIR | libc::EISDIR | libc::EINVAL | libc::ENAMETOOLONG) => {
            ErrorCode::InvalidArgument
        }
        _ => ErrorCode::Internal,
    };
    RpcError::new(code, error.to_string())
}

fn check_request_id(request_id: &str) -> Result<(), RpcError> {
    let valid = (1..=MAX_REQUEST_ID_BYTES).contains(&request_id.len())
        && request_id.bytes().all(|byte| byte.is_ascii_graphic());
    if valid {
        Ok(())
    } else {
        Err(RpcError::new(ErrorCode::InvalidArgument, "request_id"))
    }
}
