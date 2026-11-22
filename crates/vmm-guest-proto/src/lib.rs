//! Contract between dom0 and `vmm-guest-init`, the PID 1 of each workload VM.
//!
//! dom0 serves one [`boot::BootConfig`] at `/v1/boot`, then calls the Guest over
//! the framed RPC in [`frame`] with the configuration's token.
//!
//! The Guest is one trust domain: its workload can run as root beside PID 1.
//! The token authenticates dom0 to the Guest. A reply never proves more than
//! the authority of that workload.

pub mod auth;
pub mod boot;
pub mod client;
pub mod frame;
pub mod rpc;
mod secret;

pub use secret::Secret;

/// Increment when a frame or message layout changes.
pub const PROTOCOL_VERSION: u32 = 2;
