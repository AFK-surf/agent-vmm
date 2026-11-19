//! Process-local bridges between the public host protocol and dom0.
mod build;
mod workload;
mod workspace;
pub use workspace::Dom0Workspaces;
pub mod control;
pub mod host;
#[cfg(target_os = "macos")]
mod keychain;
pub mod registrations;
pub mod secrets;
mod socket;
pub mod storage;

#[cfg(target_os = "macos")]
#[allow(unsafe_code)] // Security.framework tagged SecKey query and constants.
mod apple_roots;
mod identity;

#[cfg(target_os = "macos")]
pub mod carrier;

mod guest;

mod entry;
mod observation;
pub use entry::main_from_args;

pub mod tunnel;

mod service_forward;
mod service_tunnel;
