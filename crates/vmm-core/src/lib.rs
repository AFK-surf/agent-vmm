mod cleanup;
pub use cleanup::{CleanupInventory, CleanupKind, CleanupOperation, CleanupResource};
mod authority;
pub mod backend;
pub mod fake;
mod host_socket;
pub use host_socket::HostSocketApproval;
mod image_copy;
pub use image_copy::{CopyAuthority, CopySource, ImageCopyReceipt, ImageGrantMetadata};
mod lifecycle;
mod list;
pub mod mesh;
mod model;
mod operator_disposal;
mod receipt;
mod registration_policy;
mod registration_retirement;
pub use registration_policy::{
    ManagedConnection, ManagedPhase, ManagedPool, ManagedRegistration, RegistrationChange,
};
mod retained_image;
pub use retained_image::RetainedImage;
mod remote;
mod route;
mod storage;
mod store;

pub use lifecycle::Reconciliation;
pub use model::*;
pub use receipt::*;
pub use remote::{RemoteAllocation, RemoteInventory, RemoteScopePolicy};
pub use store::Store;

use backend::Dom0Backend;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("persistent data: {0}")]
    Codec(#[from] serde_json::Error),
    #[error("store: {0}")]
    Io(#[from] std::io::Error),
    #[error("not found: {0}")]
    Missing(&'static str),
    #[error("conflict: {0}")]
    Conflict(&'static str),
    #[error("stale authority or resource identity")]
    Fenced,
    #[error("admission is closed")]
    Draining,
    #[error("invalid {0}")]
    Invalid(&'static str),
    #[error("capacity exhausted: {0:?}")]
    Capacity(CapacityDimension),
    #[error("backend operation interrupted")]
    Interrupted,
}
pub type Result<T> = std::result::Result<T, Error>;

/// One durable store and its independently surviving dom0 backend.
#[derive(Clone)]
pub struct Core {
    pub store: Arc<Store>,
    backend: Arc<dyn Dom0Backend>,
}

impl Core {
    pub fn new(store: Arc<Store>, backend: Arc<dyn Dom0Backend>) -> Self {
        Self { store, backend }
    }
}

mod host_vm;
pub use host_vm::HostVmObservation;
