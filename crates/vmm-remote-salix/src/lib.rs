mod allocation;
mod command;
mod control;
mod gateway;
mod registration;
mod tunnel;

pub use agent_vmm_proto::{host::v1 as host, remote::v1 as remote, trust::v1 as trust};
pub use control::{Connector, ConnectorConfig, Observation};
pub use gateway::{Gateway, RegistryClient};
pub use registration::*;
pub use tunnel::{RuntimeSession, TunnelIo};

use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid {0}")]
    Invalid(&'static str),
    #[error("Salix registration request failed")]
    HttpTransport,
    #[error(transparent)]
    Registration(#[from] RegistrationError),
    #[error("random source unavailable")]
    Random,
    #[error("gateway transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gateway RPC: {0}")]
    Grpc(#[source] Box<tonic::Status>),
    #[error("gateway rejected the registration credential: {0}")]
    Authentication(#[source] Box<tonic::Status>),
    #[error("gateway rejected Host protocol {}", .0.host_protocol)]
    ProtocolUnsupported(host::RemoteProtocolStatus),
    #[error("gateway control protocol: {0}")]
    Protocol(&'static str),
    #[error("gateway connection ended")]
    Disconnected,
    #[error("command exceeded its deadline")]
    Deadline,
    #[error("runtime session transport unavailable")]
    Runtime,
    #[error("core request failed: {0}")]
    Core(#[from] vmm_core::Error),
    #[error("signing or credential storage: {0}")]
    Signing(#[from] vmm_signing::Error),
    #[error("invalid operation receipt: {0}")]
    Receipt(#[from] prost::DecodeError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn unix_millis() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}
