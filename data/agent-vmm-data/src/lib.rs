use anyhow::{Context, Result, ensure};
use iroh::{Endpoint, EndpointAddr};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

pub mod node;
pub mod pairing;
mod pairing_runtime;
pub mod pairing_session;
pub mod pairing_wire;

pub const PAIRING_ALPN: &[u8] = b"agent-vmm/pairing/2";
pub const MESH_CONTROL_ALPN: &[u8] = b"agent-vmm/mesh-control/1";
pub const SERVICE_TUNNEL_ALPN: &[u8] = b"agent-vmm/service-tunnel/1";
pub const ALPNS: [&[u8]; 3] = [PAIRING_ALPN, MESH_CONTROL_ALPN, SERVICE_TUNNEL_ALPN];
pub const MAX_STREAMS: usize = 32;
pub const MAX_FRAME_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PairingSecretKind {
    Pin,
    Qr,
}

#[derive(Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    Status,
    Drain,
    Probe {
        endpoint_addr: EndpointAddr,
        payload: String,
    },
    OpenServiceTunnel {
        endpoint_addr: EndpointAddr,
    },
    OpenMeshControlTunnel {
        endpoint_addr: EndpointAddr,
    },
    PreparePairingManager {
        context: pairing::OwnedPairingContext,
        secret_kind: PairingSecretKind,
        secret: Vec<u8>,
        acceptance: Vec<u8>,
    },
    JoinPairing {
        endpoint_addr: EndpointAddr,
        context: pairing::OwnedPairingContext,
        secret_kind: PairingSecretKind,
        secret: Vec<u8>,
        proposal: Vec<u8>,
    },
    PairingStatus {
        session_id: Vec<u8>,
    },
    ConfirmPairing {
        session_id: Vec<u8>,
    },
    CommitPairing {
        session_id: Vec<u8>,
        registry_cas_succeeded: bool,
    },
    RejectPairing {
        session_id: Vec<u8>,
        #[serde(default)]
        context_mismatch: bool,
    },
}

impl core::fmt::Debug for Command {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let name = match self {
            Self::Status => "Status",
            Self::Drain => "Drain",
            Self::Probe { .. } => "Probe",
            Self::OpenServiceTunnel { .. } => "OpenServiceTunnel",
            Self::OpenMeshControlTunnel { .. } => "OpenMeshControlTunnel",
            Self::PreparePairingManager { .. } => "PreparePairingManager([REDACTED])",
            Self::JoinPairing { .. } => "JoinPairing([REDACTED])",
            Self::PairingStatus { .. } => "PairingStatus",
            Self::ConfirmPairing { .. } => "ConfirmPairing",
            Self::CommitPairing { .. } => "CommitPairing",
            Self::RejectPairing { .. } => "RejectPairing",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Serialize)]
pub struct Status {
    pub endpoint_id: String,
    pub endpoint_generation: u64,
    pub accepting: bool,
    pub active_streams: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub endpoint_addr: EndpointAddr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_payload: Option<String>,
}

pub struct Accounting {
    accepting: AtomicBool,
    active_streams: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
}

impl Default for Accounting {
    fn default() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            active_streams: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
        }
    }
}

impl Accounting {
    pub fn drain(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }
    pub fn accepting(&self) -> bool {
        self.accepting.load(Ordering::SeqCst)
    }
    pub fn stream_opened(&self) -> bool {
        if !self.accepting() {
            return false;
        }
        let previous = self.active_streams.fetch_add(1, Ordering::SeqCst);
        if previous >= MAX_STREAMS as u64 {
            self.active_streams.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        true
    }
    pub fn stream_closed(&self) {
        self.active_streams.fetch_sub(1, Ordering::SeqCst);
    }
    pub fn add_in(&self, bytes: u64) {
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn add_out(&self, bytes: u64) {
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn status(&self, endpoint_addr: EndpointAddr, endpoint_generation: u64) -> Status {
        Status {
            endpoint_id: endpoint_addr.id.to_string(),
            endpoint_generation,
            accepting: self.accepting(),
            active_streams: self.active_streams.load(Ordering::SeqCst),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            endpoint_addr,
            probe_payload: None,
        }
    }
}

pub async fn probe(
    endpoint: &Endpoint,
    endpoint_addr: EndpointAddr,
    payload: &[u8],
) -> Result<Vec<u8>> {
    ensure!(
        !payload.is_empty() && payload.len() <= MAX_FRAME_BYTES,
        "probe payload is outside frame bounds"
    );
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let connection = endpoint.connect(endpoint_addr, SERVICE_TUNNEL_ALPN).await?;
        let (mut send, mut receive) = connection.open_bi().await?;
        send.write_all(payload).await?;
        send.finish()?;
        let echoed = receive.read_to_end(MAX_FRAME_BYTES).await?;
        connection.close(0u32.into(), b"probe complete");
        Ok(echoed)
    })
    .await
    .context("service tunnel probe timed out")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_three_versioned_alpns_exist() {
        assert_eq!(
            ALPNS,
            [
                b"agent-vmm/pairing/2".as_slice(),
                b"agent-vmm/mesh-control/1".as_slice(),
                b"agent-vmm/service-tunnel/1".as_slice()
            ]
        );
    }

    #[test]
    fn drain_rejects_new_streams_and_preserves_accounting() {
        let accounting = Accounting::default();
        assert!(accounting.stream_opened());
        accounting.add_in(5);
        accounting.add_out(3);
        accounting.drain();
        assert!(!accounting.stream_opened());
        accounting.stream_closed();
        let endpoint = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let status = accounting.status(EndpointAddr::new(endpoint), 7);
        assert!(!status.accepting);
        assert_eq!(status.active_streams, 0);
        assert_eq!((status.bytes_in, status.bytes_out), (5, 3));
    }

    #[test]
    fn pairing_command_debug_redacts_pin_and_secret_material() {
        let command: Command = serde_json::from_value(serde_json::json!({
            "command": "prepare_pairing_manager",
            "context": {
                "session_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                "mesh_id": [109, 101, 115, 104],
                "invite_revision": 1,
                "manager_identity": [109],
                "joiner_identity": [106],
                "manager_root_key": [2, 3],
                "joiner_root_key": [3, 4],
                "roles_and_features": [117, 115, 101]
            },
            "secret_kind": "pin",
            "secret": [49, 50, 51, 52, 53, 54, 55, 56],
            "acceptance": [1]
        }))
        .unwrap();
        let debug = format!("{command:?}");
        assert_eq!(debug, "PreparePairingManager([REDACTED])");
        assert!(!debug.contains("12345678"));
    }
}
