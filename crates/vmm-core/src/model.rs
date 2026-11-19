use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::Ipv4Addr};

pub type Digest = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityDimension {
    ComputeSlots,
    Pids,
    StorageHeadroom,
}

impl CapacityDimension {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ComputeSlots => "CAPACITY_DIMENSION_COMPUTE_SLOTS",
            Self::Pids => "CAPACITY_DIMENSION_PIDS",
            Self::StorageHeadroom => "CAPACITY_DIMENSION_STORAGE_HEADROOM",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Origin {
    Local,
    Remote { registration_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeState {
    Active,
    Draining,
    Retained,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EgressMode {
    DenyAll,
    PublicInternet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSocketRule {
    pub port: u16,
    pub socket_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub pids_max: u64,
    pub disk_quota_bytes: u64,
    pub vcpus: u32,
    pub boot_ram_mb: u64,
    pub max_ram_mb: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scope {
    pub id: String,
    pub origin: Origin,
    pub generation: u64,
    pub revision: u64,
    pub limits: Limits,
    pub egress_mode: EgressMode,
    pub host_socket_rules: Vec<HostSocketRule>,
    pub state: ScopeState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    pub id: String,
    pub connection_epoch: u64,
    pub connected: bool,
    pub draining: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Channel {
    pub registration_id: String,
    pub connection_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub id: String,
    pub scope_id: String,
    pub capability_hash: Digest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApplianceState {
    Installed,
    Active,
    Stopped,
    Failed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appliance {
    pub id: String,
    pub registration_id: String,
    pub state: ApplianceState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageArtifact {
    pub manifest_digest: Digest,
    pub extent_id: String,
    pub physical_bytes: u64,
}
/// Identifies a verified, immutable EROFS input accessible to dom0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageSource {
    pub manifest_digest: Digest,
    pub staged_extent: String,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Volume {
    pub scope_id: String,
    pub volume_id: String,
    pub quota_bytes: u64,
    #[serde(default)]
    pub used_bytes: u64,
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub mode: u32,
    pub key_id: Option<String>,
}
impl Volume {
    pub(crate) fn same_allocation(&self, other: &Self) -> bool {
        self.scope_id == other.scope_id
            && self.volume_id == other.volume_id
            && self.quota_bytes == other.quota_bytes
            && self.owner_uid == other.owner_uid
            && self.owner_gid == other.owner_gid
            && self.mode == other.mode
            && self.key_id == other.key_id
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mount {
    pub volume_id: String,
    pub guest_path: String,
    pub access: AccessMode,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SandboxKey {
    pub scope_id: String,
    pub sandbox_id: String,
    pub generation_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootOverlay {
    pub extent_id: String,
    pub quota_bytes: u64,
    #[serde(default)]
    pub used_bytes: u64,
}
impl RootOverlay {
    pub(crate) fn allocation(&self) -> (&str, u64) {
        (&self.extent_id, self.quota_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostShareMount {
    pub share_id: String,
    pub guest_path: String,
    pub read_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxDefinition {
    pub key: SandboxKey,
    pub image_reference: String,
    pub manifest_digest: Digest,
    pub read_only_root: bool,
    pub root_overlay: Option<RootOverlay>,
    #[serde(default)]
    pub pids_max: Option<u64>,
    pub mounts: Vec<Mount>,
    pub host_shares: Vec<HostShareMount>,
    /// None inherits the image setting; Some(empty) explicitly clears it.
    pub entrypoint: Option<Vec<String>>,
    pub command: Option<Vec<String>>,
    pub workdir: Option<String>,
    pub user: Option<String>,
    pub log_limit_bytes: u64,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BootIdentity {
    pub sandbox: SandboxKey,
    pub instance_id: String,
    pub boot_token_hash: Digest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VMIncarnation {
    pub boot: BootIdentity,
    pub l4h_vm_id: String,
    pub slot_ip: Ipv4Addr,
    pub started_at: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SandboxState {
    Defined,
    Starting,
    Running,
    Quiesced,
    Unverified,
    Stopping,
    Stopped,
    Deleting,
    Deleted,
}
impl SandboxState {
    pub(crate) fn parse(value: &str) -> crate::Result<Self> {
        Ok(match value {
            "defined" => Self::Defined,
            "starting" => Self::Starting,
            "running" => Self::Running,
            "quiesced" => Self::Quiesced,
            "unverified" => Self::Unverified,
            "stopping" => Self::Stopping,
            "stopped" => Self::Stopped,
            "deleting" => Self::Deleting,
            "deleted" => Self::Deleted,
            _ => return Err(crate::Error::Invalid("stored sandbox state")),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sandbox {
    pub definition: SandboxDefinition,
    pub state: SandboxState,
    pub boot: Option<BootIdentity>,
    pub vm: Option<VMIncarnation>,
    pub exit_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceExport {
    pub id: String,
    pub boot: BootIdentity,
    pub port: u16,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteBinding {
    pub id: String,
    pub source: BootIdentity,
    pub export: ServiceExport,
    pub vip: Ipv4Addr,
    #[serde(default)]
    pub expires_at_unix_seconds: Option<u64>,
    #[serde(default)]
    pub virtual_service_name: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceRoute {
    pub binding: RouteBinding,
    pub revision: i64,
}

/// Metadata supplied by the authenticated control tunnel when it opens a session.
#[derive(Clone)]
pub struct RemoteIdentity {
    pub session_id: String,
    pub registration_id: String,
    pub allocation_id: String,
    pub generation: u64,
    pub execution_owner_id: String,
}
