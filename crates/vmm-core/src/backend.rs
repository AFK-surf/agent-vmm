use crate::*;
use async_trait::async_trait;

pub struct Launch<'a> {
    pub definition: &'a SandboxDefinition,
    pub scope: &'a Scope,
    pub image: &'a ImageArtifact,
    pub volumes: &'a [Volume],
    pub boot: &'a BootIdentity,
    /// Ephemeral secret presented by the guest; only its hash is stored.
    pub boot_token: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessState {
    Running,
    Exited(i32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceRequest {
    Read(String),
    Write { path: String, bytes: Vec<u8> },
}

/// Resource creation/discard and VM lifecycle calls must tolerate replay of the
/// same identity. Enumeration reports authenticated boots and quiesce state.
/// A completed stop fences a delayed start of that same boot.
#[async_trait]
pub trait Dom0Backend: Send + Sync {
    async fn image_installed(&self, digest: Digest) -> Result<bool>;
    async fn install_image_artifact(&self, source: &ImageSource) -> Result<ImageArtifact>;
    async fn discard_image_artifact(&self, digest: Digest) -> Result<u64>;
    async fn allocate_volume(&self, volume: &Volume) -> Result<()>;
    async fn discard_volume(&self, scope: &str, volume: &str) -> Result<()>;
    async fn allocate_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()>;
    async fn discard_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()>;
    async fn start_vm(&self, launch: Launch<'_>) -> Result<VMIncarnation>;
    async fn quiesce_vm(&self, boot: &BootIdentity) -> Result<()>;
    async fn shutdown_vm(&self, boot: &BootIdentity) -> Result<()>;
    async fn destroy_vm(&self, boot: &BootIdentity) -> Result<()>;
    async fn list_live_vms(&self) -> Result<Vec<LiveVm>>;
    async fn set_network_policy(&self, boot: &BootIdentity, scope: &Scope) -> Result<()>;
    async fn bind_route(&self, route: &ServiceRoute) -> Result<()>;
    async fn revoke_route(&self, route: &str, expected_revision: i64) -> Result<()>;
    async fn exec(&self, boot: &BootIdentity, argv: &[String]) -> Result<String>;
    async fn process(&self, boot: &BootIdentity, id: &str) -> Result<ProcessState>;
    async fn workspace(&self, boot: &BootIdentity, request: WorkspaceRequest) -> Result<Vec<u8>>;
    async fn logs(&self, boot: &BootIdentity, process: &str) -> Result<Vec<u8>>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveVm {
    pub incarnation: VMIncarnation,
    pub quiesced: bool,
}
