#![forbid(unsafe_code)]
use crate::{client::CarrierClient, protocol::Request, sparse::EMERGENCY_BYTES};
use std::sync::Arc;
use vmm_core::{backend::*, *};
use vmm_dom0::remote::RemoteDom0Client;

#[derive(Clone)]
pub struct CarrierDom0Backend {
    carrier: CarrierClient,
    remote: RemoteDom0Client,
}
impl CarrierDom0Backend {
    pub(crate) fn new(carrier: CarrierClient) -> Self {
        Self {
            remote: RemoteDom0Client::transport(Arc::new(carrier.clone())),
            carrier,
        }
    }
    pub async fn open_guest_runtime(
        &self,
        boot: &BootIdentity,
    ) -> Result<vmm_dom0::remote::GuestEndpoint> {
        self.remote.open_guest_runtime(boot).await
    }
    pub async fn guest_address(&self, boot: &BootIdentity) -> Result<std::net::SocketAddrV4> {
        self.remote.guest_address(boot).await
    }
    pub async fn memory_summary(&self) -> Result<vmm_dom0::memory::Dom0MemorySummary> {
        self.remote.memory_summary().await
    }
    async fn admit_storage(&self, needed: u64) -> Result<()> {
        let available = self.carrier.control(Request::Status).await?.host_free_bytes;
        if available
            .checked_sub(needed)
            .is_none_or(|left| left < EMERGENCY_BYTES)
        {
            return Err(Error::Capacity(CapacityDimension::StorageHeadroom));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Dom0Backend for CarrierDom0Backend {
    async fn image_installed(&self, digest: Digest) -> Result<bool> {
        self.remote.image_installed(digest).await
    }
    async fn install_image_artifact(&self, source: &ImageSource) -> Result<ImageArtifact> {
        self.admit_storage(source.physical_bytes).await?;
        self.remote.install_image_artifact(source).await
    }
    async fn discard_image_artifact(&self, digest: Digest) -> Result<u64> {
        self.remote.discard_image_artifact(digest).await
    }
    async fn allocate_volume(&self, volume: &Volume) -> Result<()> {
        self.admit_storage(0).await?;
        self.remote.allocate_volume(volume).await
    }
    async fn discard_volume(&self, scope: &str, volume: &str) -> Result<()> {
        self.remote.discard_volume(scope, volume).await
    }
    async fn allocate_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()> {
        self.admit_storage(0).await?;
        self.remote.allocate_overlay(key, overlay).await
    }
    async fn discard_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()> {
        self.remote.discard_overlay(key, overlay).await
    }
    async fn start_vm(&self, launch: Launch<'_>) -> Result<VMIncarnation> {
        self.remote.start_vm(launch).await
    }
    async fn quiesce_vm(&self, boot: &BootIdentity) -> Result<()> {
        self.remote.quiesce_vm(boot).await
    }
    async fn shutdown_vm(&self, boot: &BootIdentity) -> Result<()> {
        self.remote.shutdown_vm(boot).await
    }
    async fn destroy_vm(&self, boot: &BootIdentity) -> Result<()> {
        self.remote.destroy_vm(boot).await
    }
    async fn list_live_vms(&self) -> Result<Vec<LiveVm>> {
        self.remote.list_live_vms().await
    }
    async fn set_network_policy(&self, boot: &BootIdentity, scope: &Scope) -> Result<()> {
        self.remote.set_network_policy(boot, scope).await
    }
    async fn bind_route(&self, route: &ServiceRoute) -> Result<()> {
        self.remote.bind_route(route).await
    }
    async fn revoke_route(&self, route: &str, expected_revision: i64) -> Result<()> {
        self.remote.revoke_route(route, expected_revision).await
    }
    async fn exec(&self, boot: &BootIdentity, argv: &[String]) -> Result<String> {
        self.remote.exec(boot, argv).await
    }
    async fn process(&self, boot: &BootIdentity, id: &str) -> Result<ProcessState> {
        self.remote.process(boot, id).await
    }
    async fn workspace(&self, boot: &BootIdentity, request: WorkspaceRequest) -> Result<Vec<u8>> {
        self.remote.workspace(boot, request).await
    }
    async fn logs(&self, boot: &BootIdentity, process: &str) -> Result<Vec<u8>> {
        self.remote.logs(boot, process).await
    }
}
