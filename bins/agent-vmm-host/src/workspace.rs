use async_trait::async_trait;
use std::{
    io::{Read, Write},
    sync::Arc,
};
use vmm_rpc::Status;
use vmm_compat::{
    error::{client_status, core_status, guest_status},
    workspace::{WorkspaceDriver, WorkspaceLease},
};
use vmm_core::{AccessMode, Error};
use vmm_dom0::{backend::LocalDom0Backend, runtime::guest_rpc_error};
use vmm_guest_proto::{client::Client, rpc as g};

pub struct Dom0Workspaces(pub Arc<LocalDom0Backend>);

fn dom0_status(err: Error) -> Status {
    if let Some(rpc) = guest_rpc_error(&err) {
        return guest_status(rpc);
    }
    match err {
        Error::Conflict("workload mounted volume read-only" | "read-only workspace") => {
            Status::permission_denied("workspace volume is read-only")
        }
        other => core_status(other),
    }
}

#[async_trait]
impl WorkspaceDriver for Dom0Workspaces {
    async fn resolve_workspace(
        &self,
        scope: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Arc<dyn WorkspaceLease>, Status> {
        let workspace = self
            .0
            .resolve_workspace(scope, volume, access.clone())
            .await
            .map_err(dom0_status)?;
        let (client, alias) = workspace
            .blocking_client(access.clone())
            .map_err(dom0_status)?;
        Ok(Arc::new(Mounted {
            client: client.clone(),
            alias: alias.to_owned(),
            access,
            _lease: Box::new(workspace),
        }))
    }
}

#[cfg(target_os = "macos")]
#[async_trait]
impl WorkspaceDriver for crate::carrier::CarrierClientAdapter {
    async fn resolve_workspace(
        &self,
        scope: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Arc<dyn WorkspaceLease>, Status> {
        use vmm_compat::guest::GuestDriver;
        let lease =
            vmm_dom0::remote::RemoteWorkspace::acquire(&self.0, scope, volume, access.clone())
                .await
                .map_err(dom0_status)?;
        let client = self.client(&lease.boot).await?;
        Ok(Arc::new(Mounted {
            client,
            alias: lease.alias.clone(),
            access,
            _lease: Box::new(lease),
        }))
    }
}

struct Mounted {
    client: Client,
    alias: String,
    access: AccessMode,
    // Dropping the local guard or carrier control socket releases the same dom0 lease.
    _lease: Box<dyn Send + Sync>,
}
impl Mounted {
    fn client(
        &self,
        target: &mut g::WorkspaceTarget,
        access: AccessMode,
    ) -> Result<&Client, Status> {
        if access == AccessMode::ReadWrite && self.access != access {
            return Err(Status::permission_denied("workspace volume is read-only"));
        }
        target.volume = Some(self.alias.clone());
        Ok(&self.client)
    }
}
impl WorkspaceLease for Mounted {
    fn stat(&self, mut target: g::WorkspaceTarget) -> Result<g::WorkspaceEntry, Status> {
        self.client(&mut target, AccessMode::ReadOnly)?
            .workspace_stat(target)
            .map_err(client_status)
    }
    fn list(&self, mut req: g::WorkspaceListRequest) -> Result<g::WorkspaceListReply, Status> {
        self.client(&mut req.target, AccessMode::ReadOnly)?
            .workspace_list(req)
            .map_err(client_status)
    }
    fn read(
        &self,
        mut req: g::WorkspaceReadRequest,
        sink: &mut dyn Write,
    ) -> Result<g::WorkspaceReadReply, Status> {
        self.client(&mut req.target, AccessMode::ReadOnly)?
            .workspace_read(req, sink)
            .map_err(client_status)
    }
    fn write(
        &self,
        mut req: g::WorkspaceWriteRequest,
        source: &mut dyn Read,
    ) -> Result<g::WorkspaceWriteReply, Status> {
        self.client(&mut req.target, AccessMode::ReadWrite)?
            .workspace_write(req, source)
            .map_err(client_status)
    }
    fn import_archive(
        &self,
        mut req: g::WorkspaceImportRequest,
        source: &mut dyn Read,
    ) -> Result<g::WorkspaceImportReply, Status> {
        self.client(&mut req.target, AccessMode::ReadWrite)?
            .workspace_import_archive(req, source)
            .map_err(client_status)
    }
    fn export_archive(
        &self,
        mut target: g::WorkspaceTarget,
        sink: &mut dyn Write,
    ) -> Result<g::WorkspaceExportReply, Status> {
        self.client(&mut target, AccessMode::ReadOnly)?
            .workspace_export_archive(target, sink)
            .map_err(client_status)
    }
    fn used_bytes(&self) -> Result<u64, Status> {
        let mut nonce = [0; vmm_guest_proto::auth::NONCE_LEN];
        getrandom::fill(&mut nonce)
            .map_err(|_| Status::unavailable("guest health nonce unavailable"))?;
        self.client
            .health(nonce)
            .map_err(client_status)?
            .usage
            .volume_used_bytes
            .into_iter()
            .find_map(|(name, bytes)| (name == self.alias).then_some(bytes))
            .ok_or_else(|| Status::unavailable("guest workspace usage is unavailable"))
    }
}

#[async_trait]
impl vmm_compat::guest::GuestDriver for Dom0Workspaces {
    async fn client(&self, boot: &vmm_core::BootIdentity) -> Result<Client, Status> {
        Ok(self.0.bound_guest_client(boot.clone()))
    }
    async fn connect_port(
        &self,
        boot: &vmm_core::BootIdentity,
        port: u16,
    ) -> Result<Box<dyn vmm_compat::guest::Duplex>, Status> {
        Ok(Box::new(
            self.0.forward_tcp(boot, port).await.map_err(dom0_status)?,
        ))
    }
}

impl vmm_compat::image::ImageStaging for Dom0Workspaces {
    fn stage_image(
        &self,
        archive: &std::path::Path,
        manifest: vmm_core::Digest,
    ) -> vmm_core::Result<vmm_core::ImageSource> {
        self.0.stage_image(archive, manifest)
    }

    fn discard_staged_image(&self, token: &str) -> vmm_core::Result<()> {
        self.0.discard_staged_image(token)
    }
}
