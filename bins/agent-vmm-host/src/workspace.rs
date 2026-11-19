use async_trait::async_trait;
use std::{
    io::{Read, Write},
    sync::Arc,
};
use tonic::Status;
use vmm_compat::{
    error::{client_status, core_status, guest_status},
    workspace::{WorkspaceDriver, WorkspaceLease},
};
use vmm_core::{AccessMode, Error};
use vmm_dom0::{backend::LocalDom0Backend, helper_vm::Workspace, runtime::guest_rpc_error};
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
            .resolve_workspace(scope, volume, access)
            .await
            .map_err(dom0_status)?;
        Ok(Arc::new(Mounted {
            workspace,
            runtime: tokio::runtime::Handle::current(),
        }))
    }
}

struct Mounted {
    workspace: Workspace,
    runtime: tokio::runtime::Handle,
}
impl Mounted {
    fn client(
        &self,
        target: &mut g::WorkspaceTarget,
        access: AccessMode,
    ) -> Result<&Client, Status> {
        let (client, alias) = self
            .workspace
            .blocking_client(access)
            .map_err(dom0_status)?;
        target.volume = Some(alias.to_owned());
        Ok(client)
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
        self.runtime
            .block_on(self.workspace.used_bytes())
            .map_err(dom0_status)?
            .ok_or_else(|| Status::unavailable("guest workspace usage is unavailable"))
    }
}

#[async_trait]
impl vmm_compat::guest::GuestDriver for Dom0Workspaces {
    async fn client(&self, boot: &vmm_core::BootIdentity) -> Result<Client, Status> {
        Ok(self
            .0
            .runtime(boot)
            .map_err(dom0_status)?
            .blocking_client()
            .clone())
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
