//! Resolve workspace storage through dom0. The lease pins the selected mount
//! for the duration of an RPC, including the final usage sample.
mod download;
mod upload;

use crate::{
    RuntimeContext,
    context::{core_status, resource_id},
};
use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
pub use download::WorkspaceStream;
use std::{
    collections::HashMap,
    io::{Read, Write},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Mutex as AsyncMutex;
use tonic::Status;
use vmm_core::{AccessMode, Error};
use vmm_guest_proto::rpc as g;

pub const DEFAULT_VOLUME: &str = "__default_workspace";
const MAX_BYTES: u64 = 256 << 20;

#[async_trait]
pub trait WorkspaceDriver: Send + Sync {
    async fn resolve_workspace(
        &self,
        scope_id: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Arc<dyn WorkspaceLease>, Status>;
}

/// Blocking I/O methods run on the blocking pool. An interrupted upload returns
/// a read error to the Guest; only an explicit finish supplies clean EOF.
pub trait WorkspaceLease: Send + Sync {
    fn stat(&self, target: g::WorkspaceTarget) -> Result<g::WorkspaceEntry, Status>;
    fn list(&self, request: g::WorkspaceListRequest) -> Result<g::WorkspaceListReply, Status>;
    fn read(
        &self,
        request: g::WorkspaceReadRequest,
        sink: &mut dyn Write,
    ) -> Result<g::WorkspaceReadReply, Status>;
    fn write(
        &self,
        request: g::WorkspaceWriteRequest,
        source: &mut dyn Read,
    ) -> Result<g::WorkspaceWriteReply, Status>;
    fn import_archive(
        &self,
        request: g::WorkspaceImportRequest,
        source: &mut dyn Read,
    ) -> Result<g::WorkspaceImportReply, Status>;
    fn export_archive(
        &self,
        target: g::WorkspaceTarget,
        sink: &mut dyn Write,
    ) -> Result<g::WorkspaceExportReply, Status>;
    fn used_bytes(&self) -> Result<u64, Status>;
}

#[derive(Clone)]
pub struct WorkspaceService {
    driver: Arc<dyn WorkspaceDriver>,
    writers: Arc<Mutex<HashMap<String, Weak<AsyncMutex<()>>>>>,
}

impl WorkspaceService {
    pub fn new(driver: Arc<dyn WorkspaceDriver>) -> Self {
        Self {
            driver,
            writers: Default::default(),
        }
    }

    fn writer(&self, scope: &str) -> Result<Arc<AsyncMutex<()>>, Status> {
        let mut locks = self
            .writers
            .lock()
            .map_err(|_| Status::internal("workspace admission unavailable"))?;
        locks.retain(|_, v| v.strong_count() > 0);
        if let Some(lock) = locks.get(scope).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(scope.into(), Arc::downgrade(&lock));
        Ok(lock)
    }

    async fn resolve(
        &self,
        ctx: &RuntimeContext,
        target: &g::WorkspaceTarget,
        access: AccessMode,
    ) -> Result<Arc<dyn WorkspaceLease>, Status> {
        if let Some(id) = &target.volume {
            resource_id(id)?;
            ctx.core
                .store
                .volume(&ctx.scope.id, id)
                .map_err(|e| match e {
                    Error::Missing(_) => {
                        Status::permission_denied("workspace volume is not owned by this namespace")
                    }
                    e => core_status(e),
                })?;
        } else {
            match ctx.core.store.volume(&ctx.scope.id, DEFAULT_VOLUME) {
                Ok(_) => {}
                Err(Error::Missing(_)) => ctx
                    .core
                    .create_volume(&vmm_core::Volume {
                        scope_id: ctx.scope.id.clone(),
                        volume_id: DEFAULT_VOLUME.into(),
                        quota_bytes: ctx.scope.limits.disk_quota_bytes,
                        used_bytes: 0,
                        owner_uid: 0,
                        owner_gid: 0,
                        mode: 0o700,
                        key_id: None,
                    })
                    .await
                    .map_err(core_status)?,
                Err(e) => return Err(core_status(e)),
            }
        }
        self.driver
            .resolve_workspace(&ctx.scope.id, target.volume.as_deref(), access)
            .await
    }

    pub async fn stat(
        &self,
        ctx: &RuntimeContext,
        request: h::StatWorkspaceRequest,
    ) -> Result<h::StatWorkspaceResponse, Status> {
        let target = target("", request.path)?;
        let lease = self.resolve(ctx, &target, AccessMode::ReadOnly).await?;
        let entry = blocking(move || lease.stat(target)).await?;
        Ok(h::StatWorkspaceResponse {
            entry: Some(project(entry)),
        })
    }

    pub async fn list(
        &self,
        ctx: &RuntimeContext,
        request: h::ListWorkspaceRequest,
    ) -> Result<h::ListWorkspaceResponse, Status> {
        let target = target(&request.volume_id, request.path)?;
        let lease = self.resolve(ctx, &target, AccessMode::ReadOnly).await?;
        let limit = match request.limit {
            0 => g::MAX_LIST_ENTRIES,
            n => n.min(g::MAX_LIST_ENTRIES),
        };
        let result =
            blocking(move || lease.list(g::WorkspaceListRequest { target, limit })).await?;
        Ok(h::ListWorkspaceResponse {
            entries: result
                .entries
                .into_iter()
                .take(limit as usize)
                .map(project)
                .collect(),
        })
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|_| Status::internal("workspace task interrupted"))?
}

fn target(volume: &str, path: String) -> Result<g::WorkspaceTarget, Status> {
    if path.len() > g::MAX_WORKSPACE_PATH_BYTES || path.contains('\0') || path.starts_with('/') {
        return Err(Status::invalid_argument("invalid workspace path"));
    }
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts
                    .pop()
                    .ok_or_else(|| Status::invalid_argument("path escapes workspace"))?;
            }
            part => parts.push(part),
        }
    }
    let path = if parts.is_empty() {
        ".".into()
    } else {
        parts.join("/")
    };
    Ok(g::WorkspaceTarget {
        volume: (!volume.is_empty()).then(|| volume.to_owned()),
        path,
    })
}

fn project(entry: g::WorkspaceEntry) -> h::WorkspaceEntry {
    h::WorkspaceEntry {
        path: entry.path,
        kind: match entry.kind {
            g::EntryKind::File => h::WorkspaceEntryKind::File,
            g::EntryKind::Directory => h::WorkspaceEntryKind::Directory,
        } as i32,
        size: entry.size,
        mode: entry.mode,
        modified_unix_nanos: entry.modified_unix_nanos,
        sha256: entry.sha256.map_or_else(Vec::new, |s| s.to_vec()),
    }
}
