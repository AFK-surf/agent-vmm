use crate::{
    backend::LocalDom0Backend,
    catalog::{image_id, volume_id},
    keys::random,
    runtime::Dom0Runtime,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use vmm_core::{backend::Launch, *};
use vmm_guest_proto::rpc::*;

pub(crate) struct HelperSpec<'a> {
    pub scope: &'a Scope,
    pub digest: Digest,
    pub access: AccessMode,
    pub build: bool,
}

#[derive(Default)]
pub(crate) struct MountLocks(Mutex<BTreeMap<String, Weak<AsyncMutex<()>>>>);
impl MountLocks {
    pub async fn acquire(&self, id: String) -> Result<OwnedMutexGuard<()>> {
        let lock = {
            let mut locks = self.0.lock().map_err(|_| Error::Interrupted)?;
            locks.retain(|_, l| l.strong_count() != 0);
            if let Some(l) = locks.get(&id).and_then(Weak::upgrade) {
                l
            } else {
                let l = Arc::new(AsyncMutex::new(()));
                locks.insert(id, Arc::downgrade(&l));
                l
            }
        };
        Ok(lock.lock_owned().await)
    }
}

pub struct Workspace {
    guest: Dom0Runtime,
    name: String,
    access: AccessMode,
    lease: Option<Lease>,
}
struct Lease {
    dom0: Arc<LocalDom0Backend>,
    helper: Option<BootIdentity>,
    _mount: OwnedMutexGuard<()>,
}
impl Lease {
    async fn release(self) -> Result<()> {
        if let Some(boot) = &self.helper {
            let _effects = self.dom0.effects.lock().await;
            self.dom0.stop(boot, false).await?;
        }
        Ok(())
    }
}
impl Drop for Workspace {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take()
            && lease.helper.is_some()
        {
            tokio::spawn(async move {
                let _ = lease.release().await;
            });
        }
    }
}
impl Workspace {
    /// Borrows the selected Guest client and mount alias for a synchronous
    /// streaming adapter. The caller holds this lease through the last frame.
    pub fn blocking_client(
        &self,
        access: AccessMode,
    ) -> Result<(&vmm_guest_proto::client::Client, &str)> {
        if access == AccessMode::ReadWrite {
            self.writable()?;
        }
        Ok((self.guest.blocking_client(), &self.name))
    }

    pub async fn close(mut self) -> Result<()> {
        self.lease.take().expect("live lease").release().await
    }
    fn target(&self, path: &str) -> WorkspaceTarget {
        WorkspaceTarget {
            volume: Some(self.name.clone()),
            path: path.into(),
        }
    }
    fn writable(&self) -> Result<()> {
        if self.access != AccessMode::ReadWrite {
            Err(Error::Conflict("read-only workspace"))
        } else {
            Ok(())
        }
    }
    pub async fn used_bytes(&self) -> Result<Option<u64>> {
        Ok(self
            .guest
            .health()
            .await?
            .usage
            .volume_used_bytes
            .into_iter()
            .find(|(name, _)| *name == self.name)
            .map(|(_, bytes)| bytes))
    }

    pub async fn stat(&self, path: &str) -> Result<WorkspaceEntry> {
        self.guest.workspace_stat(self.target(path)).await
    }
    pub async fn list(&self, path: &str, limit: u32) -> Result<WorkspaceListReply> {
        self.guest
            .workspace_list(WorkspaceListRequest {
                target: self.target(path),
                limit,
            })
            .await
    }
    pub async fn read<W: Write + Send + 'static>(
        &self,
        path: &str,
        offset: u64,
        max_bytes: u64,
        out: W,
    ) -> Result<WorkspaceReadReply> {
        self.guest
            .workspace_read(
                WorkspaceReadRequest {
                    target: self.target(path),
                    offset,
                    max_bytes,
                },
                out,
            )
            .await
    }
    pub async fn write<R: Read + Send + 'static>(
        &self,
        mut req: WorkspaceWriteRequest,
        src: R,
    ) -> Result<WorkspaceWriteReply> {
        self.writable()?;
        req.target.volume = Some(self.name.clone());
        self.guest.workspace_write(req, src).await
    }
    pub async fn extract<R: Read + Send + 'static>(
        &self,
        path: &str,
        request_id: String,
        src: R,
    ) -> Result<WorkspaceImportReply> {
        self.writable()?;
        self.guest
            .workspace_import_archive(
                WorkspaceImportRequest {
                    target: self.target(path),
                    request_id,
                },
                src,
            )
            .await
    }
    pub async fn archive<W: Write + Send + 'static>(
        &self,
        path: &str,
        out: W,
    ) -> Result<WorkspaceExportReply> {
        self.guest
            .workspace_export_archive(self.target(path), out)
            .await
    }
}
impl LocalDom0Backend {
    pub async fn resolve_workspace(
        self: &Arc<Self>,
        scope_id: &str,
        volume: Option<&str>,
        access: AccessMode,
    ) -> Result<Workspace> {
        if scope_id.is_empty() || volume == Some("") {
            return Err(Error::Invalid("workspace scope or volume"));
        }
        // The host creates the reserved core Volume before resolving a default workspace.
        let name = volume.unwrap_or("__default_workspace");
        let id = volume_id(scope_id, name)?;
        let mount = self.mounts.acquire(id.clone()).await?;
        let _effects = self.effects.lock().await;
        let vms = self.inventory()?;
        for b in self
            .catalog()
            .boots()?
            .into_iter()
            .filter(|b| !b.retired && b.disks.contains(&id))
        {
            if b.helper {
                // A helper with no lease owner survived a daemon restart or an interrupted close.
                self.stop(&b.identity, true).await?;
                continue;
            }
            if self.present(&b, &vms)?.is_none() {
                return Err(Error::Interrupted);
            }
            let m = b
                .definition
                .mounts
                .iter()
                .find(|m| m.volume_id == name)
                .ok_or(Error::Fenced)?;
            if access == AccessMode::ReadWrite && m.access != access {
                return Err(Error::Conflict("workload mounted volume read-only"));
            }
            return Ok(Workspace {
                guest: self.runtime(&b.identity)?,
                name: m.volume_id.clone(),
                access,
                lease: Some(Lease {
                    dom0: self.clone(),
                    helper: None,
                    _mount: mount,
                }),
            });
        }
        self.pool.lookup(&id)?;
        let mut vol: Volume = self
            .catalog()
            .get(&format!("volume/{id}"))?
            .ok_or(Error::Missing("volume metadata"))?;
        if volume.is_none() {
            vol.volume_id = "workspace".into();
        }
        if access == AccessMode::ReadOnly {
            self.initialize_for_read(&vol, &id).await?;
        }
        let (boot, guest) = self.helper(scope_id, &vol, id, access.clone()).await?;
        Ok(Workspace {
            guest,
            name: vol.volume_id,
            access,
            lease: Some(Lease {
                dom0: self.clone(),
                helper: Some(boot),
                _mount: mount,
            }),
        })
    }

    async fn helper(
        &self,
        scope_id: &str,
        v: &Volume,
        object: String,
        access: AccessMode,
    ) -> Result<(BootIdentity, Dom0Runtime)> {
        let digest = self
            .cfg
            .helper_image
            .ok_or(Error::Missing("trusted helper image"))?;
        let scope = Scope {
            id: scope_id.into(),
            origin: Origin::Local,
            generation: 0,
            revision: 0,
            limits: Limits {
                pids_max: 64,
                disk_quota_bytes: v.quota_bytes,
                vcpus: 1,
                boot_ram_mb: 128,
                max_ram_mb: 128,
            },
            egress_mode: EgressMode::DenyAll,
            host_socket_rules: Vec::new(),
            state: ScopeState::Active,
        };
        self.launch_helper(
            HelperSpec {
                scope: &scope,
                digest,
                access,
                build: false,
            },
            v,
            &object,
        )
        .await
    }

    pub(crate) async fn launch_helper(
        &self,
        spec: HelperSpec<'_>,
        v: &Volume,
        object: &str,
    ) -> Result<(BootIdentity, Dom0Runtime)> {
        let HelperSpec {
            scope,
            digest,
            access,
            build,
        } = spec;
        let scope_id = &scope.id;
        let scratch = build.then(|| RootOverlay {
            extent_id: format!("{object}/root"),
            quota_bytes: v.quota_bytes,
            used_bytes: 0,
        });
        let user = if build { "1000:1000" } else { "0" };
        let image = ImageArtifact {
            manifest_digest: digest,
            extent_id: image_id(digest),
            physical_bytes: self
                .pool
                .lookup(&image_id(digest))?
                .iter()
                .map(|e| e.sectors * 512)
                .sum(),
        };
        let instance = hex::encode(random::<16>()?.as_ref());
        let key = SandboxKey {
            scope_id: scope_id.clone(),
            sandbox_id: format!("helper-{instance}"),
            generation_id: instance.clone(),
        };
        let token = random::<32>()?;
        let boot = BootIdentity {
            sandbox: key.clone(),
            instance_id: instance,
            boot_token_hash: Sha256::digest(token.as_ref()).into(),
        };
        let def = SandboxDefinition {
            pids_max: None,
            key,
            image_reference: "workspace-helper".into(),
            manifest_digest: digest,
            read_only_root: scratch.is_none(),
            root_overlay: scratch,
            mounts: vec![Mount {
                volume_id: v.volume_id.clone(),
                guest_path: "/workspace".into(),
                access,
            }],
            host_shares: Vec::new(),
            entrypoint: Some(vec![]),
            command: Some(vec![]),
            workdir: Some("/".into()),
            user: Some(user.into()),
            log_limit_bytes: 4096,
            env: Default::default(),
        };
        let launched = self
            .launch(
                Launch {
                    definition: &def,
                    scope,
                    image: &image,
                    volumes: std::slice::from_ref(v),
                    boot: &boot,
                    boot_token: token.as_ref(),
                },
                Some(object),
            )
            .await;
        if let Err(e) = launched {
            self.stop(&boot, true).await?;
            return Err(e);
        }
        let client = self.runtime(&boot)?;
        Ok((boot, client))
    }
    pub(crate) async fn initialize_for_read(&self, v: &Volume, id: &str) -> Result<()> {
        if self.catalog().needs_recovery(id)? {
            let (boot, _) = self
                .helper(&v.scope_id, v, id.to_owned(), AccessMode::ReadWrite)
                .await?;
            self.stop(&boot, false).await?;
            if self.catalog().needs_recovery(id)? {
                return Err(Error::Conflict("helper did not confirm a clean unmount"));
            }
        }
        Ok(())
    }
}
