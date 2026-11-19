use crate::{
    boot_server::BootServer,
    catalog::{Catalog, SavedBoot, image_id, overlay_id, secret_text, volume_id},
    extent::{Control, Pool, ctl_error},
    keys::KeySealer,
    network::FirewallDriver,
    proxy::Proxies,
    runtime::Dom0Runtime,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddrV4},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use vmm_core::{
    backend::{Dom0Backend, Launch, LiveVm, ProcessState, WorkspaceRequest},
    *,
};
use vmm_guest_proto::{boot, rpc};
use vmm_l4hctl::{DiskVolume, VmInfo};

pub use crate::proxy::ExternalServiceRoute;

pub struct Options {
    pub initrd: Arc<[u8]>,
    pub staging: PathBuf,
    pub switch: Option<u64>,
    pub gateway: Ipv4Addr,
    pub boot_port: u16,
    pub dns_port: u16,
    pub upstream_dns: Option<SocketAddrV4>,
    pub rpc_port: u16,
    pub slot_address: fn(u32) -> Option<Ipv4Addr>,
    pub boot_timeout: Duration,
    pub helper_image: Option<Digest>,
    pub host_shares: BTreeMap<String, PathBuf>,
}

impl Options {
    pub fn new(initrd: Arc<[u8]>, staging: PathBuf) -> Self {
        Self {
            initrd,
            staging,
            switch: None,
            gateway: boot::DEFAULT_GATEWAY,
            boot_port: boot::DEFAULT_BOOT_PORT,
            dns_port: 0,
            upstream_dns: None,
            rpc_port: boot::DEFAULT_RPC_PORT,
            slot_address: boot::slot_address,
            boot_timeout: Duration::from_secs(60),
            helper_image: None,
            host_shares: BTreeMap::new(),
        }
    }
}
pub struct LocalDom0Backend {
    pub(crate) pool: Arc<Pool>,
    pub(crate) ctl: Control,
    pub(crate) cfg: Options,
    pub(crate) sealer: Arc<dyn KeySealer>,
    pub(crate) firewall: Arc<dyn FirewallDriver>,
    pub(crate) proxies: Proxies,
    pub(crate) boots: BootServer,
    pub(crate) switch: u64,
    pub(crate) effects: Mutex<()>,
    pub(crate) routes: Mutex<crate::policy::Routes>,
    pub(crate) shares: crate::host_share::Shares,
    pub(crate) mounts: crate::helper_vm::MountLocks,
    pub(crate) recovery: Mutex<crate::build_vm::Recovery>,
    _instance: std::fs::File,
}

impl LocalDom0Backend {
    pub fn open(
        pool: Arc<Pool>,
        ctl: Control,
        cfg: Options,
        sealer: Arc<dyn KeySealer>,
        firewall: Arc<dyn FirewallDriver>,
        boots: BootServer,
    ) -> Result<Self> {
        std::fs::create_dir_all(&cfg.staging)?;
        let instance = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cfg.staging.join("dom0.lock"))?;
        instance
            .try_lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let catalog = Catalog(&pool);
        catalog.init()?;
        let saved = catalog.get::<u64>("switch")?;
        if let (Some(id), Some(want)) = (saved, cfg.switch)
            && id != want
        {
            return Err(Error::Conflict("configured switch changed"));
        }
        let switch = ctl
            .lock()
            .map_err(|_| Error::Interrupted)?
            .ensure_switch(saved.or(cfg.switch))
            .map_err(ctl_error)?;
        catalog.put("switch", &switch)?;
        let routes = catalog.get("routes")?.unwrap_or_default();
        firewall.replace(&BTreeMap::new())?;
        let recovery = Mutex::new(crate::build_vm::Recovery {
            helpers: catalog
                .boots()?
                .into_iter()
                .filter(|b| b.helper && !b.retired)
                .map(|b| b.identity)
                .collect(),
            objects: catalog.transient_objects()?,
            stages: crate::staging::staged_at_startup(&catalog, &cfg.staging)?,
        });
        Ok(Self {
            recovery,
            pool,
            ctl,
            cfg,
            sealer,
            firewall,
            proxies: Proxies::default(),
            boots,
            switch,
            effects: Mutex::new(()),
            routes: Mutex::new(crate::policy::Routes {
                local: routes,
                ..Default::default()
            }),
            shares: Default::default(),
            mounts: Default::default(),
            _instance: instance,
        })
    }
    pub(crate) fn catalog(&self) -> Catalog<'_> {
        Catalog(&self.pool)
    }
    pub(crate) fn slot(&self, boot: &SavedBoot) -> Result<Ipv4Addr> {
        (self.cfg.slot_address)(boot.slot).ok_or(Error::Invalid("VM slot"))
    }
    pub(crate) fn inventory(&self) -> Result<Vec<VmInfo>> {
        self.ctl
            .lock()
            .map_err(|_| Error::Interrupted)?
            .list_vms()
            .map_err(ctl_error)
    }
    pub(crate) fn present<'a>(
        &self,
        boot: &SavedBoot,
        vms: &'a [VmInfo],
    ) -> Result<Option<&'a VmInfo>> {
        let mut found = vms.iter().filter(|v| v.name == boot.name);
        let first = found.next();
        if found.next().is_some()
            || first.is_some_and(|v| {
                boot.vm
                    .as_ref()
                    .is_some_and(|known| known.l4h_vm_id != v.id.to_string())
            })
        {
            return Err(Error::Fenced);
        }
        Ok(first)
    }
    pub(crate) fn client(&self, b: &SavedBoot) -> Result<Dom0Runtime> {
        let token = secret_text(
            self.sealer
                .open(b.identity.instance_id.as_bytes(), &b.encrypted_token)?,
        )?;
        Ok(Dom0Runtime::new(
            SocketAddrV4::new(self.slot(b)?, self.cfg.rpc_port).into(),
            token,
        ))
    }
    pub fn runtime(&self, boot: &BootIdentity) -> Result<Dom0Runtime> {
        let b = self.catalog().boot(boot)?;
        if b.retiring || b.retired || self.present(&b, &self.inventory()?)?.is_none() {
            return Err(Error::Fenced);
        }
        self.client(&b)
    }
    pub(crate) fn disk(&self, id: &str, read_only: bool) -> Result<DiskVolume> {
        Ok(DiskVolume {
            disk: self.pool.disk(),
            read_only,
            volatile: false,
            extents: self.pool.lookup(id)?,
        })
    }
    pub(crate) fn ensure_disk(&self, id: &str, quota: u64) -> Result<()> {
        self.pool.allocate_sparse(id, quota)?;
        self.catalog().ensure_key(id, self.sealer.as_ref())
    }
}

#[async_trait]
impl Dom0Backend for LocalDom0Backend {
    async fn install_image_artifact(&self, s: &ImageSource) -> Result<ImageArtifact> {
        let _lock = self.effects.lock().await;
        self.install(s)
    }
    async fn discard_image_artifact(&self, d: Digest) -> Result<u64> {
        let _lock = self.effects.lock().await;
        self.pool.discard(&image_id(d))
    }
    async fn allocate_volume(&self, v: &Volume) -> Result<()> {
        let _lock = self.effects.lock().await;
        let id = volume_id(&v.scope_id, &v.volume_id)?;
        self.ensure_disk(&id, v.quota_bytes)?;
        self.catalog().put(&format!("volume/{id}"), v)
    }
    async fn discard_volume(&self, s: &str, v: &str) -> Result<()> {
        let _lock = self.effects.lock().await;
        let id = volume_id(s, v)?;
        self.pool.discard(&id)?;
        self.catalog().delete(&format!("volume/{id}"))
    }
    async fn allocate_overlay(&self, k: &SandboxKey, o: &RootOverlay) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.ensure_disk(&overlay_id(k, &o.extent_id)?, o.quota_bytes)
    }
    async fn discard_overlay(&self, k: &SandboxKey, o: &RootOverlay) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.pool.discard(&overlay_id(k, &o.extent_id)?)?;
        Ok(())
    }
    async fn start_vm(&self, l: Launch<'_>) -> Result<VMIncarnation> {
        let ids = l
            .volumes
            .iter()
            .map(|v| volume_id(&v.scope_id, &v.volume_id))
            .collect::<Result<std::collections::BTreeSet<_>>>()?;
        let mut guards = Vec::new();
        for id in ids {
            guards.push(self.mounts.acquire(id).await?);
        }
        let _lock = self.effects.lock().await;
        for m in &l.definition.mounts {
            if m.access == AccessMode::ReadOnly {
                let v = l
                    .volumes
                    .iter()
                    .find(|v| v.scope_id == l.scope.id && v.volume_id == m.volume_id)
                    .ok_or(Error::Missing("volume"))?;
                self.initialize_for_read(v, &volume_id(&v.scope_id, &v.volume_id)?)
                    .await?;
            }
        }
        self.launch(l, None).await
    }
    async fn list_live_vms(&self) -> Result<Vec<LiveVm>> {
        let _lock = self.effects.lock().await;
        self.live().await
    }
    async fn shutdown_vm(&self, b: &BootIdentity) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.stop(b, false).await
    }
    async fn destroy_vm(&self, b: &BootIdentity) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.stop(b, true).await
    }
    async fn quiesce_vm(&self, b: &BootIdentity) -> Result<()> {
        let r = self
            .runtime(b)?
            .call(|c| {
                c.quiesce(rpc::QuiesceRequest {
                    cancel: false,
                    minimum_idle_ms: 0,
                })
            })
            .await?;
        if !r.quiesced {
            return Err(Error::Conflict("guest has active executions"));
        }
        Ok(())
    }
    async fn set_network_policy(&self, b: &BootIdentity, s: &Scope) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.policy(b, s).await
    }
    async fn bind_route(&self, r: &ServiceRoute) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.bind(r).await
    }
    async fn revoke_route(&self, id: &str, rev: i64) -> Result<()> {
        let _lock = self.effects.lock().await;
        self.revoke(id, rev).await
    }
    async fn exec(&self, b: &BootIdentity, argv: &[String]) -> Result<String> {
        let req = rpc::ProcessStartRequest {
            request_id: hex::encode(crate::keys::random::<16>()?.as_ref()),
            command: rpc::CommandSpec {
                argv: argv.to_vec(),
                env: vec![],
                user: None,
                workdir: None,
            },
            output_limit_bytes: 0,
        };
        Ok(self
            .runtime(b)?
            .process_start(req)
            .await?
            .process
            .process_id)
    }
    async fn process(&self, b: &BootIdentity, id: &str) -> Result<ProcessState> {
        let p = self
            .runtime(b)?
            .process_list()
            .await?
            .into_iter()
            .find(|p| p.process_id == id)
            .ok_or(Error::Missing("guest process"))?;
        Ok(match p.state {
            rpc::ProcessState::Running => ProcessState::Running,
            rpc::ProcessState::Exited(s) => ProcessState::Exited(s.code),
        })
    }
    async fn workspace(&self, b: &BootIdentity, q: WorkspaceRequest) -> Result<Vec<u8>> {
        let def = self.catalog().boot(b)?.definition;
        let (path, bytes) = match q {
            WorkspaceRequest::Read(p) => (p, None),
            WorkspaceRequest::Write { path, bytes } => (path, Some(bytes)),
        };
        let path = Path::new("/").join(path);
        let mount = def
            .mounts
            .iter()
            .filter_map(|m| path.strip_prefix(&m.guest_path).ok().map(|p| (m, p)))
            .max_by_key(|(m, _)| m.guest_path.len());
        let (volume, relative) = match mount {
            Some((m, p)) => (Some(m.volume_id.clone()), p),
            None => (
                None,
                path.strip_prefix("/workspace")
                    .map_err(|_| Error::Invalid("workspace root"))?,
            ),
        };
        let target = rpc::WorkspaceTarget {
            volume,
            path: relative
                .to_str()
                .ok_or(Error::Invalid("workspace path"))?
                .into(),
        };
        let runtime = self.runtime(b)?;
        if let Some(bytes) = bytes {
            let request = rpc::WorkspaceWriteRequest {
                target,
                request_id: hex::encode(crate::keys::random::<16>()?.as_ref()),
                offset: 0,
                expected_prefix_sha256: None,
                mode: 0,
            };
            runtime
                .workspace_write(request, std::io::Cursor::new(bytes))
                .await?;
            Ok(Vec::new())
        } else {
            runtime
                .call(move |client| {
                    let mut buf = Vec::new();
                    client.workspace_read(
                        rpc::WorkspaceReadRequest {
                            target,
                            offset: 0,
                            max_bytes: 0,
                        },
                        &mut buf,
                    )?;
                    Ok(buf)
                })
                .await
        }
    }
    async fn logs(&self, b: &BootIdentity, id: &str) -> Result<Vec<u8>> {
        let runtime = self.runtime(b)?;
        let id = id.to_owned();
        runtime
            .call(move |c| {
                let mut data = Vec::new();
                let mut offset = 0;
                loop {
                    let tail = c.process_tail(rpc::ProcessTailRequest {
                        process_id: id.clone(),
                        since_offset: offset,
                        max_bytes: 0,
                        wait_ms: 0,
                    })?;
                    if tail.data.is_empty() {
                        break;
                    }
                    offset = tail.next_offset;
                    data.extend(tail.data);
                }
                Ok(data)
            })
            .await
    }
}
