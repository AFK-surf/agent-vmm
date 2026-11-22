use crate::{
    backend::LocalDom0Backend,
    catalog::{SavedBoot, image_id, overlay_id, volume_id},
    extent::ctl_error,
    image::Defaults,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddrV4,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use vmm_core::{
    backend::{Launch, LiveVm},
    *,
};
use vmm_guest_proto::{
    Secret,
    boot::{self, BootConfig, EncryptedDisk, NetworkConfig, RootConfig, VolumeMount, WorkloadSpec},
    rpc::{HealthReply, PowerOffRequest},
};
use vmm_l4hctl::{DiskVolume, IsolatedNic, VmSpec, VmState};

fn device(n: usize) -> Result<String> {
    // l4h assigns virtio-blk devices in attach order.
    let name = match n {
        0..26 => ((b'a' + n as u8) as char).to_string(),
        26..702 => format!(
            "{}{}",
            (b'a' + (n / 26 - 1) as u8) as char,
            (b'a' + (n % 26) as u8) as char
        ),
        _ => return Err(Error::Invalid("too many disks")),
    };
    Ok(format!("vd{name}"))
}
fn workload(l: &Launch<'_>, d: Defaults, subid_caps: bool) -> WorkloadSpec {
    let def = l.definition;
    let mut env: BTreeMap<String, String> = d
        .env
        .unwrap_or_default()
        .into_iter()
        .filter_map(|p| p.split_once('=').map(|(k, v)| (k.to_owned(), v.to_owned())))
        .collect();
    env.extend(def.env.clone());
    WorkloadSpec {
        entrypoint: def.entrypoint.clone().or(d.entrypoint).unwrap_or_default(),
        cmd: def.command.clone().or(d.cmd).unwrap_or_default(),
        env: env.into_iter().map(|(k, v)| format!("{k}={v}")).collect(),
        workdir: def
            .workdir
            .clone()
            .or(d.working_dir)
            .filter(|s| !s.is_empty()),
        user: def.user.clone().or(d.user).filter(|s| !s.is_empty()),
        pids_max: Some(
            def.pids_max
                .unwrap_or(l.scope.limits.pids_max)
                .min(l.scope.limits.pids_max),
        ),
        log_limit_bytes: def.log_limit_bytes,
        keep_alive_idle: true,
        subid_caps,
    }
}
impl LocalDom0Backend {
    pub(crate) async fn launch(
        &self,
        l: Launch<'_>,
        helper: Option<(&str, bool)>,
    ) -> Result<VMIncarnation> {
        if l.boot.sandbox != l.definition.key
            || l.scope.id != l.boot.sandbox.scope_id
            || l.image.manifest_digest != l.definition.manifest_digest
            || l.boot.boot_token_hash != <[u8; 32]>::from(Sha256::digest(l.boot_token))
        {
            return Err(Error::Fenced);
        }
        let catalog = self.catalog();
        if catalog
            .get::<BootIdentity>(&format!("stopped/{}", l.boot.instance_id))?
            .is_some()
        {
            return Err(Error::Fenced);
        }
        let saved = catalog.boots()?;
        if let Some(b) = saved
            .iter()
            .find(|b| b.identity.instance_id == l.boot.instance_id)
        {
            if b.identity != *l.boot || b.retiring || b.retired {
                return Err(Error::Fenced);
            }
            return self.authenticate(b.clone()).await.map(|v| v.incarnation);
        }
        if saved
            .iter()
            .any(|b| !b.retired && b.identity.sandbox == l.boot.sandbox)
        {
            return Err(Error::Conflict("sandbox has a pending boot"));
        }
        let slots = saved
            .iter()
            .filter(|b| !b.retired)
            .map(|b| b.slot)
            .collect::<BTreeSet<_>>();
        let slot = (0..=65532)
            .find(|n| !slots.contains(n))
            .ok_or(Error::Capacity(CapacityDimension::ComputeSlots))?;
        let ip = (self.cfg.slot_address)(slot)
            .ok_or(Error::Capacity(CapacityDimension::ComputeSlots))?;
        let token = Secret::new(hex::encode(l.boot_token));
        let mut b = SavedBoot {
            identity: l.boot.clone(),
            name: format!("vmm-{}", hex::encode(crate::keys::random::<13>()?.as_ref())),
            slot,
            vm: None,
            encrypted_token: self
                .sealer
                .seal(l.boot.instance_id.as_bytes(), token.expose().as_bytes())?,
            definition: l.definition.clone(),
            scope: l.scope.clone(),
            disks: Vec::new(),
            writers: Vec::new(),
            retiring: false,
            retired: false,
            helper: helper.is_some(),
            share_ports: Default::default(),
        };
        let id = image_id(l.image.manifest_digest);
        if id != l.image.extent_id {
            return Err(Error::Fenced);
        }
        let defaults = catalog
            .defaults(&id)?
            .ok_or(Error::Missing("installed image"))?;
        let mut disks = vec![self.disk(&id, true)?];
        let mut encrypted = |id: String, ro: bool| -> Result<EncryptedDisk> {
            let (key, format) = catalog.disk_secret(&id, self.sealer.as_ref())?;
            let device = device(disks.len())?;
            disks.push(self.disk(&id, ro)?);
            if !ro {
                b.writers.push(id.clone());
            }
            b.disks.push(id);
            Ok(EncryptedDisk {
                device,
                key,
                allow_discards: !ro,
                format,
            })
        };
        let overlay = match &l.definition.root_overlay {
            Some(o) => Some(encrypted(
                if let Some((object, _)) = helper {
                    format!("{object}/root")
                } else {
                    overlay_id(&l.definition.key, &o.extent_id)?
                },
                false,
            )?),
            None => None,
        };
        let mut volumes = Vec::new();
        for m in &l.definition.mounts {
            let v = l
                .volumes
                .iter()
                .find(|v| v.volume_id == m.volume_id && v.scope_id == l.scope.id)
                .ok_or(Error::Missing("launch volume"))?;
            let ro = m.access == AccessMode::ReadOnly;
            volumes.push(VolumeMount {
                name: m.volume_id.clone(),
                disk: encrypted(
                    helper
                        .map(|(object, _)| object.to_owned())
                        .unwrap_or(volume_id(&v.scope_id, &v.volume_id)?),
                    ro,
                )?,
                mount_path: m.guest_path.clone(),
                owner_uid: v.owner_uid,
                owner_gid: v.owner_gid,
                mode: v.mode,
                read_only: ro,
            });
        }
        if saved.iter().filter(|other| !other.retired).any(|other| {
            other.disks.iter().any(|id| b.writers.contains(id))
                || other.writers.iter().any(|id| b.disks.contains(id))
        }) {
            return Err(Error::Conflict("encrypted disk already mounted"));
        }
        // Record the intent before creating any listener. Failed or interrupted
        // setup can then revoke every grant using the durable boot identity.
        catalog.save(&b)?;
        if let Err(error) = self.prepare_network(&mut b).await {
            self.stop(&b.identity, true).await?;
            return Err(error);
        }
        let shares = b
            .definition
            .host_shares
            .iter()
            .map(|m| boot::HostShareMount {
                name: m.share_id.clone(),
                port: b.share_ports[&m.share_id],
                mount_path: m.guest_path.clone(),
                read_only: m.read_only,
            })
            .collect();
        let cfg = BootConfig {
            version: boot::BOOT_CONFIG_VERSION,
            instance_id: l.boot.instance_id.clone(),
            rpc_auth_token: token,
            rpc_port: self.cfg.rpc_port,
            network: NetworkConfig {
                address: ip,
                prefix_len: 16,
                gateway: self.cfg.gateway,
                dns: (self.cfg.dns_port != 0)
                    .then_some(self.cfg.gateway)
                    .into_iter()
                    .collect(),
                hostname: None,
            },
            root: RootConfig {
                device: device(0)?,
                read_only_root: l.definition.read_only_root,
                overlay,
            },
            scratch: None,
            volumes,
            host_shares: shares,
            workspace_path: Some("/workspace".into()),
            workload: workload(&l, defaults, helper.is_some_and(|(_, build)| build)),
        };
        let pool = self.pool.clone();
        let disks_to_format = b.writers.clone();
        let instance = b.identity.instance_id.clone();
        let result = async {
            self.boots
                .register_with(&cfg, move || {
                    crate::catalog::Catalog(&pool).format_issued(&disks_to_format, &instance)
                })
                .await?;
            drop(cfg);
            catalog.set_dirty(&b.writers, true)?;
            self.create(&b, &disks, ip)
        }
        .await;
        let vm = match result {
            Ok(vm) => vm,
            Err(e) => {
                self.stop(&b.identity, true).await?;
                return Err(e);
            }
        };
        b.vm = Some(self.incarnation(&b, vm)?);
        catalog.save(&b)?;
        let deadline = tokio::time::Instant::now() + self.cfg.boot_timeout;
        loop {
            match tokio::time::timeout_at(deadline, self.authenticate(b.clone())).await {
                Ok(Ok(live)) => return Ok(live.incarnation),
                Ok(Err(Error::Fenced)) => return Err(Error::Fenced),
                Ok(Err(_)) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                _ => {
                    let mut tail = Vec::new();
                    let mut at = 0;
                    let ctl = self.ctl.lock().map_err(|_| Error::Interrupted)?;
                    for _ in 0..512 {
                        let Ok((next, data, _)) = ctl.console_read(vm, at) else {
                            break;
                        };
                        if data.is_empty() || next <= at {
                            break;
                        }
                        tail.extend(data);
                        at = next;
                    }
                    let from = tail.len().saturating_sub(4096);
                    return Err(std::io::Error::other(format!(
                        "guest health timed out; console: {}",
                        String::from_utf8_lossy(&tail[from..])
                    ))
                    .into());
                }
            }
        }
    }
    fn create(&self, b: &SavedBoot, disks: &[DiskVolume], ip: std::net::Ipv4Addr) -> Result<u64> {
        let cmdline = format!(
            "console=hvc0 psi=1 l4h_memhp.demand=1 vmm.slot={} vmm.boot_port={}",
            b.slot, self.cfg.boot_port
        );
        let initrd_buf;
        let initrd: &[u8] = match &self.cfg.initrd_path {
            Some(path) => {
                initrd_buf = std::fs::read(path)?;
                &initrd_buf
            }
            None => &self.cfg.initrd,
        };
        let boot_ram_mb = b.scope.limits.boot_ram_mb;
        let max_ram_mb = b.scope.limits.max_ram_mb.max(boot_ram_mb);
        let boot_ram_bytes = boot_ram_mb
            .checked_mul(1 << 20)
            .ok_or(Error::Invalid("boot RAM"))?;
        let max_ram_bytes = max_ram_mb
            .checked_mul(1 << 20)
            .ok_or(Error::Invalid("max RAM"))?;
        let mut ctl = self.ctl.lock().map_err(|_| Error::Interrupted)?;
        let id = ctl
            .create_networked_vm(
                &VmSpec {
                    name: &b.name,
                    memory_bytes: boot_ram_bytes,
                    vcpus: b.scope.limits.vcpus,
                    initrd,
                    cmdline: &cmdline,
                    volumes: disks,
                },
                IsolatedNic {
                    switch: self.switch,
                    address: ip,
                },
            )
            .map_err(ctl_error)?;
        if let Err(err) = ctl.vm_set_mem(id, max_ram_bytes) {
            let _ = ctl.destroy_vm(id);
            return Err(ctl_error(err));
        }
        Ok(id)
    }
    fn incarnation(&self, b: &SavedBoot, id: u64) -> Result<VMIncarnation> {
        Ok(VMIncarnation {
            boot: b.identity.clone(),
            slot_ip: self.slot(b)?,
            l4h_vm_id: id.to_string(),
            started_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(std::io::Error::other)?
                .as_secs(),
        })
    }
    async fn authenticate(&self, mut b: SavedBoot) -> Result<LiveVm> {
        let vms = self.inventory()?;
        let vm = self
            .present(&b, &vms)?
            .filter(|v| v.state == VmState::Running)
            .ok_or(Error::Missing("running VM"))?;
        let HealthReply {
            instance_id,
            quiesced,
            ..
        } = self.client(&b)?.health().await?;
        if instance_id != b.identity.instance_id {
            return Err(Error::Fenced);
        }
        let inc = match &b.vm {
            Some(i) => i.clone(),
            None => self.incarnation(&b, vm.id)?,
        };
        b.vm = Some(inc.clone());
        self.catalog().save(&b)?;
        self.catalog().confirm(&b.disks)?;
        Ok(LiveVm {
            incarnation: inc,
            quiesced,
        })
    }
    pub(crate) async fn live(&self) -> Result<Vec<LiveVm>> {
        let vms = self.inventory()?;
        let mut out = Vec::new();
        for mut b in self
            .catalog()
            .boots()?
            .into_iter()
            .filter(|b| !b.retired && !b.helper)
        {
            if b.retiring {
                self.stop(&b.identity, true).await?;
                continue;
            }
            if self
                .present(&b, &vms)?
                .is_some_and(|vm| vm.state == VmState::Running)
            {
                self.prepare_network(&mut b).await?;
                out.push(
                    tokio::time::timeout(self.cfg.boot_timeout, self.authenticate(b))
                        .await
                        .map_err(|_| Error::Interrupted)??,
                );
            } else {
                self.stop(&b.identity, true).await?;
            }
        }
        Ok(out)
    }
    pub(crate) async fn stop(&self, id: &BootIdentity, force: bool) -> Result<()> {
        let catalog = self.catalog();
        let mut b = match catalog.boot(id) {
            Err(Error::Missing(_)) => {
                catalog.put(&format!("stopped/{}", id.instance_id), id)?;
                return Ok(());
            }
            other => other?,
        };
        if b.retired {
            return Ok(());
        }
        b.retiring = true;
        catalog.save(&b)?;
        let mut clean_unmount = false;
        if let Some(vm) = self.present(&b, &self.inventory()?)? {
            if !force && vm.state == VmState::Running {
                clean_unmount = self
                    .client(&b)?
                    .call(|c| c.power_off(PowerOffRequest { grace_ms: 3000 }))
                    .await?
                    .clean_unmount;
            }
            self.ctl
                .lock()
                .map_err(|_| Error::Interrupted)?
                .destroy_vm(vm.id)
                .map_err(ctl_error)?;
        }
        if clean_unmount {
            catalog.set_dirty(&b.writers, false)?;
        }
        self.boots
            .cancel(self.slot(&b)?, &b.identity.instance_id)
            .await;
        // Keep the slot reserved until every listener and firewall grant is gone.
        self.release_network(&b).await?;
        b.retired = true;
        catalog.save(&b)
    }
    pub async fn forward_tcp(&self, b: &BootIdentity, port: u16) -> Result<tokio::net::TcpStream> {
        if port == 0 || port == self.cfg.rpc_port {
            return Err(Error::Invalid("forward port"));
        }
        let _lock = self.effects.lock().await;
        self.runtime(b)?;
        let ip = self.slot(&self.catalog().boot(b)?)?;
        Ok(tokio::net::TcpStream::connect(SocketAddrV4::new(ip, port)).await?)
    }
}
