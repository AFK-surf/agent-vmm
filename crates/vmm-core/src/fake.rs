use crate::{backend::*, *};
use async_trait::async_trait;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::Ipv4Addr,
    sync::{Mutex, MutexGuard},
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    ComputeSlots,
    StorageHeadroom,
    /// Guest exists, but the host never receives the successful start reply.
    LostStartReply,
    /// Host accepted the start and lost contact before a VM was created.
    InterruptedStart,
    /// Inventory handshake returns a different token for a live VM.
    HandshakeMismatch,
}

#[derive(Default)]
struct Memory {
    images: HashMap<Digest, ImageArtifact>,
    volumes: HashMap<(String, String), Volume>,
    overlays: HashMap<SandboxKey, RootOverlay>,
    guests: HashMap<SandboxKey, Guest>,
    retired_boots: HashSet<String>,
    routes: HashMap<String, ServiceRoute>,
    faults: VecDeque<Fault>,
    next_slot: u32,
    reclaimed: u64,
    shutdown_calls: u64,
    files: HashMap<Backing, HashMap<String, Vec<u8>>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum Backing {
    Root(SandboxKey),
    Volume(String, String),
}

struct Guest {
    live: LiveVm,
    policy: Scope,
    processes: HashMap<String, (ProcessState, Vec<u8>)>,
    definition: SandboxDefinition,
}

impl Memory {
    fn guest(&mut self, boot: &BootIdentity) -> Result<&mut Guest> {
        self.guests
            .get_mut(&boot.sandbox)
            .filter(|g| g.live.incarnation.boot == *boot)
            .ok_or(Error::Fenced)
    }
    fn runnable(&mut self, boot: &BootIdentity) -> Result<&mut Guest> {
        let guest = self.guest(boot)?;
        if guest.live.quiesced {
            return Err(Error::Conflict("VM quiesced"));
        }
        Ok(guest)
    }
    fn consume(&mut self, fault: Fault) -> bool {
        if self.faults.front() == Some(&fault) {
            self.faults.pop_front();
            true
        } else {
            false
        }
    }
    fn storage(&mut self) -> Result<()> {
        if self.consume(Fault::StorageHeadroom) {
            return Err(Error::Capacity(CapacityDimension::StorageHeadroom));
        }
        Ok(())
    }
    fn terminate(&mut self, boot: &BootIdentity) -> Result<()> {
        if let Some(guest) = self.guests.get(&boot.sandbox)
            && &guest.live.incarnation.boot != boot
        {
            return Err(Error::Fenced);
        }
        self.guests.remove(&boot.sandbox);
        self.routes
            .retain(|_, r| r.binding.source != *boot && r.binding.export.boot != *boot);
        self.retired_boots.insert(boot.instance_id.clone());
        Ok(())
    }
}

#[derive(Default)]
pub struct FakeDom0Backend {
    memory: Mutex<Memory>,
}

impl FakeDom0Backend {
    fn state(&self) -> Result<MutexGuard<'_, Memory>> {
        self.memory.lock().map_err(|_| Error::Interrupted)
    }
    pub fn inject(&self, fault: Fault) -> Result<()> {
        self.state()?.faults.push_back(fault);
        Ok(())
    }
    pub fn network_policy(&self, boot: &BootIdentity) -> Result<Scope> {
        Ok(self.state()?.guest(boot)?.policy.clone())
    }
    pub fn shutdown_calls(&self) -> Result<u64> {
        Ok(self.state()?.shutdown_calls)
    }
    pub fn reclaimed_image_bytes(&self) -> Result<u64> {
        Ok(self.state()?.reclaimed)
    }
    pub fn overlay(&self, key: &SandboxKey) -> Result<Option<RootOverlay>> {
        Ok(self.state()?.overlays.get(key).cloned())
    }
    pub fn route(&self, id: &str) -> Result<Option<ServiceRoute>> {
        Ok(self.state()?.routes.get(id).cloned())
    }
    pub fn finish_process(
        &self,
        boot: &BootIdentity,
        id: &str,
        status: i32,
        output: Vec<u8>,
    ) -> Result<()> {
        let mut state = self.state()?;
        let process = state
            .guest(boot)?
            .processes
            .get_mut(id)
            .ok_or(Error::Missing("process"))?;
        *process = (ProcessState::Exited(status), output);
        Ok(())
    }
}

#[async_trait]
impl Dom0Backend for FakeDom0Backend {
    async fn install_image_artifact(&self, source: &ImageSource) -> Result<ImageArtifact> {
        let mut state = self.state()?;
        if let Some(image) = state.images.get(&source.manifest_digest) {
            return Ok(image.clone());
        }
        state.storage()?;
        let image = ImageArtifact {
            manifest_digest: source.manifest_digest,
            extent_id: Uuid::new_v4().to_string(),
            physical_bytes: source.physical_bytes,
        };
        state.images.insert(source.manifest_digest, image.clone());
        Ok(image)
    }
    async fn discard_image_artifact(&self, digest: Digest) -> Result<u64> {
        let mut state = self.state()?;
        let bytes = state.images.remove(&digest).map_or(0, |i| i.physical_bytes);
        state.reclaimed = state
            .reclaimed
            .checked_add(bytes)
            .ok_or(Error::Invalid("reclaimed bytes overflow"))?;
        Ok(bytes)
    }
    async fn allocate_volume(&self, volume: &Volume) -> Result<()> {
        let mut state = self.state()?;
        let key = (volume.scope_id.clone(), volume.volume_id.clone());
        if let Some(old) = state.volumes.get(&key) {
            return if old.same_allocation(volume) {
                Ok(())
            } else {
                Err(Error::Conflict("volume definition"))
            };
        }
        state.storage()?;
        state.volumes.insert(key, volume.clone());
        Ok(())
    }
    async fn discard_volume(&self, scope: &str, volume: &str) -> Result<()> {
        let mut state = self.state()?;
        state.volumes.remove(&(scope.into(), volume.into()));
        state
            .files
            .remove(&Backing::Volume(scope.into(), volume.into()));
        Ok(())
    }
    async fn allocate_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()> {
        let mut state = self.state()?;
        if let Some(old) = state.overlays.get(key) {
            return if old.allocation() == overlay.allocation() {
                Ok(())
            } else {
                Err(Error::Conflict("overlay definition"))
            };
        }
        state.storage()?;
        state.overlays.insert(key.clone(), overlay.clone());
        Ok(())
    }
    async fn discard_overlay(&self, key: &SandboxKey, overlay: &RootOverlay) -> Result<()> {
        let mut state = self.state()?;
        if state
            .overlays
            .get(key)
            .is_some_and(|old| old.allocation() != overlay.allocation())
        {
            return Err(Error::Fenced);
        }
        state.overlays.remove(key);
        state.files.remove(&Backing::Root(key.clone()));
        Ok(())
    }
    async fn start_vm(&self, launch: Launch<'_>) -> Result<VMIncarnation> {
        let mut state = self.state()?;
        let key = &launch.definition.key;
        if launch.boot.boot_token_hash != Digest::from(Sha256::digest(launch.boot_token))
            || &launch.boot.sandbox != key
            || state.retired_boots.contains(&launch.boot.instance_id)
        {
            return Err(Error::Fenced);
        }
        if let Some(guest) = state.guests.get(key) {
            if guest.live.incarnation.boot == *launch.boot {
                return Ok(guest.live.incarnation.clone());
            }
            return Err(Error::Conflict("VM already exists"));
        }
        if !state.images.contains_key(&launch.image.manifest_digest)
            || launch.volumes.iter().any(|v| {
                !state
                    .volumes
                    .contains_key(&(key.scope_id.clone(), v.volume_id.clone()))
            })
            || state.overlays.get(key).map(RootOverlay::allocation)
                != launch
                    .definition
                    .root_overlay
                    .as_ref()
                    .map(RootOverlay::allocation)
        {
            return Err(Error::Missing("VM storage"));
        }
        if state.consume(Fault::ComputeSlots) {
            return Err(Error::Capacity(CapacityDimension::ComputeSlots));
        }
        if state.consume(Fault::InterruptedStart) {
            return Err(Error::Interrupted);
        }
        state.next_slot = state
            .next_slot
            .checked_add(1)
            .filter(|s| *s < 1 << 24)
            .ok_or(Error::Capacity(CapacityDimension::ComputeSlots))?;
        let vm = VMIncarnation {
            boot: launch.boot.clone(),
            l4h_vm_id: Uuid::new_v4().to_string(),
            slot_ip: Ipv4Addr::from(0x0a000000 + state.next_slot),
            started_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Interrupted)?
                .as_secs(),
        };
        state.guests.insert(
            key.clone(),
            Guest {
                live: LiveVm {
                    incarnation: vm.clone(),
                    quiesced: false,
                },
                policy: launch.scope.clone(),
                processes: Default::default(),
                definition: launch.definition.clone(),
            },
        );
        if state.consume(Fault::LostStartReply) {
            return Err(Error::Interrupted);
        }
        Ok(vm)
    }
    async fn quiesce_vm(&self, boot: &BootIdentity) -> Result<()> {
        self.state()?.guest(boot)?.live.quiesced = true;
        Ok(())
    }
    async fn shutdown_vm(&self, boot: &BootIdentity) -> Result<()> {
        let mut state = self.state()?;
        state.shutdown_calls += 1;
        state.terminate(boot)
    }
    async fn destroy_vm(&self, boot: &BootIdentity) -> Result<()> {
        self.state()?.terminate(boot)
    }
    async fn list_live_vms(&self) -> Result<Vec<LiveVm>> {
        let mut state = self.state()?;
        let mut inventory = state
            .guests
            .values()
            .map(|g| g.live.clone())
            .collect::<Vec<_>>();
        if state.consume(Fault::HandshakeMismatch)
            && let Some(first) = inventory.first_mut()
        {
            first.incarnation.boot.boot_token_hash[0] ^= 1;
        }
        Ok(inventory)
    }
    async fn set_network_policy(&self, boot: &BootIdentity, scope: &Scope) -> Result<()> {
        self.state()?.guest(boot)?.policy = scope.clone();
        Ok(())
    }
    async fn bind_route(&self, route: &ServiceRoute) -> Result<()> {
        let mut state = self.state()?;
        state.runnable(&route.binding.source)?;
        state.runnable(&route.binding.export.boot)?;
        if state
            .routes
            .get(&route.binding.id)
            .is_some_and(|r| r != route)
        {
            return Err(Error::Conflict("route revision"));
        }
        state.routes.insert(route.binding.id.clone(), route.clone());
        Ok(())
    }
    async fn revoke_route(&self, id: &str, expected_revision: i64) -> Result<()> {
        let mut state = self.state()?;
        if state
            .routes
            .get(id)
            .is_some_and(|r| r.revision != expected_revision)
        {
            return Err(Error::Fenced);
        }
        state.routes.remove(id);
        Ok(())
    }
    async fn exec(&self, boot: &BootIdentity, argv: &[String]) -> Result<String> {
        if argv.is_empty() {
            return Err(Error::Invalid("argv"));
        }
        let id = Uuid::new_v4().to_string();
        self.state()?
            .runnable(boot)?
            .processes
            .insert(id.clone(), (ProcessState::Running, vec![]));
        Ok(id)
    }
    async fn process(&self, boot: &BootIdentity, id: &str) -> Result<ProcessState> {
        self.state()?
            .guest(boot)?
            .processes
            .get(id)
            .map(|p| p.0.clone())
            .ok_or(Error::Missing("process"))
    }
    async fn workspace(&self, boot: &BootIdentity, request: WorkspaceRequest) -> Result<Vec<u8>> {
        let mut state = self.state()?;
        let def = &state.runnable(boot)?.definition;
        let (path, bytes) = match request {
            WorkspaceRequest::Read(path) => (path, None),
            WorkspaceRequest::Write { path, bytes } => (path, Some(bytes)),
        };
        let path = std::path::Path::new("/").join(path);
        if path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(Error::Invalid("workspace path"));
        }
        let mount = def
            .mounts
            .iter()
            .filter_map(|mount| {
                path.strip_prefix(&mount.guest_path)
                    .ok()
                    .map(|relative| (mount, relative))
            })
            .max_by_key(|(m, _)| m.guest_path.len());
        let (backing, file, writable) = match mount {
            Some((m, relative)) => (
                Backing::Volume(def.key.scope_id.clone(), m.volume_id.clone()),
                relative,
                m.access == AccessMode::ReadWrite,
            ),
            None => (
                Backing::Root(def.key.clone()),
                path.as_path(),
                !def.read_only_root,
            ),
        };
        let file = file.to_str().ok_or(Error::Invalid("workspace filename"))?;
        if let Some(bytes) = bytes {
            if !writable {
                return Err(Error::Conflict("read-only mount"));
            }
            state
                .files
                .entry(backing)
                .or_default()
                .insert(file.into(), bytes);
            Ok(vec![])
        } else {
            state
                .files
                .get(&backing)
                .and_then(|files| files.get(file))
                .cloned()
                .ok_or(Error::Missing("workspace file"))
        }
    }
    async fn logs(&self, boot: &BootIdentity, process: &str) -> Result<Vec<u8>> {
        self.state()?
            .guest(boot)?
            .processes
            .get(process)
            .map(|p| p.1.clone())
            .ok_or(Error::Missing("process"))
    }
}
