//! Disposable rootless build VM, with separate encrypted work and root disks.
use crate::{backend::LocalDom0Backend, helper_vm::HelperSpec, keys::random, runtime::Dom0Runtime};
use std::sync::Arc;
use vmm_core::*;

#[derive(Default)]
pub(crate) struct Recovery {
    pub helpers: Vec<BootIdentity>,
    pub objects: Vec<String>,
    pub stages: Vec<String>,
}

struct Pending {
    backend: Arc<LocalDom0Backend>,
    object: String,
}
impl Pending {
    async fn cleanup(&self) -> Result<()> {
        let b = &self.backend;
        let _lock = b.effects.lock().await;
        self.cleanup_locked().await
    }
    async fn cleanup_locked(&self) -> Result<()> {
        let b = &self.backend;
        for boot in b
            .catalog()
            .boots()?
            .into_iter()
            .filter(|r| !r.retired && r.helper && r.disks.contains(&self.object))
        {
            b.stop(&boot.identity, true).await?;
        }
        b.pool.discard(&format!("{}/root", self.object))?;
        b.pool.discard(&self.object)?;
        Ok(())
    }
}
pub struct BuildVm {
    pub guest: Dom0Runtime,
    pub boot: BootIdentity,
    cleanup: Option<Pending>,
}
impl BuildVm {
    pub async fn close(mut self) -> Result<()> {
        self.cleanup.as_ref().expect("live build").cleanup().await?;
        self.cleanup.take();
        Ok(())
    }
}
struct Cancel(Option<Pending>);
impl Drop for Cancel {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            spawn_cleanup(p)
        }
    }
}
fn spawn_cleanup(p: Pending) {
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        rt.spawn(async move {
            let _ = p.cleanup().await;
        });
    }
}
impl Drop for BuildVm {
    fn drop(&mut self) {
        if let Some(p) = self.cleanup.take() {
            spawn_cleanup(p)
        }
    }
}
impl LocalDom0Backend {
    /// Launch the builder selected by dom0 under the requesting scope's limits.
    pub async fn begin_build(
        self: &Arc<Self>,
        scope: &Scope,
        network: EgressMode,
        quota_bytes: u64,
    ) -> Result<BuildVm> {
        let id = hex::encode(random::<16>()?.as_ref());
        self.begin_owned_build(scope, network, quota_bytes, &id)
            .await
    }

    pub async fn settle_build(self: &Arc<Self>, id: &str) -> Result<()> {
        let object = build_object(id)?;
        let _lock = self.effects.lock().await;
        self.catalog().put(&format!("build-lease/{id}"), &true)?;
        Pending {
            backend: self.clone(),
            object,
        }
        .cleanup_locked()
        .await
    }

    pub async fn begin_owned_build(
        self: &Arc<Self>,
        scope: &Scope,
        network: EgressMode,
        quota_bytes: u64,
        id: &str,
    ) -> Result<BuildVm> {
        let builder = self
            .cfg
            .builder_image
            .ok_or(Error::Missing("trusted builder image"))?;
        if scope.state != ScopeState::Active {
            return Err(Error::Draining);
        }
        if quota_bytes == 0 || quota_bytes > scope.limits.disk_quota_bytes {
            return Err(Error::Invalid("build quota"));
        }
        let object = build_object(id)?;
        let _lock = self.effects.lock().await;
        let key = format!("build-lease/{id}");
        if self.catalog().get::<bool>(&key)?.is_some() {
            return Err(Error::Fenced);
        }
        self.catalog().put(&key, &false)?;
        let mut cancel = Cancel(Some(Pending {
            backend: self.clone(),
            object: object.clone(),
        }));
        let mut isolated = scope.clone();
        isolated.host_socket_rules.clear();
        if network == EgressMode::DenyAll {
            isolated.egress_mode = network;
        }
        let volume = Volume {
            scope_id: scope.id.clone(),
            volume_id: "workspace".into(),
            quota_bytes,
            used_bytes: 0,
            owner_uid: 1000,
            owner_gid: 1000,
            mode: 0o700,
            key_id: None,
        };
        self.ensure_disk(&object, quota_bytes)?;
        self.ensure_disk(&format!("{object}/root"), quota_bytes)?;
        let (boot, guest) = self
            .launch_helper(
                HelperSpec {
                    scope: &isolated,
                    digest: builder,
                    access: AccessMode::ReadWrite,
                    build: true,
                },
                &volume,
                &object,
            )
            .await?;
        Ok(BuildVm {
            guest,
            boot,
            cleanup: cancel.0.take(),
        })
    }
    /// Reclaim transient ownership left by the previous daemon, before
    /// admitting work. A failed cleanup retains its snapshot for a retry.
    pub async fn recover_transients(&self) -> Result<()> {
        let _lock = self.effects.lock().await;
        let mut pending = self.recovery.lock().await;
        for boot in &pending.helpers {
            self.stop(boot, true).await?;
        }
        for object in &pending.objects {
            self.pool.discard(object)?;
        }
        for token in &pending.stages {
            self.discard_staged_image(token)?;
        }
        *pending = Default::default();
        Ok(())
    }
}

fn build_object(id: &str) -> Result<String> {
    if id.len() != 32
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("build lease ID"));
    }
    Ok(format!("build/{id}"))
}
