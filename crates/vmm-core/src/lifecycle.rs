use crate::{
    backend::{Launch, LiveVm},
    host_socket::boot_scope,
    storage::{active, artifact, check_quota, volume},
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

impl SandboxKey {
    pub(crate) fn parts(&self) -> [&str; 3] {
        [&self.scope_id, &self.sandbox_id, &self.generation_id]
    }
}

pub(crate) fn sandbox(db: &Connection, key: &SandboxKey) -> Result<Sandbox> {
    let mut stmt = db.prepare_cached("SELECT definition,phase,boot,vm,exit_reason FROM sandboxes WHERE scope=? AND id=? AND generation=?")?;
    let mut rows = stmt.query(key.parts())?;
    sandbox_from_row(rows.next()?.ok_or(Error::Missing("sandbox generation"))?)
}

pub(crate) fn sandbox_from_row(row: &rusqlite::Row<'_>) -> Result<Sandbox> {
    let boot: Option<String> = row.get(2)?;
    let vm: Option<String> = row.get(3)?;
    Ok(Sandbox {
        definition: decode(&row.get::<_, String>(0)?)?,
        state: SandboxState::parse(&row.get::<_, String>(1)?)?,
        boot: boot.as_deref().map(decode).transpose()?,
        vm: vm.as_deref().map(decode).transpose()?,
        exit_reason: row.get(4)?,
    })
}

impl Store {
    pub fn sandbox(&self, key: &SandboxKey) -> Result<Sandbox> {
        self.read(|db| sandbox(db, key))
    }
}

#[derive(Debug, Default)]
pub struct Reconciliation {
    pub adopted: Vec<VMIncarnation>,
    pub stopped: Vec<SandboxKey>,
    /// Retain locks for a mismatched identity; only explicit owner action can resolve it.
    pub mismatched: Vec<SandboxKey>,
    pub unowned: Vec<VMIncarnation>,
}

fn validate_sandbox_pids(definition: &SandboxDefinition, scope: &Scope) -> Result<()> {
    match definition.pids_max {
        Some(0) => Err(Error::Invalid("pids_max")),
        Some(pids) if pids > scope.limits.pids_max => Err(Error::Capacity(CapacityDimension::Pids)),
        _ => Ok(()),
    }
}

impl Core {
    pub async fn define_sandbox(&self, definition: &SandboxDefinition) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        let key = &definition.key;
        if key.parts().contains(&"")
            || definition.read_only_root == definition.root_overlay.is_some()
        {
            return Err(Error::Invalid("sandbox definition"));
        }
        self.store.write(|db| {
            let scope = active(db, &key.scope_id)?;
            validate_sandbox_pids(definition, &scope)?;
            let bound: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM image_refs WHERE scope=? AND digest=?)",
                params![key.scope_id, definition.manifest_digest],
                |r| r.get(0),
            )?;
            if !bound {
                return Err(Error::Missing("scope image reference"));
            }
            let (quota, used) = if let Some(root) = &definition.root_overlay {
                if root.extent_id.is_empty() || root.quota_bytes == 0 {
                    return Err(Error::Invalid("root overlay"));
                }
                (root.quota_bytes, root.used_bytes)
            } else { (0, 0) };
            check_quota(db, &scope, quota, used)?;
            db.execute(
                "INSERT INTO sandboxes(scope,id,generation,digest,definition,phase) VALUES(?,?,?,?,?,'defined')",
                params![
                    key.scope_id,
                    key.sandbox_id,
                    key.generation_id,
                    definition.manifest_digest,
                    encode(definition)?
                ],
            )?;
            for mount in &definition.mounts {
                volume(db, &key.scope_id, &mount.volume_id)?;
                db.execute(
                    "INSERT INTO mounts VALUES(?,?,?,?,?)",
                    params![
                        key.scope_id,
                        key.sandbox_id,
                        key.generation_id,
                        mount.volume_id,
                        mount.access == AccessMode::ReadWrite
                    ],
                )?;
            }
            Ok(())
        })
    }

    pub async fn start(&self, key: &SandboxKey) -> Result<VMIncarnation> {
        let _guard = self.store.effects.lock().await;
        if self.store.host_vm_observation()?.state != "running" {
            return Err(Error::Draining);
        }
        let token = Uuid::new_v4().into_bytes();
        let boot = BootIdentity {
            sandbox: key.clone(),
            instance_id: Uuid::new_v4().to_string(),
            boot_token_hash: Sha256::digest(token).into(),
        };
        let (definition, scope, image, volumes) = self.store.write(|db| {
            let sb = sandbox(db, key)?;
            if !matches!(sb.state, SandboxState::Defined | SandboxState::Stopped) {
                return Err(Error::Conflict("sandbox is not stopped"));
            }
            let scope = boot_scope(db, &key.scope_id)?;
            crate::authority::admit_existing(db, &scope)?;
            validate_sandbox_pids(&sb.definition, &scope)?;
            let conflict: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM mounts wanted JOIN mounts owner
                ON (owner.scope,owner.volume)=(wanted.scope,wanted.volume)
                JOIN sandboxes live ON (live.scope,live.id,live.generation)=(owner.scope,owner.sandbox,owner.generation)
                WHERE wanted.scope=? AND wanted.sandbox=? AND wanted.generation=?
                AND (wanted.writable OR owner.writable) AND live.phase IN ('starting','running','quiesced','unverified','stopping'))", key.parts(), |r| r.get(0))?;
            if conflict {
                return Err(Error::Conflict("volume already mounted"));
            }
            let image = artifact(db, sb.definition.manifest_digest)?;
            let volumes = sb
                .definition
                .mounts
                .iter()
                .map(|m| volume(db, &key.scope_id, &m.volume_id))
                .collect::<Result<Vec<_>>>()?;
            db.execute(
                "UPDATE sandboxes SET phase='starting',boot=?4,vm=NULL,exit_reason=NULL WHERE scope=?1 AND id=?2 AND generation=?3",
                params![
                    key.scope_id,
                    key.sandbox_id,
                    key.generation_id,
                    encode(&boot)?
                ],
            )?;
            Ok((sb.definition, scope, image, volumes))
        })?;
        let result = async {
            if let Some(overlay) = &definition.root_overlay {
                self.backend.allocate_overlay(key, overlay).await?;
            }
            self.backend
                .start_vm(Launch {
                    definition: &definition,
                    scope: &scope,
                    image: &image,
                    volumes: &volumes,
                    boot: &boot,
                    boot_token: &token,
                })
                .await
        }
        .await;
        match result {
            Ok(vm) if vm.boot == boot => {
                self.record_live(&vm, false)?;
                self.project_mesh(false).await?;
                Ok(vm)
            }
            Ok(_) => Err(Error::Fenced),
            Err(error) => {
                // An interrupted reply can hide a successful boot. Leave the intent
                // for inventory reconciliation. Definitive rejection released no VM.
                if matches!(error, Error::Capacity(_)) {
                    self.record_stopped(key, &error.to_string())?;
                }
                Err(error)
            }
        }
    }

    fn record_live(&self, vm: &VMIncarnation, quiesced: bool) -> Result<()> {
        let key = &vm.boot.sandbox;
        self.store.write(|db| {
            db.execute(
                "UPDATE sandboxes SET phase=?4,vm=?5 WHERE scope=?1 AND id=?2 AND generation=?3",
                params![
                    key.scope_id,
                    key.sandbox_id,
                    key.generation_id,
                    if quiesced { "quiesced" } else { "running" },
                    encode(vm)?
                ],
            )?;
            Ok(())
        })
    }
    fn record_stopped(&self, key: &SandboxKey, reason: &str) -> Result<()> {
        self.store.write(|db| {
            db.execute(
                "UPDATE sandboxes SET phase='stopped',vm=NULL,boot=NULL,exit_reason=?4 WHERE scope=?1 AND id=?2 AND generation=?3",
                params![key.scope_id, key.sandbox_id, key.generation_id, reason],
            )?;
            Ok(())
        })
    }
    pub async fn quiesce(&self, boot: &BootIdentity) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        let sb = self.store.sandbox(&boot.sandbox)?;
        if sb.boot.as_ref() != Some(boot) {
            return Err(Error::Fenced);
        }
        if sb.state == SandboxState::Quiesced {
            return Ok(());
        }
        if sb.state != SandboxState::Running {
            return Err(Error::Conflict("sandbox is not running"));
        }
        self.backend.quiesce_vm(boot).await?;
        self.record_live(&sb.vm.ok_or(Error::Missing("VM incarnation"))?, true)
    }
    pub async fn stop(&self, boot: &BootIdentity, force: bool) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.stop_locked(boot, force).await
    }
    pub(crate) async fn stop_locked(&self, boot: &BootIdentity, force: bool) -> Result<()> {
        let key = &boot.sandbox;
        self.store.write(|db| {
            let sb = sandbox(db, key)?;
            if sb.boot.as_ref() != Some(boot) {
                return Err(Error::Fenced);
            }
            db.execute(
                "UPDATE sandboxes SET phase='stopping' WHERE scope=? AND id=? AND generation=?",
                key.parts(),
            )?;
            Ok(())
        })?;
        if force {
            self.backend.destroy_vm(boot).await?;
        } else {
            self.backend.shutdown_vm(boot).await?;
        }
        self.record_stopped(key, if force { "destroyed" } else { "shutdown" })?;
        self.project_mesh(false).await
    }
    pub async fn delete_sandbox(&self, key: &SandboxKey) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.delete_locked(key).await
    }
    pub(crate) async fn delete_locked(&self, key: &SandboxKey) -> Result<()> {
        let definition = self.store.write(|db| {
            let sb = sandbox(db, key)?;
            if sb.state == SandboxState::Deleted {
                return Ok(None);
            }
            if !matches!(
                sb.state,
                SandboxState::Defined | SandboxState::Stopped | SandboxState::Deleting
            ) {
                return Err(Error::Conflict("stop sandbox before deleting"));
            }
            db.execute(
                "UPDATE sandboxes SET phase='deleting' WHERE scope=? AND id=? AND generation=?",
                key.parts(),
            )?;
            Ok(Some(sb.definition))
        })?;
        if let Some(def) = definition {
            if let Some(overlay) = &def.root_overlay {
                self.backend.discard_overlay(key, overlay).await?;
            }
            self.store.write(|db| {
                db.execute("DELETE FROM mounts WHERE scope=? AND sandbox=? AND generation=?", key.parts())?;
                db.execute("UPDATE sandboxes SET phase='deleted',digest=NULL WHERE scope=? AND id=? AND generation=?", key.parts())?;
                Ok(())
            })?;
            self.collect_image(def.manifest_digest).await?;
        }
        Ok(())
    }

    pub async fn reconcile_after_restart(&self) -> Result<Reconciliation> {
        let _guard = self.store.effects.lock().await;
        self.reconcile_locked().await
    }

    pub(crate) async fn reconcile_locked(&self) -> Result<Reconciliation> {
        let mut live = self.backend.list_live_vms().await?;
        let mut report = Reconciliation::default();
        let rows = self.store.read(|db| {
            let mut stmt = db.prepare("SELECT scope,id,generation FROM sandboxes WHERE phase IN ('starting','running','quiesced','unverified','stopping','deleting')")?;
            Ok(stmt
                .query_map([], |r| {
                    Ok(SandboxKey {
                        scope_id: r.get(0)?,
                        sandbox_id: r.get(1)?,
                        generation_id: r.get(2)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        for key in rows {
            let sb = self.store.sandbox(&key)?;
            if sb.state == SandboxState::Deleting {
                self.delete_locked(&key).await?;
                continue;
            }
            let boot = sb.boot.ok_or(Error::Missing("persisted boot identity"))?;
            let candidates = live
                .iter()
                .enumerate()
                .filter(|(_, item)| {
                    let live_key = &item.incarnation.boot.sandbox;
                    (live_key.scope_id == key.scope_id && live_key.sandbox_id == key.sandbox_id)
                        || sb
                            .vm
                            .as_ref()
                            .is_some_and(|known| known.l4h_vm_id == item.incarnation.l4h_vm_id)
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let matched = match candidates.as_slice() {
                [] => None,
                [index]
                    if live[*index].incarnation.boot == boot
                        && sb.vm.as_ref().is_none_or(|known| {
                            known.l4h_vm_id == live[*index].incarnation.l4h_vm_id
                        }) =>
                {
                    Some(*index)
                }
                _ => {
                    self.store.write(|db| {
                        db.execute("UPDATE sandboxes SET phase='unverified' WHERE scope=? AND id=? AND generation=? AND phase!='stopping'", key.parts())?;
                        Ok(())
                    })?;
                    report.mismatched.push(key);
                    continue;
                }
            };
            if let Some(index) = matched {
                let LiveVm {
                    incarnation,
                    quiesced,
                } = live.swap_remove(index);
                if sb.state != SandboxState::Stopping {
                    let scope = self.store.read(|db| boot_scope(db, &key.scope_id))?;
                    self.backend.set_network_policy(&boot, &scope).await?;
                    self.record_live(&incarnation, quiesced)?;
                    report.adopted.push(incarnation);
                    continue;
                }
                self.backend.shutdown_vm(&boot).await?;
            }
            self.record_stopped(
                &key,
                if sb.state == SandboxState::Stopping {
                    "shutdown"
                } else {
                    "VM absent during reconciliation"
                },
            )?;
            report.stopped.push(key);
        }
        report.unowned = live.into_iter().map(|v| v.incarnation).collect();
        let unused = self.store.read(|db| {
            let mut stmt = db.prepare("SELECT digest FROM images i WHERE NOT EXISTS(SELECT 1 FROM image_refs WHERE digest=i.digest)
                AND NOT EXISTS(SELECT 1 FROM retained_images WHERE digest=i.digest) AND NOT EXISTS(SELECT 1 FROM sandboxes WHERE digest=i.digest)")?;
            Ok(stmt
                .query_map([], |r| r.get::<_, Digest>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        for digest in unused {
            self.collect_image(digest).await?;
        }
        self.reconcile_volumes().await?;
        self.project_mesh(true).await?;
        Ok(report)
    }
}
