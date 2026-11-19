use crate::{
    authority::{authorize, ensure_scope, issue_session, load_scope, registration},
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest as _, Sha256};

pub struct RemoteAllocation {
    pub allocation_id: String,
    pub display_label: String,
    pub scope: Scope,
}

pub struct RemoteInventory {
    pub watermark: u64,
    pub allocations: Vec<RemoteAllocation>,
}

pub struct RemoteScopePolicy {
    pub revision: u64,
    pub max_pids: u64,
    pub max_disk_quota_bytes: u64,
    pub max_egress_mode: EgressMode,
}

fn origin(channel: &Channel) -> Origin {
    Origin::Remote {
        registration_id: channel.registration_id.clone(),
    }
}

fn scope_id(registration: &str, allocation: &str) -> String {
    let mut hash = Sha256::new();
    hash.update((registration.len() as u64).to_be_bytes());
    hash.update(registration);
    hash.update(allocation);
    format!("remote:{:x}", hash.finalize())
}

fn allocation(db: &Connection, channel: &Channel, id: &str) -> Result<RemoteAllocation> {
    authorize(db, &origin(channel), Some(channel))?;
    let value = db.query_row(
        "SELECT display_label,data FROM scopes WHERE registration=? AND COALESCE(remote_id,id)=?",
        params![channel.registration_id, id],
        |row| Ok((row.get(0)?, row.get::<_, String>(1)?)),
    ).optional()?.ok_or(Error::Missing("allocation"))?;
    Ok(RemoteAllocation {
        allocation_id: id.into(),
        display_label: value.0,
        scope: decode(&value.1)?,
    })
}

fn check_target(scope: &Scope, generation: u64, revision: u64) -> Result<()> {
    if scope.generation != generation || scope.revision != revision {
        return Err(Error::Fenced);
    }
    Ok(())
}

impl Store {
    pub fn remote_inventory(&self, channel: &Channel, limit: usize) -> Result<RemoteInventory> {
        self.read(|db| {
            let tx = db.unchecked_transaction()?;
            authorize(&tx, &origin(channel), Some(channel))?;
            let watermark: i64 = tx.query_row(
                "SELECT inventory_watermark FROM registrations WHERE id=?",
                [&channel.registration_id], |row| row.get(0),
            )?;
            let watermark = u64::try_from(watermark).map_err(|_| Error::Invalid("inventory watermark"))?;
            let mut query = tx.prepare_cached(
                "SELECT COALESCE(remote_id,id),display_label,data FROM scopes WHERE registration=?
                 AND json_extract(data,'$.state')!='Released' ORDER BY COALESCE(remote_id,id) LIMIT ?",
            )?;
            let rows = query.query_map(
                params![channel.registration_id, i64::try_from(limit).unwrap_or(i64::MAX)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, String>(2)?)),
            )?;
            let mut allocations = Vec::new();
            for row in rows {
                let (allocation_id, display_label, data) = row?;
                allocations.push(RemoteAllocation { allocation_id, display_label, scope: decode(&data)? });
            }
            Ok(RemoteInventory { watermark, allocations })
        })
    }

    pub fn remote_allocation(&self, channel: &Channel, id: &str) -> Result<RemoteAllocation> {
        self.read(|db| allocation(db, channel, id))
    }

    /// Read a remote allocation for the local operator by its internal scope ID.
    pub fn operator_remote_allocation(&self, scope_id: &str) -> Result<RemoteAllocation> {
        self.read(|db| {
            let (allocation_id, display_label, data) = db
                .query_row(
                    "SELECT COALESCE(remote_id,id),display_label,data FROM scopes WHERE id=? AND registration IS NOT NULL",
                    [scope_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get::<_, String>(2)?)),
                )
                .optional()?
                .ok_or(Error::Missing("allocation"))?;
            Ok(RemoteAllocation { allocation_id, display_label, scope: decode(&data)? })
        })
    }

    pub fn ensure_remote_scope(
        &self,
        channel: &Channel,
        id: &str,
        display_label: &str,
        mut desired: Scope,
        policy: &RemoteScopePolicy,
    ) -> Result<RemoteAllocation> {
        if desired.origin != origin(channel) || id.is_empty() || desired.generation == 0 {
            return Err(Error::Invalid("remote scope"));
        }
        self.write(|db| {
            if registration(db, &channel.registration_id)?.draining {
                return Err(Error::Draining);
            }
            let exists = match allocation(db, channel, id) {
                Ok(known) => {
                    if known.scope.generation == desired.generation
                        && known.scope.limits.pids_max == desired.limits.pids_max
                        && known.scope.limits.disk_quota_bytes == desired.limits.disk_quota_bytes
                        && known.scope.egress_mode == desired.egress_mode
                        && known.scope.state != ScopeState::Released
                    {
                        return Ok(known);
                    }
                    true
                }
                Err(Error::Missing(_)) => false,
                Err(error) => return Err(error),
            };
            if desired.revision != policy.revision {
                return Err(Error::Fenced);
            }
            if exists {
                return Err(Error::Conflict("allocation policy"));
            }
            if desired.egress_mode == EgressMode::PublicInternet
                && policy.max_egress_mode == EgressMode::DenyAll
            {
                return Err(Error::Conflict("pool egress policy"));
            }
            if desired.limits.disk_quota_bytes > policy.max_disk_quota_bytes {
                return Err(Error::Capacity(CapacityDimension::StorageHeadroom));
            }
            if desired.limits.pids_max > policy.max_pids {
                return Err(Error::Capacity(CapacityDimension::Pids));
            }
            desired.id = scope_id(&channel.registration_id, id);
            let scope = ensure_scope(db, &desired, Some(channel))?;
            let label = display_label;
            db.execute(
                "UPDATE scopes SET remote_id=?,display_label=? WHERE id=?",
                params![id, label, scope.id],
            )?;
            Ok(RemoteAllocation {
                allocation_id: id.into(),
                display_label: label.into(),
                scope,
            })
        })
    }

    pub fn issue_remote_session(
        &self,
        channel: &Channel,
        allocation_id: &str,
        generation: u64,
    ) -> Result<(Session, [u8; 16])> {
        self.write(|db| {
            let current = allocation(db, channel, allocation_id)?;
            if current.scope.generation != generation || current.scope.state != ScopeState::Active {
                return Err(Error::Fenced);
            }
            issue_session(db, &current.scope.id, Some(channel))
        })
    }

    /// Cancels one issued session. Session IDs are never reused.
    pub fn revoke_session(&self, id: &str) -> Result<()> {
        self.write(|db| {
            db.execute(
                "UPDATE sessions SET revoked=1 WHERE id=? AND revoked=0",
                [id],
            )?;
            Ok(())
        })
    }

    fn prepare_remote_retirement(
        &self,
        channel: &Channel,
        id: &str,
        generation: u64,
        revision: u64,
        disposal_request: Option<&str>,
    ) -> Result<RemoteAllocation> {
        self.write(|db| {
            let mut current = allocation(db, channel, id)?;
            let scope = &mut current.scope;
            check_target(scope, generation, revision)?;
            let accepted: Option<String> = db.query_row(
                "SELECT disposal_request FROM scopes WHERE id=?",
                [&scope.id],
                |row| row.get(0),
            )?;
            if accepted.is_some() && disposal_request != accepted.as_deref() {
                return Err(Error::Conflict("scope disposal already accepted"));
            }
            if scope.state == ScopeState::Active
                || (scope.state == ScopeState::Retained && disposal_request.is_some())
            {
                scope.state = ScopeState::Draining;
            }
            db.execute(
                "UPDATE scopes SET data=?,disposal_request=? WHERE id=?",
                params![encode(scope)?, disposal_request, scope.id],
            )?;
            db.execute(
                "UPDATE sessions SET revoked=1 WHERE scope=? AND revoked=0",
                [&scope.id],
            )?;
            Ok(current)
        })
    }

    fn finish_remote_retirement(
        &self,
        current: &mut RemoteAllocation,
        disposal_request: Option<&str>,
    ) -> Result<()> {
        self.write(|db| {
            let previous = &current.scope;
            let mut scope = load_scope(db, &previous.id)?;
            if scope != *previous {
                return Err(Error::Fenced);
            }
            let accepted: Option<String> = db.query_row(
                "SELECT disposal_request FROM scopes WHERE id=?", [&scope.id], |row| row.get(0),
            )?;
            if accepted.as_deref() != disposal_request {
                return Err(Error::Fenced);
            }
            let discard = disposal_request.is_some();
            let live: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM sandboxes WHERE scope=? AND phase NOT IN ('stopped','defined','deleted'))",
                [&scope.id],
                |row| row.get(0),
            )?;
            if live {
                return Err(Error::Interrupted);
            }
            if discard {
                let used: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM volumes WHERE scope=?1 UNION ALL SELECT 1 FROM image_refs WHERE scope=?1 UNION ALL SELECT 1 FROM sandboxes WHERE scope=?1 AND phase!='deleted')",
                    [&scope.id], |row| row.get(0),
                )?;
                if used {
                    return Err(Error::Interrupted);
                }
            }
            scope.state = if discard { ScopeState::Released } else { ScopeState::Retained };
            db.execute("UPDATE scopes SET data=? WHERE id=?", params![encode(&scope)?, scope.id])?;
            current.scope = scope;
            Ok(())
        })
    }
}

impl Core {
    /// Accepts exact remote retirement authority before driving its target's durable resources.
    /// A resumed disposal must carry the same request identity.
    pub async fn retire_remote_scope(
        &self,
        channel: &Channel,
        allocation_id: &str,
        generation: u64,
        revision: u64,
        disposal_request: Option<&str>,
    ) -> Result<RemoteAllocation> {
        let mut current = self.store.prepare_remote_retirement(
            channel,
            allocation_id,
            generation,
            revision,
            disposal_request,
        )?;
        let discard = disposal_request.is_some();
        if matches!(
            current.scope.state,
            ScopeState::Retained | ScopeState::Released
        ) {
            return Ok(current);
        }
        let scope = &current.scope.id;
        let mut cursor = None;
        loop {
            let page = self.store.list_sandboxes(scope, cursor.as_deref(), 32)?;
            if page.is_empty() {
                break;
            }
            for sandbox in page {
                let key = &sandbox.definition.key;
                if !matches!(
                    sandbox.state,
                    SandboxState::Defined | SandboxState::Stopped | SandboxState::Deleting
                ) {
                    self.stop(&sandbox.boot.ok_or(Error::Interrupted)?, false)
                        .await?;
                }
                if discard {
                    self.delete_sandbox(key).await?;
                }
                cursor = Some(key.sandbox_id.clone());
            }
        }
        if discard {
            // Include durable discard intents that may have outlived a previous attempt.
            loop {
                let volumes = self.store.remote_volume_ids(scope)?;
                if volumes.is_empty() {
                    break;
                }
                for id in volumes {
                    self.discard_volume(scope, &id).await?;
                }
            }
            loop {
                let page = self.store.list_images(scope, None, 32)?;
                if page.is_empty() {
                    break;
                }
                for (reference, image) in page {
                    self.remove_image_reference(scope, &reference, image.manifest_digest)
                        .await?;
                }
            }
        }
        self.store
            .finish_remote_retirement(&mut current, disposal_request)?;
        Ok(current)
    }
}

impl Store {
    fn remote_volume_ids(&self, scope: &str) -> Result<Vec<String>> {
        self.read(|db| {
            let mut query =
                db.prepare_cached("SELECT id FROM volumes WHERE scope=? ORDER BY id LIMIT 32")?;
            Ok(query
                .query_map([scope], |row| row.get(0))?
                .collect::<std::result::Result<_, _>>()?)
        })
    }
}
