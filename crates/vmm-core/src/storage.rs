use crate::{
    authority::load_scope,
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) fn active(db: &Connection, id: &str) -> Result<Scope> {
    let scope = load_scope(db, id)?;
    if scope.state != ScopeState::Active {
        return Err(Error::Draining);
    }
    Ok(scope)
}

fn scope_used_bytes(db: &Connection, scope_id: &str) -> Result<u64> {
    // A single SQLite snapshot covers both device types. The JSON arrow returns text,
    // preserving unsigned counts above i64::MAX. Legacy records have no usage field.
    let mut query = db.prepare_cached(
        "SELECT data -> '$.used_bytes' FROM volumes WHERE scope=?1
         UNION ALL
         SELECT definition -> '$.root_overlay.used_bytes' FROM sandboxes
         WHERE scope=?1 AND phase!='deleted'",
    )?;
    let mut rows = query.query([scope_id])?;
    let mut total = 0u64;
    while let Some(row) = rows.next()? {
        if let Some(bytes) = row.get::<_, Option<String>>(0)? {
            total = total
                .checked_add(decode::<u64>(&bytes)?)
                .ok_or(Error::Capacity(CapacityDimension::StorageHeadroom))?;
        }
    }
    Ok(total)
}

pub(crate) fn check_quota(
    db: &Connection,
    scope: &Scope,
    device_quota_bytes: u64,
    initial_used_bytes: u64,
) -> Result<()> {
    let limit = scope.limits.disk_quota_bytes;
    if device_quota_bytes > limit
        || scope_used_bytes(db, &scope.id)?
            .checked_add(initial_used_bytes)
            .is_none_or(|total| total > limit)
    {
        return Err(Error::Capacity(CapacityDimension::StorageHeadroom));
    }
    Ok(())
}

pub(crate) fn artifact(db: &Connection, digest: Digest) -> Result<ImageArtifact> {
    let data: String = db
        .query_row(
            "SELECT artifact FROM images WHERE digest=? AND phase='ready'",
            [digest],
            |r| r.get(0),
        )
        .optional()?
        .ok_or(Error::Missing("ready artifact"))?;
    decode(&data)
}

pub(crate) fn volume(db: &Connection, scope: &str, id: &str) -> Result<Volume> {
    let data: String = db
        .query_row(
            "SELECT data FROM volumes WHERE scope=? AND id=? AND phase='ready'",
            [scope, id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or(Error::Missing("ready volume"))?;
    decode(&data)
}

impl Core {
    pub async fn import_image(
        &self,
        scope: &str,
        reference: &str,
        source: &ImageSource,
    ) -> Result<ImageArtifact> {
        self.publish_image(scope, reference, source, |_| Ok(()))
            .await
    }

    pub(crate) async fn publish_image(
        &self,
        scope: &str,
        reference: &str,
        source: &ImageSource,
        published: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<()> + Send,
    ) -> Result<ImageArtifact> {
        let _guard = self.store.effects.lock().await;
        let digest = source.manifest_digest;
        if reference.is_empty() || source.physical_bytes == 0 {
            return Err(Error::Invalid("image source"));
        }
        let ready = self.store.write(|db| {
            active(db, scope)?;
            let previous: Option<Digest> = db
                .query_row(
                    "SELECT digest FROM image_refs WHERE scope=? AND reference=?",
                    [scope, reference],
                    |r| r.get(0),
                )
                .optional()?;
            if previous.is_some_and(|old| old != digest) {
                return Err(Error::Conflict("image reference already bound"));
            }
            db.execute(
                "INSERT INTO images(digest,phase) VALUES(?,'installing') ON CONFLICT DO NOTHING",
                [digest],
            )?;
            let phase: String =
                db.query_row("SELECT phase FROM images WHERE digest=?", [digest], |r| {
                    r.get(0)
                })?;
            match phase.as_str() {
                "ready" => Ok(Some(artifact(db, digest)?)),
                "installing" => Ok(None),
                _ => Err(Error::Conflict("image discard in progress")),
            }
        })?;
        let image = match ready {
            Some(image) => image,
            None => self.backend.install_image_artifact(source).await?,
        };
        if image.manifest_digest != digest {
            return Err(Error::Fenced);
        }
        let result = self.store.write(|db| {
            active(db, scope)?;
            db.execute(
                "UPDATE images SET artifact=?,phase='ready' WHERE digest=?",
                params![encode(&image)?, digest],
            )?;
            db.execute(
                "INSERT INTO image_refs VALUES(?,?,?) ON CONFLICT DO NOTHING",
                params![scope, reference, digest],
            )?;
            published(db)?;
            Ok(image)
        });
        if result.is_err() {
            self.collect_image(digest).await?;
        }
        result
    }

    pub async fn remove_image_reference(
        &self,
        scope: &str,
        reference: &str,
        expected: Digest,
    ) -> Result<u64> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            let changed = db.execute(
                "DELETE FROM image_refs WHERE scope=? AND reference=? AND digest=?",
                params![scope, reference, expected],
            )?;
            if changed == 0 {
                return Err(Error::Fenced);
            }
            Ok(())
        })?;
        self.collect_image(expected).await
    }

    // Called with effects locked. A discard tombstone survives cancellation.
    pub(crate) async fn collect_image(&self, digest: Digest) -> Result<u64> {
        let disposable = self
            .store
            .write(|db| Ok(db.execute("UPDATE images SET phase='discarding' WHERE digest=?1 AND NOT EXISTS
            (SELECT 1 FROM image_refs WHERE digest=?1) AND NOT EXISTS(SELECT 1 FROM retained_images WHERE digest=?1) AND NOT EXISTS(SELECT 1 FROM sandboxes WHERE digest=?1)", [digest])? == 1))?;
        if !disposable {
            return Ok(0);
        }
        let reclaimed = self.backend.discard_image_artifact(digest).await?;
        self.store.write(|db| {
            db.execute(
                "DELETE FROM images WHERE digest=? AND phase='discarding'",
                [digest],
            )?;
            Ok(reclaimed)
        })
    }

    pub async fn create_volume(&self, spec: &Volume) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        if spec.volume_id.is_empty() || spec.quota_bytes == 0 || spec.mode > 0o7777 {
            return Err(Error::Invalid("volume"));
        }
        let ready = self.store.write(|db| {
            let scope = active(db, &spec.scope_id)?;
            let row: Option<(String, String)> = db
                .query_row(
                    "SELECT data,phase FROM volumes WHERE scope=? AND id=?",
                    params![spec.scope_id, spec.volume_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((data, phase)) = row {
                if !decode::<Volume>(&data)?.same_allocation(spec) || phase == "discarding" {
                    return Err(Error::Conflict("volume identity"));
                }
                return Ok(phase == "ready");
            }
            check_quota(db, &scope, spec.quota_bytes, spec.used_bytes)?;
            db.execute(
                "INSERT INTO volumes VALUES(?,?,?,'allocating')",
                params![spec.scope_id, spec.volume_id, encode(spec)?],
            )?;
            Ok(false)
        })?;
        if !ready {
            self.allocate_reserved_volume(spec).await?;
        }
        Ok(())
    }
    async fn allocate_reserved_volume(&self, spec: &Volume) -> Result<()> {
        let key = [&spec.scope_id, &spec.volume_id];
        if let Err(error) = self.backend.allocate_volume(spec).await {
            if matches!(error, Error::Capacity(_)) {
                self.store.write(|db| {
                    db.execute(
                        "DELETE FROM volumes WHERE scope=? AND id=? AND phase='allocating'",
                        key,
                    )?;
                    Ok(())
                })?;
            }
            return Err(error);
        }
        self.store.write(|db| {
            db.execute(
                "UPDATE volumes SET phase='ready' WHERE scope=? AND id=?",
                key,
            )?;
            Ok(())
        })
    }
    pub(crate) async fn reconcile_volumes(&self) -> Result<()> {
        let pending = self.store.read(|db| {
            let mut stmt = db.prepare("SELECT data,phase FROM volumes WHERE phase!='ready'")?;
            Ok(stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        for (data, phase) in pending {
            let vol: Volume = decode(&data)?;
            match phase.as_str() {
                "allocating" => self.allocate_reserved_volume(&vol).await?,
                "discarding" => {
                    self.discard_reserved_volume(&vol.scope_id, &vol.volume_id)
                        .await?
                }
                _ => return Err(Error::Invalid("stored volume phase")),
            }
        }
        Ok(())
    }

    pub async fn discard_volume(&self, scope: &str, id: &str) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            let used: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM mounts WHERE scope=? AND volume=?)",
                [scope, id],
                |r| r.get(0),
            )?;
            if used {
                return Err(Error::Conflict("volume in sandbox definition"));
            }
            if db.execute(
                "UPDATE volumes SET phase='discarding' WHERE scope=? AND id=?",
                [scope, id],
            )? == 0
            {
                return Err(Error::Missing("volume"));
            }
            Ok(())
        })?;
        self.discard_reserved_volume(scope, id).await
    }
    async fn discard_reserved_volume(&self, scope: &str, id: &str) -> Result<()> {
        self.backend.discard_volume(scope, id).await?;
        self.store.write(|db| {
            db.execute("DELETE FROM volumes WHERE scope=? AND id=?", [scope, id])?;
            Ok(())
        })
    }
}

impl Store {
    pub fn scope_used_bytes(&self, scope_id: &str) -> Result<u64> {
        self.read(|db| scope_used_bytes(db, scope_id))
    }

    /// Record an absolute measurement even when the environment has exceeded its limit.
    pub fn record_volume_usage(
        &self,
        scope_id: &str,
        volume_id: &str,
        used_bytes: u64,
    ) -> Result<()> {
        self.write(|db| {
            let changed = db.execute(
                "UPDATE volumes SET data=json_set(data,'$.used_bytes',json(?3))
                 WHERE scope=?1 AND id=?2 AND phase='ready'",
                params![scope_id, volume_id, used_bytes.to_string()],
            )?;
            if changed == 0 {
                return Err(Error::Missing("ready volume"));
            }
            Ok(())
        })
    }

    pub fn record_overlay_usage(&self, key: &SandboxKey, used_bytes: u64) -> Result<()> {
        self.write(|db| {
            let changed = db.execute(
                "UPDATE sandboxes SET definition=json_set(definition,'$.root_overlay.used_bytes',json(?4))
                 WHERE scope=?1 AND id=?2 AND generation=?3 AND phase!='deleted'
                 AND json_type(definition,'$.root_overlay')='object'",
                params![key.scope_id, key.sandbox_id, key.generation_id, used_bytes.to_string()],
            )?;
            if changed == 0 { return Err(Error::Missing("root overlay")); }
            Ok(())
        })
    }

    pub fn image_artifact(&self, digest: Digest) -> Result<ImageArtifact> {
        self.read(|db| artifact(db, digest))
    }
    pub fn resolve_image(&self, scope: &str, reference: &str) -> Result<ImageArtifact> {
        self.read(|db| {
            let data: String = db
                .query_row(
                    "SELECT i.artifact FROM image_refs r JOIN images i ON i.digest=r.digest
                 WHERE r.scope=? AND r.reference=? AND i.phase='ready'",
                    [scope, reference],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or(Error::Missing("image reference"))?;
            decode(&data)
        })
    }

    pub fn volume(&self, scope: &str, id: &str) -> Result<Volume> {
        self.read(|db| volume(db, scope, id))
    }

    pub fn volume_mounts(&self, scope: &str, id: &str) -> Result<Vec<(SandboxKey, AccessMode)>> {
        self.read(|db| {
            let mut stmt = db.prepare("SELECT m.sandbox,m.generation,m.writable FROM mounts m JOIN sandboxes s
                ON (s.scope,s.id,s.generation)=(m.scope,m.sandbox,m.generation)
                WHERE m.scope=? AND m.volume=? AND s.phase IN ('starting','running','quiesced','unverified','stopping') ORDER BY m.sandbox")?;
            Ok(stmt
                .query_map([scope, id], |r| {
                    Ok((
                        SandboxKey {
                            scope_id: scope.into(),
                            sandbox_id: r.get(0)?,
                            generation_id: r.get(1)?,
                        },
                        if r.get(2)? {
                            AccessMode::ReadWrite
                        } else {
                            AccessMode::ReadOnly
                        },
                    ))
                })?
                .collect::<rusqlite::Result<_>>()?)
        })
    }
}
