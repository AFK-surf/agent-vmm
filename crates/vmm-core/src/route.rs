use crate::{
    lifecycle::sandbox,
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, OptionalExtension, params};

fn running(db: &Connection, boot: &BootIdentity) -> Result<bool> {
    let sb = match sandbox(db, &boot.sandbox) {
        Ok(sb) => sb,
        Err(Error::Missing(_)) => return Ok(false),
        Err(err) => return Err(err),
    };
    Ok(sb.state == SandboxState::Running && sb.boot.as_ref() == Some(boot))
}
fn current(db: &Connection, binding: &RouteBinding) -> Result<bool> {
    if let Some(expires) = binding.expires_at_unix_seconds {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Invalid("system time"))?;
        if now.as_secs() >= expires {
            return Ok(false);
        }
    }
    let published: Option<String> = db
        .query_row(
            "SELECT data FROM service_exports WHERE id=?",
            [&binding.export.id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(published
        .map(|data| decode::<ServiceExport>(&data))
        .transpose()?
        .as_ref()
        == Some(&binding.export)
        && running(db, &binding.source)?
        && running(db, &binding.export.boot)?)
}

impl Core {
    pub async fn publish_export(&self, export: &ServiceExport) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        if export.port == 0 || export.id.is_empty() {
            return Err(Error::Invalid("service export"));
        }
        self.store.write(|db| {
            Store::check_cleanup_owner(db, &export.boot.sandbox.scope_id)?;
            if !running(db, &export.boot)? {
                return Err(Error::Fenced);
            }
            let data = encode(export)?;
            let changed = db.execute("INSERT INTO service_exports VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data WHERE data=excluded.data", params![export.id, data])?;
            if changed != 1 {
                return Err(Error::Conflict("export identity"));
            }
            Ok(())
        })
    }

    pub async fn withdraw_export(&self, export: &ServiceExport) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            if db.execute(
                "DELETE FROM service_exports WHERE id=? AND data=?",
                params![export.id, encode(export)?],
            )? == 0
            {
                return Err(Error::Fenced);
            }
            Ok(())
        })?;
        self.reconcile_routes(false).await
    }

    pub async fn bind_route(&self, binding: &RouteBinding) -> Result<ServiceRoute> {
        let _guard = self.store.effects.lock().await;
        self.bind_locked(binding).await
    }
    pub(crate) async fn bind_locked(&self, binding: &RouteBinding) -> Result<ServiceRoute> {
        if binding.id.is_empty() {
            return Err(Error::Invalid("route id"));
        }
        let (route, applied) = self.store.write(|db| {
            Store::check_cleanup_owner(db, &binding.source.sandbox.scope_id)?;
            Store::check_cleanup_owner(db, &binding.export.boot.sandbox.scope_id)?;
            if !current(db, binding)? {
                return Err(Error::Fenced);
            }
            let row: Option<(i64, String, String)> = db
                .query_row(
                    "SELECT revision,binding,phase FROM routes WHERE id=? AND phase!='revoked'",
                    [&binding.id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let (revision, applied) = if let Some((rev, prior, phase)) = row {
                if phase == "revoking" || decode::<RouteBinding>(&prior)? != *binding {
                    return Err(Error::Conflict("route already bound"));
                }
                (rev, phase == "active")
            } else {
                db.execute(
                    "INSERT INTO routes(id,binding,phase) VALUES(?,?,'binding')",
                    params![binding.id, encode(binding)?],
                )?;
                (db.last_insert_rowid(), false)
            };
            Ok((
                ServiceRoute {
                    binding: binding.clone(),
                    revision,
                },
                applied,
            ))
        })?;
        if !applied {
            self.apply_route(&route).await?;
        }
        Ok(route)
    }
    async fn apply_route(&self, route: &ServiceRoute) -> Result<()> {
        if let Err(error) = self.backend.bind_route(route).await {
            if matches!(error, Error::Invalid(_) | Error::Conflict(_)) {
                // A backend admission refusal is final for this revision. A lost
                // transport response leaves the revision pending for exact replay.
                self.store.write(|db| {
                    db.execute(
                        "UPDATE routes SET phase='revoking' WHERE revision=?",
                        [route.revision],
                    )?;
                    db.execute(
                        "UPDATE mesh_imports SET revoked_by='',revision=revision+1
                         WHERE id=? AND revoked_by IS NULL",
                        [&route.binding.id],
                    )?;
                    Ok(())
                })?;
                self.revoke_locked(&route.binding.id, route.revision)
                    .await?;
            }
            return Err(error);
        }
        self.store.write(|db| {
            db.execute(
                "UPDATE routes SET phase='active' WHERE revision=?",
                [route.revision],
            )?;
            Ok(())
        })
    }
    pub async fn revoke_route(&self, id: &str, expected_revision: i64) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.revoke_locked(id, expected_revision).await
    }
    async fn revoke_locked(&self, id: &str, expected_revision: i64) -> Result<()> {
        let done = self.store.write(|db| {
            let (revision, phase): (i64, String) = db
                .query_row(
                    "SELECT revision,phase FROM routes WHERE id=? ORDER BY revision DESC LIMIT 1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?
                .ok_or(Error::Missing("route"))?;
            if revision != expected_revision {
                return Err(Error::Fenced);
            }
            if phase == "revoked" {
                return Ok(true);
            }
            db.execute(
                "UPDATE routes SET phase='revoking' WHERE revision=?",
                [revision],
            )?;
            Ok(false)
        })?;
        if !done {
            self.backend.revoke_route(id, expected_revision).await?;
            self.store.write(|db| {
                db.execute(
                    "UPDATE routes SET phase='revoked' WHERE revision=?",
                    [expected_revision],
                )?;
                Ok(())
            })?;
        }
        Ok(())
    }
    pub(crate) async fn reconcile_routes(&self, restore: bool) -> Result<()> {
        let rows = self.store.read(|db| {
            let mut stmt = db.prepare("SELECT revision,binding,phase FROM routes WHERE phase!='revoked' ORDER BY revision")?;
            Ok(stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?)
        })?;
        for (revision, data, phase) in rows {
            let binding: RouteBinding = decode(&data)?;
            if phase == "revoking" || !self.store.read(|db| current(db, &binding))? {
                self.revoke_locked(&binding.id, revision).await?;
            } else if (restore || phase == "binding")
                && let Err(error) = self.apply_route(&ServiceRoute { binding, revision }).await
            {
                let retired = self.store.read(|db| {
                    Ok(db.query_row(
                        "SELECT phase='revoked' FROM routes WHERE revision=?",
                        [revision],
                        |row| row.get::<_, bool>(0),
                    )?)
                })?;
                if !retired {
                    return Err(error);
                }
            }
        }
        Ok(())
    }
}

impl Store {
    /// Resolve only a published port on the same running boot.
    pub fn current_service_export(&self, id: &str) -> Result<ServiceExport> {
        self.read(|db| {
            let data: String = db
                .query_row("SELECT data FROM service_exports WHERE id=?", [id], |row| {
                    row.get(0)
                })
                .optional()?
                .ok_or(Error::Missing("service export"))?;
            let export: ServiceExport = decode(&data)?;
            Self::check_cleanup_owner(db, &export.boot.sandbox.scope_id)?;
            crate::storage::active(db, &export.boot.sandbox.scope_id)?;
            if !running(db, &export.boot)? {
                return Err(Error::Fenced);
            }
            Ok(export)
        })
    }
}
