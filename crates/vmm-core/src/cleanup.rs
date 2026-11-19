//! One transaction binds a reviewed operator cleanup to its scoped inventory.
use crate::{Core, Error, Origin, Result, ScopeState, Store, authority::load_scope};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupKind {
    Reset,
    Discard,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CleanupResource {
    pub kind: String,
    pub id: String,
    pub generation: String,
}

pub struct CleanupInventory {
    pub revision: u64,
    pub resources: Vec<CleanupResource>,
}

#[derive(Debug)]
pub struct CleanupOperation {
    pub id: String,
    pub request: String,
    pub scope: String,
    pub kind: CleanupKind,
    pub complete: bool,
    pub started_at: i64,
    pub updated_at: i64,
}

const DEFAULT_WORKSPACE: &str = "__default_workspace";
const MAX_OBJECTS: usize = 100;

fn inventory(
    db: &Connection,
    scope: &str,
    kind: CleanupKind,
    appliance_session: Option<&crate::Session>,
) -> Result<CleanupInventory> {
    let current = load_scope(db, scope)?;
    let discard = kind == CleanupKind::Discard;
    if current.state == ScopeState::Released {
        return Err(Error::Missing("environment"));
    }
    let private: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM appliances a JOIN scopes s ON s.registration=a.registration WHERE s.id=?)",
        [scope], |r|r.get(0))?;
    if let Some(session) = appliance_session {
        if session.scope_id != scope || !crate::retained_image::current(db, session)? {
            return Err(Error::Fenced);
        }
        let owned: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM appliances a JOIN scopes s ON s.registration=a.registration
              WHERE s.id=? AND a.id='agent-vmm-mcp')", [scope], |row| row.get(0),
        )?;
        if !owned {
            return Err(Error::Fenced);
        }
    } else if private || scope.starts_with("system.") {
        return Err(Error::Conflict("private system environment"));
    }
    if appliance_session.is_none() && current.origin != Origin::Local {
        if !discard {
            return Err(Error::Conflict("controller-owned allocation"));
        }
        if current.state != ScopeState::Retained {
            return Err(Error::Conflict("remote allocation active"));
        }
    }
    if current.state == ScopeState::Draining {
        return Err(Error::Draining);
    }
    let mut parts = vec![
        match kind {
            CleanupKind::Reset => "CLEANUP_KIND_RESET",
            CleanupKind::Discard => "CLEANUP_KIND_DISCARD",
        }
        .to_owned(),
        current.revision.to_string(),
        scope.to_owned(),
    ];
    let mut resources = Vec::new();
    let mut stmt = db.prepare_cached(
        "SELECT 'container',id,generation,'' FROM sandboxes WHERE scope=?1 AND phase!='deleted'
         UNION ALL SELECT 'volume',id,generation,'' FROM volume_generations WHERE scope=?1 AND id!=?2
         UNION ALL SELECT 'image',reference,generation,hex(digest) FROM image_refs JOIN image_ref_generations USING(scope,reference) WHERE scope=?1
         UNION ALL SELECT 'grant',id,CAST(revision AS TEXT),binding FROM routes WHERE phase!='revoked' AND id NOT IN (SELECT id FROM mesh_imports) AND
            (json_extract(binding,'$.source.sandbox.scope_id')=?1 OR json_extract(binding,'$.export.boot.sandbox.scope_id')=?1)
         UNION ALL SELECT 'grant',id,'',data FROM service_exports WHERE json_extract(data,'$.boot.sandbox.scope_id')=?1
            AND id NOT IN (SELECT id FROM mesh_exports)
         UNION ALL SELECT 'grant',id,CAST(revision AS TEXT),'mesh-export' FROM mesh_exports
            WHERE scope=?1 AND revoked_by IS NULL
         UNION ALL SELECT 'grant',i.id,CAST(i.revision AS TEXT),'mesh-import' FROM mesh_imports i
            JOIN mesh_exports e ON e.id=i.export WHERE (i.scope=?1 OR e.scope=?1) AND i.revoked_by IS NULL
         LIMIT 101")?;
    let mut rows = stmt.query(params![scope, DEFAULT_WORKSPACE])?;
    let mut count = usize::from(discard);
    while let Some(row) = rows.next()? {
        count += 1;
        if count > MAX_OBJECTS {
            return Err(Error::Conflict("cleanup inventory limit"));
        }
        let item = CleanupResource {
            kind: row.get(0)?,
            id: row.get(1)?,
            generation: row.get(2)?,
        };
        let detail: String = row.get(3)?;
        parts.push(format!(
            "{}:{}:{}:{}",
            item.kind, item.id, item.generation, detail
        ));
        if discard || item.kind != "image" {
            resources.push(item);
        }
    }
    resources.sort_unstable_by(|a, b| (&a.kind, &a.id).cmp(&(&b.kind, &b.id)));
    parts[3..].sort_unstable();
    let mut hash = 0xcbf29ce484222325u64;
    for part in parts {
        for byte in part.bytes().chain([0]) {
            hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    Ok(CleanupInventory {
        revision: (hash & i64::MAX as u64).max(1),
        resources,
    })
}

impl Store {
    pub fn cleanup_inventory(&self, scope: &str, kind: CleanupKind) -> Result<CleanupInventory> {
        self.read(|db| {
            let snapshot = db.unchecked_transaction()?;
            inventory(&snapshot, scope, kind, None)
        })
    }
    pub fn mcp_cleanup_inventory(
        &self,
        session: &crate::Session,
        kind: CleanupKind,
    ) -> Result<CleanupInventory> {
        self.read(|db| {
            let snapshot = db.unchecked_transaction()?;
            inventory(&snapshot, &session.scope_id, kind, Some(session))
        })
    }

    pub fn cleanup_operation(&self, id: &str) -> Result<Option<CleanupOperation>> {
        self.read(|db| lookup(db, id, false))
    }
    pub fn cleanup_request(&self, request: &str) -> Result<Option<CleanupOperation>> {
        self.read(|db| lookup(db, request, true))
    }
    pub fn pending_cleanups(&self, after: &str) -> Result<Vec<String>> {
        self.read(|db| {
            let mut stmt = db.prepare_cached(
                "SELECT id FROM environment_cleanups WHERE complete=0 AND id>? ORDER BY id LIMIT 32",
            )?;
            let rows = stmt.query_map([after], |r| r.get(0))?;
            Ok(rows.collect::<rusqlite::Result<_>>()?)
        })
    }
    pub(crate) fn check_cleanup_owner(db: &Connection, scope: &str) -> Result<()> {
        if db.query_row(
            "SELECT EXISTS(SELECT 1 FROM environment_cleanups WHERE scope=? AND complete=0)",
            [scope],
            |row| row.get::<_, bool>(0),
        )? {
            return Err(Error::Conflict("environment cleanup already accepted"));
        }
        Ok(())
    }
    pub async fn accept_cleanup(
        &self,
        scope: &str,
        kind: CleanupKind,
        revision: u64,
        confirmed: &[String],
        request: &str,
    ) -> Result<CleanupOperation> {
        self.accept_scoped_cleanup(scope, kind, revision, confirmed, request, None)
            .await
    }

    pub async fn accept_mcp_cleanup(
        &self,
        session: &crate::Session,
        kind: CleanupKind,
        revision: u64,
        confirmed: &[String],
        request: &str,
    ) -> Result<CleanupOperation> {
        self.accept_scoped_cleanup(
            &session.scope_id,
            kind,
            revision,
            confirmed,
            request,
            Some(session),
        )
        .await
    }
    async fn accept_scoped_cleanup(
        &self,
        scope: &str,
        kind: CleanupKind,
        revision: u64,
        confirmed: &[String],
        request: &str,
        session: Option<&crate::Session>,
    ) -> Result<CleanupOperation> {
        if request.is_empty() || request.len() > 128 || revision == 0 {
            return Err(Error::Invalid("cleanup request"));
        }
        let _effects = self.effects.lock().await;
        self.write(|db| {
            if let Some(session) = session
                && !crate::retained_image::current(db, session)?
            {
                return Err(Error::Fenced);
            }
            if let Some(prior) = lookup(db, request, true)? {
                if prior.scope != scope || prior.kind != kind {
                    return Err(Error::Conflict("request fingerprint"));
                }
                return Ok(prior);
            }
            let plan = inventory(db, scope, kind, session)?;
            if plan.revision != revision {
                return Err(Error::Fenced);
            }
            let discard = kind == CleanupKind::Discard;
            if discard {
                let expected: Vec<_> = plan.resources.iter()
                    .filter(|r| r.kind == "volume")
                    .map(|r| &r.id).collect();
                let mut supplied: Vec<_> = confirmed.iter().collect();
                supplied.sort_unstable();
                if expected != supplied {
                    return Err(Error::Conflict("volume confirmation required"));
                }
            }
            let id = uuid::Uuid::new_v4().to_string();
            db.execute(
                "INSERT INTO environment_cleanups(id,request,scope,discard,revision) VALUES(?,?,?,?,?)",
                params![id, request, scope, discard, revision as i64],
            )?;
            db.execute(
                "UPDATE scopes SET data=json_set(data,'$.state','Draining'),disposal_request=? WHERE id=?",
                params![id, scope],
            )?;
            db.execute("UPDATE sessions SET revoked=1 WHERE scope=? AND revoked=0", [scope])?;
            lookup(db, &id, false)?.ok_or(Error::Missing("accepted operation"))
        })
    }
}

fn lookup(db: &Connection, key: &str, by_request: bool) -> Result<Option<CleanupOperation>> {
    let query = if by_request {
        "SELECT id,request,scope,discard,complete,started_at,updated_at FROM environment_cleanups WHERE request=?"
    } else {
        "SELECT id,request,scope,discard,complete,started_at,updated_at FROM environment_cleanups WHERE id=?"
    };
    Ok(db
        .query_row(query, [key], |r| {
            Ok(CleanupOperation {
                id: r.get(0)?,
                request: r.get(1)?,
                scope: r.get(2)?,
                kind: if r.get(3)? {
                    CleanupKind::Discard
                } else {
                    CleanupKind::Reset
                },
                complete: r.get(4)?,
                started_at: r.get(5)?,
                updated_at: r.get(6)?,
            })
        })
        .optional()?)
}

impl Core {
    pub async fn drive_cleanup(&self, id: &str) -> Result<CleanupOperation> {
        let _guard = self.store.operator_disposals.lock().await;
        let op = self
            .store
            .cleanup_operation(id)?
            .ok_or(Error::Missing("operation"))?;
        if op.complete {
            return Ok(op);
        }
        let scope = self.store.scope(&op.scope)?;
        let discard = op.kind == CleanupKind::Discard;
        // A discard may have finished retirement before the final receipt commit.
        if discard {
            if scope.state != ScopeState::Released {
                self.drive_retirement(scope, Some(id)).await?;
            }
        } else {
            self.retire_workloads(&op.scope, true).await?;
        }
        {
            let _effects = self.store.effects.lock().await;
            self.store.write(|db| {
                crate::mesh::retire_scope(db,&op.scope)?;
                db.execute(
                    "DELETE FROM service_exports WHERE json_extract(data,'$.boot.sandbox.scope_id')=?",
                    [&op.scope],
                )?;
                Ok(())
            })?;
            self.reconcile_routes(false).await?;
        }
        self.store.write(|db| {
            if !discard
                && db.execute(
                    "UPDATE scopes SET disposal_request=NULL,data=json_set(data,'$.state','Active')
                    WHERE id=? AND disposal_request=? AND json_extract(data,'$.state')='Draining'",
                    params![op.scope, id],
                )? != 1
            {
                return Err(Error::Fenced);
            }
            if discard {
                db.execute(
                    "DELETE FROM operator_host_sockets WHERE scope=?",
                    [&op.scope],
                )?;
            }
            db.execute(
                "UPDATE environment_cleanups SET complete=1,updated_at=unixepoch() WHERE id=?",
                [id],
            )?;
            lookup(db, id, false)?.ok_or(Error::Missing("operation"))
        })
    }
}
