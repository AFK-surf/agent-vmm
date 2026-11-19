//! Durable service declarations and their boot-fenced local forwarding rules.
use crate::{
    retained_image::current,
    storage::active,
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const SCHEMA: &str = include_str!("mesh.sql");

#[derive(Clone, Debug)]
pub struct MeshExport {
    pub id: String,
    pub owner: SandboxKey,
    pub instance: String,
    pub port: u16,
    pub protocol: i32,
    pub created: i64,
    pub revision: u64,
    pub revoked: bool,
}

#[derive(Clone, Debug)]
pub struct MeshImport {
    pub id: String,
    pub owner: SandboxKey,
    pub instance: String,
    pub destination: String,
    pub device: String,
    pub port: u16,
    pub alias: String,
    pub revision: u64,
    pub revoked: bool,
}

const EXPORTS: &str = "SELECT id,scope,container,generation,instance,port,protocol,created,revision,revoked_by IS NOT NULL FROM mesh_exports";
const IMPORTS: &str = "SELECT i.id,i.scope,i.container,i.generation,i.instance,i.export,i.device,e.port,i.alias,i.revision,i.revoked_by IS NOT NULL FROM mesh_imports i JOIN mesh_exports e ON i.export=e.id";

fn unsigned(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let raw: i64 = row.get(index)?;
    u64::try_from(raw).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, raw))
}
fn owner(row: &Row<'_>) -> rusqlite::Result<SandboxKey> {
    Ok(SandboxKey {
        scope_id: row.get(1)?,
        sandbox_id: row.get(2)?,
        generation_id: row.get(3)?,
    })
}
fn export(row: &Row<'_>) -> rusqlite::Result<MeshExport> {
    Ok(MeshExport {
        id: row.get(0)?,
        owner: owner(row)?,
        instance: row.get(4)?,
        port: row.get(5)?,
        protocol: row.get(6)?,
        created: row.get(7)?,
        revision: unsigned(row, 8)?,
        revoked: row.get(9)?,
    })
}
fn import(row: &Row<'_>) -> rusqlite::Result<MeshImport> {
    Ok(MeshImport {
        id: row.get(0)?,
        owner: owner(row)?,
        instance: row.get(4)?,
        destination: row.get(5)?,
        device: row.get(6)?,
        port: row.get(7)?,
        alias: row.get(8)?,
        revision: unsigned(row, 9)?,
        revoked: row.get(10)?,
    })
}
fn authorize(db: &Connection, session: &Session) -> Result<Scope> {
    if !current(db, session)? {
        return Err(Error::Fenced);
    }
    Store::check_cleanup_owner(db, &session.scope_id)?;
    active(db, &session.scope_id)
}
fn live(db: &Connection, scope: &str, container: &str) -> Result<Option<BootIdentity>> {
    let boot: Option<String> = db
        .query_row(
            "SELECT boot FROM sandboxes WHERE scope=? AND id=? AND phase='running'",
            [scope, container],
            |r| r.get(0),
        )
        .optional()?;
    boot.as_deref().map(decode).transpose()
}
fn checked_boot(
    db: &Connection,
    session: &Session,
    container: &str,
    instance: &str,
) -> Result<BootIdentity> {
    live(db, &session.scope_id, container)?
        .filter(|boot| boot.instance_id == instance)
        .ok_or(Error::Fenced)
}
pub fn valid_alias(s: &str) -> bool {
    let bytes = s.as_bytes();
    (2..=63).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes.last() != Some(&b'-')
        && bytes
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn now() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_secs()).ok())
        .ok_or(Error::Invalid("system clock"))
}
impl Store {
    pub fn mesh_exports(&self, session: &Session) -> Result<Vec<MeshExport>> {
        self.read(|db| {
            authorize(db, session)?;
            Ok(db
                .prepare(&format!("{EXPORTS} WHERE scope=? ORDER BY id"))?
                .query_map([&session.scope_id], export)?
                .collect::<rusqlite::Result<_>>()?)
        })
    }
    pub fn mesh_imports(&self, session: &Session) -> Result<Vec<MeshImport>> {
        self.read(|db| {
            authorize(db, session)?;
            Ok(db
                .prepare(&format!("{IMPORTS} WHERE i.scope=? ORDER BY i.id"))?
                .query_map([&session.scope_id], import)?
                .collect::<rusqlite::Result<_>>()?)
        })
    }
}

impl Core {
    pub async fn create_mesh_export(
        &self,
        session: &Session,
        request: &str,
        container: &str,
        instance: &str,
        port: u16,
        protocol: i32,
    ) -> Result<MeshExport> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            authorize(db, session)?;
            let boot = checked_boot(db, session, container, instance)?;
            if port == 0 || request.is_empty() || !matches!(protocol, 1 | 2) {
                return Err(Error::Invalid("service export"));
            }
            let id = uuid::Uuid::new_v4().to_string();
            db.execute(
                "INSERT INTO mesh_exports(id,scope,container,request,protocol,port,created,instance,generation) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(scope,request) DO NOTHING",
                params![id, session.scope_id, container, request, protocol, port, now()?, instance, boot.sandbox.generation_id],
            )?;
            let value = db.query_row(&format!("{EXPORTS} WHERE scope=? AND request=?"), params![session.scope_id, request], export)?;
            if value.owner.sandbox_id != container || value.port != port || value.protocol != protocol {
                return Err(Error::Conflict("request fingerprint"));
            }
            if !value.revoked {
                let projection = ServiceExport { id: value.id.clone(), boot, port };
                db.execute("INSERT INTO service_exports VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data", params![value.id, encode(&projection)?])?;
            }
            Ok(value)
        })
    }

    pub async fn create_mesh_import(
        &self,
        session: &Session,
        req: LocalServiceImport<'_>,
    ) -> Result<MeshImport> {
        let _guard = self.store.effects.lock().await;
        if req.request.is_empty() || req.device.is_empty() || !valid_alias(req.alias) {
            return Err(Error::Invalid("service import"));
        }
        self.reconcile_routes(false).await?;
        let (record, binding) = self.store.write(|db| {
            let source_scope = authorize(db, session)?;
            let source = checked_boot(db, session, req.container, req.instance)?;
            let dest = db
                .query_row(&format!("{EXPORTS} WHERE id=? AND revoked_by IS NULL"), [req.export], export)
                .optional()?
                .ok_or(Error::Missing("service export"))?;
            let destination_scope = active(db, &dest.owner.scope_id)?;
            if source_scope.id != destination_scope.id && !matches!((&source_scope.origin,&destination_scope.origin),(Origin::Remote{registration_id:a},Origin::Remote{registration_id:b}) if a==b && !a.is_empty()) {
                return Err(Error::Conflict("cross-domain service route"));
            }
            let boot = live(db, &dest.owner.scope_id, &dest.owner.sandbox_id)?.ok_or(Error::Conflict("destination instance stale"))?;
            let id = uuid::Uuid::new_v4().to_string();
            let existing = db.query_row(&format!("{IMPORTS} WHERE i.scope=? AND i.request=?"), params![session.scope_id, req.request], import).optional()?;
            let record = if let Some(prior) = existing {
                if prior.owner.sandbox_id != req.container || prior.destination != dest.id || prior.device != req.device || prior.alias != req.alias {
                    return Err(Error::Conflict("request fingerprint"));
                }
                prior
            } else {
                let occupied: Vec<(String, u8)> = db
                    .prepare("SELECT alias,vip FROM mesh_imports WHERE scope=? AND container=? AND revoked_by IS NULL AND expires>?")?
                    .query_map(params![session.scope_id, req.container, now()?], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()?;
                if occupied.iter().any(|(a, _)| a == req.alias) {
                    return Err(Error::Conflict("service alias in use"));
                }
                let vip = (1..=254).find(|v| !occupied.iter().any(|(_, old)| old == v)).ok_or(Error::Conflict("service addresses exhausted"))?;
                db.execute(
                    "INSERT INTO mesh_imports(id,scope,container,request,export,device,alias,vip,expires,instance,generation) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    params![
                        id,
                        session.scope_id,
                        req.container,
                        req.request,
                        dest.id,
                        req.device,
                        req.alias,
                        vip,
                        now()? + 86400,
                        req.instance,
                        source.sandbox.generation_id
                    ],
                )?;
                db.query_row(&format!("{IMPORTS} WHERE i.id=?"), [&id], import)?
            };
            let (vip, expires): (u8, u64) = db.query_row("SELECT vip,expires FROM mesh_imports WHERE id=?", [&record.id], |r| Ok((r.get(0)?, unsigned(r, 1)?)))?;
            let binding = RouteBinding {
                id: record.id.clone(),
                source,
                export: ServiceExport { id: dest.id, boot, port: dest.port },
                vip: [100, 127, 254, vip].into(),
                virtual_service_name: record.alias.clone(),
                expires_at_unix_seconds: Some(expires),
            };
            Ok((record, binding))
        })?;
        if !record.revoked {
            self.bind_locked(&binding).await?;
        }
        Ok(record)
    }
    pub async fn revoke_mesh_service(
        &self,
        session: &Session,
        resource: MeshResource,
        id: &str,
        revision: u64,
        request: &str,
    ) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            authorize(db, session)?;
            let revision = i64::try_from(revision).map_err(|_| Error::Fenced)?;
            let table = match resource {
                MeshResource::Export => "mesh_exports",
                MeshResource::Import => "mesh_imports",
            };
            let changed = db.execute(
                &format!("UPDATE {table} SET revision=revision+1,revoked_by=? WHERE id=? AND scope=? AND revision=? AND revoked_by IS NULL"),
                params![request, id, session.scope_id, revision],
            )?;
            if changed == 0 {
                let replay: bool = db.query_row(
                    &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=? AND scope=? AND revision-1=? AND revoked_by=?)"),
                    params![id, session.scope_id, revision, request],
                    |r| r.get(0),
                )?;
                if !replay {
                    return Err(Error::Fenced);
                }
            }
            match resource {
                MeshResource::Export => {
                    db.execute("DELETE FROM service_exports WHERE id=?", [id])?;
                    db.execute("UPDATE mesh_imports SET revision=revision+1,revoked_by=? WHERE export=? AND revoked_by IS NULL", params![request, id])?;
                }
                MeshResource::Import => {
                    db.execute("UPDATE routes SET phase='revoking' WHERE id=? AND phase!='revoked'", [id])?;
                }
            }
            Ok(())
        })?;
        self.reconcile_routes(false).await
    }
}
pub enum MeshResource {
    Export,
    Import,
}

pub struct LocalServiceImport<'a> {
    pub request: &'a str,
    pub container: &'a str,
    pub instance: &'a str,
    pub device: &'a str,
    pub export: &'a str,
    pub alias: &'a str,
}

/// Clear declarative authority before a reset or retirement can reuse the names.
pub(crate) fn retire_scope(db: &Connection, scope: &str) -> Result<()> {
    db.execute("UPDATE mesh_exports SET revoked_by='',revision=revision+1 WHERE scope=? AND revoked_by IS NULL", [scope])?;
    db.execute(
        "UPDATE mesh_imports SET revoked_by='',revision=revision+1 WHERE (scope=?1 OR export IN (SELECT id FROM mesh_exports WHERE scope=?1)) AND revoked_by IS NULL",
        [scope],
    )?;
    db.execute(
        "DELETE FROM service_exports WHERE id IN (SELECT id FROM mesh_exports WHERE scope=?)",
        [scope],
    )?;
    db.execute(
        "UPDATE routes SET phase='revoking' WHERE id IN (SELECT id FROM mesh_imports WHERE revoked_by IS NOT NULL) AND phase IN ('active','binding')",
        [],
    )?;
    Ok(())
}

impl Core {
    /// Caller holds the effects lock. Retire the previous boot's projection before binding a replacement.
    pub(crate) async fn project_mesh(&self, restore: bool) -> Result<()> {
        let bindings = self.store.write(|db| {
            let exports = db.prepare(&format!("{EXPORTS} WHERE revoked_by IS NULL"))?.query_map([], export)?.collect::<rusqlite::Result<Vec<_>>>()?;
            for e in exports {
                if let Some(boot) = live(db, &e.owner.scope_id, &e.owner.sandbox_id)? {
                    db.execute("UPDATE mesh_exports SET instance=?,generation=? WHERE id=?", params![boot.instance_id, boot.sandbox.generation_id, e.id])?;
                    let projected = ServiceExport { id: e.id.clone(), boot, port: e.port };
                    db.execute(
                        "INSERT INTO service_exports VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data WHERE data!=excluded.data",
                        params![e.id, encode(&projected)?],
                    )?;
                } else {
                    db.execute("DELETE FROM service_exports WHERE id=?", [&e.id])?;
                    db.execute("UPDATE mesh_exports SET instance='' WHERE id=?", [&e.id])?;
                }
            }
            let imports = db
                .prepare(&format!("{IMPORTS} WHERE i.revoked_by IS NULL AND e.revoked_by IS NULL AND i.expires>?"))?
                .query_map([now()?], import)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut bindings = Vec::new();
            for i in imports {
                let Some(source) = live(db, &i.owner.scope_id, &i.owner.sandbox_id)? else {
                    db.execute("UPDATE mesh_imports SET instance='' WHERE id=?", [&i.id])?;
                    continue;
                };
                db.execute("UPDATE mesh_imports SET instance=?,generation=? WHERE id=?", params![source.instance_id, source.sandbox.generation_id, i.id])?;
                let projection: Option<String> = db.query_row("SELECT data FROM service_exports WHERE id=?", [&i.destination], |r| r.get(0)).optional()?;
                if let Some(data) = projection {
                    let (vip, expires): (u8, u64) = db.query_row("SELECT vip,expires FROM mesh_imports WHERE id=?", [&i.id], |r| Ok((r.get(0)?, unsigned(r, 1)?)))?;
                    bindings.push(RouteBinding {
                        id: i.id,
                        source,
                        export: decode(&data)?,
                        vip: [100, 127, 254, vip].into(),
                        virtual_service_name: i.alias,
                        expires_at_unix_seconds: Some(expires),
                    });
                }
            }
            Ok(bindings)
        })?;
        self.reconcile_routes(restore).await?;
        for binding in bindings {
            // Recovery may have retired this intent while restoring its previous revision.
            if self.store.read(|db| {
                Ok(db.query_row(
                    "SELECT revoked_by IS NULL FROM mesh_imports WHERE id=?",
                    [&binding.id],
                    |row| row.get::<_, bool>(0),
                )?)
            })? {
                self.bind_locked(&binding).await?;
            }
        }
        Ok(())
    }
}
