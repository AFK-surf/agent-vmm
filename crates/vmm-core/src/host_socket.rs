//! Host-owned Unix socket approvals. A scoped approval overrides the host-wide port.
use crate::{authority::load_scope, *};
use rusqlite::{Connection, OptionalExtension, params};
use std::collections::{BTreeMap, HashSet};

pub(crate) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS operator_host_sockets(
    scope TEXT NOT NULL, port INTEGER NOT NULL, path TEXT, revision INTEGER NOT NULL,
    pending INTEGER NOT NULL, PRIMARY KEY(scope,port));
    CREATE TABLE IF NOT EXISTS operator_socket_requests(
        request TEXT PRIMARY KEY, scope TEXT NOT NULL, port INTEGER NOT NULL, path TEXT, revision INTEGER NOT NULL)";

#[derive(Debug, PartialEq, Eq)]
pub struct HostSocketApproval {
    pub scope: String,
    pub port: u16,
    pub path: String,
    pub revision: u64,
}

fn approvals(db: &Connection, scope: &str) -> Result<Vec<HostSocketApproval>> {
    let mut stmt = db.prepare_cached(
        "SELECT scope,port,path,revision FROM operator_host_sockets
        WHERE path IS NOT NULL AND (?1='' OR scope='' OR scope=?1) ORDER BY scope,port",
    )?;
    let mut rows = stmt.query([scope])?;
    let mut items = Vec::new();
    while let Some(row) = rows.next()? {
        items.push(HostSocketApproval {
            scope: row.get(0)?,
            port: row.get(1)?,
            path: row.get(2)?,
            revision: u64::try_from(row.get::<_, i64>(3)?)
                .map_err(|_| Error::Invalid("host socket revision"))?,
        });
    }
    Ok(items)
}

pub(crate) fn boot_scope(db: &Connection, id: &str) -> Result<Scope> {
    let mut scope = load_scope(db, id)?;
    let mut rules: BTreeMap<_, _> = scope
        .host_socket_rules
        .into_iter()
        .map(|r| (r.port, r))
        .collect();
    for rule in approvals(db, id)? {
        rules.insert(
            rule.port,
            HostSocketRule {
                port: rule.port,
                socket_path: rule.path,
            },
        );
    }
    scope.host_socket_rules = rules.into_values().collect();
    Ok(scope)
}

impl Store {
    pub fn host_sockets(&self, scope: &str) -> Result<Vec<HostSocketApproval>> {
        self.read(|db| approvals(db, scope))
    }

    pub async fn change_host_socket(
        &self,
        scope: &str,
        port: u16,
        path: Option<&str>,
        request: &str,
    ) -> Result<Option<HostSocketApproval>> {
        if request.is_empty() || matches!(port, 0 | 53) {
            return Err(Error::Invalid("host socket target"));
        }
        if let Some(path) = path
            && (!path.starts_with('/')
                || path.contains('\0')
                || path != "/"
                    && path
                        .split('/')
                        .skip(1)
                        .any(|part| matches!(part, "" | "." | "..")))
        {
            return Err(Error::Invalid("host socket path"));
        }
        let _guard = self.effects.lock().await;
        self.write(|db| {
            let previous = db.query_row("SELECT scope,port,path,revision FROM operator_socket_requests WHERE request=?",
                [request], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u16>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,i64>(3)?))).optional()?;
            let revision = if let Some((prior_scope,prior_port,prior_path,revision)) = previous {
                if prior_scope != scope || prior_port != port || prior_path.as_deref() != path {return Err(Error::Conflict("request fingerprint"));}
                revision
            } else {
                if !scope.is_empty() {load_scope(db,scope)?;}
                let revision = if let Some(path) = path {
                    db.query_row("INSERT INTO operator_host_sockets(scope,port,path,revision,pending)
                        VALUES(?1,?2,?3,1,1) ON CONFLICT(scope,port) DO UPDATE SET
                        revision=CASE WHEN path IS NULL THEN 1 ELSE revision+(path!=excluded.path) END,
                        path=excluded.path,pending=1 RETURNING revision",params![scope,port,path],|r|r.get::<_,i64>(0))?
                } else {
                    if db.execute("UPDATE operator_host_sockets SET path=NULL,pending=1
                        WHERE scope=?1 AND port=?2 AND path IS NOT NULL",params![scope,port])? == 0 {
                        return Err(Error::Missing("host socket"));
                    }
                    0
                };
                db.execute("INSERT INTO operator_socket_requests VALUES(?,?,?,?,?)",params![request,scope,port,path,revision])?;
                revision
            };
            let revision = u64::try_from(revision).map_err(|_| Error::Invalid("host socket revision"))?;
            Ok(path.map(|path| HostSocketApproval {scope:scope.into(), port, path:path.into(), revision}))
        })
    }
}

impl Core {
    /// Publish committed approvals to owned live guests. A failed publication remains pending.
    pub async fn project_host_sockets(&self) -> Result<()> {
        let _effects = self.store.effects.lock().await;
        let dirty = self.store.read(|db| {
            let mut stmt = db.prepare_cached(
                "SELECT DISTINCT scope FROM operator_host_sockets WHERE pending=1",
            )?;
            Ok(stmt
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<HashSet<_>>>()?)
        })?;
        if dirty.is_empty() {
            return Ok(());
        }
        for vm in self.backend.list_live_vms().await? {
            let boot = &vm.incarnation.boot;
            let id = &boot.sandbox.scope_id;
            if !dirty.contains("") && !dirty.contains(id) {
                continue;
            }
            match self.store.sandbox(&boot.sandbox) {
                Ok(sb)
                    if sb.vm.as_ref() == Some(&vm.incarnation)
                        && sb.boot.as_ref() == Some(boot) =>
                {
                    let scope = self.store.read(|db| boot_scope(db, id))?;
                    self.backend.set_network_policy(boot, &scope).await?;
                }
                Ok(sb) if sb.boot.as_ref() == Some(boot) => return Err(Error::Fenced),
                Ok(_) | Err(Error::Missing(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        self.store.write(|db| {
            db.execute(
                "UPDATE operator_host_sockets SET pending=0 WHERE pending=1",
                [],
            )?;
            Ok(())
        })
    }
}
