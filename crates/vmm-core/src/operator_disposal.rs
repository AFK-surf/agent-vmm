//! Trusted local operator retirement. Remote commands require their live channel.
use crate::{
    Core, Error, Origin, Result, Scope, ScopeState, Store, authority::registration, store::decode,
};
use rusqlite::{Connection, OptionalExtension, params};

pub(crate) fn accepted_request(db: &Connection, id: &str) -> Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT request FROM operator_registration_disposals WHERE registration=?",
            [id],
            |row| row.get(0),
        )
        .optional()?)
}

fn check_external_disposal(db: &Connection, id: &str, request: &str) -> Result<bool> {
    let private: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM appliances WHERE registration=?)",
        [id],
        |r| r.get(0),
    )?;
    if private {
        return Err(Error::Conflict("private appliance registration"));
    }
    let cleaning: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM environment_cleanups c JOIN scopes s ON s.id=c.scope WHERE s.registration=? AND c.complete=0)",[id],|r|r.get(0))?;
    if cleaning {
        return Err(Error::Conflict("environment cleanup already accepted"));
    }
    if let Some(previous) = accepted_request(db, id)? {
        return if previous == request {
            Ok(true)
        } else {
            Err(Error::Conflict("registration disposal already accepted"))
        };
    }
    let conflicting: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM scopes WHERE registration=?1
         AND json_extract(data,'$.state') != 'Released' AND disposal_request != ?2)",
        params![id, request],
        |r| r.get(0),
    )?;
    if conflicting {
        return Err(Error::Conflict("scope disposal already accepted"));
    }
    Ok(false)
}

impl Store {
    /// Admit only an external connector whose registration has no local disposal fence.
    pub fn check_external_connector(&self, id: &str) -> Result<()> {
        self.read(|db| {
            let (private, retired): (bool, bool) = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM appliances a WHERE a.registration=r.id),
                        EXISTS(SELECT 1 FROM operator_registration_disposals d WHERE d.registration=r.id)
                 FROM registrations r WHERE r.id=?",
                [id], |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional()?.ok_or(Error::Missing("registration"))?;
            if private { return Err(Error::Conflict("private appliance registration")); }
            if retired || crate::registration_retirement::revoking(db,id)? { return Err(Error::Draining); }
            Ok(())
        })
    }

    /// Preflight a connector stop and distinguish a replay of the same durable request.
    pub fn check_operator_registration_disposal(&self, id: &str, request: &str) -> Result<bool> {
        self.read(|db| {
            registration(db, id)?;
            check_external_disposal(db, id, request)
        })
    }

    /// Atomically isolate the complete external registration before acknowledging acceptance.
    /// The caller stops the external connector before entering this transaction.
    pub fn begin_operator_registration_disposal(&self, id: &str, request: &str) -> Result<()> {
        if request.is_empty() || request.len() > 128 {
            return Err(Error::Invalid("request_id"));
        }
        self.write(|db| {
            let owner = registration(db, id)?;
            if check_external_disposal(db, id, request)? {
                return Ok(());
            }
            db.execute(
                "INSERT INTO operator_registration_disposals(registration,request) VALUES(?,?)",
                params![id, request],
            )?;
            // Even a terminal epoch is fenced by the permanent disposal record.
            db.execute(
                "UPDATE registrations SET connected=0,draining=1,epoch=? WHERE id=?",
                params![owner.connection_epoch.saturating_add(1).to_be_bytes(), id],
            )?;
            db.execute(
                "UPDATE sessions SET revoked=1 WHERE revoked=0 AND scope IN
                        (SELECT id FROM scopes WHERE registration=?)",
                [id],
            )?;
            db.execute(
                "UPDATE scopes SET data=json_set(data,'$.state','Draining'),disposal_request=?1
                        WHERE registration=?2 AND json_extract(data,'$.state') != 'Released'",
                params![request, id],
            )?;
            Ok(())
        })
    }
}

impl Core {
    /// Resume disposal with the same registration and request after a restart or backend failure.
    pub async fn dispose_operator_registration(&self, id: &str, request: &str) -> Result<()> {
        self.store
            .begin_operator_registration_disposal(id, request)?;
        let _guard = self.store.operator_disposals.lock().await;
        let mut after = String::new();
        loop {
            let next = self.store.read(|db| {
                let mut stmt = db.prepare_cached(
                    "SELECT data FROM scopes
                     WHERE registration=? AND id>? AND json_extract(data,'$.state') != 'Released'
                     ORDER BY id LIMIT 1",
                )?;
                let mut rows = stmt.query(params![id, after])?;
                let Some(row) = rows.next()? else {
                    return Ok(None);
                };
                Ok(Some(decode(&row.get::<_, String>(0)?)?))
            })?;
            let Some(current) = next else { return Ok(()) };
            after = self.drive_retirement(current, Some(request)).await?.id;
        }
    }
}

impl Store {
    /// Isolate an exact local environment. Each remote allocation belongs to its registration.
    pub fn begin_operator_environment_disposal(
        &self,
        id: &str,
        revision: u64,
        request: &str,
    ) -> Result<Scope> {
        if request.is_empty() || request.len() > 128 {
            return Err(Error::Invalid("request_id"));
        }
        self.write(|db| {
            Self::check_cleanup_owner(db, id)?;
            let mut scope = crate::authority::load_scope(db, id)?;
            if scope.origin != Origin::Local {
                return Err(Error::Conflict(
                    "remote environments require whole registration disposal",
                ));
            }
            if scope.id.starts_with("system.") {
                return Err(Error::Conflict("private system environment"));
            }
            if revision == 0 || scope.revision != revision {
                return Err(Error::Fenced);
            }
            let accepted: Option<String> = db.query_row(
                "SELECT disposal_request FROM scopes WHERE id=?",
                [id],
                |r| r.get(0),
            )?;
            if let Some(previous) = accepted {
                return if previous == request {
                    Ok(scope)
                } else {
                    Err(Error::Conflict("scope disposal already accepted"))
                };
            }
            if scope.state == ScopeState::Released {
                return Err(Error::Conflict("scope already released"));
            }
            scope.state = ScopeState::Draining;
            db.execute(
                "UPDATE scopes SET disposal_request=?,data=? WHERE id=?",
                params![request, crate::store::encode(&scope)?, id],
            )?;
            db.execute(
                "UPDATE sessions SET revoked=1 WHERE scope=? AND revoked=0",
                [id],
            )?;
            Ok(scope)
        })
    }
}

impl Core {
    pub async fn dispose_operator_environment(
        &self,
        id: &str,
        revision: u64,
        request: &str,
    ) -> Result<()> {
        let _guard = self.store.operator_disposals.lock().await;
        let scope = self
            .store
            .begin_operator_environment_disposal(id, revision, request)?;
        self.drive_retirement(scope, Some(request)).await?;
        Ok(())
    }
}

impl Store {
    /// Count live allocation mappings only after checking the external registration owner.
    pub fn operator_registration_scope_count(&self, id: &str) -> Result<u64> {
        self.read(|db| {
            let mut stmt = db.prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM appliances WHERE registration=r.id),
                 (SELECT count(*) FROM scopes WHERE registration=r.id AND json_extract(data,'$.state')!='Released')
                 FROM registrations r WHERE r.id=?",
            )?;
            let (private, count): (bool,i64) = stmt.query_row([id],|r| Ok((r.get(0)?,r.get(1)?)))
                .optional()?.ok_or(Error::Missing("registration"))?;
            if private { return Err(Error::Conflict("private appliance registration")); }
            u64::try_from(count).map_err(|_| Error::Invalid("allocation count"))
        })
    }
}
