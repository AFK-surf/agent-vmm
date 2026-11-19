//! A revoked controller loses admission immediately. Its workloads stop and its storage is retained.
use crate::{Core, Error, Result, Scope, Store, authority::registration, store::decode};
use rusqlite::{Connection, OptionalExtension};

pub(crate) fn revoking(db: &Connection, id: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM registration_revocations WHERE registration=?)",
        [id],
        |row| row.get(0),
    )?)
}

pub(crate) fn begin_revocation(db: &Connection, id: &str) -> Result<()> {
    registration(db, id)?;
    let private: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM appliances WHERE registration=?)",
        [id],
        |r| r.get(0),
    )?;
    if private {
        return Err(Error::Conflict("private appliance registration"));
    }
    if crate::operator_disposal::accepted_request(db, id)?.is_some() {
        return Err(Error::Conflict("registration disposal already accepted"));
    }
    if revoking(db, id)? {
        return Ok(());
    }
    let pending: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM scopes WHERE registration=? AND disposal_request IS NOT NULL
                    AND json_extract(data,'$.state')!='Released')",
        [id],
        |r| r.get(0),
    )?;
    if pending {
        return Err(Error::Conflict("scope disposal already accepted"));
    }
    db.execute("INSERT INTO registration_revocations VALUES(?)", [id])?;
    db.execute(
        "UPDATE registrations SET connected=0, draining=1 WHERE id=?",
        [id],
    )?;
    db.execute(
        "UPDATE sessions SET revoked=1 WHERE revoked=0 AND scope IN
                       (SELECT id FROM scopes WHERE registration=?)",
        [id],
    )?;
    db.execute(
        "UPDATE scopes SET data=json_set(data,'$.state','Draining') WHERE registration=?
                       AND json_extract(data,'$.state')='Active'",
        [id],
    )?;
    Ok(())
}

impl Store {
    /// Fence reconnects, new boots and active capabilities in the same commit.
    /// An accepted destructive cleanup retains its own exact request authority.
    pub fn begin_registration_revocation(&self, id: &str) -> Result<()> {
        self.write(|db| begin_revocation(db, id))
    }
}

impl Core {
    /// Resume the non-destructive portion of operator revocation after a restart.
    pub async fn retain_revoked_registration(&self, id: &str) -> Result<()> {
        let _guard = self.store.operator_disposals.lock().await;
        self.store.begin_registration_revocation(id)?;
        loop {
            let next =
                self.store.read(|db| {
                    let data: Option<String> = db.query_row(
                    "SELECT data FROM scopes WHERE registration=? AND disposal_request IS NULL
                     AND json_extract(data,'$.state')='Draining' ORDER BY id LIMIT 1", [id],
                    |r| r.get(0),
                ).optional()?;
                    data.as_deref().map(decode::<Scope>).transpose()
                })?;
            let Some(scope) = next else { return Ok(()) };
            self.drive_retirement(scope, None).await?;
        }
    }
}
