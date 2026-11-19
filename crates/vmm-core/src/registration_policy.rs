//! Public external registration policy and its admission fence share the Core transaction.
use crate::{
    EgressMode, Error, Result, Store,
    authority::insert_registration,
    operator_disposal::accepted_request,
    registration_retirement::{begin_revocation, revoking},
    store::{decode, encode},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

pub(crate) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS managed_registrations(
    id TEXT PRIMARY KEY REFERENCES registrations(id),
    enrollment_request TEXT UNIQUE NOT NULL,
    data TEXT NOT NULL
)";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedConnection {
    pub id: String,
    pub request: String,
    pub endpoint: String,
    pub trust_bundle: Vec<u8>,
    pub display_name: String,
    pub source_revision: u64,
    pub pool: ManagedPool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedPool {
    pub pids: u32,
    pub disk_bytes: u64,
    pub egress: EgressMode,
    pub stop_grace_seconds: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManagedPhase {
    PendingCredential,
    Enabled,
    Draining,
    Revoking,
    Revoked,
}
impl ManagedPhase {
    pub fn accepts_connection(self) -> bool {
        matches!(self, Self::Enabled | Self::Draining)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedRegistration {
    pub config: ManagedConnection,
    pub phase: ManagedPhase,
    pub revision: u64,
    pub pool_revision: u64,
    pub created: SystemTime,
    pub updated: SystemTime,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RegistrationChange {
    CredentialReady,
    SetDraining(bool),
    BeginRevocation,
    CompleteRevocation,
}

pub(crate) fn load(db: &Connection, id: &str) -> Result<Option<ManagedRegistration>> {
    let raw: Option<String> = db
        .query_row(
            "SELECT data FROM managed_registrations WHERE id=?",
            [id],
            |row| row.get(0),
        )
        .optional()?;
    raw.as_deref().map(decode).transpose()
}
fn save(db: &Connection, record: &mut ManagedRegistration) -> Result<()> {
    record.updated = SystemTime::now();
    record.revision = record
        .revision
        .checked_add(1)
        .ok_or(Error::Conflict("registration revision exhausted"))?;
    db.execute(
        "UPDATE managed_registrations SET data=? WHERE id=?",
        params![encode(record)?, record.config.id],
    )?;
    Ok(())
}
impl Store {
    pub fn managed_registration(&self, id: &str) -> Result<ManagedRegistration> {
        self.read(|db| load(db, id)?.ok_or(Error::Missing("registration")))
    }
    pub fn configure_registration(
        &self,
        config: &ManagedConnection,
    ) -> Result<ManagedRegistration> {
        self.write(|db| configure(db, config))
    }
    pub fn change_registration(
        &self,
        id: &str,
        expected: u64,
        change: RegistrationChange,
    ) -> Result<ManagedRegistration> {
        self.write(|db| transition(db, id, expected, change))
    }
}

fn configure(db: &Connection, config: &ManagedConnection) -> Result<ManagedRegistration> {
    if config.id.is_empty() || config.request.is_empty() {
        return Err(Error::Invalid("registration identity"));
    }
    if accepted_request(db, &config.id)?.is_some() || revoking(db, &config.id)? {
        return Err(Error::Draining);
    }
    if let Some(mut record) = load(db, &config.id)? {
        if matches!(record.phase, ManagedPhase::Revoked | ManagedPhase::Revoking) {
            return Err(Error::Draining);
        }
        let old = &record.config;
        if old.endpoint != config.endpoint || old.trust_bundle != config.trust_bundle {
            return Err(Error::Conflict("controller identity changed"));
        }
        let changed = old.pool != config.pool;
        if config.source_revision < old.source_revision
            || (changed && config.source_revision == old.source_revision)
        {
            return Err(Error::Fenced);
        }
        if changed {
            record.pool_revision = record
                .pool_revision
                .checked_add(1)
                .ok_or(Error::Conflict("pool revision exhausted"))?;
            db.execute(
                "UPDATE registrations SET connected=0 WHERE id=?",
                [&config.id],
            )?;
        }
        if changed
            || old.display_name != config.display_name
            || old.source_revision != config.source_revision
        {
            record.config.display_name.clone_from(&config.display_name);
            record.config.pool.clone_from(&config.pool);
            record.config.source_revision = config.source_revision;
            save(db, &mut record)?;
        }
        return Ok(record);
    }
    let used: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM registrations WHERE id=?1 UNION ALL
                       SELECT 1 FROM managed_registrations WHERE enrollment_request=?2)",
        params![config.id, config.request],
        |row| row.get(0),
    )?;
    if used {
        return Err(Error::Conflict("registration identity already in use"));
    }
    let now = SystemTime::now();
    let record = ManagedRegistration {
        config: config.clone(),
        phase: ManagedPhase::PendingCredential,
        revision: 1,
        pool_revision: 1,
        created: now,
        updated: now,
    };
    insert_registration(db, &config.id)?;
    db.execute(
        "INSERT INTO managed_registrations VALUES(?,?,?)",
        params![config.id, config.request, encode(&record)?],
    )?;
    Ok(record)
}
fn transition(
    db: &Connection,
    id: &str,
    expected: u64,
    change: RegistrationChange,
) -> Result<ManagedRegistration> {
    use ManagedPhase::*;
    let mut record = load(db, id)?.ok_or(Error::Missing("registration"))?;
    if record.revision != expected {
        return Err(Error::Fenced);
    }
    if accepted_request(db, id)?.is_some()
        || (matches!(
            change,
            RegistrationChange::CredentialReady | RegistrationChange::SetDraining(_)
        ) && revoking(db, id)?)
    {
        return Err(Error::Draining);
    }
    let next = match change {
        RegistrationChange::CredentialReady if record.phase == PendingCredential => Enabled,
        RegistrationChange::CredentialReady if record.phase.accepts_connection() => {
            return Ok(record);
        }
        RegistrationChange::SetDraining(drain) if record.phase.accepts_connection() => {
            db.execute(
                "UPDATE registrations SET draining=? WHERE id=?",
                params![drain, id],
            )?;
            if drain { Draining } else { Enabled }
        }
        RegistrationChange::BeginRevocation if matches!(record.phase, Revoking | Revoked) => {
            return Ok(record);
        }
        RegistrationChange::BeginRevocation => {
            begin_revocation(db, id)?;
            Revoking
        }
        RegistrationChange::CompleteRevocation if record.phase == Revoked => return Ok(record),
        RegistrationChange::CompleteRevocation if record.phase == Revoking => {
            let live: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM scopes WHERE registration=?
                 AND json_extract(data,'$.state') NOT IN ('Retained','Released'))",
                [id],
                |row| row.get(0),
            )?;
            if live {
                return Err(Error::Conflict("registration workloads still draining"));
            }
            Revoked
        }
        _ => return Err(Error::Conflict("registration enrollment state")),
    };
    record.phase = next;
    save(db, &mut record)?;
    Ok(record)
}
