use crate::{
    store::{decode, encode},
    *,
};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

fn insert_registration(db: &Connection, id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(Error::Invalid("registration id"));
    }
    db.execute(
        "INSERT INTO registrations(id,epoch) VALUES(?,?)",
        params![id, 0_u64.to_be_bytes()],
    )?;
    Ok(())
}

pub(crate) fn load_scope(db: &Connection, id: &str) -> Result<Scope> {
    let data: String = db
        .query_row("SELECT data FROM scopes WHERE id=?", [id], |r| r.get(0))
        .optional()?
        .ok_or(Error::Missing("scope"))?;
    decode(&data)
}

fn registration(db: &Connection, id: &str) -> Result<Registration> {
    db.query_row(
        "SELECT id,epoch,connected,draining FROM registrations WHERE id=?",
        [id],
        registration_from_row,
    )
    .optional()?
    .ok_or(Error::Missing("registration"))
}

pub(crate) fn registration_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Registration> {
    Ok(Registration {
        id: row.get(0)?,
        connection_epoch: u64::from_be_bytes(row.get(1)?),
        connected: row.get(2)?,
        draining: row.get(3)?,
    })
}

fn authorize(db: &Connection, origin: &Origin, command: Option<&Channel>) -> Result<()> {
    match (origin, command) {
        (Origin::Local, None) => Ok(()),
        (Origin::Remote { registration_id }, Some(channel))
            if registration_id == &channel.registration_id =>
        {
            let reg = registration(db, registration_id)?;
            if reg.connected && reg.connection_epoch == channel.connection_epoch {
                Ok(())
            } else {
                Err(Error::Fenced)
            }
        }
        _ => Err(Error::Fenced),
    }
}

fn validate_policy(scope: &Scope) -> Result<()> {
    if scope.revision == 0 {
        return Err(Error::Invalid("scope revision"));
    }
    if scope.id.is_empty()
        || scope.limits.pids_max == 0
        || scope.limits.vcpus == 0
        || scope.limits.boot_ram_mb == 0
        || scope.limits.max_ram_mb < scope.limits.boot_ram_mb
    {
        return Err(Error::Invalid("scope"));
    }
    Ok(())
}

fn insert_scope(db: &Connection, scope: &Scope, channel: Option<&Channel>) -> Result<()> {
    validate_policy(scope)?;
    if scope.state != ScopeState::Active {
        return Err(Error::Invalid("scope"));
    }
    authorize(db, &scope.origin, channel)?;
    let registration_id = match &scope.origin {
        Origin::Local => None,
        Origin::Remote { registration_id } => {
            if registration(db, registration_id)?.draining {
                return Err(Error::Draining);
            }
            Some(registration_id)
        }
    };
    db.execute(
        "INSERT INTO scopes(id,registration,data) VALUES(?,?,?)",
        params![scope.id, registration_id, encode(scope)?],
    )?;
    Ok(())
}

impl Store {
    pub fn create_registration(&self, id: &str) -> Result<()> {
        self.write(|db| insert_registration(db, id))
    }
    pub fn registration(&self, id: &str) -> Result<Registration> {
        self.read(|db| registration(db, id))
    }
    pub fn connect(&self, id: &str, epoch: u64) -> Result<Channel> {
        self.write(|db| {
            let previous = registration(db, id)?;
            if epoch <= previous.connection_epoch {
                return Err(Error::Fenced);
            }
            db.execute(
                "UPDATE registrations SET epoch=?, connected=1 WHERE id=?",
                params![epoch.to_be_bytes(), id],
            )?;
            Ok(Channel {
                registration_id: id.into(),
                connection_epoch: epoch,
            })
        })
    }
    pub fn disconnect(&self, channel: &Channel) -> Result<()> {
        self.write(|db| {
            let changed = db.execute(
                "UPDATE registrations SET connected=0 WHERE id=? AND epoch=? AND connected",
                params![
                    channel.registration_id,
                    channel.connection_epoch.to_be_bytes()
                ],
            )?;
            if changed == 0 {
                return Err(Error::Fenced);
            }
            Ok(())
        })
    }
    pub fn set_admission(&self, id: &str, draining: bool) -> Result<()> {
        self.write(|db| {
            if db.execute(
                "UPDATE registrations SET draining=? WHERE id=?",
                params![draining, id],
            )? == 0
            {
                return Err(Error::Missing("registration"));
            }
            Ok(())
        })
    }

    pub fn create_scope(&self, scope: &Scope, channel: Option<&Channel>) -> Result<()> {
        self.write(|db| insert_scope(db, scope, channel))
    }

    /// Reconcile policy on an active scope. An old revision or a scope leaving
    /// service returns its current record; lifecycle transitions have their own authority.
    pub fn ensure_scope(&self, desired: &Scope, channel: Option<&Channel>) -> Result<Scope> {
        self.write(|db| {
            let current = match load_scope(db, &desired.id) {
                Ok(scope) => scope,
                Err(Error::Missing(_)) => {
                    insert_scope(db, desired, channel)?;
                    return Ok(desired.clone());
                }
                Err(error) => return Err(error),
            };
            if current.origin != desired.origin || current.generation != desired.generation {
                return Err(Error::Fenced);
            }
            authorize(db, &current.origin, channel)?;
            validate_policy(desired)?;
            if desired.revision < current.revision || current.state != ScopeState::Active {
                return Ok(current);
            }
            let updated = Scope {
                state: current.state.clone(),
                ..desired.clone()
            };
            if updated != current {
                if updated.limits.disk_quota_bytes < current.limits.disk_quota_bytes {
                    crate::storage::check_quota(db, &updated, 0, 0)?;
                }
                db.execute(
                    "UPDATE scopes SET data=? WHERE id=?",
                    params![encode(&updated)?, updated.id],
                )?;
            }
            Ok(updated)
        })
    }

    pub fn scope(&self, id: &str) -> Result<Scope> {
        self.read(|db| load_scope(db, id))
    }

    pub fn transition_scope(
        &self,
        id: &str,
        expected_revision: u64,
        next: ScopeState,
        channel: Option<&Channel>,
    ) -> Result<Scope> {
        self.write(|db| {
            let mut scope = load_scope(db, id)?;
            authorize(db, &scope.origin, channel)?;
            if scope.revision != expected_revision {
                return Err(Error::Fenced);
            }
            use ScopeState::*;
            if !matches!(
                (&scope.state, &next),
                (Active, Draining)
                    | (Draining, Active | Retained | Released)
                    | (Retained, Active | Released)
            ) {
                return Err(Error::Conflict("scope transition"));
            }
            if matches!(next, Retained | Released) {
                let busy: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM sandboxes WHERE scope=? AND phase NOT IN ('stopped','defined','deleted'))", [id], |r| r.get(0))?;
                if busy {
                    return Err(Error::Conflict("scope has live workloads"));
                }
                db.execute("UPDATE sessions SET revoked=1 WHERE scope=?", [id])?;
            }
            if next == Released {
                let used: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM volumes WHERE scope=?1 UNION ALL SELECT 1 FROM image_refs WHERE scope=?1 UNION ALL SELECT 1 FROM sandboxes WHERE scope=?1 AND phase!='deleted')", [id], |r| r.get(0))?;
                if used {
                    return Err(Error::Conflict("scope still owns resources"));
                }
            }
            scope.state = next;
            scope.revision = scope
                .revision
                .checked_add(1)
                .ok_or(Error::Conflict("scope revision exhausted"))?;
            db.execute("UPDATE scopes SET data=? WHERE id=?", params![encode(&scope)?, id])?;
            Ok(scope)
        })
    }

    /// Validate at command acceptance. A later disconnect does not cancel the accepted command.
    pub fn accept_command(
        &self,
        channel: &Channel,
        scope_id: &str,
        generation: u64,
    ) -> Result<Scope> {
        self.write(|db| {
            let scope = load_scope(db, scope_id)?;
            authorize(db, &scope.origin, Some(channel))?;
            if scope.generation != generation || scope.state == ScopeState::Released {
                return Err(Error::Fenced);
            }
            Ok(scope)
        })
    }

    pub fn issue_session(
        &self,
        scope_id: &str,
        channel: Option<&Channel>,
    ) -> Result<(Session, [u8; 16])> {
        self.write(|db| {
            let scope = load_scope(db, scope_id)?;
            authorize(db, &scope.origin, channel)?;
            if !matches!(scope.state, ScopeState::Active | ScopeState::Draining) {
                return Err(Error::Draining);
            }
            db.execute(
                "UPDATE sessions SET revoked=1 WHERE scope=? AND revoked=0",
                [scope_id],
            )?;
            let token = Uuid::new_v4().into_bytes();
            let session = Session {
                id: Uuid::new_v4().to_string(),
                scope_id: scope_id.into(),
                capability_hash: Sha256::digest(token).into(),
            };
            db.execute(
                "INSERT INTO sessions(id,scope,capability) VALUES(?,?,?)",
                params![session.id, session.scope_id, session.capability_hash],
            )?;
            Ok((session, token))
        })
    }
    pub fn authenticate_session(&self, id: &str, token: &[u8]) -> Result<Session> {
        let hash: Digest = Sha256::digest(token).into();
        self.read(|db| {
            db.query_row(
                "SELECT scope FROM sessions WHERE id=? AND capability=? AND revoked=0",
                params![id, hash],
                |r| {
                    Ok(Session {
                        id: id.into(),
                        scope_id: r.get(0)?,
                        capability_hash: hash,
                    })
                },
            )
            .optional()?
            .ok_or(Error::Fenced)
        })
    }

    pub fn install_appliance(&self, id: &str, private_registration: &str) -> Result<Appliance> {
        let appliance = Appliance {
            id: id.into(),
            registration_id: private_registration.into(),
            state: ApplianceState::Installed,
        };
        self.write(|db| {
            insert_registration(db, private_registration)?;
            db.execute(
                "INSERT INTO appliances(id,registration,data) VALUES(?,?,?)",
                params![id, private_registration, encode(&appliance)?],
            )?;
            Ok(appliance)
        })
    }
    pub fn transition_appliance(
        &self,
        id: &str,
        private_registration: &str,
        expected: ApplianceState,
        next: ApplianceState,
    ) -> Result<Appliance> {
        self.write(|db| {
            let data: String = db
                .query_row(
                    "SELECT data FROM appliances WHERE id=? AND registration=?",
                    params![id, private_registration],
                    |r| r.get(0),
                )
                .optional()?
                .ok_or(Error::Fenced)?;
            let mut appliance: Appliance = decode(&data)?;
            if appliance.state != expected {
                return Err(Error::Fenced);
            }
            use ApplianceState::*;
            if !matches!(
                (&expected, &next),
                (Installed | Stopped | Failed, Active)
                    | (Active, Stopped)
                    | (Installed | Active, Failed)
            ) {
                return Err(Error::Conflict("appliance transition"));
            }
            appliance.state = next;
            db.execute(
                "UPDATE appliances SET data=? WHERE id=?",
                params![encode(&appliance)?, id],
            )?;
            Ok(appliance)
        })
    }
}
