//! An image copy publishes a reference to existing immutable content and its receipt together.
use crate::{
    Digest, Error, ManagedPhase, Result, Scope, ScopeState, Session, Store,
    authority::registration,
    registration_policy,
    store::{decode, encode},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum CopySource {
    Allocation { id: String, generation: u64 },
    Retained { build_request: String },
}

pub struct CopyAuthority<'a> {
    pub session: &'a Session,
    pub registration: &'a str,
    pub target: &'a str,
    pub generation: u64,
    pub source: &'a CopySource,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ImageCopyReceipt {
    pub source: CopySource,
    pub reference: String,
    pub digest: Digest,
    pub fingerprint: Digest,
    pub generation: String,
    pub build_receipt: Option<Vec<u8>>,
    pub request_id: String,
}

pub(crate) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS published_builds(
    scope TEXT REFERENCES scopes(id) ON DELETE CASCADE,request TEXT NOT NULL,receipt BLOB NOT NULL,PRIMARY KEY(scope,request));
    CREATE TABLE IF NOT EXISTS retained_images(
    registration TEXT NOT NULL REFERENCES registrations(id), reference TEXT NOT NULL,
    build_request TEXT NOT NULL, digest BLOB NOT NULL REFERENCES images(digest),
    generation TEXT NOT NULL, build_receipt BLOB NOT NULL, logical_bytes INTEGER NOT NULL,
    PRIMARY KEY(registration,reference,digest,build_request), UNIQUE(registration,generation));
    CREATE TABLE IF NOT EXISTS image_provenance(
        scope TEXT NOT NULL, reference TEXT NOT NULL, build_receipt BLOB, copy_request TEXT,
        PRIMARY KEY(scope,reference), FOREIGN KEY(scope,reference) REFERENCES image_refs ON DELETE CASCADE)";

fn allocation(db: &Connection, reg: &str, id: &str, generation: u64, ready: bool) -> Result<Scope> {
    let raw: String = db
        .query_row(
            "SELECT data FROM scopes WHERE registration=? AND COALESCE(remote_id,id)=?",
            [reg, id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or(Error::Missing("allocation"))?;
    let scope: Scope = decode(&raw)?;
    if scope.generation != generation || (ready && scope.state != ScopeState::Active) {
        return Err(Error::Fenced);
    }
    Ok(scope)
}

pub(crate) fn authorized_controller(
    db: &Connection,
    session: &Session,
    controller: &str,
) -> Result<()> {
    let current: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions s JOIN scopes e ON s.scope=e.id
                WHERE s.id=? AND s.scope=? AND s.capability=? AND s.revoked=0 AND e.registration=?)",
            params![session.id, session.scope_id, session.capability_hash, controller],
            |r| r.get(0),
        )?;
    if !current
        || registration(db, controller)?.draining
        || registration_policy::load(db, controller)?
            .is_some_and(|p| p.phase != ManagedPhase::Enabled)
    {
        return Err(Error::Fenced);
    }
    Ok(())
}

impl CopyAuthority<'_> {
    fn authorize(&self, db: &Connection, ready: bool) -> Result<Option<String>> {
        authorized_controller(db, self.session, self.registration)?;
        let target = allocation(db, self.registration, self.target, self.generation, ready)?;
        if target.id != self.session.scope_id {
            return Err(Error::Fenced);
        }
        match self.source {
            CopySource::Allocation { id, generation } => Ok(Some(
                allocation(db, self.registration, id, *generation, ready)?.id,
            )),
            CopySource::Retained { build_request } if !build_request.is_empty() => Ok(None),
            _ => Err(Error::Invalid("retained build request")),
        }
    }
}

fn previous(db: &Connection, scope: &str, request: &str) -> Result<Option<ImageCopyReceipt>> {
    let bytes: Option<Vec<u8>> = db.query_row(
        "SELECT response FROM receipts WHERE domain='runtime-copy-image' AND scope=? AND request=? AND state='succeeded'",
        [scope, request], |r| r.get(0),
    ).optional()?;
    bytes
        .map(|b| serde_json::from_slice(&b).map_err(Error::from))
        .transpose()
}

pub struct ImageGrantMetadata {
    pub generation: String,
    pub build_receipt: Option<Vec<u8>>,
    pub copy_receipt: Option<ImageCopyReceipt>,
}
impl Store {
    pub fn image_grant_metadata(
        &self,
        scope: &str,
        reference: &str,
        expected: Digest,
    ) -> Result<ImageGrantMetadata> {
        self.read(|db|{
            let snapshot = db.unchecked_transaction()?;
            let (generation,build_receipt,request): (String,Option<Vec<u8>>,Option<String>) = snapshot.query_row(
                "SELECT generation, build_receipt, copy_request FROM image_refs JOIN image_ref_generations USING(scope,reference)
                 LEFT JOIN image_provenance USING(scope,reference) WHERE scope=? AND reference=? AND digest=?",
                params![scope,reference,expected], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
            ).optional()?.ok_or(Error::Missing("image reference"))?;
            let copy_receipt = match request {
                Some(id) => previous(&snapshot,scope,&id)?,
                None => None,
            };
            Ok(ImageGrantMetadata { generation, build_receipt, copy_receipt })
        })
    }

    pub async fn copy_image_grant(
        &self,
        auth: &CopyAuthority<'_>,
        request: &str,
        fingerprint: Digest,
        reference: &str,
        canonical: &str,
        expected: Digest,
    ) -> Result<ImageCopyReceipt> {
        if request.is_empty() || reference.is_empty() || canonical.is_empty() {
            return Err(Error::Invalid("copy request"));
        }
        let _guard = self.effects.lock().await;
        self.write(|db| {
            let source = auth.authorize(db, true)?;
            let scope = &auth.session.scope_id;
            if let Some(prior) = previous(db, scope, request)? {
                if prior.fingerprint != fingerprint || &prior.source != auth.source {
                    return Err(Error::Conflict("request fingerprint"));
                }
                return Ok(prior);
            }
            let (name, digest, build_receipt): (String, Digest, Option<Vec<u8>>) = match (source, auth.source) {
                (Some(scope), _) => db.query_row(
                    "SELECT reference,digest,build_receipt FROM image_refs LEFT JOIN image_provenance USING(scope,reference) WHERE scope=?1 AND reference IN (?2,?3)
                     ORDER BY reference=?2 DESC LIMIT 1",
                    params![scope, reference, canonical], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                ).optional()?.ok_or(Error::Missing("scope image reference"))?,
                (None, CopySource::Retained { build_request }) => db.query_row(
                    "SELECT reference,digest,build_receipt FROM retained_images WHERE registration=? AND build_request=?
                     AND reference IN (?,?) AND digest=?",
                    params![auth.registration, build_request, reference, canonical, expected], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                ).optional()?.ok_or(Error::Missing("retained image"))?,
                _ => return Err(Error::Fenced),
            };
            if digest != expected {
                return Err(Error::Conflict("copy digest"));
            }
            crate::storage::artifact(db, digest)?;
            db.execute(
                "INSERT INTO image_refs VALUES(?,?,?) ON CONFLICT(scope,reference) DO NOTHING",
                params![scope, name, digest],
            )?;
            let target_digest: Digest = db.query_row(
                "SELECT digest FROM image_refs WHERE scope=? AND reference=?",
                params![scope, name], |r| r.get(0),
            )?;
            if target_digest != digest {
                return Err(Error::Conflict("copy digest"));
            }
            let generation = db.query_row(
                "SELECT generation FROM image_ref_generations WHERE scope=? AND reference=?",
                params![scope,name], |r| r.get(0),
            )?;
            db.execute(
                "INSERT INTO image_provenance VALUES(?,?,?,?) ON CONFLICT(scope,reference) DO UPDATE
                 SET copy_request=excluded.copy_request,build_receipt=excluded.build_receipt",
                params![scope,name,build_receipt,request],
            )?;
            let receipt = ImageCopyReceipt { source: auth.source.clone(), reference: name, digest, fingerprint,
                generation, build_receipt, request_id: request.to_owned() };
            db.execute(
                "INSERT INTO receipts VALUES('runtime-copy-image',?,?,?,'succeeded',?)",
                params![scope, request, fingerprint, encode(&receipt)?.as_bytes()],
            )?;
            Ok(receipt)
        })
    }

    pub fn observe_image_copy(
        &self,
        auth: &CopyAuthority<'_>,
        request: &str,
        fingerprint: Digest,
    ) -> Result<ImageCopyReceipt> {
        self.read(|db| {
            let tx = db.unchecked_transaction()?;
            auth.authorize(&tx, false)?;
            let receipt = previous(&tx, &auth.session.scope_id, request)?
                .ok_or(Error::Missing("image operation"))?;
            if receipt.fingerprint != fingerprint || &receipt.source != auth.source {
                return Err(Error::Conflict("request fingerprint"));
            }
            Ok(receipt)
        })
    }
}
