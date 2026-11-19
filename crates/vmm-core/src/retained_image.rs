//! Retained build outputs keep independent references to shared immutable extents.
use crate::{Core, Digest, Error, Result, Session, Store, image_copy::authorized_controller};
use rusqlite::{OptionalExtension, params};

pub struct RetainedImage {
    pub reference: String,
    pub digest: Digest,
    pub generation: String,
    pub build_request: String,
    pub build_receipt: Vec<u8>,
    pub logical_bytes: u64,
}

impl Store {
    /// The build must still own the published source reference at the point of retention.
    pub fn retain_build_output(
        &self,
        controller: &str,
        session: &Session,
        output: &RetainedImage,
    ) -> Result<()> {
        if output.reference.is_empty()
            || output.build_request.is_empty()
            || output.generation.is_empty()
            || output.build_receipt.is_empty()
        {
            return Err(Error::Invalid("retained build output"));
        }
        self.write(|db| {
            authorized_controller(db, session, controller)?;
            let source: Option<Digest> = db
                .query_row(
                    "SELECT digest FROM image_refs WHERE scope=? AND reference=?",
                    params![session.scope_id, output.reference],
                    |r| r.get(0),
                )
                .optional()?;
            if source != Some(output.digest) {
                return Err(Error::Fenced);
            }
            retain(db, controller, output)?;
            db.execute(
                "INSERT INTO image_provenance(scope,reference,build_receipt) VALUES(?,?,?)
                 ON CONFLICT(scope,reference) DO UPDATE SET build_receipt=excluded.build_receipt",
                params![session.scope_id, output.reference, output.build_receipt],
            )?;
            Ok(())
        })
    }
    pub fn list_retained_images(
        &self,
        controller: &str,
        session: &Session,
    ) -> Result<Vec<RetainedImage>> {
        self.read(|db| {
            let tx=db.unchecked_transaction()?;
            authorized_controller(&tx, session, controller)?;
            let mut q = tx.prepare_cached(
                "SELECT reference,digest,generation,build_request,build_receipt,logical_bytes FROM retained_images
                WHERE registration=? ORDER BY reference,build_request,digest",
            )?;
            let mut rows = q.query([controller])?;
            let mut images = Vec::new();
            while let Some(r) = rows.next()? {
                images.push(RetainedImage {
                    reference:r.get(0)?, digest:r.get(1)?, generation:r.get(2)?, build_request:r.get(3)?, build_receipt:r.get(4)?,
                    logical_bytes:u64::try_from(r.get::<_,i64>(5)?).map_err(|_|Error::Invalid("image size"))?,
                });
            }
            Ok(images)
        })
    }
}

impl Core {
    pub async fn remove_retained_image(
        &self,
        controller: &str,
        session: &Session,
        reference: &str,
        canonical: &str,
        generation: &str,
        expected: Digest,
    ) -> Result<()> {
        let _guard = self.store.effects.lock().await;
        self.store.write(|db| {
            authorized_controller(db, session, controller)?;
            let changed=db.execute(
                "DELETE FROM retained_images WHERE registration=? AND reference IN (?,?) AND generation=? AND digest=?",
                params![controller,reference,canonical,generation,expected],
            )?;
            if changed==0 {return Err(Error::Missing("retained image grant"));}
            Ok(())
        })?;
        self.collect_image(expected).await?;
        Ok(())
    }
}

fn retain(db: &rusqlite::Connection, controller: &str, output: &RetainedImage) -> Result<()> {
    let bytes = i64::try_from(output.logical_bytes).map_err(|_| Error::Invalid("image size"))?;
    db.execute(
                "INSERT INTO retained_images(registration,reference,build_request,digest,generation,build_receipt,logical_bytes)
                 VALUES(?,?,?,?,?,?,?) ON CONFLICT(registration,reference,digest,build_request) DO NOTHING",
                params![controller, output.reference, output.build_request, output.digest, output.generation, output.build_receipt, bytes],
            )?;
    let prior: Vec<u8> = db.query_row(
                "SELECT build_receipt FROM retained_images WHERE registration=? AND reference=? AND digest=? AND build_request=?",
                params![controller,output.reference,output.digest,output.build_request], |r| r.get(0),
            )?;
    if prior != output.build_receipt {
        return Err(Error::Conflict("request fingerprint"));
    }
    Ok(())
}

pub(crate) fn current(db: &rusqlite::Connection, session: &Session) -> Result<bool> {
    Ok(db.query_row("SELECT EXISTS(SELECT 1 FROM sessions WHERE id=? AND scope=? AND capability=? AND revoked=0)",
        params![session.id,session.scope_id,session.capability_hash],|r|r.get(0))?)
}

impl Store {
    pub fn session_is_current(&self, session: &Session) -> Result<bool> {
        self.read(|db| current(db, session))
    }

    pub fn published_build(&self, scope: &str, request: &str) -> Result<Option<Vec<u8>>> {
        self.read(|db| {
            Ok(db
                .query_row(
                    "SELECT receipt FROM published_builds WHERE scope=? AND request=?",
                    [scope, request],
                    |r| r.get(0),
                )
                .optional()?)
        })
    }
}

impl Core {
    pub async fn publish_build(
        &self,
        session: &Session,
        controller: Option<&str>,
        source: &crate::ImageSource,
        output: &RetainedImage,
        check_writer: impl FnOnce() -> Result<()> + Send,
    ) -> Result<crate::ImageArtifact> {
        if source.manifest_digest != output.digest
            || output.build_request.is_empty()
            || output.build_receipt.is_empty()
        {
            return Err(Error::Invalid("build output"));
        }
        self.publish_image(&session.scope_id, &output.reference, source, |db| {
            check_writer()?;
            if let Some(controller) = controller {
                authorized_controller(db, session, controller)?;
                retain(db, controller, output)?;
            } else {
                let scope = crate::storage::active(db, &session.scope_id)?;
                if !matches!(scope.origin, crate::Origin::Local) || !current(db, session)? {
                    return Err(Error::Fenced);
                }
            }
            db.execute(
                "INSERT INTO published_builds(scope,request,receipt) VALUES(?,?,?)",
                params![session.scope_id, output.build_request, output.build_receipt],
            )?;
            db.execute(
                "INSERT INTO image_provenance(scope,reference,build_receipt) VALUES(?,?,?)
                 ON CONFLICT(scope,reference) DO UPDATE SET build_receipt=excluded.build_receipt,copy_request=NULL",
                params![session.scope_id, output.reference, output.build_receipt],
            )?;
            Ok(())
        })
        .await
    }
}
