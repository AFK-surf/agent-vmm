use crate::{authority::registration_from_row, lifecycle::sandbox_from_row, store::decode, *};
use rusqlite::{Params, Row, params};

fn sql_limit(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

impl Store {
    fn page<T>(
        &self,
        sql: &str,
        params: impl Params,
        row: impl Fn(&Row<'_>) -> Result<T>,
    ) -> Result<Vec<T>> {
        self.read(|db| {
            let mut statement = db.prepare_cached(sql)?;
            let mut rows = statement.query(params)?;
            let mut page = Vec::new();
            while let Some(item) = rows.next()? {
                page.push(row(item)?);
            }
            Ok(page)
        })
    }

    pub fn list_registrations(
        &self,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Registration>> {
        self.page(
            "SELECT id,epoch,connected,draining FROM registrations WHERE id>? ORDER BY id LIMIT ?",
            params![after_id.unwrap_or_default(), sql_limit(limit)],
            |row| Ok(registration_from_row(row)?),
        )
    }

    /// Bounded inventory generations for whole-host impact observations, including private owners.
    pub fn registration_inventory_watermarks(&self, limit: usize) -> Result<Vec<(String, u64)>> {
        self.page(
            "SELECT id,inventory_watermark FROM registrations ORDER BY id LIMIT ?",
            [sql_limit(limit)],
            |row| {
                let watermark: i64 = row.get(1)?;
                let watermark =
                    u64::try_from(watermark).map_err(|_| Error::Invalid("inventory watermark"))?;
                Ok((row.get(0)?, watermark))
            },
        )
    }

    /// Return at most one indexed page of registration IDs, including cursor positions
    /// occupied by private appliances. The operator filters those positions from its response.
    pub fn operator_registration_page(
        &self,
        after: &str,
        limit: usize,
    ) -> Result<Vec<(Registration, bool, bool)>> {
        self.page(
            "SELECT r.id,r.epoch,r.connected,r.draining,
             EXISTS(SELECT 1 FROM appliances a WHERE a.registration=r.id),
             EXISTS(SELECT 1 FROM operator_registration_disposals d WHERE d.registration=r.id
                    UNION ALL SELECT 1 FROM managed_registrations m WHERE m.id=r.id AND json_extract(m.data,'$.phase')='Revoked')
             FROM registrations r WHERE r.id>? ORDER BY r.id LIMIT ?",
            params![after, sql_limit(limit)],
            |row| Ok((registration_from_row(row)?, row.get(4)?, row.get(5)?)),
        )
    }

    pub fn list_scopes(&self, after_id: Option<&str>, limit: usize) -> Result<Vec<Scope>> {
        self.page(
            "SELECT data FROM scopes WHERE id>? ORDER BY id LIMIT ?",
            params![after_id.unwrap_or_default(), sql_limit(limit)],
            |row| decode(&row.get::<_, String>(0)?),
        )
    }

    /// The local operator's resource view excludes the runtime's system namespaces
    /// and the private registrations created for managed appliances.
    pub fn list_operator_scopes(&self, after: &str, limit: usize) -> Result<Vec<Scope>> {
        self.page(
            "SELECT s.data FROM scopes s WHERE s.id>? AND s.id NOT LIKE 'system.%'
             AND NOT EXISTS (SELECT 1 FROM appliances a WHERE a.registration=s.registration)
             ORDER BY s.id LIMIT ?",
            params![after, sql_limit(limit)],
            |row| decode(&row.get::<_, String>(0)?),
        )
    }

    /// The cursor is the scope-local image reference. Several references can share one artifact.
    pub fn list_images(
        &self,
        scope_id: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, ImageArtifact)>> {
        self.page(
            "SELECT r.reference,i.artifact FROM image_refs r JOIN images i ON i.digest=r.digest
             WHERE r.scope=? AND r.reference>? AND i.phase='ready' ORDER BY r.reference LIMIT ?",
            params![scope_id, after_id.unwrap_or_default(), sql_limit(limit)],
            |row| Ok((row.get(0)?, decode(&row.get::<_, String>(1)?)?)),
        )
    }

    pub fn list_volumes(
        &self,
        scope_id: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Volume>> {
        self.page(
            "SELECT data FROM volumes WHERE scope=? AND id>? AND phase='ready' ORDER BY id LIMIT ?",
            params![scope_id, after_id.unwrap_or_default(), sql_limit(limit)],
            |row| decode(&row.get::<_, String>(0)?),
        )
    }

    pub fn list_sandboxes(
        &self,
        scope_id: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Sandbox>> {
        self.page(
            "SELECT definition,phase,boot,vm,exit_reason FROM sandboxes
             WHERE scope=? AND id>? AND phase!='deleted' ORDER BY id LIMIT ?",
            params![scope_id, after_id.unwrap_or_default(), sql_limit(limit)],
            sandbox_from_row,
        )
    }
}
