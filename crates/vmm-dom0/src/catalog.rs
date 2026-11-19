use crate::extent::Pool;
use crate::keys::{KeySealer, disk_key};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::MutexGuard;
use vmm_core::{
    BootIdentity, Digest, Error, Result, SandboxDefinition, SandboxKey, Scope, VMIncarnation,
};
use vmm_guest_proto::Secret;
use zeroize::Zeroizing;

pub(crate) fn image_id(digest: Digest) -> String {
    format!("image/{}", hex::encode(digest))
}
pub(crate) fn volume_id(scope: &str, id: &str) -> Result<String> {
    Ok(serde_json::to_string(&("volume", scope, id))?)
}
pub(crate) fn overlay_id(key: &SandboxKey, id: &str) -> Result<String> {
    Ok(serde_json::to_string(&("overlay", key, id))?)
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedBoot {
    pub identity: BootIdentity,
    pub name: String,
    pub slot: u32,
    pub vm: Option<VMIncarnation>,
    pub encrypted_token: Vec<u8>,
    pub definition: SandboxDefinition,
    pub scope: Scope,
    pub disks: Vec<String>,
    pub writers: Vec<String>,
    #[serde(default)]
    pub retiring: bool,
    pub retired: bool,
    pub helper: bool,
    pub share_ports: std::collections::BTreeMap<String, u16>,
}

pub(crate) struct Catalog<'a>(pub &'a Pool);

impl Catalog<'_> {
    fn db(&self) -> Result<MutexGuard<'_, rusqlite::Connection>> {
        self.0.db.lock().map_err(|_| Error::Interrupted)
    }

    pub fn init(&self) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        tx.execute_batch("CREATE TABLE IF NOT EXISTS sealed_disks(
            object TEXT PRIMARY KEY REFERENCES objects(name) ON DELETE CASCADE,
            secret BLOB NOT NULL, format_owner TEXT, ready INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS properties(id TEXT PRIMARY KEY,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS images(object TEXT PRIMARY KEY REFERENCES objects(name) ON DELETE CASCADE,defaults TEXT NOT NULL);")?;
        if !tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('sealed_disks') WHERE name='dirty')",
            [],
            |r| r.get::<_, bool>(0),
        )? {
            tx.execute_batch(
                "ALTER TABLE sealed_disks ADD COLUMN dirty INTEGER NOT NULL DEFAULT 0;
                UPDATE sealed_disks SET dirty=1 WHERE ready=1 OR format_owner IS NOT NULL;",
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn needs_recovery(&self, id: &str) -> Result<bool> {
        Ok(self.db()?.query_row(
            "SELECT dirty OR NOT ready FROM sealed_disks WHERE object=?",
            [id],
            |r| r.get(0),
        )?)
    }

    pub fn set_dirty(&self, writers: &[String], dirty: bool) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        for id in writers {
            if tx.execute(
                "UPDATE sealed_disks SET dirty=? WHERE object=?",
                params![dirty, id],
            )? != 1
            {
                return Err(Error::Missing("encrypted disk"));
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn get<T: DeserializeOwned>(&self, id: &str) -> Result<Option<T>> {
        let value: Option<String> = self
            .db()?
            .query_row("SELECT data FROM properties WHERE id=?", [id], |r| r.get(0))
            .optional()?;
        Ok(value.map(|v| serde_json::from_str(&v)).transpose()?)
    }
    pub fn put<T: Serialize>(&self, id: &str, value: &T) -> Result<()> {
        self.db()?.execute(
            "INSERT INTO properties VALUES(?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
            params![id, serde_json::to_string(value)?],
        )?;
        Ok(())
    }
    pub fn delete(&self, id: &str) -> Result<()> {
        self.db()?
            .execute("DELETE FROM properties WHERE id=?", [id])?;
        Ok(())
    }
    pub fn staged_tokens(&self) -> Result<Vec<String>> {
        let db = self.db()?;
        Ok(db
            .prepare("SELECT substr(id,7) FROM properties WHERE id GLOB 'stage/*'")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn revoke_route(
        &self,
        key: &str,
        fence: i64,
        local: &std::collections::BTreeMap<String, vmm_core::ServiceRoute>,
    ) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        for (id, data) in [
            (key, serde_json::to_string(&fence)?),
            ("routes", serde_json::to_string(local)?),
        ] {
            tx.execute(
                "INSERT OR REPLACE INTO properties(id,data) VALUES(?,?)",
                params![id, data],
            )?;
        }
        Ok(tx.commit()?)
    }
    pub fn boots(&self) -> Result<Vec<SavedBoot>> {
        let db = self.db()?;
        let mut stmt = db.prepare("SELECT data FROM properties WHERE id LIKE 'boot/%'")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|v| Ok(serde_json::from_str(&v?)?)).collect()
    }
    pub fn boot(&self, identity: &BootIdentity) -> Result<SavedBoot> {
        let v: SavedBoot = self
            .get(&format!("boot/{}", identity.instance_id))?
            .ok_or(Error::Missing("boot"))?;
        if v.identity != *identity {
            return Err(Error::Fenced);
        }
        Ok(v)
    }
    pub fn save(&self, boot: &SavedBoot) -> Result<()> {
        self.put(&format!("boot/{}", boot.identity.instance_id), boot)
    }
    pub fn ensure_key(&self, object: &str, sealer: &dyn KeySealer) -> Result<()> {
        let db = self.db()?;
        if db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sealed_disks WHERE object=?)",
            [object],
            |r| r.get(0),
        )? {
            return Ok(());
        }
        let secret = disk_key()?;
        db.execute(
            "INSERT INTO sealed_disks(object,secret) VALUES(?,?)",
            params![object, sealer.seal(object.as_bytes(), secret.as_bytes())?],
        )?;
        Ok(())
    }
    pub fn disk_secret(&self, object: &str, sealer: &dyn KeySealer) -> Result<(Secret, bool)> {
        let (sealed, fresh): (Vec<u8>, bool) = self.db()?.query_row(
            "SELECT secret,ready=0 AND format_owner IS NULL FROM sealed_disks WHERE object=?",
            [object],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        Ok((
            secret_text(sealer.open(object.as_bytes(), &sealed)?)?,
            fresh,
        ))
    }
    pub fn format_issued(&self, objects: &[String], instance: &str) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        for id in objects {
            tx.execute("UPDATE sealed_disks SET format_owner=? WHERE object=? AND ready=0 AND format_owner IS NULL",params![instance,id])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn confirm(&self, objects: &[String]) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        for id in objects {
            tx.execute(
                "UPDATE sealed_disks SET ready=1,format_owner=NULL WHERE object=?",
                [id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn defaults(&self, object: &str) -> Result<Option<crate::image::Defaults>> {
        let value: Option<String> = self
            .db()?
            .query_row(
                "SELECT defaults FROM images WHERE object=?",
                [object],
                |r| r.get(0),
            )
            .optional()?;
        Ok(value.map(|v| serde_json::from_str(&v)).transpose()?)
    }
    pub fn store_image(&self, object: &str, defaults: &crate::image::Defaults) -> Result<()> {
        self.db()?.execute(
            "INSERT INTO images VALUES(?,?)",
            params![object, serde_json::to_string(defaults)?],
        )?;
        Ok(())
    }
}
pub(crate) fn secret_text(bytes: Zeroizing<Vec<u8>>) -> Result<Secret> {
    Ok(Secret::new(
        std::str::from_utf8(&bytes).map_err(|_| Error::Invalid("sealed secret encoding"))?,
    ))
}

impl Catalog<'_> {
    pub fn transient_objects(&self) -> Result<Vec<String>> {
        let db = self.db()?;
        Ok(db
            .prepare("SELECT name FROM objects WHERE name LIKE 'build/%'")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }
}
