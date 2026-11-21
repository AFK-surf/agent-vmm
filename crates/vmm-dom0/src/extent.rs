//! The pool occupies an explicitly provisioned region of a backing disk.
//! Allocation metadata lives on the control filesystem, outside the pool.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use vmm_core::{CapacityDimension, Error, Result};
use vmm_l4hctl::{DiskExtent, HypervisorCtl};

pub const BLOCK_BYTES: u64 = 4 << 20;
pub const BLOCK_SECTORS: u64 = BLOCK_BYTES / 512;
pub const DEFAULT_EMERGENCY_BYTES: u64 = 10 << 30;
pub type Control = Arc<Mutex<dyn HypervisorCtl>>;

pub trait Headroom: Send + Sync {
    /// Physical free space on the filesystem that backs this pool.
    fn available_bytes(&self) -> Result<u64>;
}

pub struct FilesystemHeadroom(pub PathBuf);
impl Headroom for FilesystemHeadroom {
    fn available_bytes(&self) -> Result<u64> {
        let stat = rustix::fs::statvfs(&self.0).map_err(std::io::Error::from)?;
        stat.f_frsize
            .checked_mul(stat.f_bavail)
            .ok_or(Error::Invalid("free space"))
    }
}

/// Used when the pool lives on a dedicated block device whose host enforces physical headroom.
pub struct UnlimitedHeadroom;
impl Headroom for UnlimitedHeadroom {
    fn available_bytes(&self) -> Result<u64> {
        Ok(u64::MAX)
    }
}

pub struct Pool {
    pub(crate) db: Mutex<Connection>,
    ctl: Control,
    disk: u64,
    first: u64,
    end: u64,
    headroom: Arc<dyn Headroom>,
    emergency: u64,
}

fn sql_integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::Invalid("pool value exceeds SQLite integer"))
}

fn read_unsigned(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value: i64 = row.get(index)?;
    u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
}

fn capacity() -> Error {
    Error::Capacity(CapacityDimension::StorageHeadroom)
}

pub(crate) fn ctl_error(e: vmm_l4hctl::Error) -> Error {
    match e {
        vmm_l4hctl::Error::Busy => Error::Conflict("extent still attached"),
        other => std::io::Error::other(other).into(),
    }
}

impl Pool {
    pub fn open(
        path: &Path,
        ctl: Control,
        disk: u64,
        region: DiskExtent,
        headroom: Arc<dyn Headroom>,
        emergency: u64,
    ) -> Result<Self> {
        let end = region
            .sector
            .checked_add(region.sectors)
            .ok_or(Error::Invalid("pool region"))?;
        let disks = ctl
            .lock()
            .map_err(|_| Error::Interrupted)?
            .blk_disks()
            .map_err(ctl_error)?;
        if !region.sector.is_multiple_of(BLOCK_SECTORS)
            || !region.sectors.is_multiple_of(BLOCK_SECTORS)
            || region.sectors == 0
            || !disks.iter().any(|d| {
                d.id == disk && !d.failed && (!d.owned || region.sector > 0) && end <= d.sectors
            })
        {
            return Err(Error::Invalid("pool region"));
        }
        let marker = path.with_added_extension("pool");
        let marked = marker.try_exists()?;
        let existing = marked || path.try_exists()?;
        let mut db = if existing {
            Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?
        } else {
            Connection::open(path)?
        };
        if existing {
            let saved: (i64, i64, i64) =
                db.query_row("SELECT disk,first,end FROM pool WHERE id=1", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
            if saved
                != (
                    sql_integer(disk)?,
                    sql_integer(region.sector)?,
                    sql_integer(end)?,
                )
            {
                return Err(Error::Conflict("pool geometry changed"));
            }
            for table in ["objects", "extents"] {
                let exists: bool = db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?)",
                    [table],
                    |r| r.get(0),
                )?;
                if !exists {
                    return Err(Error::Missing("pool allocation table"));
                }
            }
        }
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS pool (id INTEGER PRIMARY KEY CHECK(id=1),disk INTEGER,first INTEGER,end INTEGER);
            CREATE TABLE IF NOT EXISTS objects (name TEXT PRIMARY KEY, bytes INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS extents (start INTEGER PRIMARY KEY, count INTEGER NOT NULL CHECK(count>0),
              object TEXT NOT NULL REFERENCES objects(name) ON DELETE CASCADE);")?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO pool VALUES(1,?1,?2,?3)",
            params![
                sql_integer(disk)?,
                sql_integer(region.sector)?,
                sql_integer(end)?
            ],
        )?;
        let matches: bool = tx.query_row(
            "SELECT disk=?1 AND first=?2 AND end=?3 FROM pool WHERE id=1",
            params![
                sql_integer(disk)?,
                sql_integer(region.sector)?,
                sql_integer(end)?
            ],
            |r| r.get(0),
        )?;
        if !matches {
            return Err(Error::Conflict("pool geometry changed"));
        }
        let mut prev = region.sector;
        for extent in extents(&tx, None)? {
            let next = extent
                .sector
                .checked_add(extent.sectors)
                .ok_or(Error::Invalid("saved extent"))?;
            if extent.sector < prev
                || next > end
                || !extent.sector.is_multiple_of(BLOCK_SECTORS)
                || !extent.sectors.is_multiple_of(BLOCK_SECTORS)
            {
                return Err(Error::Invalid("saved extent"));
            }
            prev = next;
        }
        let corrupt: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects o LEFT JOIN extents e ON e.object=o.name
                GROUP BY o.name HAVING o.bytes<=0 OR COALESCE(SUM(e.count),0)/8192 != ((o.bytes-1)/4194304)+1)
                OR EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [], |row| row.get(0))?;
        if corrupt {
            return Err(Error::Invalid("incomplete allocation metadata"));
        }
        tx.commit()?;
        if !marked {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)?;
            file.sync_all()?;
            std::fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
        }
        Ok(Self {
            db: Mutex::new(db),
            ctl,
            disk,
            first: region.sector,
            end,
            headroom,
            emergency,
        })
    }

    pub(crate) fn allocated_and_capacity_bytes(&self) -> Result<(u64, u64)> {
        let db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let allocated = db.query_row("SELECT COALESCE(SUM(count),0) FROM extents", [], |r| {
            read_unsigned(r, 0)
        })?;
        let bytes = |sectors: u64| {
            sectors
                .checked_mul(512)
                .ok_or(Error::Invalid("pool byte count"))
        };
        Ok((bytes(allocated)?, bytes(self.end - self.first)?))
    }

    pub fn disk(&self) -> u64 {
        self.disk
    }

    pub fn lookup(&self, name: &str) -> Result<Vec<DiskExtent>> {
        let db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let ext = extents(&db, Some(name))?;
        if ext.is_empty() {
            return Err(Error::Missing("pool object"));
        }
        Ok(ext)
    }

    pub(crate) fn has_object(&self, name: &str) -> Result<bool> {
        let db = self.db.lock().map_err(|_| Error::Interrupted)?;
        Ok(db.query_row(
            "SELECT EXISTS(SELECT 1 FROM objects WHERE name=?)",
            [name],
            |r| r.get(0),
        )?)
    }

    /// Atomically promote a pool-allocated `stage/<token>` object into `image/<hex>`
    /// in a single `Immediate` transaction (since `extents` has no `ON UPDATE CASCADE`).
    /// Returns `Ok(true)` if promoted, or `Ok(false)` if `image/<hex>` was already
    /// installed (caller should then `discard(stage_obj)`).
    pub(crate) fn promote_staged_image(
        &self,
        stage_obj: &str,
        image_obj: &str,
        defaults: &crate::image::Defaults,
        expected_bytes: u64,
    ) -> Result<bool> {
        let mut db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let already_installed: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM images i JOIN objects o ON o.name=i.object WHERE i.object=?1)",
            [image_obj],
            |r| r.get(0),
        )?;
        if already_installed {
            tx.execute("DELETE FROM properties WHERE id=?1", [stage_obj])?;
            tx.commit()?;
            return Ok(false);
        }
        let bytes: u64 = tx
            .query_row("SELECT bytes FROM objects WHERE name=?1", [stage_obj], |r| {
                read_unsigned(r, 0)
            })
            .optional()?
            .ok_or(Error::Missing("staged image"))?;
        if bytes != expected_bytes || bytes == 0 || !bytes.is_multiple_of(BLOCK_BYTES) {
            return Err(Error::Invalid("staged image length"));
        }
        tx.execute(
            "INSERT INTO objects(name,bytes) VALUES(?1,?2)",
            params![image_obj, sql_integer(bytes)?],
        )?;
        tx.execute(
            "UPDATE extents SET object=?1 WHERE object=?2",
            params![image_obj, stage_obj],
        )?;
        tx.execute("DELETE FROM objects WHERE name=?1", [stage_obj])?;
        tx.execute(
            "INSERT INTO images(object,defaults) VALUES(?1,?2)",
            params![image_obj, serde_json::to_string(defaults)?],
        )?;
        tx.execute("DELETE FROM properties WHERE id=?1", [stage_obj])?;
        tx.commit()?;
        Ok(true)
    }

    pub fn allocate(&self, name: &str, bytes: u64) -> Result<Vec<DiskExtent>> {
        self.reserve(name, bytes, false)
    }

    pub(crate) fn allocate_sparse(&self, name: &str, bytes: u64) -> Result<Vec<DiskExtent>> {
        self.reserve(name, bytes, true)
    }

    fn reserve(&self, name: &str, bytes: u64, sparse: bool) -> Result<Vec<DiskExtent>> {
        if bytes == 0 || name.is_empty() {
            return Err(Error::Invalid("allocation"));
        }
        let mut db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<u64> = tx
            .query_row("SELECT bytes FROM objects WHERE name=?", [name], |r| {
                read_unsigned(r, 0)
            })
            .optional()?;
        if let Some(old) = old {
            if old != bytes {
                return Err(Error::Conflict("pool object resized"));
            }
            return extents(&tx, Some(name));
        }
        let size = bytes
            .checked_next_multiple_of(BLOCK_BYTES)
            .ok_or_else(capacity)?;
        if self
            .headroom
            .available_bytes()?
            .saturating_sub(self.emergency)
            < if sparse { BLOCK_BYTES } else { size }
        {
            return Err(capacity());
        }
        let used = extents(&tx, None)?;
        let mut left = size / 512;
        let mut start = self.first;
        let mut allocated = Vec::new();
        for e in used.into_iter().chain([DiskExtent {
            sector: self.end,
            sectors: 0,
        }]) {
            let take = left.min(e.sector - start);
            if take > 0 {
                allocated.push(DiskExtent {
                    sector: start,
                    sectors: take,
                });
                left -= take;
            }
            if left == 0 {
                break;
            }
            start = e.sector + e.sectors;
        }
        if left != 0 || allocated.len() > 4096 {
            return Err(capacity());
        }
        tx.execute(
            "INSERT INTO objects VALUES(?,?)",
            params![name, sql_integer(bytes)?],
        )?;
        for e in &allocated {
            tx.execute(
                "INSERT INTO extents VALUES(?,?,?)",
                params![sql_integer(e.sector)?, sql_integer(e.sectors)?, name],
            )?;
        }
        tx.commit()?;
        Ok(allocated)
    }

    /// The caller holds the lifecycle lock and supplies a complete inventory,
    /// including pending objects. A missing allocation fences all reclamation.
    pub fn reconcile(&self, referenced: &BTreeSet<String>) -> Result<Vec<String>> {
        let db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let allocated = db
            .prepare("SELECT name FROM objects")?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<BTreeSet<String>>>()?;
        if !referenced.is_subset(&allocated) {
            return Err(Error::Missing("referenced pool allocation"));
        }
        let orphaned = allocated
            .difference(referenced)
            .cloned()
            .collect::<Vec<_>>();
        drop(db);
        for name in &orphaned {
            self.discard(name)?;
        }
        Ok(orphaned)
    }

    pub fn discard(&self, name: &str) -> Result<u64> {
        let mut db = self.db.lock().map_err(|_| Error::Interrupted)?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let allocated = extents(&tx, Some(name))?;
        let mut ctl = self.ctl.lock().map_err(|_| Error::Interrupted)?;
        let attached = ctl.attached_extents(self.disk).map_err(ctl_error)?;
        if allocated.iter().any(|a| {
            attached.iter().any(|b| {
                a.sector < b.sector.saturating_add(b.sectors) && b.sector < a.sector + a.sectors
            })
        }) {
            return Err(Error::Conflict("extent still attached"));
        }
        let mut bytes = 0;
        for e in allocated {
            ctl.blk_discard(self.disk, e).map_err(ctl_error)?;
            bytes += e.sectors * 512;
        }
        tx.execute("DELETE FROM objects WHERE name=?", [name])?;
        tx.commit()?;
        Ok(bytes)
    }
}

fn extents(db: &Connection, name: Option<&str>) -> Result<Vec<DiskExtent>> {
    let mut query =
        db.prepare("SELECT start,count FROM extents WHERE ?1 IS NULL OR object=?1 ORDER BY start")?;
    Ok(query
        .query_map([name], |r| {
            Ok(DiskExtent {
                sector: read_unsigned(r, 0)?,
                sectors: read_unsigned(r, 1)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_domain_is_nonnegative_signed_64_bit() -> Result<()> {
        let max = i64::MAX as u64;
        assert_eq!(sql_integer(max)?, i64::MAX);
        assert!(sql_integer(max + 1).is_err());
        let db = Connection::open_in_memory()?;
        assert!(
            db.query_row("SELECT -1", [], |r| read_unsigned(r, 0))
                .is_err()
        );
        assert_eq!(
            db.query_row("SELECT ?", [i64::MAX], |r| read_unsigned(r, 0))?,
            max
        );
        Ok(())
    }
}
