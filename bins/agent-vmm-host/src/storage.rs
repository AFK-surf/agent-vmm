//! Check the on-disk generation before vmm-core acquires ownership or selects WAL.
use rusqlite::{Connection, OpenFlags};
use std::{
    fs,
    io::{ErrorKind, Write},
    path::Path,
};
use tempfile::NamedTempFile;
use vmm_core::{Error, Result, Store};

const APPLICATION_ID: i64 = 0x564d_4d43;
const CURRENT_SCHEMA: i64 = 2;
const FAILURE_FILE: &str = "host-start-failure.json";

pub fn open_database(path: &Path) -> Result<Store> {
    let directory = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if path.file_name().is_some_and(|name| name == FAILURE_FILE) {
        return Err(Error::Invalid("database path names the startup report"));
    }
    let failure = directory.join(FAILURE_FILE);
    if let Some(observed) = incompatible_version(path)? {
        let mut report = NamedTempFile::new_in(directory)?;
        write!(
            report,
            "{{\"code\":\"STATE_SCHEMA_INCOMPATIBLE\",\"dataSchema\":{observed},\"currentSchema\":{CURRENT_SCHEMA}}}"
        )?;
        report.as_file().sync_all()?;
        report.persist(&failure).map_err(|e| e.error)?;
        fs::File::open(directory)?.sync_all()?;
        return Err(Error::Conflict("STATE_SCHEMA_INCOMPATIBLE"));
    }
    let store = Store::open(path)?;
    match fs::remove_file(failure) {
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        result => result?,
    }
    Ok(store)
}

fn incompatible_version(path: &Path) -> Result<Option<i64>> {
    if !path.try_exists()? {
        return Ok(None);
    }
    let mut db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let tx = db.transaction()?;
    let has_tables: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*')",
        [], |row| row.get(0),
    )?;
    if !has_tables {
        return Ok(None);
    }
    let app: i64 = tx.pragma_query_value(None, "application_id", |r| r.get(0))?;
    if app != APPLICATION_ID {
        return Ok(Some(0));
    }
    let version: i64 = tx.pragma_query_value(None, "user_version", |r| r.get(0))?;
    Ok((!(1..=CURRENT_SCHEMA).contains(&version)).then_some(version.max(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_foreign_and_unsupported_databases_without_modifying_them() {
        for (app, version, observed) in [
            (0, 0, 0),
            (77, 1, 0),
            (APPLICATION_ID, 0, 0),
            (APPLICATION_ID, 3, 3),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("host.sqlite");
            let db = Connection::open(&path).unwrap();
            db.execute_batch("CREATE TABLE schema_migrations(version INTEGER); INSERT INTO schema_migrations VALUES (35)").unwrap();
            db.pragma_update(None, "application_id", app).unwrap();
            db.pragma_update(None, "user_version", version).unwrap();
            drop(db);
            let before = fs::read(&path).unwrap();
            for _ in 0..2 {
                assert!(matches!(
                    open_database(&path),
                    Err(Error::Conflict("STATE_SCHEMA_INCOMPATIBLE"))
                ));
                assert_eq!(fs::read(&path).unwrap(), before);
                assert_eq!(
                    fs::read_to_string(temp.path().join(FAILURE_FILE)).unwrap(),
                    format!(
                        "{{\"code\":\"STATE_SCHEMA_INCOMPATIBLE\",\"dataSchema\":{observed},\"currentSchema\":2}}"
                    )
                );
                assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
            }
        }
    }

    #[test]
    fn opens_fresh_and_supported_databases_and_only_clears_report_after_success() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("host.sqlite");
        let failure = temp.path().join(FAILURE_FILE);
        fs::write(&failure, "stale").unwrap();
        let store = open_database(&path).unwrap();
        assert!(!failure.exists());
        fs::write(&failure, "preserve").unwrap();
        assert!(open_database(&path).is_err());
        assert_eq!(fs::read(&failure).unwrap(), b"preserve");
        drop(store);
        drop(open_database(&path).unwrap());
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        assert_eq!(
            db.pragma_query_value(None, "application_id", |r| r.get::<_, i64>(0))
                .unwrap(),
            APPLICATION_ID
        );
        assert_eq!(
            db.pragma_query_value(None, "user_version", |r| r.get::<_, i64>(0))
                .unwrap(),
            CURRENT_SCHEMA
        );
        assert!(!failure.exists());
    }
}
