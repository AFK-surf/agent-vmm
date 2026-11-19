//! The schema of the Host database, read without the Host owner lock.

use crate::error::{Result, err};
use crate::{CURRENT_SCHEMA, DATABASE_APPLICATION_ID, MINIMUM_SCHEMA};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataSchema {
    /// No database, or a database without tables.
    Fresh,
    Version(u32),
    /// A database that this Host generation did not create.
    Foreign,
}

impl DataSchema {
    pub fn number(self) -> u32 {
        match self {
            Self::Version(version) => version,
            Self::Fresh | Self::Foreign => 0,
        }
    }
}

pub const FOREIGN_MESSAGE: &str = "the existing Host database belongs to an earlier Agent VMM Host generation; keep the current Host or reset this installation";

pub fn read(path: &Path) -> Result<DataSchema> {
    match std::fs::metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(DataSchema::Fresh),
        Err(error) => return Err(error.into()),
        Ok(_) => {}
    }
    let query = || -> rusqlite::Result<(i64, i64, i64)> {
        let database = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        database.busy_timeout(Duration::from_secs(2))?;
        let tables = database.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'", [], |row| row.get(0))?;
        let application = database.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let version = database.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        Ok((tables, application, version))
    };
    let (tables, application, version) = query().map_err(|error| err(error.to_string()))?;
    if tables == 0 {
        return Ok(DataSchema::Fresh);
    }
    if application != i64::from(DATABASE_APPLICATION_ID) {
        return Ok(DataSchema::Foreign);
    }
    match u32::try_from(version) {
        Ok(version) if version > 0 => Ok(DataSchema::Version(version)),
        _ => Err(err("existing Host database has no schema version; keep the data or reset this installation")),
    }
}

// Only the macOS maintenance uses this item.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
/// The refusal message when a target with the schema range `[minimum, current]` cannot open `data`.
pub fn refusal(data: DataSchema, minimum: u32, current: u32) -> Option<String> {
    let version = match data {
        DataSchema::Fresh => 0,
        DataSchema::Version(version) => version,
        DataSchema::Foreign => return Some(FOREIGN_MESSAGE.into()),
    };
    if version == current && current > 0 {
        return None;
    }
    (minimum == 0 || current < minimum || version != 0 && (version < minimum || version > current)).then(|| {
        format!("host database schema {version} cannot migrate to schema {current} (minimum {minimum}); keep the current Host or reset this installation")
    })
}

pub fn version_report() -> serde_json::Value {
    let release = crate::release_id();
    serde_json::json!({
        "contractVersion": crate::CONTRACT_VERSION,
        "component": "agent-vmm-host",
        "version": release,
        "release_id": release,
        "currentSchema": CURRENT_SCHEMA,
        "minimumSchema": MINIMUM_SCHEMA,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_target_must_open_the_existing_schema() {
        assert_eq!(refusal(DataSchema::Fresh, 1, 1), None);
        assert_eq!(refusal(DataSchema::Version(1), 1, 1), None);
        assert_eq!(refusal(DataSchema::Version(2), 1, 3), None);
        assert!(refusal(DataSchema::Version(37), 1, 1).is_some());
        assert!(refusal(DataSchema::Version(1), 2, 3).is_some());
        assert_eq!(refusal(DataSchema::Foreign, 1, 1).as_deref(), Some(FOREIGN_MESSAGE));
    }

    #[test]
    fn this_release_opens_the_data_of_each_schema_in_its_range() {
        for version in MINIMUM_SCHEMA..=CURRENT_SCHEMA {
            assert_eq!(refusal(DataSchema::Version(version), MINIMUM_SCHEMA, CURRENT_SCHEMA), None, "schema {version}");
        }
        assert_eq!(refusal(DataSchema::Fresh, MINIMUM_SCHEMA, CURRENT_SCHEMA), None);
        assert!(refusal(DataSchema::Version(CURRENT_SCHEMA + 1), MINIMUM_SCHEMA, CURRENT_SCHEMA).is_some(), "the data of a newer Host is kept for that Host");
        assert_eq!((version_report()["minimumSchema"].as_u64(), version_report()["currentSchema"].as_u64()), (Some(1), Some(2)));
    }

    #[test]
    fn the_database_names_its_generation_and_version() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.sqlite");
        assert_eq!(read(&path).unwrap(), DataSchema::Fresh);
        let database = Connection::open(&path).unwrap();
        assert_eq!(read(&path).unwrap(), DataSchema::Fresh);
        database.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE schema_migrations(version INTEGER);").unwrap();
        assert_eq!(read(&path).unwrap(), DataSchema::Foreign);
        database.execute_batch(&format!("PRAGMA application_id={DATABASE_APPLICATION_ID}; PRAGMA user_version=1;")).unwrap();
        assert_eq!(read(&path).unwrap(), DataSchema::Version(1));
    }
}
