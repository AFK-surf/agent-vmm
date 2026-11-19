use crate::{Error, Result};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fs::File,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

pub struct Store {
    writer: Mutex<Connection>,
    readers: Vec<Mutex<Connection>>,
    next: AtomicUsize,
    // An async backend call and its SQL commits are one host operation.
    pub(crate) effects: tokio::sync::Mutex<()>,
    _owner: File,
}

impl Store {
    /// Open a dedicated vmm-core database. A second host cannot drive the
    /// backend from the same database until the first releases ownership.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        File::options().create(true).append(true).open(path)?;
        let path = path.canonicalize()?;
        let mut lock_name = path.as_os_str().to_owned();
        lock_name.push(".owner");
        let owner = File::options().create(true).append(true).open(lock_name)?;
        owner.try_lock().map_err(|err| match err {
            std::fs::TryLockError::WouldBlock => Error::Conflict("database already owned"),
            std::fs::TryLockError::Error(e) => Error::Io(e),
        })?;
        let mut writer = connection(&path)?;
        let mode: String = writer.query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))?;
        if mode != "wal" {
            return Err(Error::Conflict("WAL unavailable"));
        }
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: i64 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        let app: i64 = tx.query_row("PRAGMA application_id", [], |r| r.get(0))?;
        match (app, version) {
            (0, 0) => {
                let tables: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type='table'",
                    [],
                    |r| r.get(0),
                )?;
                if tables != 0 {
                    return Err(Error::Conflict("database already in use"));
                }
                tx.execute_batch(include_str!("schema.sql"))?;
            }
            (0x564d4d43, 1) => (),
            _ => return Err(Error::Conflict("unsupported database schema")),
        }
        tx.execute("UPDATE registrations SET connected=0 WHERE connected=1", [])?;
        // A previous host cannot publish a terminal reply after ownership changes.
        // Keep the fingerprint so a retry can inspect the resource without re-executing.
        tx.execute(
            "UPDATE receipts SET state='interrupted' WHERE state='running'",
            [],
        )?;
        tx.commit()?;
        let readers = (0..4)
            .map(|_| {
                let reader = connection(&path)?;
                reader.pragma_update(None, "query_only", true)?;
                Ok(Mutex::new(reader))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            writer: Mutex::new(writer),
            readers,
            next: AtomicUsize::new(0),
            effects: Default::default(),
            _owner: owner,
        })
    }

    pub(crate) fn read<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let reader = self.readers[index].lock().map_err(|_| Error::Interrupted)?;
        f(&reader)
    }

    pub(crate) fn write<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let mut writer = self.writer.lock().map_err(|_| Error::Interrupted)?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = f(&tx)?;
        tx.commit()?;
        Ok(result)
    }
}

fn connection(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_millis(5000))?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(connection)
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}
pub(crate) fn decode<T: DeserializeOwned>(value: &str) -> Result<T> {
    Ok(serde_json::from_str(value)?)
}
