//! Environment-scoped admission for host directory mounts and authenticated guest observation.
use rusqlite::{Connection, params};
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use vmm_rpc::Status;
use vmm_compat::RuntimeDriver;
use vmm_compat::guest::GuestDriver;
use vmm_core::{BootIdentity, HostShareMount};
use vmm_guest_proto::rpc::HealthReply;

pub(crate) struct Dom0Workloads {
    guest: Arc<dyn GuestDriver>,
    grants: Mutex<Connection>,
}

impl Dom0Workloads {
    pub fn open(guest: Arc<dyn GuestDriver>, compat_path: &Path) -> Result<Self, rusqlite::Error> {
        let grants = Connection::open(compat_path)?;
        grants.execute_batch(
            "CREATE TABLE IF NOT EXISTS host_share_grants (
            id TEXT PRIMARY KEY, environment TEXT NOT NULL, source_path TEXT NOT NULL,
            read_only INTEGER NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0),
            available INTEGER NOT NULL
        )",
        )?;
        Ok(Self {
            guest,
            grants: Mutex::new(grants),
        })
    }
}

#[async_trait::async_trait]
impl RuntimeDriver for Dom0Workloads {
    async fn health(&self, boot: &BootIdentity) -> Result<HealthReply, Status> {
        crate::guest::health(self.guest.as_ref(), boot).await
    }

    async fn authorize_shares(&self, scope: &str, mounts: &[HostShareMount]) -> Result<(), Status> {
        let mut db = self
            .grants
            .lock()
            .map_err(|_| Status::unavailable("share grants unavailable"))?;
        let tx = db
            .transaction()
            .map_err(|_| Status::unavailable("share grants unavailable"))?;
        for mount in mounts {
            let allowed: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM host_share_grants
                WHERE id=? AND environment=? AND available=1 AND (read_only=0 OR ?))",
                    params![mount.share_id, scope, mount.read_only],
                    |row| row.get(0),
                )
                .map_err(|_| Status::unavailable("share grants unavailable"))?;
            if !allowed {
                return Err(Status::permission_denied(
                    "host share mount is not authorized",
                ));
            }
        }
        Ok(())
    }
}
