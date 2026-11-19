//! External control connections and their operator policy share one admission lock.
mod management;
mod policy;
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::task::JoinHandle;
use tonic::Status;
use vmm_compat::operator::RegistrationControl;
use vmm_core::Store;
use vmm_remote_salix::Connector;
use vmm_signing::CredentialStore;

struct Active {
    connector: Arc<Connector>,
    task: JoinHandle<()>,
}
impl Active {
    fn start(connector: Arc<Connector>) -> Self {
        let run = connector.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = run.run().await {
                eprintln!("external connector stopped: {err}");
            }
        });
        Self { connector, task }
    }
}
impl Drop for Active {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct State {
    database: Connection,
    active: HashMap<String, Active>,
    runtime: Option<management::ConnectorRuntime>,
}
pub struct ExternalRegistrations {
    core: Arc<Store>,
    state: Mutex<State>,
    credentials: Arc<dyn CredentialStore>,
    disk_capacity: u64,
}
fn unavailable(_: rusqlite::Error) -> Status {
    Status::unavailable("registration policy unavailable")
}

impl State {
    fn metadata(&self, id: &str) -> Result<(u64, String), Status> {
        let row = self
            .database
            .query_row(
                "SELECT revision,display_name FROM external_registration_policy WHERE id=?",
                [id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(unavailable)?;
        match row {
            Some((revision, name)) => Ok((
                u64::try_from(revision)
                    .map_err(|_| Status::data_loss("invalid registration revision"))?,
                name,
            )),
            // Core registrations created before the operator stored display metadata have the original revision.
            None => Ok((1, id.into())),
        }
    }
}
impl ExternalRegistrations {
    pub fn open(
        path: &Path,
        core: Arc<Store>,
        credentials: Arc<dyn CredentialStore>,
        disk_capacity: u64,
    ) -> Result<Self, Status> {
        let database = Connection::open(path).map_err(unavailable)?;
        database.execute_batch("CREATE TABLE IF NOT EXISTS external_registration_policy(id TEXT PRIMARY KEY,revision INTEGER NOT NULL CHECK(revision>0),display_name TEXT NOT NULL)").map_err(unavailable)?;
        Ok(Self {
            core,
            credentials,
            disk_capacity,
            state: Mutex::new(State {
                database,
                active: HashMap::new(),
                runtime: None,
            }),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, Status> {
        self.state
            .lock()
            .map_err(|_| Status::unavailable("registration admission unavailable"))
    }
    pub(crate) fn with_recoverable<T>(
        &self,
        id: &str,
        sign: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        let _admission = self.lock()?;
        match self.core.check_external_connector(id) {
            Ok(()) | Err(vmm_core::Error::Missing("registration")) => sign(),
            Err(
                vmm_core::Error::Conflict("private appliance registration")
                | vmm_core::Error::Draining,
            ) => Err(Status::failed_precondition(
                "the original local registration is not recoverable",
            )),
            Err(error) => Err(vmm_compat::error::core_status(error)),
        }
    }

    /// Install the validated policy and replace only this registration's connector.
    pub fn attach(
        &self,
        id: &str,
        revision: u64,
        name: &str,
        connector: Arc<Connector>,
    ) -> Result<(), Status> {
        let revision = i64::try_from(revision)
            .ok()
            .filter(|r| *r > 0)
            .ok_or_else(|| Status::invalid_argument("invalid policy revision"))?;
        let mut state = self.lock()?;
        self.core
            .check_external_connector(id)
            .map_err(vmm_compat::error::core_status)?;
        let updated=state.database.execute("INSERT INTO external_registration_policy VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,display_name=excluded.display_name WHERE revision<excluded.revision OR (revision=excluded.revision AND display_name=excluded.display_name)", params![id,revision,name]).map_err(unavailable)?;
        if updated == 0 {
            return Err(Status::aborted("stale registration policy"));
        }
        state.active.remove(id);
        state.active.insert(id.into(), Active::start(connector));
        Ok(())
    }
}
impl RegistrationControl for ExternalRegistrations {
    fn metadata(&self, id: &str) -> Result<(u64, String), Status> {
        let state = self.lock()?;
        self.current_metadata(&state, id)
    }
    fn isolate(
        &self,
        id: &str,
        revision: u64,
        fence: &mut dyn FnMut() -> Result<(), Status>,
    ) -> Result<(), Status> {
        let mut state = self.lock()?;
        if self.current_metadata(&state, id)?.0 != revision {
            return Err(Status::aborted("registration policy changed"));
        }
        let reconnect = state
            .active
            .remove(id)
            .map(|active| active.connector.clone());
        if let Err(err) = fence() {
            if let Some(connector) = reconnect {
                state.active.insert(id.into(), Active::start(connector));
            }
            return Err(err);
        }
        Ok(())
    }
    fn delete_credential(&self, id: &str) -> Result<(), Status> {
        self.credentials
            .delete(id)
            .map_err(|_| Status::unavailable("registration credential unavailable"))
    }
}
