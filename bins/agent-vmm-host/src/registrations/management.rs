use super::{Active, ExternalRegistrations, State, policy};
use agent_vmm_proto::host::v1 as h;
use std::sync::Arc;
use tokio::sync::mpsc;
use tonic::{Code, Status};
use vmm_compat::{
    error::{host_status, invalid_field},
    operator::RegistrationManagement,
};
use vmm_core::{Core, Error, Limits, ManagedPhase, ManagedRegistration, RegistrationChange};
use vmm_remote_salix::{Connector, ConnectorConfig, Gateway, RuntimeSession};
use zeroize::Zeroizing;

pub(super) struct ConnectorRuntime {
    core: Core,
    shape: Limits,
    sessions: mpsc::Sender<RuntimeSession>,
    endpoint: Option<tokio::sync::watch::Receiver<agent_vmm_proto::trust::v1::EndpointObservation>>,
}
fn status(e: Error) -> Status {
    match e {
        Error::Fenced => host_status(
            Code::Aborted,
            h::ErrorReason::RegistrationRevisionConflict,
            "registration revision changed",
        ),
        Error::Draining => Status::permission_denied("registration has been retired"),
        Error::Conflict(reason) => Status::failed_precondition(reason),
        other => vmm_compat::error::core_status(other),
    }
}
impl ExternalRegistrations {
    pub(super) fn current_metadata(
        &self,
        state: &State,
        id: &str,
    ) -> Result<(u64, String), Status> {
        match self.core.managed_registration(id) {
            Ok(r) => Ok((r.revision, r.config.display_name)),
            Err(Error::Missing(_)) => state.metadata(id),
            Err(e) => Err(status(e)),
        }
    }
    fn snapshot(
        &self,
        state: &State,
        record: ManagedRegistration,
    ) -> Result<h::RemoteRegistration, Status> {
        let id = &record.config.id;
        let inventory = self
            .core
            .operator_registration_inventory(id, usize::MAX)
            .map_err(status)?;
        let phase = match state.active.get(id) {
            Some(active) if active.connector.is_connected() => h::ConnectorPhase::Connected,
            Some(active) if !active.task.is_finished() => h::ConnectorPhase::Connecting,
            _ if record.phase == ManagedPhase::PendingCredential => h::ConnectorPhase::Enrolling,
            _ => h::ConnectorPhase::Stopped,
        };
        policy::project(record, inventory, phase)
    }
    fn start_connector(
        &self,
        state: &mut State,
        record: &ManagedRegistration,
    ) -> Result<(), Status> {
        let Some(runtime) = &state.runtime else {
            return Ok(());
        };
        if !record.phase.accepts_connection() {
            return Ok(());
        }
        if let Some(active) = state.active.get(&record.config.id)
            && !active.task.is_finished()
            && active.connector.pool_revision() == record.pool_revision
        {
            return Ok(());
        }
        let config = policy::transport(&record.config, record.pool_revision);
        let gateway = Gateway::new(&record.config.id, &config, self.credentials.clone())
            .map_err(|_| Status::failed_precondition("invalid gateway trust configuration"))?;
        let mut connector = Connector::new(
            runtime.core.clone(),
            gateway,
            ConnectorConfig {
                connection: config,
                scope_defaults: runtime.shape.clone(),
                runtime: runtime.sessions.clone(),
                observe: None,
            },
        );
        if let Some(endpoint) = runtime.endpoint.clone() {
            connector = connector.with_endpoint(endpoint);
        }
        state
            .active
            .insert(record.config.id.clone(), Active::start(Arc::new(connector)));
        Ok(())
    }
    /// Restore each durable, enrolled connector once the host can accept session streams.
    pub fn enable_connectors(
        &self,
        core: Core,
        shape: Limits,
        sessions: mpsc::Sender<RuntimeSession>,
        endpoint: Option<
            tokio::sync::watch::Receiver<agent_vmm_proto::trust::v1::EndpointObservation>,
        >,
    ) -> Result<(), Status> {
        let mut state = self.lock()?;
        state.runtime = Some(ConnectorRuntime {
            core,
            shape,
            sessions,
            endpoint,
        });
        let mut cursor = None;
        loop {
            let page = self
                .core
                .list_registrations(cursor.as_deref(), 64)
                .map_err(status)?;
            for row in &page {
                match self.core.managed_registration(&row.id) {
                    Ok(record) => match self.core.check_external_connector(&row.id) {
                        Ok(()) => self.start_connector(&mut state, &record)?,
                        Err(
                            Error::Draining | Error::Conflict("private appliance registration"),
                        ) => {}
                        Err(e) => return Err(status(e)),
                    },
                    Err(Error::Missing(_)) => {}
                    Err(e) => return Err(status(e)),
                }
            }
            if page.len() < 64 {
                break;
            }
            cursor = page.last().map(|v| v.id.clone());
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl RegistrationManagement for ExternalRegistrations {
    fn get(&self, id: &str) -> Result<h::RemoteRegistration, Status> {
        let state = self.lock()?;
        let record = self.core.managed_registration(id).map_err(status)?;
        self.snapshot(&state, record)
    }
    fn apply(
        &self,
        req: h::ApplyRemoteRegistrationRequest,
    ) -> Result<h::RemoteRegistration, Status> {
        let credential = Zeroizing::new(req.credential);
        if credential.is_empty() || credential.len() > vmm_signing::MAX_CREDENTIAL_BYTES {
            return Err(invalid_field(
                "credential",
                "a bounded credential is required",
            ));
        }
        let config = policy::parse(
            req.registration
                .ok_or_else(|| invalid_field("registration", "public registration required"))?,
            self.disk_capacity,
        )?;
        Gateway::new(
            &config.id,
            &policy::transport(&config, 1),
            self.credentials.clone(),
        )
        .map_err(|_| invalid_field("endpoint/trust_bundle", "invalid TLS gateway configuration"))?;
        let mut state = self.lock()?;
        let record = self.core.configure_registration(&config).map_err(status)?;
        self.credentials
            .replace(&config.id, &credential)
            .map_err(|_| Status::unavailable("credential store unavailable"))?;
        let ready = self
            .core
            .change_registration(
                &config.id,
                record.revision,
                RegistrationChange::CredentialReady,
            )
            .map_err(status)?;
        self.start_connector(&mut state, &ready)?;
        self.snapshot(&state, ready)
    }
    async fn change(
        &self,
        id: &str,
        expected: u64,
        change: RegistrationChange,
        core: Option<Core>,
    ) -> Result<h::RemoteRegistration, Status> {
        let record = {
            let mut state = self.lock()?;
            match change {
                RegistrationChange::SetDraining(_) => {
                    let record = self
                        .core
                        .change_registration(id, expected, change)
                        .map_err(status)?;
                    return self.snapshot(&state, record);
                }
                RegistrationChange::BeginRevocation => {
                    let record = self
                        .core
                        .change_registration(id, expected, change)
                        .map_err(status)?;
                    state.active.remove(id);
                    record
                }
                RegistrationChange::CompleteRevocation => {
                    let record = self.core.managed_registration(id).map_err(status)?;
                    if record.revision != expected {
                        return Err(status(Error::Fenced));
                    }
                    if !matches!(record.phase, ManagedPhase::Revoking | ManagedPhase::Revoked) {
                        return Err(Status::failed_precondition("revocation was not started"));
                    }
                    record
                }
                RegistrationChange::CredentialReady => {
                    return Err(Status::invalid_argument("credential required"));
                }
            }
        };
        if record.phase != ManagedPhase::Revoked {
            if let Some(core) = core {
                core.retain_revoked_registration(id).await.map_err(status)?;
            } else if !self
                .core
                .operator_registration_inventory(id, usize::MAX)
                .map_err(status)?
                .allocations
                .iter()
                .all(|a| a.scope.state == vmm_core::ScopeState::Retained)
            {
                return Err(Status::failed_precondition(
                    "workload backend required to retain allocations",
                ));
            }
        }
        let state = self.lock()?;
        if change == RegistrationChange::CompleteRevocation {
            let record = self
                .core
                .change_registration(id, expected, change)
                .map_err(status)?;
            self.snapshot(&state, record)
        } else {
            self.snapshot(&state, self.core.managed_registration(id).map_err(status)?)
        }
    }
}
