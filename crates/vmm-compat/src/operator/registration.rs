//! Operator registration management stays reachable while the workload VM is offline.
use super::{LocalOperator, h};
use std::sync::Arc;
use vmm_rpc::Status;
use vmm_core::{Core, RegistrationChange};

#[async_trait::async_trait]
pub trait RegistrationManagement: Send + Sync {
    fn get(&self, id: &str) -> Result<h::RemoteRegistration, Status>;
    fn apply(
        &self,
        request: h::ApplyRemoteRegistrationRequest,
    ) -> Result<h::RemoteRegistration, Status>;
    async fn change(
        &self,
        id: &str,
        expected: u64,
        change: RegistrationChange,
        core: Option<Core>,
    ) -> Result<h::RemoteRegistration, Status>;
}
impl LocalOperator {
    fn management(&self) -> Result<Arc<dyn RegistrationManagement>, Status> {
        self.management
            .clone()
            .ok_or_else(|| Status::unavailable("registration management unavailable"))
    }
    pub(super) async fn get_registration(
        &self,
        req: h::GetRemoteRegistrationRequest,
    ) -> Result<h::GetRemoteRegistrationResponse, Status> {
        let registration = self.management()?.get(&req.registration_id)?;
        Ok(h::GetRemoteRegistrationResponse {
            registration: Some(registration),
        })
    }
    pub(super) async fn apply_registration(
        &self,
        req: h::ApplyRemoteRegistrationRequest,
    ) -> Result<h::ApplyRemoteRegistrationResponse, Status> {
        let owner = self.management()?;
        let registration = tokio::task::spawn_blocking(move || owner.apply(req))
            .await
            .map_err(|_| Status::unavailable("registration management unavailable"))??;
        Ok(h::ApplyRemoteRegistrationResponse {
            registration: Some(registration),
        })
    }
    async fn registration_change(
        &self,
        id: &str,
        expected: u64,
        request: &str,
        change: RegistrationChange,
    ) -> Result<h::RemoteRegistration, Status> {
        if id.is_empty() || expected == 0 || request.is_empty() {
            return Err(crate::error::invalid_field(
                "request_id/registration_id/expected_revision",
                "request ID, registration ID and revision are required",
            ));
        }
        let core = self
            .backend
            .clone()
            .map(|b| Core::new(self.inventory.store.clone(), b));
        self.management()?.change(id, expected, change, core).await
    }
    pub(super) async fn set_registration(
        &self,
        req: h::SetRemoteRegistrationStateRequest,
    ) -> Result<h::SetRemoteRegistrationStateResponse, Status> {
        let change = match req.state() {
            h::RemoteRegistrationState::Enabled => RegistrationChange::SetDraining(false),
            h::RemoteRegistrationState::Draining => RegistrationChange::SetDraining(true),
            _ => {
                return Err(crate::error::invalid_field(
                    "state",
                    "expected enabled or draining",
                ));
            }
        };
        let registration = self
            .registration_change(
                &req.registration_id,
                req.expected_revision,
                &req.request_id,
                change,
            )
            .await?;
        Ok(h::SetRemoteRegistrationStateResponse {
            registration: Some(registration),
        })
    }
    pub(super) async fn begin_revocation(
        &self,
        req: h::BeginRemoteRevocationRequest,
    ) -> Result<h::BeginRemoteRevocationResponse, Status> {
        let change = RegistrationChange::BeginRevocation;
        let registration = self
            .registration_change(
                &req.registration_id,
                req.expected_revision,
                &req.request_id,
                change,
            )
            .await?;
        Ok(h::BeginRemoteRevocationResponse {
            registration: Some(registration),
        })
    }
    pub(super) async fn complete_revocation(
        &self,
        req: h::CompleteRemoteRevocationRequest,
    ) -> Result<h::CompleteRemoteRevocationResponse, Status> {
        let change = RegistrationChange::CompleteRevocation;
        let registration = self
            .registration_change(
                &req.registration_id,
                req.expected_revision,
                &req.request_id,
                change,
            )
            .await?;
        Ok(h::CompleteRemoteRevocationResponse {
            registration: Some(registration),
        })
    }
}
