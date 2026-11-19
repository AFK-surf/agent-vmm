use super::{LocalOperator, h};
use tonic::Status;

pub trait DeviceTrust: Send + Sync {
    fn identity(&self) -> Result<h::GetDeviceIdentityResponse, Status>;
    fn sign_service_endpoint(
        &self,
        endpoint: &str,
        generation: u64,
    ) -> Result<agent_vmm_proto::trust::v1::EndpointObservation, Status>;
    fn sign_recovery(
        &self,
        challenge: &[u8],
    ) -> Result<h::SignRegistrationRecoveryResponse, Status>;
}

impl LocalOperator {
    pub(super) async fn device_identity(
        &self,
        _: h::GetDeviceIdentityRequest,
    ) -> Result<h::GetDeviceIdentityResponse, Status> {
        let trust = self
            .trust
            .clone()
            .ok_or_else(|| Status::unavailable("device identity unavailable"))?;
        tokio::task::spawn_blocking(move || trust.identity())
            .await
            .map_err(|_| Status::unavailable("device identity unavailable"))?
    }

    pub(super) async fn sign_recovery(
        &self,
        req: h::SignRegistrationRecoveryRequest,
    ) -> Result<h::SignRegistrationRecoveryResponse, Status> {
        let trust = self
            .trust
            .clone()
            .ok_or_else(|| Status::unavailable("device identity unavailable"))?;
        tokio::task::spawn_blocking(move || trust.sign_recovery(&req.challenge_json))
            .await
            .map_err(|_| Status::unavailable("device identity unavailable"))?
    }
}
