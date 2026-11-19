//! The RPC client of the Host operator socket.

use crate::error::{Deadline, Error, Kind, Result, RpcCode, err};
use crate::operator::Operator;
use agent_vmm_proto::google::rpc::Status as RpcStatus;
use agent_vmm_proto::host::v1 as hostv1;
use prost::Message;
use std::path::{Path, PathBuf};

/// The domain of the `google.rpc.ErrorInfo` detail that the Host attaches.
const ERROR_DOMAIN: &str = "agent-vmm";

/// `google.rpc.ErrorInfo`.
#[derive(Clone, PartialEq, Message)]
pub struct ErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: std::collections::HashMap<String, String>,
}

/// The last `ErrorInfo` detail of the Host that names a known `host.v1.ErrorReason`.
pub fn error_info(status: &vmm_rpc::Status) -> Option<ErrorInfo> {
    let details = RpcStatus::decode(status.details()).ok()?;
    details.details.iter().rev().find_map(|detail| {
        let info = detail.type_url.ends_with("/google.rpc.ErrorInfo").then(|| ErrorInfo::decode(detail.value.as_slice()).ok()).flatten()?;
        let known = hostv1::ErrorReason::from_str_name(&format!("ERROR_REASON_{}", info.reason)).is_some_and(|reason| reason != hostv1::ErrorReason::Unspecified);
        (info.domain == ERROR_DOMAIN && known).then_some(info)
    })
}

fn code(code: vmm_rpc::Code) -> RpcCode {
    match code {
        vmm_rpc::Code::Cancelled => RpcCode::Canceled,
        vmm_rpc::Code::InvalidArgument => RpcCode::InvalidArgument,
        vmm_rpc::Code::DeadlineExceeded => RpcCode::DeadlineExceeded,
        vmm_rpc::Code::NotFound => RpcCode::NotFound,
        vmm_rpc::Code::AlreadyExists => RpcCode::AlreadyExists,
        vmm_rpc::Code::PermissionDenied => RpcCode::PermissionDenied,
        vmm_rpc::Code::ResourceExhausted => RpcCode::ResourceExhausted,
        vmm_rpc::Code::FailedPrecondition => RpcCode::FailedPrecondition,
        vmm_rpc::Code::Aborted => RpcCode::Aborted,
        vmm_rpc::Code::OutOfRange => RpcCode::OutOfRange,
        vmm_rpc::Code::Unimplemented => RpcCode::Unimplemented,
        vmm_rpc::Code::Internal => RpcCode::Internal,
        vmm_rpc::Code::Unavailable => RpcCode::Unavailable,
        vmm_rpc::Code::DataLoss => RpcCode::DataLoss,
        vmm_rpc::Code::Unauthenticated => RpcCode::Unauthenticated,
        vmm_rpc::Code::Ok | vmm_rpc::Code::Unknown => RpcCode::Unknown,
    }
}

/// Converts a RPC status and keeps the Host error reason of its `ErrorInfo` detail.
pub fn status_error(status: &vmm_rpc::Status) -> Error {
    let reason = error_info(status).map(|info| info.reason);
    let code = code(status.code());
    Error::new(Kind::Rpc { code, reason }, format!("rpc error: code = {} desc = {}", code.name(), status.message()))
}

pub struct SocketOperator { socket: PathBuf }

impl SocketOperator {
    pub fn connect(socket: &Path) -> Result<Self> { Ok(Self { socket: socket.to_path_buf() }) }
}

macro_rules! rpc {
    ($operator:ident, $deadline:expr, $method:ident, $output:ty, $request:expr) => {{
        vmm_rpc::sync_unary::<_, $output>(&$operator.socket, concat!("/agentvmm.host.v1.OperatorService/", stringify!($method)), &$request, $deadline.remaining()).map_err(|s| status_error(&s))
    }};
}

impl Operator for SocketOperator {
    fn health(&mut self, deadline: Deadline) -> Result<hostv1::GetSystemHealthSummaryResponse> {
        rpc!(self, deadline, GetSystemHealthSummary, hostv1::GetSystemHealthSummaryResponse, hostv1::GetSystemHealthSummaryRequest {})
    }

    fn registration(&mut self, id: &str, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        let request = hostv1::GetRemoteRegistrationRequest { registration_id: id.to_string() };
        Ok(rpc!(self, deadline, GetRemoteRegistration, hostv1::GetRemoteRegistrationResponse, request)?.registration.unwrap_or_default())
    }

    fn vm(&mut self, deadline: Deadline) -> Result<hostv1::Vm> {
        Ok(rpc!(self, deadline, GetVM, hostv1::GetVmResponse, hostv1::GetVmRequest {})?.vm.unwrap_or_default())
    }

    fn vm_impact(&mut self, deadline: Deadline) -> Result<hostv1::Vm> {
        rpc!(self, deadline, GetVMImpactSummary, hostv1::GetVmImpactSummaryResponse, hostv1::GetVmImpactSummaryRequest {})?.vm.ok_or_else(|| err("the impact summary names no VM"))
    }

    fn start_vm(&mut self, request_id: &str, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()> {
        let request = hostv1::StartVmRequest { request_id: request_id.to_string(), fence: Some(fence) };
        rpc!(self, deadline, StartVM, hostv1::StartVmResponse, request).map(|_| ())
    }

    fn stop_vm(&mut self, request_id: &str, force: bool, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()> {
        let request = hostv1::StopVmRequest { request_id: request_id.to_string(), force, fence: Some(fence) };
        rpc!(self, deadline, StopVM, hostv1::StopVmResponse, request).map(|_| ())
    }

    fn device_identity(&mut self, deadline: Deadline) -> Result<hostv1::GetDeviceIdentityResponse> {
        rpc!(self, deadline, GetDeviceIdentity, hostv1::GetDeviceIdentityResponse, hostv1::GetDeviceIdentityRequest {})
    }

    fn sign_recovery(&mut self, challenge: &[u8], deadline: Deadline) -> Result<hostv1::SignRegistrationRecoveryResponse> {
        rpc!(self, deadline, SignRegistrationRecovery, hostv1::SignRegistrationRecoveryResponse, hostv1::SignRegistrationRecoveryRequest { challenge_json: challenge.to_vec() })
    }

    fn apply_registration(&mut self, request: hostv1::ApplyRemoteRegistrationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        Ok(rpc!(self, deadline, ApplyRemoteRegistration, hostv1::ApplyRemoteRegistrationResponse, request)?.registration.unwrap_or_default())
    }

    fn set_registration_state(&mut self, request: hostv1::SetRemoteRegistrationStateRequest, deadline: Deadline) -> Result<()> {
        rpc!(self, deadline, SetRemoteRegistrationState, hostv1::SetRemoteRegistrationStateResponse, request).map(|_| ())
    }

    fn begin_revocation(&mut self, request: hostv1::BeginRemoteRevocationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        Ok(rpc!(self, deadline, BeginRemoteRevocation, hostv1::BeginRemoteRevocationResponse, request)?.registration.unwrap_or_default())
    }

    fn complete_revocation(&mut self, request: hostv1::CompleteRemoteRevocationRequest, deadline: Deadline) -> Result<()> {
        rpc!(self, deadline, CompleteRemoteRevocation, hostv1::CompleteRemoteRevocationResponse, request).map(|_| ())
    }

    fn local(&mut self, command: &str, request: &[u8], deadline: Deadline) -> Result<Vec<u8>> {
        macro_rules! relay {
            ($method:ident, $output:ty, $message:ty) => {{
                let message = <$message>::decode(request).map_err(|error| err(error.to_string()))?;
                rpc!(self, deadline, $method, $output, message).map(|response| response.encode_to_vec())
            }};
        }
        match command {
            "local-disk" => relay!(GetLocalDiskSummary, hostv1::GetLocalDiskSummaryResponse, hostv1::GetLocalDiskSummaryRequest),
            "local-environments" => relay!(ListLocalEnvironmentPage, hostv1::ListLocalEnvironmentPageResponse, hostv1::ListLocalEnvironmentPageRequest),
            "local-registrations" => relay!(ListLocalRegistrationPage, hostv1::ListLocalRegistrationPageResponse, hostv1::ListLocalRegistrationPageRequest),
            "local-plan" => relay!(PlanLocalDisposal, hostv1::PlanLocalDisposalResponse, hostv1::PlanLocalDisposalRequest),
            "local-execute" => relay!(ExecuteLocalDisposal, hostv1::ExecuteLocalDisposalResponse, hostv1::ExecuteLocalDisposalRequest),
            "local-operation" => relay!(GetOperation, hostv1::GetOperationResponse, hostv1::GetOperationRequest),
            "registration-recovery-proof" => relay!(SignRegistrationRecovery, hostv1::SignRegistrationRecoveryResponse, hostv1::SignRegistrationRecoveryRequest),
            _ => Err(err("unknown local operator command")),
        }
    }
}
