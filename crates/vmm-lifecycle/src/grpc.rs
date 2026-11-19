//! The gRPC client of the Host operator socket.

use crate::error::{Deadline, Error, Kind, Result, RpcCode, err};
use crate::operator::Operator;
use agent_vmm_proto::google::rpc::Status as RpcStatus;
use agent_vmm_proto::host::v1 as hostv1;
use hostv1::operator_service_client::OperatorServiceClient;
use prost::Message;
use std::future::Future;
use std::path::{Path, PathBuf};
use tonic::transport::{Channel, Endpoint, Uri};

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
pub fn error_info(status: &tonic::Status) -> Option<ErrorInfo> {
    let details = RpcStatus::decode(status.details()).ok()?;
    details.details.iter().rev().find_map(|detail| {
        let info = detail.type_url.ends_with("/google.rpc.ErrorInfo").then(|| ErrorInfo::decode(detail.value.as_slice()).ok()).flatten()?;
        let known = hostv1::ErrorReason::from_str_name(&format!("ERROR_REASON_{}", info.reason)).is_some_and(|reason| reason != hostv1::ErrorReason::Unspecified);
        (info.domain == ERROR_DOMAIN && known).then_some(info)
    })
}

fn code(code: tonic::Code) -> RpcCode {
    match code {
        tonic::Code::Cancelled => RpcCode::Canceled,
        tonic::Code::InvalidArgument => RpcCode::InvalidArgument,
        tonic::Code::DeadlineExceeded => RpcCode::DeadlineExceeded,
        tonic::Code::NotFound => RpcCode::NotFound,
        tonic::Code::AlreadyExists => RpcCode::AlreadyExists,
        tonic::Code::PermissionDenied => RpcCode::PermissionDenied,
        tonic::Code::ResourceExhausted => RpcCode::ResourceExhausted,
        tonic::Code::FailedPrecondition => RpcCode::FailedPrecondition,
        tonic::Code::Aborted => RpcCode::Aborted,
        tonic::Code::OutOfRange => RpcCode::OutOfRange,
        tonic::Code::Unimplemented => RpcCode::Unimplemented,
        tonic::Code::Internal => RpcCode::Internal,
        tonic::Code::Unavailable => RpcCode::Unavailable,
        tonic::Code::DataLoss => RpcCode::DataLoss,
        tonic::Code::Unauthenticated => RpcCode::Unauthenticated,
        tonic::Code::Ok | tonic::Code::Unknown => RpcCode::Unknown,
    }
}

/// Converts a gRPC status and keeps the Host error reason of its `ErrorInfo` detail.
pub fn status_error(status: &tonic::Status) -> Error {
    let reason = error_info(status).map(|info| info.reason);
    let code = code(status.code());
    Error::new(Kind::Rpc { code, reason }, format!("rpc error: code = {} desc = {}", code.name(), status.message()))
}

/// Opens a lazy channel to a Unix socket. The caller must be inside a Tokio runtime.
pub fn channel(socket: &Path) -> Result<Channel> {
    let socket: PathBuf = socket.to_path_buf();
    let connector = tower::service_fn(move |_: Uri| {
        let socket = socket.clone();
        async move { tokio::net::UnixStream::connect(socket).await.map(hyper_util::rt::TokioIo::new) }
    });
    // The URI is not resolved: the connector always dials the socket.
    let endpoint = Endpoint::try_from("http://agent-vmm.invalid").map_err(|error| err(error.to_string()))?;
    Ok(endpoint.connect_with_connector_lazy(connector))
}

pub struct GrpcOperator {
    runtime: tokio::runtime::Runtime,
    client: OperatorServiceClient<Channel>,
}

impl GrpcOperator {
    pub fn connect(socket: &Path) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let channel = {
            let _guard = runtime.enter();
            channel(socket)?
        };
        Ok(Self { runtime, client: OperatorServiceClient::new(channel) })
    }

    fn block<T>(&self, deadline: Deadline, call: impl Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>) -> Result<T> {
        self.runtime.block_on(async {
            match tokio::time::timeout(deadline.remaining(), call).await {
                Ok(Ok(response)) => Ok(response.into_inner()),
                Ok(Err(status)) => Err(status_error(&status)),
                Err(_) => Err(status_error(&tonic::Status::deadline_exceeded("context deadline exceeded"))),
            }
        })
    }
}

macro_rules! rpc {
    ($operator:ident, $deadline:expr, $method:ident, $request:expr) => {{
        let mut client = $operator.client.clone();
        let request = $request;
        $operator.block($deadline, async move { client.$method(request).await })
    }};
}

impl Operator for GrpcOperator {
    fn health(&mut self, deadline: Deadline) -> Result<hostv1::GetSystemHealthSummaryResponse> {
        rpc!(self, deadline, get_system_health_summary, hostv1::GetSystemHealthSummaryRequest {})
    }

    fn registration(&mut self, id: &str, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        let request = hostv1::GetRemoteRegistrationRequest { registration_id: id.to_string() };
        Ok(rpc!(self, deadline, get_remote_registration, request)?.registration.unwrap_or_default())
    }

    fn vm(&mut self, deadline: Deadline) -> Result<hostv1::Vm> {
        Ok(rpc!(self, deadline, get_vm, hostv1::GetVmRequest {})?.vm.unwrap_or_default())
    }

    fn vm_impact(&mut self, deadline: Deadline) -> Result<hostv1::Vm> {
        rpc!(self, deadline, get_vm_impact_summary, hostv1::GetVmImpactSummaryRequest {})?.vm.ok_or_else(|| err("the impact summary names no VM"))
    }

    fn start_vm(&mut self, request_id: &str, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()> {
        let request = hostv1::StartVmRequest { request_id: request_id.to_string(), fence: Some(fence) };
        rpc!(self, deadline, start_vm, request).map(|_| ())
    }

    fn stop_vm(&mut self, request_id: &str, force: bool, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()> {
        let request = hostv1::StopVmRequest { request_id: request_id.to_string(), force, fence: Some(fence) };
        rpc!(self, deadline, stop_vm, request).map(|_| ())
    }

    fn device_identity(&mut self, deadline: Deadline) -> Result<hostv1::GetDeviceIdentityResponse> {
        rpc!(self, deadline, get_device_identity, hostv1::GetDeviceIdentityRequest {})
    }

    fn sign_recovery(&mut self, challenge: &[u8], deadline: Deadline) -> Result<hostv1::SignRegistrationRecoveryResponse> {
        rpc!(self, deadline, sign_registration_recovery, hostv1::SignRegistrationRecoveryRequest { challenge_json: challenge.to_vec() })
    }

    fn apply_registration(&mut self, request: hostv1::ApplyRemoteRegistrationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        Ok(rpc!(self, deadline, apply_remote_registration, request)?.registration.unwrap_or_default())
    }

    fn set_registration_state(&mut self, request: hostv1::SetRemoteRegistrationStateRequest, deadline: Deadline) -> Result<()> {
        rpc!(self, deadline, set_remote_registration_state, request).map(|_| ())
    }

    fn begin_revocation(&mut self, request: hostv1::BeginRemoteRevocationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration> {
        Ok(rpc!(self, deadline, begin_remote_revocation, request)?.registration.unwrap_or_default())
    }

    fn complete_revocation(&mut self, request: hostv1::CompleteRemoteRevocationRequest, deadline: Deadline) -> Result<()> {
        rpc!(self, deadline, complete_remote_revocation, request).map(|_| ())
    }

    fn local(&mut self, command: &str, request: &[u8], deadline: Deadline) -> Result<Vec<u8>> {
        macro_rules! relay {
            ($method:ident, $message:ty) => {{
                let message = <$message>::decode(request).map_err(|error| err(error.to_string()))?;
                rpc!(self, deadline, $method, message).map(|response| response.encode_to_vec())
            }};
        }
        match command {
            "local-disk" => relay!(get_local_disk_summary, hostv1::GetLocalDiskSummaryRequest),
            "local-environments" => relay!(list_local_environment_page, hostv1::ListLocalEnvironmentPageRequest),
            "local-registrations" => relay!(list_local_registration_page, hostv1::ListLocalRegistrationPageRequest),
            "local-plan" => relay!(plan_local_disposal, hostv1::PlanLocalDisposalRequest),
            "local-execute" => relay!(execute_local_disposal, hostv1::ExecuteLocalDisposalRequest),
            "local-operation" => relay!(get_operation, hostv1::GetOperationRequest),
            "registration-recovery-proof" => relay!(sign_registration_recovery, hostv1::SignRegistrationRecoveryRequest),
            _ => Err(err("unknown local operator command")),
        }
    }
}
