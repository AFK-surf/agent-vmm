//! The production effects: the Host operator socket over gRPC, the helper process, and the files of the installation.

use crate::envelope::{LOG_BYTES, LogRecord, redact_text};
use crate::facts::{Facts, enum_name};
use crate::options::Options;
use crate::runtime::{Call, Failure, HelperOutput, LogPage, PreparationError, Runtime};
use agent_vmm_proto::host::v1 as hostv1;
use hostv1::operator_service_client::OperatorServiceClient;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tonic::transport::Channel;
use vmm_lifecycle::{Deadline, timefmt};

pub type Client = OperatorServiceClient<Channel>;

/// Converts a gRPC status and keeps the reason and the metadata that the Host attached.
pub fn failure(status: &tonic::Status) -> Failure {
    let error = vmm_lifecycle::grpc::status_error(status);
    let info = vmm_lifecycle::grpc::error_info(status).unwrap_or_default();
    Failure { code: error.rpc_code(), reason: info.reason, metadata: info.metadata, message: error.message }
}

fn deadline_exceeded() -> tonic::Status {
    tonic::Status::deadline_exceeded("context deadline exceeded")
}

/// Opens a lazy channel to the operator socket. The caller must be inside the Tokio runtime.
pub fn connect(socket: &str) -> Call<Channel> {
    if socket.is_empty() || !Path::new(socket).is_absolute() {
        return Err(Failure { message: "operator socket is unavailable".into(), ..Failure::default() });
    }
    vmm_lifecycle::grpc::channel(Path::new(socket)).map_err(|error| Failure { message: error.message, ..Failure::default() })
}

pub struct Production {
    runtime: tokio::runtime::Runtime,
}

impl Production {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self { runtime: tokio::runtime::Builder::new_current_thread().enable_all().build()? })
    }

    /// Makes one unary call within the budget.
    fn call<T, F>(&self, socket: &str, budget: Deadline, call: impl FnOnce(Client) -> F) -> Call<T>
    where
        F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        self.runtime.block_on(async {
            let client = OperatorServiceClient::new(connect(socket)?);
            match tokio::time::timeout(budget.remaining(), call(client)).await {
                Ok(Ok(response)) => Ok(response.into_inner()),
                Ok(Err(status)) => Err(failure(&status)),
                Err(_) => Err(failure(&deadline_exceeded())),
            }
        })
    }
}

impl Runtime for Production {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    fn euid(&self) -> u32 {
        rustix::process::geteuid().as_raw()
    }

    fn sleep(&self, interval: Duration, budget: Deadline) -> bool {
        budget.sleep(interval).is_ok()
    }

    fn helper(&self, path: &str, arguments: &[String], input: Option<&[u8]>, budget: Deadline) -> HelperOutput {
        crate::helper::run(path, arguments, input, budget)
    }

    fn operation(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::Operation>> {
        let request = hostv1::GetOperationRequest { operation_id: id.to_string() };
        Ok(self.call(socket, budget, |mut client| async move { client.get_operation(request).await })?.operation)
    }

    fn environment(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::AgentEnvironment>> {
        let request = hostv1::OperatorServiceGetEnvironmentRequest { environment_id: id.to_string() };
        Ok(self.call(socket, budget, |mut client| async move { client.get_environment(request).await })?.environment)
    }

    fn registration(&self, socket: &str, id: &str, budget: Deadline) -> Call<Option<hostv1::RemoteRegistration>> {
        let request = hostv1::GetRemoteRegistrationRequest { registration_id: id.to_string() };
        Ok(self.call(socket, budget, |mut client| async move { client.get_remote_registration(request).await })?.registration)
    }

    fn impact_summary(&self, socket: &str, budget: Deadline) -> Call<hostv1::GetVmImpactSummaryResponse> {
        self.call(socket, budget, |mut client| async move { client.get_vm_impact_summary(hostv1::GetVmImpactSummaryRequest {}).await })
    }

    fn vm(&self, socket: &str, budget: Deadline) -> Call<Option<hostv1::Vm>> {
        Ok(self.call(socket, budget, |mut client| async move { client.get_vm(hostv1::GetVmRequest {}).await })?.vm)
    }

    fn start_vm(&self, socket: &str, request: hostv1::StartVmRequest, budget: Deadline) -> Call<Option<hostv1::Operation>> {
        Ok(self.call(socket, budget, |mut client| async move { client.start_vm(request).await })?.operation)
    }

    fn stop_vm(&self, socket: &str, request: hostv1::StopVmRequest, budget: Deadline) -> Call<Option<hostv1::Operation>> {
        Ok(self.call(socket, budget, |mut client| async move { client.stop_vm(request).await })?.operation)
    }

    fn plan_cleanup(&self, socket: &str, environment_id: &str, kind: hostv1::CleanupKind, budget: Deadline) -> Call<Option<hostv1::CleanupPlan>> {
        let environment_id = environment_id.to_string();
        if kind == hostv1::CleanupKind::Reset {
            let request = hostv1::PlanResetEnvironmentRequest { environment_id };
            return Ok(self.call(socket, budget, |mut client| async move { client.plan_reset_environment(request).await })?.plan);
        }
        let request = hostv1::PlanDiscardEnvironmentRequest { environment_id };
        Ok(self.call(socket, budget, |mut client| async move { client.plan_discard_environment(request).await })?.plan)
    }

    fn execute_cleanup(&self, socket: &str, request: hostv1::ExecuteCleanupRequest, budget: Deadline) -> Call<Option<hostv1::Operation>> {
        Ok(self.call(socket, budget, |mut client| async move { client.execute_cleanup(request).await })?.operation)
    }

    fn set_host_socket(&self, socket: &str, request: hostv1::SetHostSocketRequest, budget: Deadline) -> Call<Option<hostv1::HostSocket>> {
        Ok(self.call(socket, budget, |mut client| async move { client.set_host_socket(request).await })?.host_socket)
    }

    fn remove_host_socket(&self, socket: &str, request: hostv1::RemoveHostSocketRequest, budget: Deadline) -> Call<()> {
        self.call(socket, budget, |mut client| async move { client.remove_host_socket(request).await }).map(|_| ())
    }

    fn host_sockets(&self, socket: &str, environment_id: Option<String>, budget: Deadline) -> Call<Vec<hostv1::HostSocket>> {
        let request = hostv1::ListHostSocketsRequest { environment_id };
        Ok(self.call(socket, budget, |mut client| async move { client.list_host_sockets(request).await })?.host_sockets)
    }

    fn container_logs(&self, socket: &str, options: &Options, budget: Deadline) -> Result<LogPage, String> {
        let query = hostv1::LogsRequest { container_id: options.container_id.clone(), tail_records: options.limit as u64, after_cursor: options.after_cursor.clone(), follow: options.follow, since: None };
        let request = hostv1::OperatorServiceLogsRequest { environment_id: options.environment_id.clone(), query: Some(query) };
        let mut page = LogPage { cursor: options.after_cursor.clone(), ..LogPage::default() };
        self.runtime.block_on(async {
            let end = tokio::time::Instant::now() + budget.remaining();
            let mut client = OperatorServiceClient::new(connect(socket).map_err(|failure| failure.message)?);
            let mut stream = match tokio::time::timeout_at(end, client.logs(request)).await {
                Ok(Ok(response)) => response.into_inner(),
                Ok(Err(status)) => return Err(failure(&status).message),
                Err(_) => return Err(failure(&deadline_exceeded()).message),
            };
            let mut total = 0;
            while page.records.len() < options.limit && total < LOG_BYTES {
                let record = match tokio::time::timeout_at(end, stream.message()).await {
                    Ok(Ok(Some(record))) => record,
                    Ok(Ok(None)) => break,
                    Ok(Err(status)) => return Err(failure(&status).message),
                    // The budget ended: the records that arrived are the page.
                    Err(_) => break,
                };
                if record.cursor.len() > 4096 {
                    return Err("log cursor exceeded 4096 bytes".into());
                }
                let mut data = redact_text(&String::from_utf8_lossy(&record.data));
                let mut truncated = record.truncated;
                if data.len() > LOG_BYTES - total {
                    let mut cut = LOG_BYTES - total;
                    while !data.is_char_boundary(cut) {
                        cut -= 1;
                    }
                    data.truncate(cut);
                    truncated = true;
                }
                total += data.len();
                page.cursor = record.cursor.clone();
                page.gap |= record.dropped_records != 0;
                let timestamp = record.timestamp.as_ref().map_or_else(|| timefmt::format(0, 0), |time| timefmt::format(time.seconds, time.nanos.max(0) as u32));
                let stream = enum_name(record.stream().as_str_name(), "LOG_STREAM_");
                page.records.push(LogRecord { cursor: record.cursor, timestamp, stream, data, truncated, dropped: record.dropped_records });
            }
            Ok(page)
        })
    }

    fn local_logs(&self, status: &Facts, options: &Options) -> Result<LogPage, String> {
        crate::logs::read_local(status, options)
    }

    fn prepare_update(&self, options: &Options, status: &Facts) -> Result<String, PreparationError> {
        crate::prepare::prepare(options, status)
    }

    fn remove_prepared(&self, path: &str) -> std::io::Result<()> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(path),
            Ok(_) => std::fs::remove_file(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}
