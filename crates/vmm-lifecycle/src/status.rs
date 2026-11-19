//! The `status` report. Local Host and connector observations never claim
//! controller admission; the server owns that fact.

use crate::cutover::{self, StartFailure};
use crate::error::{Deadline, Kind};
use crate::journal::{self, LifecycleOperation};
use crate::operator::{self, RemoteProtocol};
use crate::startup::{self, StartupEvidence};
use crate::{CONTRACT_VERSION, Context, Paths, local, platform, schema, timefmt};
use prost::Message;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceStatus {
    pub contract_version: u32,
    pub app_installed: bool,
    pub service_app: String,
    pub service_user: String,
    pub service_uid: u32,
    pub host_job: String,
    pub operator_socket: String,
    pub host_installed: bool,
    pub host_observed: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub host_observation_issue: String,
    pub host_loaded: bool,
    pub host_running: bool,
    #[serde(skip_serializing_if = "is_zero")]
    pub host_pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_last_exit_status: Option<i32>,
    pub connector_installed: bool,
    pub connector_loaded: bool,
    pub host_readable: bool,
    pub host_healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_health: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub registration_read: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub registration_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_protocol: Option<RemoteProtocol>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub registration_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_resources: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guest_resources: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub snapshot_observed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<LifecycleOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub startup: Option<StartupEvidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_start_failure: Option<StartFailure>,
    pub data_schema: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub data_schema_issue: String,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

fn go_json(name: &str, message: Option<&impl Message>) -> Option<Value> {
    local::dynamic(name, &message?.encode_to_vec()).ok().map(|message| local::go_json(&message))
}

pub fn read(context: &Context, paths: &Paths, registration_id: Option<&str>, deadline: Deadline) -> ServiceStatus {
    let operation = journal::read_latest(context, paths);
    let mut result = ServiceStatus {
        contract_version: CONTRACT_VERSION,
        app_installed: paths.app.is_dir(),
        service_app: paths.app.display().to_string(),
        service_user: paths.service.user.clone(),
        service_uid: paths.service.uid,
        host_job: platform::host_job(paths),
        operator_socket: paths.operator_socket().display().to_string(),
        host_installed: platform::host_installed(paths),
        startup: startup::read(paths, operation.as_ref()),
        operation,
        host_start_failure: cutover::read_start_failure(context, paths),
        ..ServiceStatus::default()
    };
    if registration_id.is_some() {
        result.registration_read = "unreadable".into();
        result.registration_reason = "operator_unavailable".into();
    }
    match schema::read(&paths.application_data.join("host.sqlite")) {
        Ok(schema::DataSchema::Foreign) => result.data_schema_issue = schema::FOREIGN_MESSAGE.into(),
        Ok(schema) => result.data_schema = schema.number(),
        Err(error) => result.data_schema_issue = error.message,
    }
    match platform::observe_host_service(context, paths, deadline) {
        Ok(service) => {
            result.host_observed = true;
            result.host_loaded = service.loaded;
            result.host_running = service.running;
            result.host_pid = service.pid;
            result.host_last_exit_status = service.last_exit_status;
        }
        Err(error) if error.kind == Kind::ServiceNotFound => result.host_observed = true,
        Err(error) => result.host_observation_issue = error.message,
    }
    result.connector_installed = result.host_installed;
    if result.host_running {
        let facts = operator::read_facts(context, paths, registration_id, deadline);
        result.host_readable = facts.host_readable;
        result.host_healthy = facts.host_healthy;
        result.registration_read = facts.registration_read;
        result.registration_state = facts.registration_state;
        result.registration_reason = facts.registration_reason;
        result.remote_protocol = facts.remote_protocol;
        result.connector_loaded = facts.connector_connected;
        if let Some(snapshot) = &facts.snapshot {
            result.host_health = go_json("HealthStatus", snapshot.health.as_ref());
            result.host_resources = go_json("LayerResources", snapshot.host_resources.as_ref());
            result.guest_resources = go_json("LayerResources", snapshot.guest_resources.as_ref());
            if let Some(collected) = &snapshot.collected_at {
                result.snapshot_observed_at = timefmt::format(collected.seconds, collected.nanos.max(0) as u32);
            }
        }
    }
    result
}
