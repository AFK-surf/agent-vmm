//! The observation commands: `inspect`, `diagnose`, `operation` and `logs`.

use crate::envelope::{DIAGNOSE_BYTES, Envelope, Issue, bounded, next};
use crate::facts::{self, Facts, enum_name, first};
use crate::helper::{self, SERVICE_SOURCE};
use crate::options::{MAX_LIST_LIMIT, Options};
use crate::runtime::{POLL_INTERVAL, Session};
use agent_vmm_proto::host::v1 as hostv1;
use serde_json::{Value, json};
use std::time::{Duration, UNIX_EPOCH};

/// Reads the status report of the lifecycle helper.
pub fn lifecycle_status(session: Session, options: &Options) -> Result<Facts, String> {
    let mut arguments = vec!["status".to_string()];
    if !options.registration_id.is_empty() {
        arguments.extend(["--registration-id".to_string(), options.registration_id.clone()]);
    }
    arguments.extend(options.service_arguments());
    let output = session.runtime.helper(&options.lifecycle, &arguments, None, session.budget);
    if let Some(error) = &output.error {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("lifecycle status failed: {}", bounded(first(&[stderr.as_ref(), error.as_str()]), 1024)));
    }
    if output.stdout.len() > DIAGNOSE_BYTES {
        return Err("lifecycle status exceeded 2 MiB".into());
    }
    serde_json::from_slice(&output.stdout).map_err(|_| "lifecycle status was not valid JSON".to_string())
}

/// Reads the status report and refuses one that does not identify its service.
pub fn identified_status(session: Session, options: &Options) -> Result<Facts, String> {
    let status = lifecycle_status(session, options)?;
    facts::validate_status_target(&status)?;
    Ok(status)
}

/// The operator socket of the observed service.
pub fn operator_socket(status: &Facts, options: &Options) -> String {
    if !options.operator_socket.is_empty() {
        return options.operator_socket.clone();
    }
    let reported = facts::string(status, "operatorSocket");
    if !reported.is_empty() {
        return reported.to_string();
    }
    let home = uzers::get_user_by_name(facts::string(status, "serviceUser")).map_or_else(helper::home, |account| uzers::os::unix::UserExt::home_dir(&account).to_path_buf());
    helper::service_data_directory(&home).join("operator.sock").display().to_string()
}

pub fn inspect(session: Session, command: &str, options: &Options) -> Result<Envelope, String> {
    let status = match lifecycle_status(session, options) {
        Ok(status) => status,
        Err(_) if session.budget.expired() => {
            let mut envelope = Envelope::new(command, session.runtime.now(), facts::explicit_target(options));
            envelope.partial = true;
            envelope.issue = Some(Issue::new("observation_timeout", &format!("{command} reached its observation budget"), "cli", Some(true)));
            envelope.next_action = next(command, "service_user");
            return Ok(envelope);
        }
        Err(error) => return Err(error),
    };
    facts::validate_status_target(&status)?;
    let now = session.runtime.now();
    let mut envelope = Envelope::new(command, now, facts::target_from_status(&status, options));
    envelope.source = vec!["lifecycle".into(), SERVICE_SOURCE.into()];
    envelope.state = facts::state(&status).into();
    (envelope.stage, envelope.issue) = facts::stage_and_issue(&status);
    envelope.freshness = facts::freshness(&status, now).into();
    match resolve_scope(session, &status, options) {
        Ok(scope) => {
            envelope.target.extend(scope);
            envelope.target.insert("scope_status".into(), json!("resolved"));
        }
        Err(error) => {
            envelope.partial = true;
            envelope.target.insert("scope_status".into(), json!("unavailable"));
            envelope.issue.get_or_insert_with(|| Issue::new("target_unavailable", &bounded(&error, 512), "host", Some(true)));
        }
    }
    let operation = facts::object(&status, "operation").map(|operation| facts::string(operation, "operationId")).unwrap_or_default();
    if !operation.is_empty() {
        envelope.operation("lifecycle", operation);
    }
    if envelope.state != "healthy" {
        envelope.next_action = next("repair", "administrator");
    }
    let readable = facts::boolean(&status, "hostReadable");
    let resources = json!({"host": status.get("hostResources"), "guest": status.get("guestResources"), "observed_at": status.get("snapshotObservedAt")});
    envelope.facts = status;
    if command == "diagnose" && readable {
        envelope.source.push("host_operator".into());
        envelope.facts.insert("resources".into(), resources);
    } else if command == "diagnose" {
        envelope.partial = true;
        envelope.issue.get_or_insert_with(|| Issue::new("host_unreadable", "Host Operator facts are unavailable", "host", Some(true)));
    }
    Ok(envelope)
}

/// Checks that the identifiers of the caller name one registration, environment and allocation of this Host.
fn resolve_scope(session: Session, status: &Facts, options: &Options) -> Result<Facts, String> {
    let mut result = Facts::new();
    if options.registration_id.is_empty() && options.environment_id.is_empty() && options.workload_id.is_empty() {
        return Ok(result);
    }
    if !facts::boolean(status, "hostReadable") {
        return Err("Host Operator cannot resolve the requested scope".into());
    }
    let socket = operator_socket(status, options);
    if !options.environment_id.is_empty() {
        let environment = owned_environment(session, &socket, options)?;
        result.insert("environment_origin".into(), json!(enum_name(environment.origin().as_str_name(), "ENVIRONMENT_ORIGIN_")));
        result.insert("environment_inventory".into(), json!(enum_name(environment.inventory_state().as_str_name(), "INVENTORY_STATE_")));
    }
    if options.registration_id.is_empty() {
        return Ok(result);
    }
    let registration = session.runtime.registration(&socket, &options.registration_id, session.budget).ok().flatten().filter(|registration| registration.id == options.registration_id);
    let registration = registration.ok_or("the requested registration was not found")?;
    if registration.allocations.len() > MAX_LIST_LIMIT {
        return Err("registration allocation inventory exceeds the 100 item observation bound".into());
    }
    result.insert("registration_state".into(), json!(enum_name(registration.state().as_str_name(), "REMOTE_REGISTRATION_STATE_")));
    result.insert("connection_state".into(), json!(enum_name(registration.connection_state().as_str_name(), "REMOTE_CONNECTION_STATE_")));
    if options.workload_id.is_empty() {
        return Ok(result);
    }
    let mut matches = registration.allocations.iter().filter(|allocation| allocation.allocation_id == options.workload_id);
    let matched = matches.next().ok_or("the requested workload has no local allocation")?;
    if matches.next().is_some() {
        return Err("the workload maps to multiple local allocations".into());
    }
    if !options.environment_id.is_empty() && matched.environment_id != options.environment_id {
        return Err("the requested workload does not map to the environment".into());
    }
    result.insert("allocation_id".into(), json!(matched.allocation_id));
    result.insert("allocation_generation".into(), json!(matched.generation));
    result.insert("allocation_state".into(), json!(enum_name(matched.state().as_str_name(), "ALLOCATION_STATE_")));
    Ok(result)
}

/// Reads the requested environment and checks that it belongs to the requested registration.
fn owned_environment(session: Session, socket: &str, options: &Options) -> Result<hostv1::AgentEnvironment, String> {
    let environment = session.runtime.environment(socket, &options.environment_id, session.budget).ok().flatten().filter(|environment| environment.id == options.environment_id);
    let environment = environment.ok_or("the requested environment was not found")?;
    let owner = environment.owner.as_ref().map(|owner| owner.registration_id.as_str()).unwrap_or_default();
    if !options.registration_id.is_empty() && owner != options.registration_id {
        return Err("the requested environment does not belong to the registration".into());
    }
    Ok(environment)
}

fn lifecycle_operation(session: Session, options: &Options) -> Result<Facts, String> {
    let mut arguments = vec!["operation".to_string(), "--operation-id".to_string(), options.operation_id.clone()];
    arguments.extend(options.service_arguments());
    let output = session.runtime.helper(&options.lifecycle, &arguments, None, session.budget);
    if let Some(error) = &output.error {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("lifecycle operation failed: {}", bounded(first(&[stderr.as_ref(), error.as_str()]), 1024)));
    }
    if output.stdout.len() > 1 << 20 {
        return Err("lifecycle operation exceeded 1 MiB".into());
    }
    let operation = serde_json::from_slice::<Facts>(&output.stdout).ok().filter(|operation| facts::string(operation, "operationId") == options.operation_id);
    operation.ok_or_else(|| "lifecycle operation was not found".to_string())
}

/// Observes one operation of the lifecycle owner or of the Host. Returns the envelope and whether the operation ended.
fn observe_operation(session: Session, options: &Options, target: &mut Facts, socket: &str) -> Result<(Envelope, bool), String> {
    let now = session.runtime.now();
    if options.owner == "lifecycle" {
        let raw = lifecycle_operation(session, options)?;
        let registration = facts::string(&raw, "targetRegistrationId");
        if !registration.is_empty() {
            target.insert("registration_id".into(), json!(registration));
        }
        let mut envelope = Envelope::new("operation", now, target.clone());
        envelope.source = vec!["lifecycle_operation".into()];
        envelope.operation("lifecycle", &options.operation_id);
        envelope.state = first(&[facts::string(&raw, "outcome"), "unknown"]).into();
        envelope.stage = first(&[facts::string(&raw, "stage"), facts::string(&raw, "failureStage"), facts::string(&raw, "kind"), "unknown"]).into();
        envelope.freshness = facts::freshness_from_millis(facts::number(&raw, "updatedAt"), now).into();
        if envelope.state == "failed" {
            let code = first(&[facts::string(&raw, "failureCode"), "operation_failed"]);
            envelope.issue = Some(Issue::new(code, &bounded(facts::string(&raw, "failureMessage"), 1024), "lifecycle", facts::optional_boolean(&raw, "retryable")));
        }
        let terminal = matches!(envelope.state.as_str(), "succeeded" | "failed");
        envelope.facts.insert("operation".into(), Value::Object(raw));
        return Ok((envelope, terminal));
    }
    let operation = session.runtime.operation(socket, &options.operation_id, session.budget).map_err(|failure| failure.message)?;
    let operation = operation.filter(|operation| operation.id == options.operation_id).ok_or("host operation was not found")?;
    let mut envelope = Envelope::new("operation", now, target.clone());
    envelope.source = vec!["host_operation".into()];
    envelope.operation("host", &options.operation_id);
    envelope.state = enum_name(operation.state().as_str_name(), "OPERATION_STATE_");
    envelope.stage = first(&[operation.stage.as_str(), "unknown"]).into();
    let updated = operation.updated_at.as_ref().and_then(|time| u64::try_from(time.seconds).ok().map(|seconds| UNIX_EPOCH + Duration::new(seconds, time.nanos.max(0) as u32)));
    envelope.freshness = facts::freshness_from_time(Some(updated.unwrap_or(UNIX_EPOCH)), now).into();
    envelope.facts.insert("kind".into(), json!(operation.kind));
    envelope.facts.insert("request_id".into(), json!(operation.request_id));
    envelope.facts.insert("resource".into(), operation.resource.as_ref().map_or(Value::Null, resource));
    if let Some(error) = &operation.error {
        envelope.issue = Some(Issue::new(&error.reason, &bounded(&error.message, 1024), "host", None));
    }
    let terminal = matches!(operation.state(), hostv1::OperationState::Succeeded | hostv1::OperationState::Failed | hostv1::OperationState::Interrupted);
    Ok((envelope, terminal))
}

/// A resource reference with the keys of its generated Go structure: a field with the zero value is omitted.
pub fn resource(reference: &hostv1::ResourceReference) -> Value {
    let mut value = Facts::new();
    for (key, text) in [("kind", &reference.kind), ("environment_id", &reference.environment_id), ("resource_id", &reference.resource_id), ("generation_id", &reference.generation_id)] {
        if !text.is_empty() {
            value.insert(key.into(), json!(text));
        }
    }
    if reference.generation != 0 {
        value.insert("generation".into(), json!(reference.generation));
    }
    Value::Object(value)
}

/// `operation get` returns one observation. `operation follow` emits each observation until the operation ends or the budget does.
pub fn operation(session: Session, follow: bool, options: &Options, emit: &mut dyn FnMut(&Envelope) -> Result<(), String>) -> Result<Vec<Envelope>, String> {
    let status = identified_status(session, options)?;
    let mut target = facts::target_from_status(&status, options);
    let socket = operator_socket(&status, options);
    let (first_envelope, mut terminal) = observe_operation(session, options, &mut target, &socket)?;
    if !follow {
        return Ok(vec![first_envelope]);
    }
    emit(&first_envelope)?;
    let mut last = first_envelope;
    while !terminal {
        if !session.runtime.sleep(POLL_INTERVAL, session.budget) {
            last.partial = true;
            last.issue = Some(Issue::new("follow_timeout", "follow reached its observation budget", &options.owner, Some(true)));
            last.next_action = next("operation_follow", "service_user");
            emit(&last)?;
            break;
        }
        (last, terminal) = observe_operation(session, options, &mut target, &socket)?;
        emit(&last)?;
    }
    Ok(Vec::new())
}

pub fn logs(session: Session, options: &Options) -> Result<Envelope, String> {
    let now = session.runtime.now();
    let status = identified_status(session, options)?;
    let mut envelope = Envelope::new("logs", now, facts::target_from_status(&status, options));
    envelope.state = "available".into();
    envelope.stage = "read".into();
    envelope.freshness = "current".into();
    if !options.component.is_empty() {
        let page = session.runtime.local_logs(&status, options)?;
        envelope.source = vec![format!("{}_log", options.component)];
        (envelope.logs, envelope.cursor, envelope.gap) = (page.records, page.cursor, page.gap);
        if !options.operation_id.is_empty() {
            envelope.operation("host", &options.operation_id);
        }
        return Ok(envelope);
    }
    let socket = operator_socket(&status, options);
    let environment = owned_environment(session, &socket, options)?;
    if environment.containers.iter().filter(|container| container.id == options.container_id).count() != 1 {
        return Err("the requested container did not resolve uniquely in the environment".into());
    }
    envelope.target.insert("scope_status".into(), json!("resolved"));
    let mut page = session.runtime.container_logs(&socket, options, session.budget)?;
    envelope.source = vec!["guest_container_log".into()];
    if !options.include_content {
        for record in &mut page.records {
            record.data = "[REDACTED: pass --include-container-output to read container output]".into();
        }
    }
    (envelope.logs, envelope.cursor, envelope.gap) = (page.records, page.cursor, page.gap);
    Ok(envelope)
}
