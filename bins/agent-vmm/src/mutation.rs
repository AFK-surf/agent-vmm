//! The commands that change the Host through its owner: `vm`, `repair`, `cleanup` and `host-socket`.

use crate::envelope::{EXCLUSIVE_TRANSITION, Envelope, Issue, NextAction, bounded, next};
use crate::facts::{self, Facts, enum_name, first};
use crate::helper::SERVICE_SOURCE;
use crate::observe::{identified_status, resource};
use crate::options::Options;
use crate::runtime::{Failure, Session};
use agent_vmm_proto::host::v1 as hostv1;
use hostv1::OperationState;
use serde_json::{Value, json};

const CLEANUP_IMPACT_LIMIT: usize = 100;

fn object(value: Value) -> Facts {
    value.as_object().cloned().unwrap_or_default()
}

fn actions(values: &[(&str, &str)]) -> Vec<NextAction> {
    values.iter().flat_map(|(action, permission)| next(action, permission)).collect()
}

/// Reports a mutation that the Host accepted and whose terminal result it could not save.
fn terminal_result_not_saved(envelope: &mut Envelope, code: &str, message: &str, failure: &Failure, request_id: &str) {
    envelope.state = "unknown_outcome".into();
    envelope.stage = "terminal_result_not_saved".into();
    envelope.partial = true;
    envelope.issue = Some(Issue::new(code, message, "host", Some(true)));
    envelope.next_action = actions(&[("retry_same_request_id", "service_user"), ("service_restart", "administrator")]);
    envelope.facts.insert("request_id".into(), json!(request_id));
    if let Some(operation) = failure.metadata.get("operation_id").filter(|id| !id.is_empty()) {
        envelope.operation("host", operation);
    }
}

fn unknown_outcome(envelope: &mut Envelope, message: &str, request_id: &str) {
    envelope.state = "unknown_outcome".into();
    envelope.stage = "owner_response_pending".into();
    envelope.partial = true;
    envelope.issue = Some(Issue::new("unknown_outcome", message, "host", Some(true)));
    envelope.next_action = next("retry_same_request_id", "service_user");
    envelope.facts.insert("request_id".into(), json!(request_id));
}

/// Reports the operation that the Host owner returned for a request.
fn report_operation(envelope: &mut Envelope, operation: &hostv1::Operation) {
    envelope.state = enum_name(operation.state().as_str_name(), "OPERATION_STATE_");
    envelope.stage = operation.stage.clone();
    envelope.operation("host", &operation.id);
    if let Some(error) = &operation.error {
        envelope.issue = Some(Issue::new(&error.reason, &bounded(&error.message, 1024), "host", None));
    }
}

pub fn vm(session: Session, action: &str, options: &Options) -> Result<Envelope, String> {
    let socket = &options.operator_socket;
    let summary = session.runtime.impact_summary(socket, session.budget).ok().filter(|summary| summary.vm.is_some()).ok_or("current Host VM impact summary is not readable")?;
    let before = summary.vm.clone().unwrap_or_default();
    // A workload always names its kind.
    let workload = |reference: &hostv1::ResourceReference| {
        let mut fact = resource(reference);
        fact["kind"] = json!(reference.kind);
        fact
    };
    let workloads: Vec<Value> = summary.known_workloads.iter().map(workload).collect();
    let mut envelope = Envelope::new("vm", session.runtime.now(), object(json!({"vm": "local"})));
    envelope.source = vec!["host_operator".into()];
    envelope.freshness = "current".into();
    envelope.state = enum_name(before.state().as_str_name(), "VM_STATE_");
    envelope.facts = object(json!({
        "boot_id": before.boot_id, "impact_scope": "whole_vm", "known_workload_count": workloads.len(), "known_workloads": workloads,
        "impact_summary_complete": summary.complete, "impact_summary_overflow": summary.overflow,
    }));
    if !summary.complete {
        envelope.partial = true;
        let message = if summary.issue.is_empty() { "the bounded VM impact summary is incomplete" } else { &summary.issue };
        envelope.issue = Some(Issue::new("impact_summary_partial", message, "host", None));
    }
    if action == "status" {
        return Ok(envelope);
    }
    let current = session.runtime.vm(socket, session.budget).ok().flatten().ok_or("VM fence could not be re-read")?;
    if current.state != before.state || current.boot_id != before.boot_id {
        return Err("VM changed after observation; inspect and retry with the same request id only after reconciling its outcome".into());
    }
    let fence = Some(hostv1::VmMutationFence { state: before.state, boot_id: before.boot_id.clone(), observed_workloads: summary.known_workloads.clone(), whole_vm_impact_acknowledged: true });
    let request_id = options.request_id.clone();
    let result = if action == "start" {
        session.runtime.start_vm(socket, hostv1::StartVmRequest { request_id, fence }, session.budget)
    } else {
        session.runtime.stop_vm(socket, hostv1::StopVmRequest { request_id, force: options.force, fence }, session.budget)
    };
    let operation = match result {
        Ok(operation) => operation.filter(|operation| operation.request_id == options.request_id).ok_or("Host owner returned an unbound operation")?,
        Err(failure) if failure.reason == "VM_TERMINAL_PERSIST_FAILED" => {
            let message = "the Host accepted the VM mutation but could not save its terminal result; query the same request, then restart the Host service if it remains running";
            terminal_result_not_saved(&mut envelope, "vm_terminal_persist_failed", message, &failure, &options.request_id);
            return Ok(envelope);
        }
        Err(failure) if failure.reason.is_empty() || failure.reason == "VM_ADMISSION_OUTCOME_UNKNOWN" => {
            unknown_outcome(&mut envelope, "the Host VM mutation result is unknown; reconcile by retrying only the same request id", &options.request_id);
            return Ok(envelope);
        }
        Err(failure) => return Err(format!("Host owner rejected vm {action}: {}", failure.message)),
    };
    report_operation(&mut envelope, &operation);
    match operation.state() {
        OperationState::Running => envelope.next_action = next("operation_follow", "service_user"),
        OperationState::Failed | OperationState::Interrupted => {
            envelope.partial = true;
            envelope.next_action = next("vm_status", "service_user");
        }
        _ => {}
    }
    Ok(envelope)
}

/// The refusal of a command that the current user must not run, with the permission that it needs.
pub fn privilege_issue(command: &str, euid: u32) -> Option<(Issue, &'static str)> {
    if cfg!(target_os = "macos") {
        let refused = command == "repair" && euid == 0;
        return refused.then(|| (Issue::new("needs_service_user", "repair must run as the configured non-root service user", "lifecycle", Some(true)), "service_user"));
    }
    (euid != 0).then(|| (Issue::new("needs_administrator", &format!("{command} changes the Linux system service; run it as root"), "service", Some(true)), "administrator"))
}

/// Parses a helper response of at most 1 MiB.
pub fn helper_object(data: &[u8]) -> Option<Facts> {
    (data.len() <= 1 << 20).then(|| serde_json::from_slice(data).ok()).flatten()
}

pub fn repair(session: Session, options: &Options) -> Result<Envelope, String> {
    let status = identified_status(session, options)?;
    let euid = session.runtime.euid();
    let mut envelope = Envelope::new("repair", session.runtime.now(), facts::target_from_status(&status, options));
    envelope.source = vec!["lifecycle".into(), SERVICE_SOURCE.into()];
    envelope.freshness = "current".into();
    envelope.state = facts::state(&status).into();
    if let Some((issue, permission)) = privilege_issue("repair", euid) {
        envelope.state = issue.code.clone();
        envelope.issue = Some(issue);
        envelope.next_action = next("repair", permission);
        return Ok(envelope);
    }
    if facts::boolean(&status, "hostLoaded") && !facts::boolean(&status, "hostRunning") && euid != 0 && facts::string(&status, "hostJob").starts_with("system/") {
        envelope.state = "needs_administrator".into();
        envelope.issue = Some(Issue::new("needs_administrator", "the installed system service is loaded but not running", "service", Some(true)));
        envelope.next_action = next("service_start", "administrator");
        return Ok(envelope);
    }
    let mut arguments = vec!["host-repair".to_string(), "--request-id".to_string(), options.request_id.clone()];
    if !options.registration_id.is_empty() {
        arguments.extend(["--registration-id".to_string(), options.registration_id.clone()]);
    }
    arguments.extend(options.service_arguments());
    let output = session.runtime.helper(&options.lifecycle, &arguments, None, session.budget);
    let operation = helper_object(&output.stdout).unwrap_or_default();
    let bound = facts::string(&operation, "requestId") == options.request_id && facts::string(&operation, "targetRegistrationId") == options.registration_id;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if let (Some(error), false) = (&output.error, bound) {
        envelope.state = "transition_in_progress".into();
        envelope.disposition = EXCLUSIVE_TRANSITION.into();
        envelope.stage = "lifecycle_response_pending".into();
        envelope.partial = true;
        envelope.issue = Some(Issue::new("unknown_outcome", "repair may have been accepted; retry only the same request id and registration target", "lifecycle", Some(true)));
        envelope.next_action = next("retry_same_request_id", "service_user");
        envelope.facts = object(json!({"request_id": options.request_id, "transport_error": bounded(first(&[stderr.as_ref(), error.as_str()]), 256)}));
        let active = facts::string(&operation, "requestId");
        if !active.is_empty() && active != options.request_id {
            envelope.facts.insert("active_request_id".into(), json!(active));
        }
        return Ok(envelope);
    }
    if !bound {
        return Err("lifecycle returned an unbound repair operation".into());
    }
    envelope.state = facts::string(&operation, "outcome").into();
    envelope.stage = facts::string(&operation, "stage").into();
    envelope.operation("lifecycle", facts::string(&operation, "operationId"));
    if output.error.is_some() {
        let code = first(&[facts::string(&operation, "failureCode"), "repair_failed"]);
        let message = bounded(first(&[facts::string(&operation, "failureMessage"), stderr.as_ref()]), 1024);
        envelope.issue = Some(Issue::new(code, &message, "lifecycle", Some(facts::boolean(&operation, "retryable"))));
    }
    Ok(envelope)
}

/// The message of a cleanup that the Host refused: the code and the reason, without the text of the Host.
fn cleanup_rejection(action: &str, failure: &Failure) -> String {
    let code = failure.code.map_or("Unknown", |code| code.name());
    let reason = if failure.reason.is_empty() { "UNKNOWN" } else { &failure.reason };
    let guidance = if failure.reason == "AGENT_ACCESS_ACTIVE" { "; complete removal of the exact agent integration before environment discard" } else { "" };
    format!("Host cleanup {action} rejected: rpc_code={code} reason={reason}{guidance}")
}

fn cleanup_impacts(impacts: &[hostv1::CleanupImpact]) -> Vec<Value> {
    let impact = |impact: &hostv1::CleanupImpact| {
        let resource = impact.resource.clone().unwrap_or_default();
        json!({
            "kind": resource.kind, "environment_id": resource.environment_id, "resource_id": resource.resource_id, "generation": resource.generation,
            "generation_id": resource.generation_id, "effect": impact.effect, "irreversible": impact.irreversible, "blocker": impact.blocker,
        })
    };
    impacts.iter().map(impact).collect()
}

pub fn cleanup(session: Session, action: &str, options: &Options) -> Result<Envelope, String> {
    let kind = if options.cleanup_kind == "discard" { hostv1::CleanupKind::Discard } else { hostv1::CleanupKind::Reset };
    let socket = &options.operator_socket;
    let mut envelope = Envelope::new("cleanup", session.runtime.now(), object(json!({"environment_id": options.environment_id})));
    envelope.source = vec!["host_cleanup_owner".into()];
    envelope.freshness = "current".into();
    if action == "preview" {
        let plan = session.runtime.plan_cleanup(socket, &options.environment_id, kind, session.budget).map_err(|failure| cleanup_rejection("preview", &failure))?;
        let plan = plan.ok_or("Host returned no cleanup plan")?;
        if plan.environment_id != options.environment_id || plan.kind() != kind || plan.revision == 0 {
            return Err("Host returned an unbound cleanup plan".into());
        }
        if plan.impacts.len() > CLEANUP_IMPACT_LIMIT {
            return Err(format!("cleanup plan has {} impacts; the managed CLI limit is {CLEANUP_IMPACT_LIMIT}", plan.impacts.len()));
        }
        envelope.state = "planned".into();
        envelope.stage = "preview".into();
        envelope.facts = object(json!({
            "kind": options.cleanup_kind, "plan_revision": plan.revision, "impacts": cleanup_impacts(&plan.impacts), "impact_count": plan.impacts.len(),
            "reclaimable_bytes": null, "reclaimable_bytes_available": false,
        }));
        return Ok(envelope);
    }
    let plan = hostv1::CleanupPlan { environment_id: options.environment_id.clone(), kind: kind as i32, revision: options.plan_revision, impacts: Vec::new() };
    let mut confirmed = options.confirm_volumes.clone();
    confirmed.sort();
    envelope.state = "submitted".into();
    envelope.stage = "execute".into();
    envelope.facts = object(json!({
        "kind": options.cleanup_kind, "plan_revision": options.plan_revision, "confirmed_volume_ids": confirmed, "reclaimable_bytes": null, "reclaimable_bytes_available": false,
    }));
    let request = hostv1::ExecuteCleanupRequest { request_id: options.request_id.clone(), plan: Some(plan), confirmed_volume_ids: confirmed };
    let operation = match session.runtime.execute_cleanup(socket, request, session.budget) {
        Ok(operation) => operation.filter(|operation| operation.request_id == options.request_id).ok_or("Host cleanup owner returned an unbound operation")?,
        Err(failure) => {
            let expired = session.budget.expired();
            return match failure.reason.as_str() {
                "CLEANUP_PLAN_STALE" => Err("cleanup references changed during execution; request a new plan".into()),
                "CLEANUP_ADMISSION_CANCELED" if !expired => Err("the Host canceled cleanup admission before it accepted an operation; inspect the competing mutation and request a new plan".into()),
                "CLEANUP_ADMISSION_TIMEOUT" if !expired => Err("cleanup admission timed out before the Host accepted an operation; wait for the competing mutation and request a new plan".into()),
                "CLEANUP_TERMINAL_PERSIST_FAILED" => {
                    let message = "the Host accepted cleanup but could not save its terminal result; query the same request, then restart the Host service if it remains running";
                    terminal_result_not_saved(&mut envelope, "cleanup_terminal_persist_failed", message, &failure, &options.request_id);
                    Ok(envelope)
                }
                reason if reason.is_empty() || reason == "CLEANUP_ADMISSION_OUTCOME_UNKNOWN" || expired => {
                    unknown_outcome(&mut envelope, "the Host cleanup result is unknown; reconcile by retrying only the same request id and plan revision", &options.request_id);
                    Ok(envelope)
                }
                _ => Err(cleanup_rejection("execute", &failure)),
            };
        }
    };
    report_operation(&mut envelope, &operation);
    if matches!(operation.state(), OperationState::Failed | OperationState::Interrupted) {
        envelope.partial = true;
        envelope.next_action = next("cleanup_preview_new_request", "service_user");
    }
    Ok(envelope)
}

fn host_socket_fact(socket: &hostv1::HostSocket) -> Value {
    json!({"environment_id": socket.environment_id, "port": socket.port, "path": socket.path, "revision": socket.revision})
}

pub fn host_socket(session: Session, action: &str, options: &Options) -> Result<Envelope, String> {
    let socket = &options.operator_socket;
    let mut envelope = Envelope::new("host-socket", session.runtime.now(), object(json!({"environment_id": options.environment_id})));
    envelope.source = vec!["host_operator".into()];
    envelope.freshness = "current".into();
    let (request_id, environment_id) = (options.request_id.clone(), options.environment_id.clone());
    let (state, facts) = match action {
        "set" => {
            let request = hostv1::SetHostSocketRequest { request_id, environment_id, port: options.port, path: options.path.clone() };
            let approved = session.runtime.set_host_socket(socket, request, session.budget).map_err(|failure| failure.message)?.unwrap_or_default();
            ("approved", json!({"host_sockets": [host_socket_fact(&approved)]}))
        }
        "remove" => {
            let request = hostv1::RemoveHostSocketRequest { request_id, environment_id, port: options.port };
            session.runtime.remove_host_socket(socket, request, session.budget).map_err(|failure| failure.message)?;
            ("removed", json!({"port": options.port}))
        }
        _ => {
            let listed = session.runtime.host_sockets(socket, Some(environment_id).filter(|id| !id.is_empty()), session.budget).map_err(|failure| failure.message)?;
            ("observed", json!({"host_sockets": listed.iter().map(host_socket_fact).collect::<Vec<_>>()}))
        }
    };
    envelope.state = state.into();
    envelope.stage = if action == "set" || action == "remove" { action.into() } else { "list".into() };
    envelope.facts = object(facts);
    Ok(envelope)
}
