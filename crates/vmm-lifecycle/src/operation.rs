//! One lifecycle request: admission against the owner journal, the action, and
//! the receipt that a repeat of the same request identity reads again.

use crate::error::{Deadline, Error, Kind, Result, RpcCode, err};
use crate::journal::{self, Journal, LifecycleOperation, receipt_name};
use crate::{Context, Paths, legacy, new_operation_id, now_millis, operator, status};
use std::time::Duration;

#[derive(Clone, Debug, Default)]
pub struct Request {
    pub kind: String,
    pub request_id: String,
    pub registration_id: String,
    pub release_id: String,
    pub artifact_sha256: String,
    pub artifact_size: u64,
    /// A failed receipt of the same request can run again.
    pub retry_failed: bool,
}

impl Request {
    pub fn new(kind: &str, request_id: &str) -> Self {
        Self { kind: kind.into(), request_id: request_id.into(), ..Self::default() }
    }

    pub fn retryable(kind: &str, request_id: &str, registration_id: &str) -> Self {
        Self { registration_id: registration_id.into(), retry_failed: true, ..Self::new(kind, request_id) }
    }
}

/// The receipt to print, and the failure that makes the command exit non-zero.
pub type Outcome = (LifecycleOperation, Result<()>);

fn refuse(operation: LifecycleOperation, message: impl Into<String>) -> Outcome {
    (operation, Err(err(message)))
}

pub fn run(context: &Context, paths: &Paths, request: Request, action: &mut dyn FnMut() -> Result<()>) -> Outcome {
    let _lock = match journal::lock(context, paths) {
        Ok(lock) => lock,
        Err(error) => {
            let mut operation = LifecycleOperation::unknown(&request.request_id, &request.kind);
            if error.kind == Kind::OwnerBusy {
                operation.failure_code = "owner_busy".into();
                operation.failure_message = error.message.clone();
                operation.retryable = true;
            }
            return (operation, Err(error));
        }
    };
    match admit(context, paths, request, action) {
        Ok(outcome) => outcome,
        Err(error) => (LifecycleOperation::default(), Err(error)),
    }
}

/// The identity of the legacy plan that the latest managed installation wrote.
fn legacy_plan_registration(context: &Context, paths: &Paths, request_id: &str) -> Option<String> {
    let data = journal::read_plan_state(context, paths, request_id).ok()?;
    let plan: serde_json::Value = serde_json::from_slice(&data).ok()?;
    plan["material"]["registration_id"].as_str().map(str::to_string)
}

fn admit(context: &Context, paths: &Paths, mut request: Request, action: &mut dyn FnMut() -> Result<()>) -> Result<Outcome> {
    if request.request_id.is_empty() {
        request.request_id = new_operation_id("req");
    }
    let Request { kind, request_id, registration_id, .. } = &request;
    let journal = Journal::open(context, paths)?;
    let mut state = journal.state()?;

    if !state.active_request.is_empty() && state.active_request != *request_id {
        let previous = journal.receipt(&state.active_request)?;
        if previous.exclusive() {
            let busy = Kind::ExclusiveBusy(state.active_request.clone());
            return Ok((previous, Err(Error::new(busy, format!("exclusive_transition_in_progress: continue request {}", state.active_request)))));
        }
        // The lock proves that the old executor exited. Remote waiting is local to its target.
        state.active_request.clear();
        journal.write_state(&state)?;
    }
    let next = state.superseded_by(request_id).to_string();
    if !next.is_empty() {
        let mut previous = journal.receipt(request_id)?;
        previous.outcome = "superseded".into();
        previous.retryable = false;
        previous.superseded_by_request_id = next.clone();
        return Ok(refuse(previous, format!("request was superseded by {next}")));
    }
    if state.uninstalled || state.blocked(request_id) || !registration_id.is_empty() && state.removed(registration_id) && kind != "registration-remove" {
        return Err(err("the local owner removed this registration or Host installation"));
    }
    if kind == "registration-recover" && !registration_id.is_empty() {
        let prior_id = state.registration_request(registration_id).to_string();
        let newer_owner = !prior_id.is_empty() && prior_id != *request_id;
        if let Ok(legacy) = journal.receipt(request_id)
            && legacy.kind == "install-managed"
            && newer_owner
        {
            return Ok(refuse(legacy, "a newer request owns this Registration"));
        }
        if newer_owner {
            let mut prior = journal.receipt(&prior_id)?;
            if matches!(prior.kind.as_str(), "registration-claim" | "registration-recover") && prior.outcome != "succeeded" && prior.outcome != "superseded" {
                prior.superseded_by_request_id = request_id.clone();
                prior.retryable = false;
                journal.write(&receipt_name(&prior_id), &prior)?;
                state.superseded_requests.insert(prior_id, request_id.clone());
                journal.write_state(&state)?;
            }
        }
    }
    if !registration_id.is_empty() && (kind == "registration-remove" || kind == "registration-drain" || kind.contains("DRAINING")) {
        let mut previous_id = state.registration_request(registration_id).to_string();
        if previous_id.is_empty()
            && !state.latest_request.is_empty()
            && let Ok(prior) = journal.receipt(&state.latest_request)
            && prior.kind == "install-managed"
            && legacy_plan_registration(context, paths, &prior.request_id).as_deref() == Some(registration_id)
        {
            previous_id = prior.request_id;
        }
        if !previous_id.is_empty() && previous_id != *request_id {
            let mut prior = journal.receipt(&previous_id)?;
            if prior.kind.starts_with("registration-") || prior.kind == "install-managed" {
                prior.outcome = "superseded".into();
                prior.retryable = false;
                prior.superseded_by_request_id = request_id.clone();
                journal.write(&receipt_name(&previous_id), &prior)?;
                state.superseded_requests.insert(previous_id.clone(), request_id.clone());
                state.blocked_installs.insert(previous_id, true);
            }
        }
        if kind == "registration-remove" {
            state.removed_registrations.insert(registration_id.clone(), true);
        }
        journal.write_state(&state)?;
    }

    let previous = match journal.receipt(request_id) {
        Ok(previous) => Some(previous),
        Err(error) if error.not_found() => None,
        Err(error) => return Err(error),
    };
    drop(journal);
    let correlation = paths.correlation_request_id.clone().unwrap_or_default();
    let mut previous_failure_stage = String::new();
    let mut operation = match previous {
        Some(mut previous) => {
            if !correlation.is_empty() && !previous.correlation_request_id.is_empty() && previous.correlation_request_id != correlation {
                return Ok(refuse(previous, "legacy request already accepted another correlation"));
            }
            if previous.outcome == "superseded" || !previous.superseded_by_request_id.is_empty() {
                let message = format!("request was superseded by {}", previous.superseded_by_request_id);
                return Ok(refuse(previous, message));
            }
            if kind == "registration-recover" && previous.kind == "install-managed" {
                if previous.outcome == "succeeded" {
                    return Ok(refuse(previous, "the legacy request already completed"));
                }
                if previous.target_registration_id != *registration_id {
                    if !previous.target_registration_id.is_empty() {
                        return Ok(refuse(previous, "legacy request belongs to another registration"));
                    }
                    match legacy::registration_target(context, paths, request_id) {
                        Ok(target) if target == *registration_id => {}
                        Ok(_) => return Ok(refuse(previous, "legacy request belongs to another registration")),
                        Err(error) => return Ok((previous, Err(error))),
                    }
                }
                previous.kind = kind.clone();
                previous.target_registration_id = registration_id.clone();
            }
            let expected = LifecycleOperation {
                request_id: request_id.clone(),
                kind: kind.clone(),
                target_registration_id: registration_id.clone(),
                target_release_id: request.release_id.clone(),
                target_artifact_sha256: request.artifact_sha256.clone(),
                target_artifact_size: request.artifact_size,
                ..LifecycleOperation::default()
            };
            if !previous.same_target(&expected) {
                return Ok(refuse(previous, "operation request identity was reused for a different target"));
            }
            if previous.outcome == "succeeded" {
                return Ok((previous, Ok(())));
            }
            if previous.outcome == "failed" {
                if !request.retry_failed {
                    return Ok(refuse(previous, "operation already failed; use a new request id after reviewing status"));
                }
                let always = kind == "registration-recover" || kind == "registration-claim";
                let limit = previous.failure_code == "retry_limit_reached" || previous.failure_code == "update_retry_limit_reached";
                let conflict = (kind == "registration-revoke" || kind == "registration-remove") && previous.failure_message.contains("registration revision conflict");
                if !previous.retryable && !always && !limit && !conflict {
                    return Ok(refuse(previous, "operation failure requires operator action"));
                }
            }
            if !request.retry_failed {
                return Ok(observe_unretried(context, paths, previous));
            }
            previous_failure_stage = previous.failure_stage.clone();
            let new_correlation = !correlation.is_empty() && previous.correlation_request_id.is_empty();
            if new_correlation {
                previous.correlation_request_id = correlation.clone();
            }
            previous.outcome = "pending".into();
            previous.attempt += 1;
            previous.failure_stage.clear();
            previous.failure_code.clear();
            previous.failure_message.clear();
            previous.retryable = false;
            previous.updated_at = now_millis();
            if let Err(error) = journal::persist(context, paths, &previous) {
                if new_correlation {
                    let unavailable = LifecycleOperation {
                        target_registration_id: registration_id.clone(),
                        failure_code: "owner_journal_unavailable".into(),
                        retryable: true,
                        ..LifecycleOperation::unknown(&correlation, kind)
                    };
                    return Ok((unavailable, Err(error)));
                }
                return Ok((previous, Err(error)));
            }
            previous
        }
        None => {
            let now = now_millis();
            let operation = LifecycleOperation {
                operation_id: new_operation_id("op"),
                request_id: request_id.clone(),
                kind: kind.clone(),
                target_registration_id: registration_id.clone(),
                target_release_id: request.release_id.clone(),
                target_artifact_sha256: request.artifact_sha256.clone(),
                target_artifact_size: request.artifact_size,
                outcome: "pending".into(),
                attempt: 1,
                started_at: now,
                updated_at: now,
                ..LifecycleOperation::default()
            };
            if let Err(error) = journal::persist(context, paths, &operation) {
                return Ok((operation, Err(error)));
            }
            operation
        }
    };

    let result = action();
    // The action records its stage and cutover progress in the same receipt.
    let current = journal::read_latest(context, paths).filter(|current| current.same_target(&operation));
    match result {
        Err(error) => {
            if let Some(current) = current {
                operation = current;
            }
            operation.outcome = "failed".into();
            let failure = classify(kind, &error);
            operation.failure_stage = failure.stage;
            operation.failure_code = failure.code;
            operation.failure_message = failure.message;
            operation.retryable = failure.retryable;
            if (kind == "repair" || kind == "update") && !operation.stage.is_empty() {
                operation.failure_stage = operation.stage.clone();
            }
            match &error.kind {
                Kind::Preparation(descriptor) => {
                    operation.failure_stage = descriptor.stage.clone();
                    operation.failure_code = descriptor.code.clone();
                    operation.failure_message = descriptor.message.clone();
                    operation.retryable = descriptor.retryable;
                }
                Kind::Maintenance(code) => {
                    operation.failure_code = code.clone();
                    operation.retryable = false;
                }
                _ if kind == "update" && matches!(operation.stage.as_str(), "stop" | "replace" | "restore" | "settled") => {
                    // After the stop, only the same request and release can repair forward.
                    if operation.stage != previous_failure_stage {
                        operation.attempt = 1;
                    }
                    operation.failure_code = "update_forward_repair_required".into();
                    operation.retryable = true;
                }
                _ => {}
            }
            operation.updated_at = now_millis();
            if let Err(persist) = journal::persist(context, paths, &operation) {
                operation.outcome = "unknown".into();
                return Ok((operation, Err(Error::new(error.kind, format!("{}\n{}", error.message, persist.message)))));
            }
            Ok((operation, Err(error)))
        }
        Ok(()) => {
            if let Some(current) = current {
                operation.stage = current.stage;
                operation.cutover = current.cutover;
            }
            operation.outcome = "succeeded".into();
            operation.updated_at = now_millis();
            if let Err(error) = journal::persist(context, paths, &operation) {
                operation.outcome = "unknown".into();
                return Ok((operation, Err(error)));
            }
            Ok((operation, Ok(())))
        }
    }
}

/// A request without a retry rule is settled only by what the service shows now.
fn observe_unretried(context: &Context, paths: &Paths, mut previous: LifecycleOperation) -> Outcome {
    let observed = status::read(context, paths, None, Deadline::after(Duration::from_secs(4)));
    let confirmed = match previous.kind.as_str() {
        "install" | "repair" => observed.host_installed && observed.host_loaded && observed.host_healthy,
        "drain" => !observed.host_loaded,
        "remove" => !observed.app_installed && !observed.host_installed,
        _ => false,
    };
    previous.updated_at = now_millis();
    if confirmed {
        previous.outcome = "succeeded".into();
        if let Err(error) = journal::persist(context, paths, &previous) {
            previous.outcome = "unknown".into();
            return (previous, Err(error));
        }
        return (previous, Ok(()));
    }
    previous.outcome = "unknown".into();
    let _ = journal::persist(context, paths, &previous);
    refuse(previous, "operation outcome is unknown; read status and use a new request id only after review")
}

struct Failure {
    stage: String,
    code: String,
    message: String,
    retryable: bool,
}

fn truncate(message: &str, limit: usize) -> &str {
    let mut end = limit.min(message.len());
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}

fn classify(kind: &str, error: &Error) -> Failure {
    let failure = |stage: &str, code: &str, message: &str, retryable: bool| Failure { stage: stage.into(), code: code.into(), message: message.into(), retryable };
    if let Kind::Http { status, code } = &error.kind {
        let code = if code.is_empty() { "registration_http_error" } else { code };
        return failure(kind, code, &error.message, *status == 408 || *status == 429 || *status >= 500);
    }
    if error.rpc_reason() == Some("LOCAL_DISPOSED") {
        return failure(kind, "local_disposed", "The local operator deleted this registration. Start a new authorized setup to connect again.", false);
    }
    let flat = error.message.replace('\n', " ");
    let message = truncate(flat.trim(), 512);
    let stage = message.strip_prefix("lifecycle ").and_then(|rest| rest.split_once(':')).map_or(kind, |(step, _)| step.trim());
    let stage = if stage.is_empty() { kind } else { stage };
    let retry_class = matches!(kind, "install" | "install-managed" | "repair" | "update") || kind.starts_with("registration-");
    if !retry_class {
        return failure(stage, "operation_failed", message, false);
    }
    if error.kind == Kind::Timeout || error.rpc_code() == Some(RpcCode::DeadlineExceeded) {
        return failure(stage, "transient_timeout", message, true);
    }
    if error.kind == Kind::Interrupted {
        return failure(stage, "interrupted", message, true);
    }
    if (kind == "registration-revoke" || kind == "registration-remove") && operator::revision_conflict(error) {
        return failure(stage, "registration_revision_changed", "The registration changed. Continue removal of the same registration.", true);
    }
    if error.rpc_code() == Some(RpcCode::Unavailable) {
        return failure(stage, "transient_unavailable", message, true);
    }
    let upstream = (500..=599).any(|status| message.contains(&format!("HTTP {status}"))) || message.contains("registration claim request failed");
    if upstream {
        return failure(stage, "upstream_unavailable", message, true);
    }
    failure(stage, if kind == "install-managed" { "install_failed" } else { "operation_failed" }, message, false)
}

/// Records the stage of a running `repair` or `update` request.
pub fn persist_stage(context: &Context, paths: &Paths, request: &Request, stage: &str) -> Result<()> {
    let owned = |operation: &LifecycleOperation| {
        operation.request_id == request.request_id
            && operation.kind == request.kind
            && operation.target_registration_id == request.registration_id
            && operation.target_release_id == request.release_id
    };
    let mut operation = journal::read_latest(context, paths).filter(owned).ok_or_else(|| err(format!("{} operation is not owned by this request", request.kind)))?;
    operation.stage = stage.to_string();
    operation.updated_at = now_millis();
    journal::persist(context, paths, &operation)
}

#[cfg(test)]
#[path = "operation_tests.rs"]
mod tests;
