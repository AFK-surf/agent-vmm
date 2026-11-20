//! `update`: prepares the selected release and gives it to the lifecycle owner.
//! One request identity is bound to one release and to one artifact.

use crate::envelope::{ADMINISTRATOR_ACTION, CONTINUE_NORMAL_WORK, EXCLUSIVE_TRANSITION, Envelope, Issue, RETRY_SAME_REQUEST_AT, TERMINAL_ACTION, bounded, next, rfc3339};
use crate::facts::{self, Facts, first};
use crate::mutation::{helper_object, privilege_issue};
use crate::observe::identified_status;
use crate::options::Options;
use crate::runtime::{OBSERVATION_BUDGET, PreparationError, Session};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vmm_lifecycle::Deadline;

/// Retryable failures use bounded exponential backoff starting at 30 seconds and capped at 30 minutes.
const BASE_RETRY_DELAY_SECS: u64 = 30;
const MAX_RETRY_DELAY_SECS: u64 = 30 * 60;
const STAGES: [&str; 7] = ["target_selection", "download", "preparation", "stop", "replace", "recovery", "complete"];

fn retry_delay(attempt: i64) -> Duration {
    let shift = u32::try_from(attempt.saturating_sub(1)).unwrap_or(0).min(6);
    Duration::from_secs((BASE_RETRY_DELAY_SECS << shift).min(MAX_RETRY_DELAY_SECS))
}

fn transition_in_progress(envelope: &mut Envelope, operation: &Facts, code: &str, message: &str) {
    envelope.state = "transition_in_progress".into();
    envelope.stage = first(&[facts::string(operation, "stage"), "lifecycle"]).into();
    envelope.disposition = EXCLUSIVE_TRANSITION.into();
    envelope.operation("lifecycle", facts::string(operation, "operationId"));
    envelope.issue = Some(Issue::new(code, message, "lifecycle", Some(true)));
}

fn target_conflict(envelope: &mut Envelope, message: &str) {
    envelope.state = "action_required".into();
    envelope.stage = "target_selection".into();
    envelope.disposition = TERMINAL_ACTION.into();
    envelope.issue = Some(Issue::new("update_target_conflict", message, "lifecycle", Some(false)));
    envelope.next_action = next("review_selected_release", "administrator");
}

fn same_artifact(operation: &Facts, options: &Options) -> bool {
    facts::string(operation, "targetArtifactSha256").eq_ignore_ascii_case(&options.artifact_sha256) && facts::number(operation, "targetArtifactSize") as u64 == options.artifact_size
}

/// Decides from the recorded operation whether this invocation must not start an update.
/// Returns true when the envelope is the answer. Otherwise the envelope keeps only the attempt count.
fn apply_recorded_operation(envelope: &mut Envelope, operation: &Facts, options: &Options, now: SystemTime, already_completed: bool) -> bool {
    let mut answer = envelope.clone();
    let decided = describe_recorded_operation(&mut answer, operation, options, now, already_completed);
    if decided {
        *envelope = answer;
    } else if let Some(attempt) = answer.facts.remove("attempt") {
        envelope.facts.insert("attempt".into(), attempt);
    }
    decided
}

/// Describes the recorded operation of this request in the envelope. Returns false when the request can run again.
fn describe_recorded_operation(envelope: &mut Envelope, operation: &Facts, options: &Options, now: SystemTime, already_completed: bool) -> bool {
    if operation.is_empty() {
        return false;
    }
    let outcome = facts::string(operation, "outcome");
    let unfinished = matches!(outcome, "pending" | "unknown");
    if facts::string(operation, "kind") != "update" || facts::string(operation, "requestId") != options.request_id {
        if unfinished {
            transition_in_progress(envelope, operation, "lifecycle_transition_in_progress", "a lifecycle transition is already in progress");
        }
        return unfinished;
    }
    if facts::string(operation, "targetReleaseId") != options.target_release {
        target_conflict(envelope, "the request identity is already bound to a different release");
        return true;
    }
    if !same_artifact(operation, options) {
        target_conflict(envelope, "the request identity is already bound to different artifact bytes");
        return true;
    }
    match outcome {
        "succeeded" => {
            envelope.state = "succeeded".into();
            envelope.stage = "complete".into();
            envelope.disposition = CONTINUE_NORMAL_WORK.into();
            envelope.operation("lifecycle", facts::string(operation, "operationId"));
            if already_completed {
                envelope.facts.insert("already_completed".into(), json!(true));
            }
            return true;
        }
        "pending" | "unknown" => {
            transition_in_progress(envelope, operation, "update_transition_in_progress", "the lifecycle owner has not reported a terminal update outcome");
            return true;
        }
        "failed" => {}
        _ => return false,
    }
    envelope.operation("lifecycle", facts::string(operation, "operationId"));
    envelope.stage = first(&[facts::string(operation, "failureStage"), facts::string(operation, "stage"), "preparation"]).into();
    let attempt = facts::number(operation, "attempt");
    envelope.facts.insert("attempt".into(), json!(attempt));
    let code = first(&[facts::string(operation, "failureCode"), "update_failed"]);
    let message = bounded(facts::string(operation, "failureMessage"), 512);
    if !facts::boolean(operation, "retryable") && facts::string(operation, "failureCode") != "update_retry_limit_reached" {
        envelope.state = "action_required".into();
        envelope.disposition = TERMINAL_ACTION.into();
        envelope.issue = Some(Issue::new(code, &message, "lifecycle", Some(false)));
        envelope.next_action = next("review_update_failure", "administrator");
        return true;
    }
    let recorded = UNIX_EPOCH + Duration::from_millis(u64::try_from(facts::number(operation, "updatedAt")).unwrap_or(0));
    let retry_at = recorded + retry_delay(attempt);
    if now < retry_at {
        envelope.state = "retry_wait".into();
        envelope.disposition = RETRY_SAME_REQUEST_AT.into();
        envelope.facts.insert("retry_at".into(), json!(rfc3339(retry_at)));
        envelope.issue = Some(Issue::new(code, &message, "lifecycle", Some(true)));
        envelope.next_action = next("retry_same_request_id", "service_user");
        return true;
    }
    false
}

fn update_arguments(options: &Options, source: &[&str]) -> Vec<String> {
    let mut arguments: Vec<String> = ["host-update"].iter().chain(source).map(|argument| argument.to_string()).collect();
    arguments.extend(["--target-release-id".to_string(), options.target_release.clone(), "--request-id".to_string(), options.request_id.clone()]);
    arguments.extend(["--artifact-sha256".to_string(), options.artifact_sha256.to_lowercase(), "--artifact-size".to_string(), options.artifact_size.to_string()]);
    arguments.extend(options.service_arguments());
    arguments
}

/// Gives the failure to the lifecycle owner, which counts the attempts of the request. Returns the recorded operation.
fn record_preparation_failure(session: Session, options: &Options, failure: &PreparationError) -> Option<Facts> {
    let descriptor = json!({"version": 1, "stage": failure.stage, "code": failure.code, "message": bounded(&failure.message, 512), "retryable": failure.retryable});
    let arguments = update_arguments(options, &["--preparation-failure-stdin"]);
    let output = session.runtime.helper(&options.lifecycle, &arguments, Some(descriptor.to_string().as_bytes()), Deadline::after(OBSERVATION_BUDGET));
    helper_object(&output.stdout).filter(|operation| facts::string(operation, "requestId") == options.request_id && facts::string(operation, "outcome") == "failed")
}

pub fn update(session: Session, options: &Options) -> Result<Envelope, String> {
    let status = identified_status(session, options)?;
    let mut envelope = Envelope::new("update", session.runtime.now(), facts::target_from_status(&status, options));
    envelope.source = vec!["release_target".into(), "lifecycle".into()];
    envelope.freshness = "current".into();
    envelope.state = "preparing".into();
    envelope.stage = "target_selection".into();
    let selected = json!({"request_id": options.request_id, "target_release_id": options.target_release, "artifact_sha256": options.artifact_sha256.to_lowercase(), "artifact_size": options.artifact_size, "stages": STAGES});
    envelope.facts = selected.as_object().cloned().unwrap_or_default();
    if facts::string(&status, "hostJob").starts_with("system/") {
        envelope.state = "needs_administrator".into();
        envelope.disposition = ADMINISTRATOR_ACTION.into();
        envelope.issue = Some(Issue::new("administrator_update_required", "the daemon Host update requires the administrator installer and fixed service executor", "service", Some(true)));
        envelope.next_action = next("administrator_update", "administrator");
        return Ok(envelope);
    }
    if let Some((issue, permission)) = privilege_issue("update", session.runtime.euid()) {
        envelope.state = issue.code.clone();
        envelope.disposition = ADMINISTRATOR_ACTION.into();
        envelope.issue = Some(issue);
        envelope.next_action = next("update", permission);
        return Ok(envelope);
    }
    let recorded = facts::object(&status, "operation").cloned().unwrap_or_default();
    if apply_recorded_operation(&mut envelope, &recorded, options, session.runtime.now(), true) {
        return Ok(envelope);
    }
    let prepared = match session.runtime.prepare_update(options, &status) {
        Ok(prepared) => prepared,
        Err(mut failure) => {
            failure.message = bounded(&failure.message, 512);
            if failure.cancelled {
                failure = PreparationError { code: "update_preparation_cancelled".into(), retryable: true, message: "the update download or preparation was cancelled; the current installation was not changed".into(), ..failure };
            }
            let Some(operation) = record_preparation_failure(session, options, &failure) else {
                envelope.state = "action_required".into();
                envelope.stage = failure.stage.into();
                envelope.disposition = TERMINAL_ACTION.into();
                envelope.issue = Some(Issue::new("update_failure_record_unknown", "the selected artifact was not installed and its retry state could not be recorded", "lifecycle", Some(false)));
                envelope.next_action = next("review_update_failure", "administrator");
                return Ok(envelope);
            };
            describe_recorded_operation(&mut envelope, &operation, options, session.runtime.now(), false);
            return Ok(envelope);
        }
    };
    if prepared.is_empty() {
        envelope.state = "succeeded".into();
        envelope.stage = "complete".into();
        envelope.disposition = CONTINUE_NORMAL_WORK.into();
        envelope.facts.insert("already_current".into(), json!(true));
        return Ok(envelope);
    }
    envelope.stage = "preparation".into();
    // The owner continues without this command when it needs longer than the observation budget.
    let output = session.runtime.helper(&options.lifecycle, &update_arguments(options, &["--source-app", prepared.as_str()]), None, Deadline::after(OBSERVATION_BUDGET));
    let operation = helper_object(&output.stdout).unwrap_or_default();
    let bound = facts::string(&operation, "requestId") == options.request_id
        && facts::string(&operation, "kind") == "update"
        && facts::string(&operation, "targetReleaseId") == options.target_release
        && same_artifact(&operation, options);
    if output.error.is_some() && !bound {
        envelope.state = "transition_in_progress".into();
        envelope.stage = "owner_response_pending".into();
        envelope.disposition = EXCLUSIVE_TRANSITION.into();
        envelope.partial = true;
        envelope.issue = Some(Issue::new("unknown_outcome", "the lifecycle owner may have accepted the update; retry only the same request id and selected release", "lifecycle", Some(true)));
        envelope.next_action = next("retry_same_request_id", "service_user");
        envelope.facts.insert("prepared_app".into(), json!(prepared));
        let active = facts::string(&operation, "requestId");
        if !active.is_empty() && active != options.request_id {
            envelope.facts.insert("active_request_id".into(), json!(active));
        }
        return Ok(envelope);
    }
    if !bound {
        return Err("lifecycle returned an unbound update operation".into());
    }
    if facts::string(&operation, "outcome") == "succeeded" && session.runtime.remove_prepared(&prepared).is_err() {
        return Err("retired Host bundle could not be removed after successful update".into());
    }
    if apply_recorded_operation(&mut envelope, &operation, options, session.runtime.now(), false) {
        return Ok(envelope);
    }
    envelope.state = facts::string(&operation, "outcome").into();
    envelope.stage = first(&[facts::string(&operation, "stage"), "preparation"]).into();
    envelope.operation("lifecycle", facts::string(&operation, "operationId"));
    envelope.disposition = CONTINUE_NORMAL_WORK.into();
    Ok(envelope)
}
