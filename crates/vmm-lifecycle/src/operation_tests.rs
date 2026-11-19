use super::*;
use crate::journal::MaintenanceState;
use crate::testing::{Fake, installed, rpc};
use std::cell::Cell;

struct Fixture {
    _root: tempfile::TempDir,
    paths: Paths,
    context: Context,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    Fixture { paths, context: Fake::new().context(), _root: root }
}

impl Fixture {
    fn run(&self, request: Request, result: Result<()>) -> (Outcome, u32) {
        let runs = Cell::new(0);
        let mut result = Some(result);
        let outcome = run(&self.context, &self.paths, request, &mut || {
            runs.set(runs.get() + 1);
            result.take().unwrap()
        });
        (outcome, runs.get())
    }

    fn state(&self) -> MaintenanceState {
        Journal::open(&self.context, &self.paths).unwrap().state().unwrap()
    }
}

#[test]
fn a_repeated_request_returns_its_receipt_without_running_again() {
    let fixture = fixture();
    let request = Request::retryable("registration-enable", "request-1", "registration-1");
    let ((first, result), runs) = fixture.run(request.clone(), Ok(()));
    assert!(result.is_ok() && runs == 1);
    assert_eq!((first.kind.as_str(), first.request_id.as_str(), first.outcome.as_str(), first.attempt), ("registration-enable", "request-1", "succeeded", 1));
    assert_eq!(first.target_registration_id, "registration-1");
    let encoded = serde_json::to_value(&first).unwrap();
    assert!(encoded.get("retryable").is_none() && encoded.get("failureCode").is_none(), "an empty optional field is omitted: {encoded}");

    let ((second, result), runs) = fixture.run(request, Ok(()));
    assert!(result.is_ok() && runs == 0);
    assert_eq!(second, first);
    let state = fixture.state();
    assert_eq!(state.registration_request("registration-1"), "request-1");
    assert!(state.completed("request-1") && state.active_request.is_empty());
}

#[test]
fn a_failed_request_keeps_its_receipt_and_retries_under_the_same_identity() {
    let fixture = fixture();
    let request = Request::retryable("registration-claim", "request-1", "registration-1");
    let unavailable = rpc(RpcCode::Unavailable, None).context("lifecycle register");
    let ((failed, result), _) = fixture.run(request.clone(), Err(unavailable));
    assert!(result.is_err());
    assert_eq!((failed.outcome.as_str(), failed.failure_stage.as_str(), failed.failure_code.as_str(), failed.retryable), ("failed", "register", "transient_unavailable", true));

    let ((retried, result), runs) = fixture.run(request.clone(), Ok(()));
    assert!(result.is_ok() && runs == 1);
    assert_eq!((retried.operation_id.as_str(), retried.attempt, retried.outcome.as_str()), (failed.operation_id.as_str(), 2, "succeeded"));
    assert!(retried.failure_code.is_empty() && !retried.retryable);

    let other = Request::retryable("registration-claim", "request-1", "registration-2");
    let ((_, result), runs) = fixture.run(other, Ok(()));
    assert_eq!(result.unwrap_err().message, "operation request identity was reused for a different target");
    assert_eq!(runs, 0);
}

#[test]
fn failures_are_classified_for_the_caller() {
    let http = |status, code: &str| Error::new(Kind::Http { status, code: code.into() }, "registration claim returned HTTP");
    let cases: Vec<(&str, Error, &str, bool)> = vec![
        ("registration-claim", http(401, "invalid_ticket"), "invalid_ticket", false),
        ("registration-claim", http(410, "ticket_expired"), "ticket_expired", false),
        ("registration-claim", http(503, ""), "registration_http_error", true),
        ("registration-claim", err("registration claim request failed"), "upstream_unavailable", true),
        ("registration-recover", rpc(RpcCode::FailedPrecondition, Some("LOCAL_DISPOSED")), "local_disposed", false),
        ("registration-remove", rpc(RpcCode::Aborted, Some("REGISTRATION_REVISION_CONFLICT")), "registration_revision_changed", true),
        ("repair", Error::new(Kind::Timeout, "Agent VMM VM did not become healthy"), "transient_timeout", true),
        ("repair", Error::new(Kind::Interrupted, "stop"), "interrupted", true),
        ("repair", err("Agent VMM application bundle is missing"), "operation_failed", false),
        ("drain", rpc(RpcCode::Unavailable, None), "operation_failed", false),
    ];
    for (kind, error, code, retryable) in cases {
        let failure = classify(kind, &error);
        assert_eq!((failure.code.as_str(), failure.retryable, failure.stage.as_str()), (code, retryable, kind), "{kind}: {error}");
    }
    let long = classify("repair", &err("é".repeat(400)));
    assert!(long.message.len() <= 512 && long.message.chars().all(|character| character == 'é'));
}

#[test]
fn an_unfinished_exclusive_transition_refuses_other_requests() {
    let fixture = fixture();
    let ((update, _), _) = fixture.run(Request { release_id: "release-2".into(), ..Request::retryable("update", "update-1", "") }, Err(err("replace failed")));
    assert_eq!((update.outcome.as_str(), update.retryable), ("failed", false));
    // A failed update that passed its stop is the only request that can continue.
    let mut pending = update.clone();
    pending.stage = "replace".into();
    pending.outcome = "pending".into();
    journal::persist(&fixture.context, &fixture.paths, &pending).unwrap();
    assert_eq!(fixture.state().active_request, "update-1");

    let ((blocked, result), runs) = fixture.run(Request::retryable("registration-enable", "request-2", "registration-1"), Ok(()));
    assert_eq!(result.unwrap_err().kind, Kind::ExclusiveBusy("update-1".into()));
    assert_eq!((blocked.request_id.as_str(), runs), ("update-1", 0));

    let ((forward, result), _) = fixture.run(Request { release_id: "release-2".into(), ..Request::retryable("update", "update-1", "") }, Err(err("restore failed")));
    assert!(result.is_err());
    assert_eq!((forward.failure_code.as_str(), forward.retryable, forward.failure_stage.as_str()), ("update_forward_repair_required", true, "replace"));
}

#[test]
fn removal_fences_the_request_that_owned_the_registration() {
    let fixture = fixture();
    let claim = Request::retryable("registration-claim", "claim-1", "registration-1");
    let _ = fixture.run(claim.clone(), Err(rpc(RpcCode::Unavailable, None)));
    let ((removed, result), _) = fixture.run(Request::retryable("registration-remove", "remove-1", "registration-1"), Ok(()));
    assert!(result.is_ok() && removed.outcome == "succeeded");
    let state = fixture.state();
    assert!(state.removed("registration-1") && state.blocked("claim-1"));
    assert_eq!(state.superseded_by("claim-1"), "remove-1");

    let ((superseded, result), runs) = fixture.run(claim, Ok(()));
    assert_eq!(runs, 0);
    assert_eq!((superseded.outcome.as_str(), superseded.superseded_by_request_id.as_str()), ("superseded", "remove-1"));
    assert_eq!(result.unwrap_err().message, "request was superseded by remove-1");

    let ((_, result), runs) = fixture.run(Request::retryable("registration-claim", "claim-2", "registration-1"), Ok(()));
    assert_eq!((result.unwrap_err().message.as_str(), runs), ("the local owner removed this registration or Host installation", 0));
}

#[test]
fn a_recovery_supersedes_an_unfinished_claim_of_the_same_registration() {
    let fixture = fixture();
    let _ = fixture.run(Request::retryable("registration-claim", "claim-1", "registration-1"), Err(err("registration claim request failed")));
    let ((recovered, result), _) = fixture.run(Request::retryable("registration-recover", "recover-1", "registration-1"), Ok(()));
    assert!(result.is_ok() && recovered.kind == "registration-recover");
    let state = fixture.state();
    assert_eq!((state.superseded_by("claim-1"), state.registration_request("registration-1")), ("recover-1", "recover-1"));
}

#[test]
fn a_held_owner_lock_reports_a_retryable_busy_owner() {
    let fixture = fixture();
    let _held = journal::lock(&fixture.context, &fixture.paths).unwrap();
    let ((operation, result), runs) = fixture.run(Request::new("drain", "drain-1"), Ok(()));
    assert_eq!((result.unwrap_err().kind, runs), (Kind::OwnerBusy, 0));
    assert_eq!((operation.failure_code.as_str(), operation.retryable, operation.outcome.as_str()), ("owner_busy", true, "unknown"));
}

#[test]
fn an_unretried_request_is_settled_by_the_observed_service() {
    let fixture = fixture();
    let mut pending = LifecycleOperation { operation_id: "op-1".into(), outcome: "pending".into(), attempt: 1, ..LifecycleOperation::unknown("drain-1", "drain") };
    journal::persist(&fixture.context, &fixture.paths, &pending).unwrap();
    // The Host job still runs, so the drain is not confirmed.
    let ((unknown, result), runs) = fixture.run(Request::new("drain", "drain-1"), Ok(()));
    assert_eq!((unknown.outcome.as_str(), runs), ("unknown", 0));
    assert!(result.is_err());

    let stopped = Fixture { context: Fake::with(crate::testing::service_manager(false)).context(), paths: fixture.paths.clone(), _root: tempfile::tempdir().unwrap() };
    pending.outcome = "pending".into();
    journal::persist(&stopped.context, &stopped.paths, &pending).unwrap();
    let ((confirmed, result), _) = stopped.run(Request::new("drain", "drain-1"), Ok(()));
    assert!(result.is_ok());
    assert_eq!(confirmed.outcome, "succeeded");
}
