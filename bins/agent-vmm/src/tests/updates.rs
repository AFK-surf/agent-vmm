//! `update` against a scripted lifecycle owner and artifact preparation.

use super::*;

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const PREPARED: &str = "/Users/worker/Applications/.Agent VMM Host.update-1.app";

fn update(fake: &Fake) -> (i32, String, String) {
    run(fake, &["update", "--json", "--lifecycle-helper", "/h", "--request-id", "update-1", "--target-release-id", "agent-vmm-2", "--artifact-url", "https://releases.example/host.zip", "--artifact-sha256", DIGEST.to_uppercase().as_str(), "--artifact-size", "42"])
}

fn answer(fake: &Fake) -> Value {
    let (code, stdout, stderr) = update(fake);
    assert_eq!((code, stderr.as_str()), (0, ""), "{stdout}");
    serde_json::from_str(&stdout).unwrap()
}

fn answer_of(fake: Fake) -> Value {
    answer(&fake)
}

/// The update operation that the lifecycle owner records for the request of these tests.
fn operation(facts: Value) -> Value {
    let mut operation = json!({"operationId": "lifecycle-1", "kind": "update", "requestId": "update-1", "targetReleaseId": "agent-vmm-2", "targetArtifactSha256": DIGEST, "targetArtifactSize": 42});
    operation.as_object_mut().unwrap().extend(facts.as_object().unwrap().clone());
    operation
}

fn machine(outputs: Vec<HelperOutput>, preparation: Option<Result<String, PreparationError>>) -> Fake {
    Fake { preparation: RefCell::new(preparation), ..Fake::new(outputs) }
}

fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[test]
fn a_prepared_release_is_given_to_the_lifecycle_owner_once() {
    let fake = machine(vec![healthy(), printed(operation(json!({"outcome": "succeeded", "stage": "complete"})))], Some(Ok(PREPARED.into())));
    let report = answer(&fake);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["disposition"].as_str(), &report["operation_ref"]), (Some("succeeded"), Some("complete"), Some("continue_normal_work"), &json!({"owner": "lifecycle", "id": "lifecycle-1"})));
    assert!(report["facts"].get("already_completed").is_none(), "a freshly completed update is not marked already_completed");
    assert_eq!((report["facts"]["target_release_id"].as_str(), report["facts"]["artifact_sha256"].as_str(), report["facts"]["artifact_size"].as_u64(), &report["source"]), (Some("agent-vmm-2"), Some(DIGEST), Some(42), &json!(["release_target", "lifecycle"])));
    assert_eq!(fake.helper_calls.borrow()[1].0, ["host-update", "--source-app", PREPARED, "--target-release-id", "agent-vmm-2", "--request-id", "update-1", "--artifact-sha256", DIGEST, "--artifact-size", "42"]);
    assert_eq!(*fake.removed.borrow(), [PREPARED], "the retired release is removed after the owner finished");

    let current = answer_of(machine(vec![healthy()], Some(Ok(String::new()))));
    assert_eq!((current["state"].as_str(), current["facts"]["already_current"].as_bool(), &current["operation_ref"]), (Some("succeeded"), Some(true), &Value::Null));
}

#[test]
fn an_update_never_starts_where_it_cannot_finish() {
    // A system job is updated by the administrator installer.
    let daemon = machine(vec![status(json!({"hostJob": "system/com.afk-surf.agent-vmm-host"}))], None);
    let report = answer(&daemon);
    assert_eq!((report["state"].as_str(), report["disposition"].as_str(), report["issue"]["code"].as_str(), &report["next_action"]), (Some("needs_administrator"), Some("administrator_action_required"), Some("administrator_update_required"), &json!([{"action": "administrator_update", "permission": "administrator"}])));

    // Another transition owns the installation.
    let busy = machine(vec![status(json!({"operation": {"operationId": "lifecycle-0", "kind": "repair", "requestId": "repair-1", "outcome": "pending", "stage": "host_start"}}))], None);
    let report = answer_of(busy);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["disposition"].as_str(), report["issue"]["code"].as_str()), (Some("transition_in_progress"), Some("host_start"), Some("exclusive_transition_in_progress"), Some("lifecycle_transition_in_progress")));

    // The request identity already names another release, or other bytes.
    for (changed, message) in [(json!({"targetReleaseId": "agent-vmm-3"}), "the request identity is already bound to a different release"), (json!({"targetArtifactSize": 43}), "the request identity is already bound to different artifact bytes")] {
        let conflict = machine(vec![status(json!({"operation": operation(changed)}))], None);
        let report = answer_of(conflict);
        assert_eq!((report["state"].as_str(), report["disposition"].as_str(), report["issue"]["message"].as_str(), report["issue"]["retryable"].as_bool()), (Some("action_required"), Some("terminal_action_required"), Some(message), Some(false)));
    }
    let finished = answer_of(machine(vec![status(json!({"operation": operation(json!({"outcome": "succeeded"}))}))], None));
    assert_eq!((finished["state"].as_str(), finished["facts"]["already_completed"].as_bool()), (Some("succeeded"), Some(true)));
}

#[test]
fn a_recorded_failure_is_retried_only_after_its_delay() {
    let failure = |attempt: u64, age: Duration, code: &str, retryable: bool| operation(json!({"outcome": "failed", "failureStage": "download", "failureCode": code, "failureMessage": "HTTP 503", "retryable": retryable, "attempt": attempt, "updatedAt": millis(now() - age)}));
    let waiting = answer_of(machine(vec![status(json!({"operation": failure(2, Duration::from_secs(10), "update_download_unavailable", true)}))], None));
    assert_eq!((waiting["state"].as_str(), waiting["stage"].as_str(), waiting["disposition"].as_str(), waiting["facts"]["retry_at"].as_str(), waiting["facts"]["attempt"].as_u64()), (Some("retry_wait"), Some("download"), Some("retry_same_request_at"), Some("2026-11-18T11:07:30Z"), Some(2)));
    assert_eq!(waiting["next_action"], json!([{"action": "retry_same_request_id", "permission": "service_user"}]));

    // Exponential backoff: 30s, 60s, 120s, ..., capped at 1800s from attempt 7 onward.
    for (attempt, expected_retry_at) in [(1, "2026-11-18T11:07:10Z"), (2, "2026-11-18T11:07:40Z"), (3, "2026-11-18T11:08:40Z"), (7, "2026-11-18T11:36:40Z"), (12, "2026-11-18T11:36:40Z")] {
        let backoff = answer_of(machine(vec![status(json!({"operation": failure(attempt, Duration::ZERO, "update_download_unavailable", true)}))], None));
        assert_eq!((backoff["state"].as_str(), backoff["facts"]["retry_at"].as_str()), (Some("retry_wait"), Some(expected_retry_at)), "attempt {attempt}");
    }

    let terminal = answer_of(machine(vec![status(json!({"operation": failure(2, Duration::from_secs(300), "update_download_failed", false)}))], None));
    assert_eq!((terminal["state"].as_str(), terminal["issue"]["code"].as_str(), &terminal["next_action"]), (Some("action_required"), Some("update_download_failed"), &json!([{"action": "review_update_failure", "permission": "administrator"}])));

    // After the delay the same request runs again; so does a request that reached the retry limit of an older owner.
    for (code, retryable) in [("update_download_unavailable", true), ("update_retry_limit_reached", false)] {
        let fake = machine(vec![status(json!({"operation": failure(2, Duration::from_secs(61), code, retryable)})), printed(operation(json!({"outcome": "succeeded"})))], Some(Ok(PREPARED.into())));
        let again = answer(&fake);
        assert_eq!((again["state"].as_str(), again["facts"]["attempt"].as_u64(), fake.helper_commands()), (Some("succeeded"), Some(2), vec!["status".to_string(), "host-update".to_string()]));
        assert!(again["facts"].get("already_completed").is_none());
    }
}

#[test]
fn a_failed_preparation_is_recorded_before_it_is_reported() {
    let recorded = |facts: Value| failed(&operation(facts).to_string(), "");
    let rejected = PreparationError::at("download", "Host artifact download returned HTTP 404");
    let fake = machine(vec![healthy(), recorded(json!({"outcome": "failed", "failureStage": "download", "failureCode": "update_download_failed", "failureMessage": "Host artifact download returned HTTP 404", "attempt": 1, "updatedAt": millis(now())}))], Some(Err(rejected)));
    let report = answer(&fake);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["disposition"].as_str(), report["issue"]["code"].as_str()), (Some("action_required"), Some("download"), Some("terminal_action_required"), Some("update_download_failed")));
    let calls = fake.helper_calls.borrow();
    assert_eq!(calls[1].0, ["host-update", "--preparation-failure-stdin", "--target-release-id", "agent-vmm-2", "--request-id", "update-1", "--artifact-sha256", DIGEST, "--artifact-size", "42"]);
    assert_eq!(serde_json::from_str::<Value>(calls[1].1.as_deref().unwrap()).unwrap(), json!({"version": 1, "stage": "download", "code": "update_download_failed", "message": "Host artifact download returned HTTP 404", "retryable": false}));
    assert!(fake.removed.borrow().is_empty());

    // A download that reached its time bound left the installation as it was and can run again.
    let timeout = PreparationError { cancelled: true, ..PreparationError::download_unavailable("timeout: global") };
    let fake = machine(vec![healthy(), recorded(json!({"outcome": "failed", "failureStage": "download", "failureCode": "update_preparation_cancelled", "retryable": true, "attempt": 1, "updatedAt": millis(now())}))], Some(Err(timeout)));
    let report = answer(&fake);
    assert_eq!((report["state"].as_str(), report["issue"]["code"].as_str(), report["issue"]["retryable"].as_bool()), (Some("retry_wait"), Some("update_preparation_cancelled"), Some(true)));
    let descriptor: Value = serde_json::from_str(fake.helper_calls.borrow()[1].1.as_deref().unwrap()).unwrap();
    assert_eq!((descriptor["code"].as_str(), descriptor["message"].as_str(), descriptor["retryable"].as_bool()), (Some("update_preparation_cancelled"), Some("the update download or preparation was cancelled; the current installation was not changed"), Some(true)));

    // Without a record, the retry state is unknown: an administrator decides.
    let unrecorded = answer_of(machine(vec![healthy(), failed("not json", "")], Some(Err(PreparationError::at("preparation", "disk full")))));
    assert_eq!((unrecorded["state"].as_str(), unrecorded["stage"].as_str(), unrecorded["issue"]["code"].as_str()), (Some("action_required"), Some("preparation"), Some("update_failure_record_unknown")));
}

#[test]
fn an_answer_that_is_not_bound_to_the_request_is_not_a_result() {
    // No answer: the owner may have accepted the update.
    let silent = answer_of(machine(vec![healthy(), failed("", "killed")], Some(Ok(PREPARED.into()))));
    assert_eq!((silent["state"].as_str(), silent["stage"].as_str(), silent["partial"].as_bool(), silent["facts"]["prepared_app"].as_str()), (Some("transition_in_progress"), Some("owner_response_pending"), Some(true), Some(PREPARED)));

    let fake = machine(vec![healthy(), printed(operation(json!({"outcome": "succeeded", "targetReleaseId": "agent-vmm-3"})))], Some(Ok(PREPARED.into())));
    let (code, stdout, stderr) = update(&fake);
    let refusal: Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!((code, stdout.as_str(), refusal["issue"]["code"].as_str(), refusal["stage"].as_str(), refusal["issue"]["message"].as_str()), (1, "", Some("update_failed"), Some("validation"), Some("lifecycle returned an unbound update operation")));
    assert!(fake.removed.borrow().is_empty(), "a release that the owner did not confirm is kept");
}

#[test]
fn an_update_needs_the_user_that_owns_the_service() {
    let fake = Fake { euid: if cfg!(target_os = "macos") { 0 } else { 1000 }, ..machine(vec![healthy()], None) };
    if cfg!(target_os = "macos") {
        // Only a repair is refused for root on macOS: the daemon check comes first for a system job.
        *fake.preparation.borrow_mut() = Some(Ok(String::new()));
        assert_eq!(answer(&fake)["state"], "succeeded");
    } else {
        let report = answer(&fake);
        assert_eq!((report["state"].as_str(), report["disposition"].as_str(), &report["next_action"]), (Some("needs_administrator"), Some("administrator_action_required"), &json!([{"action": "update", "permission": "administrator"}])));
    }
}
