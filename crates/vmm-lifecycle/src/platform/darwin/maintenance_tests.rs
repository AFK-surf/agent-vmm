use super::super::maintenance_journal::status;
use super::*;
use crate::journal::LifecycleOperation;
use crate::operation;
use crate::testing::{Fake, installed};
use std::cell::RefCell;
use std::collections::VecDeque;

struct Recorded {
    log: RefCell<Vec<&'static str>>,
    restore: RefCell<VecDeque<Error>>,
    data: DataSchema,
    /// The result of each journal commit, in order. A commit without an entry succeeds.
    commits: RefCell<VecDeque<bool>>,
}

impl Recorded {
    fn new() -> Self {
        Self { log: RefCell::default(), restore: RefCell::default(), data: DataSchema::Fresh, commits: RefCell::default() }
    }

    fn effect(&self, name: &'static str) -> Result<()> {
        self.log.borrow_mut().push(name);
        Ok(())
    }

    fn take_log(&self) -> Vec<&'static str> {
        self.log.take()
    }
}

impl Actions for Recorded {
    fn validate_source(&self, _: &MaintenanceRequest) -> Result<()> {
        Ok(())
    }
    fn target_schema(&self, _: &MaintenanceRequest) -> Result<(u32, u32)> {
        Ok((1, 1))
    }
    fn data_schema(&self, _: &Paths) -> Result<DataSchema> {
        Ok(self.data)
    }
    fn commit(&self, journal: &Journal, state: &MaintenanceState) -> Result<()> {
        if self.commits.borrow_mut().pop_front() == Some(false) {
            return Err(err("disk full"));
        }
        journal.write_state(state)
    }
    fn stop(&mut self, _: &Paths, _: &MaintenanceRequest) -> Result<()> {
        self.effect("stop")
    }
    fn remove_software(&mut self, _: &Paths) -> Result<()> {
        self.effect("remove")
    }
    fn reset_data(&mut self, _: &Paths) -> Result<()> {
        self.effect("reset")
    }
    fn replace(&mut self, _: &Paths, _: &MaintenanceRequest) -> Result<()> {
        self.effect("replace")
    }
    fn restore(&mut self, _: &Paths, _: &str) -> Result<()> {
        self.effect("restore")?;
        self.restore.borrow_mut().pop_front().map_or(Ok(()), Err)
    }
    fn verify(&mut self, _: &Paths, _: &MaintenanceRequest) -> Result<()> {
        self.effect("verify")
    }
}

struct Fixture {
    _root: tempfile::TempDir,
    paths: Paths,
    context: Context,
    actions: Recorded,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    Fixture { paths, context: Fake::new().context(), actions: Recorded::new(), _root: root }
}

impl Fixture {
    fn request(&self, id: &str, action: &str, policy: &str) -> MaintenanceRequest {
        let uninstall = action == "uninstall";
        MaintenanceRequest {
            version: 1,
            request_id: id.into(),
            action: action.into(),
            data_policy: policy.into(),
            target_app: self.paths.app.display().to_string(),
            source_app: if uninstall { String::new() } else { self.paths.home.join("staged").join(HOST_APP_NAME).display().to_string() },
            target_release_id: if uninstall { String::new() } else { "agent-vmm-2".into() },
            confirm_shared_host: true,
            confirm_data_deletion: policy == "reset",
            ..MaintenanceRequest::default()
        }
    }

    fn run(&mut self, request: &MaintenanceRequest) -> (MaintenanceReceipt, Result<()>) {
        let (receipt, result) = run(&self.context, &self.paths, request, &mut self.actions);
        (receipt.expect("the request was admitted"), result)
    }

    fn state(&self) -> MaintenanceState {
        Journal::open(&self.context, &self.paths).unwrap().state().unwrap()
    }

    fn status(&self, id: &str) -> Option<MaintenanceReceipt> {
        status(&self.context, &self.paths, id).unwrap()
    }
}

#[test]
fn an_update_runs_each_step_once_and_replays_its_completion() {
    let mut fixture = fixture();
    assert_eq!(fixture.status(""), None);
    let request = fixture.request("update-1", "update", "preserve");
    let (receipt, result) = fixture.run(&request);
    result.unwrap();
    assert_eq!(fixture.actions.take_log(), ["stop", "replace", "restore", "verify"]);
    assert_eq!((receipt.stage.as_str(), receipt.outcome.as_str(), receipt.retryable, receipt.attempt), ("settled", "succeeded", false, 1));

    let value = report(Some(&receipt));
    assert_eq!((value["contractVersion"].as_u64(), value["version"].as_u64()), (Some(1), Some(1)));
    assert_eq!((value["operationId"].as_str(), value["kind"].as_str(), value["requestId"].as_str()), (Some("update-1"), Some("update"), Some("update-1")));
    assert_eq!((value["serviceMode"].as_str(), value["effectsVerified"].as_bool()), (Some("agent"), Some(true)));
    assert!(value.as_object().unwrap().values().all(|field| !field.is_null()), "an optional field is omitted, never null: {value}");
    assert!(value.get("failureCode").is_none() && value.get("continue").is_none() && value.get("supersedesRequestId").is_none());

    let (replayed, result) = fixture.run(&request);
    result.unwrap();
    assert!(fixture.actions.take_log().is_empty());
    assert_eq!(replayed.outcome, "succeeded");
    assert_eq!(fixture.status("").unwrap().request.request_id, "update-1");
    let state = fixture.state();
    assert!(state.completed("update-1") && state.active_request.is_empty() && !state.uninstalled);

    let other = MaintenanceRequest { action: "reinstall".into(), ..request };
    let (_, result) = fixture.run(&other);
    assert_eq!(result.unwrap_err().message, "maintenance requestId belongs to a different action, data policy, service or artifact");
}

#[test]
fn a_host_that_does_not_start_pauses_the_restore_window() {
    let mut fixture = fixture();
    let request = fixture.request("reinstall-1", "reinstall", "preserve");
    fixture.actions.restore.borrow_mut().extend((0..3).map(|_| err("Host did not start")));
    for attempt in 1..=3 {
        let (receipt, result) = fixture.run(&request);
        assert!(result.is_err());
        assert_eq!((receipt.failure_code.as_str(), receipt.retryable, receipt.attempt, receipt.stage.as_str()), ("maintenance_incomplete", true, attempt, "restore"));
    }
    assert_eq!(fixture.actions.take_log(), ["stop", "replace", "restore", "restore", "restore"], "a retry resumes after the replacement");
    let (paused, result) = fixture.run(&request);
    assert_eq!(result.unwrap_err().kind, Kind::RestoreWindowExhausted);
    assert_eq!((paused.failure_code.as_str(), paused.retryable, paused.outcome.as_str()), (HOST_START_FAILED, true, "failed"));
    assert!(fixture.actions.take_log().is_empty());
    assert_eq!(fixture.status("reinstall-1").unwrap().failure_code, HOST_START_FAILED);

    let (resumed, result) = fixture.run(&MaintenanceRequest { resume: true, ..request });
    result.unwrap();
    assert_eq!((resumed.outcome.as_str(), resumed.restore_window_attempts, resumed.attempt), ("succeeded", 1, 4));
    assert_eq!(fixture.actions.take_log(), ["restore", "verify"]);
}

#[test]
fn a_schema_refusal_of_the_new_host_is_terminal() {
    let mut fixture = fixture();
    let request = fixture.request("update-1", "update", "preserve");
    fixture.actions.restore.borrow_mut().push_back(Error::new(Kind::Maintenance(HOST_START_FAILED.into()), "Host refused its database: STATE_SCHEMA_INCOMPATIBLE"));
    let (receipt, result) = fixture.run(&request);
    assert!(result.is_err());
    assert_eq!((receipt.failure_code.as_str(), receipt.retryable), (HOST_START_FAILED, false));
    assert!(fixture.state().active_request.is_empty(), "a terminal failure releases the owner");
    let (again, result) = fixture.run(&request);
    assert_eq!(result.unwrap_err().message, "Host refused its database: STATE_SCHEMA_INCOMPATIBLE");
    assert_eq!(again.failure_code, HOST_START_FAILED);
    assert_eq!(fixture.actions.take_log(), ["stop", "replace", "restore"]);
}

#[test]
fn a_target_that_cannot_open_the_data_is_refused_before_any_effect() {
    let mut fixture = fixture();
    fixture.actions.data = DataSchema::Version(37);
    let request = fixture.request("update-1", "update", "preserve");
    let (receipt, result) = fixture.run(&request);
    assert!(result.unwrap_err().message.contains("schema 37 cannot migrate to schema 1"));
    assert_eq!((receipt.failure_code.as_str(), receipt.retryable, receipt.outcome.as_str(), receipt.stage.as_str()), (TARGET_OLDER_THAN_DATA, false, "failed", "accepted"));
    assert!(fixture.actions.take_log().is_empty());
    assert_eq!(fixture.state(), MaintenanceState { version: 1, ..MaintenanceState::default() }, "a refusal does not change the owner state");
    assert_eq!(fixture.status("update-1").unwrap().failure_code, TARGET_OLDER_THAN_DATA);

    // A reset discards the data, so the same target is accepted.
    let reset = fixture.request("reinstall-1", "reinstall", "reset");
    fixture.run(&reset).1.unwrap();
    assert_eq!(fixture.actions.take_log(), ["stop", "reset", "replace", "restore", "verify"]);
}

#[test]
fn only_the_exact_failed_request_can_be_replaced() {
    let mut fixture = fixture();
    let first = fixture.request("update-1", "update", "preserve");
    fixture.actions.restore.borrow_mut().push_back(err("Host did not start"));
    assert!(fixture.run(&first).1.is_err());

    let second = fixture.request("update-2", "update", "preserve");
    let (receipt, result) = run(&fixture.context, &fixture.paths, &second, &mut fixture.actions);
    assert!(receipt.is_none());
    assert_eq!(result.unwrap_err().message, "continue the existing maintenance request before starting another");
    let wrong = MaintenanceRequest { supersedes_request_id: "update-0".into(), ..second.clone() };
    assert_eq!(run(&fixture.context, &fixture.paths, &wrong, &mut fixture.actions).1.unwrap_err().message, "only the exact active maintenance request can be replaced");

    let replacement = MaintenanceRequest { supersedes_request_id: "update-1".into(), ..second };
    fixture.actions.take_log();
    let (receipt, result) = fixture.run(&replacement);
    result.unwrap();
    assert_eq!(receipt.outcome, "succeeded");
    assert_eq!(fixture.actions.take_log(), ["stop", "replace", "restore", "verify"]);
    let superseded = fixture.status("update-1").unwrap();
    assert_eq!((superseded.outcome.as_str(), superseded.superseded_by_request_id.as_str(), superseded.retryable), ("superseded", "update-2", false));
    let (old, result) = fixture.run(&first);
    assert!(result.is_ok() && old.outcome == "superseded" && fixture.actions.take_log().is_empty());
}

#[test]
fn a_reset_fences_old_install_requests_and_an_uninstall_closes_the_installation() {
    let mut fixture = fixture();
    let legacy = LifecycleOperation { operation_id: "op-1".into(), outcome: "failed".into(), retryable: true, ..LifecycleOperation::unknown("install-1", "install-managed") };
    journal::persist(&fixture.context, &fixture.paths, &legacy).unwrap();
    let journal = Journal::open(&fixture.context, &fixture.paths).unwrap();
    journal.plan_dir("install-2", true).unwrap();
    drop(journal);

    let request = fixture.request("uninstall-1", "uninstall", "reset");
    let (receipt, result) = fixture.run(&request);
    result.unwrap();
    assert_eq!(fixture.actions.take_log(), ["stop", "reset", "remove", "verify"]);
    assert!(receipt.progress.data_cleared && receipt.progress.software_removed);
    let state = fixture.state();
    assert!(state.uninstalled && state.blocked("install-1") && state.blocked("install-2"));

    // The fence outranks the historical receipt, which still says retryable.
    let arguments = ["--request-id".to_string(), "install-1".to_string()];
    assert_eq!(crate::report::operation(&fixture.context, &fixture.paths, &arguments).unwrap_err().kind, Kind::RequestBlocked("install-1".into()));
    let (_, result) = operation::run(&fixture.context, &fixture.paths, operation::Request::new("repair", "repair-1"), &mut || Ok(()));
    assert_eq!(result.unwrap_err().message, "the local owner removed this registration or Host installation");
    assert_eq!(journal::lock_mutation(&fixture.context, &fixture.paths).err().unwrap().kind, Kind::HostUninstalled);
}

#[test]
fn a_failed_journal_commit_leaves_a_request_that_the_same_identity_finishes() {
    let mut fixture = fixture();
    let request = fixture.request("update-1", "update", "preserve");
    // The admission commit fails: the staged receipt authorizes nothing and is not reported.
    fixture.actions.commits.borrow_mut().push_back(false);
    let (receipt, result) = run(&fixture.context, &fixture.paths, &request, &mut fixture.actions);
    assert!(receipt.is_none() && result.unwrap_err().message == "disk full");
    assert!(fixture.actions.take_log().is_empty() && fixture.state().latest_request.is_empty());
    assert_eq!(fixture.status("update-1"), None);

    // The completion commit fails: the effects are verified, and the receipt stays unfinished.
    fixture.actions.commits.borrow_mut().extend([true, false]);
    let (receipt, result) = fixture.run(&request);
    assert!(result.is_err());
    assert_eq!((receipt.outcome.as_str(), receipt.retryable, receipt.failure_code.as_str()), ("failed", true, "maintenance_incomplete"));
    assert!(receipt.progress.effects_verified);
    assert_eq!(fixture.actions.take_log(), ["stop", "replace", "restore", "verify"]);
    assert_eq!(fixture.status("update-1").unwrap().outcome, "failed");

    let (settled, result) = fixture.run(&request);
    result.unwrap();
    assert!(fixture.actions.take_log().is_empty(), "verified effects are settled without running again");
    assert_eq!((settled.outcome.as_str(), settled.stage.as_str()), ("succeeded", "settled"));
    assert!(fixture.state().completed("update-1"));
}

#[test]
fn requests_are_validated_against_the_contract() {
    let fixture = fixture();
    let valid = fixture.request("update-1", "update", "preserve");
    let encoded = serde_json::to_vec(&serde_json::json!({
        "version": 1, "requestId": "r", "action": "update", "dataPolicy": "preserve", "targetApp": "/t", "sourceApp": "/s", "targetReleaseId": "x",
        "supersedesRequestId": "p", "confirmSharedHost": true, "continue": true, "confirmDataDeletion": false,
    }))
    .unwrap();
    let decoded = MaintenanceRequest::decode(&encoded).unwrap();
    assert!(decoded.resume && decoded.supersedes_request_id == "p", "every key of the contract is accepted");
    assert!(MaintenanceRequest::decode(br#"{"version":1,"requestID":"r"}"#).is_err(), "an unknown key is refused");
    assert!(MaintenanceRequest::decode(br#"{"version":"1"}"#).is_err());

    let refused = |request: MaintenanceRequest| validate(&fixture.paths, &request).unwrap_err().message;
    assert!(refused(MaintenanceRequest { confirm_shared_host: false, ..valid.clone() }).contains("explicit shared Host confirmation"));
    assert_eq!(refused(MaintenanceRequest { data_policy: "reset".into(), confirm_data_deletion: true, ..valid.clone() }), "update must preserve data");
    assert_eq!(refused(MaintenanceRequest { confirm_data_deletion: true, ..valid.clone() }), "data deletion confirmation conflicts with preserve");
    assert_eq!(refused(MaintenanceRequest { target_app: "/Applications/Agent VMM Host.app".into(), ..valid.clone() }), "maintenance target must be this service user's standard shared Host app");
    assert_eq!(refused(MaintenanceRequest { source_app: fixture.paths.logs.join(HOST_APP_NAME).display().to_string(), ..valid.clone() }), "maintenance source must be outside the target and reset data");
    assert_eq!(refused(MaintenanceRequest { action: "uninstall".into(), ..valid.clone() }), "uninstall does not accept an installation artifact");
    assert_eq!(refused(MaintenanceRequest { supersedes_request_id: "update-1".into(), ..valid.clone() }), "maintenance replacement requires an exact prior request");
    validate(&fixture.paths, &valid).unwrap();
}
