//! The managed commands against a scripted lifecycle helper and Host operator.

use crate::envelope::LogRecord;
use crate::facts::Facts;
use crate::helper::SERVICE_SOURCE;
use crate::options::Options;
use crate::runtime::{Call, Failure, HelperOutput, LogPage, PreparationError, Runtime, Session};
use agent_vmm_proto::host::v1 as hostv1;
use serde_json::{Value, json};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vmm_lifecycle::{Deadline, RpcCode};

mod mutations;
mod updates;

pub const SOCKET: &str = "/Users/worker/Library/Application Support/AgentVMM/operator.sock";
/// The user that may change the service: the service user on macOS, root on Linux.
pub const OWNER_EUID: u32 = if cfg!(target_os = "macos") { 501 } else { 0 };

pub fn now() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_795_000_000)
}

/// A scripted machine. Each helper invocation takes the next output; each operator call takes its configured answer.
#[derive(Default)]
pub struct Fake {
    pub euid: u32,
    pub helper: RefCell<VecDeque<HelperOutput>>,
    pub helper_calls: RefCell<Vec<(Vec<String>, Option<String>)>>,
    /// The number of polls that the budget allows.
    pub polls: Cell<u32>,
    pub operations: RefCell<VecDeque<hostv1::Operation>>,
    pub environment: Option<hostv1::AgentEnvironment>,
    pub registration: Option<hostv1::RemoteRegistration>,
    pub summary: Option<hostv1::GetVmImpactSummaryResponse>,
    pub vm: Option<hostv1::Vm>,
    /// The answer to a VM mutation or to a cleanup execution.
    pub mutation: RefCell<Option<Call<Option<hostv1::Operation>>>>,
    pub plan: Option<Call<Option<hostv1::CleanupPlan>>>,
    pub fences: RefCell<Vec<hostv1::VmMutationFence>>,
    pub cleanups: RefCell<Vec<hostv1::ExecuteCleanupRequest>>,
    pub host_sockets: RefCell<Vec<hostv1::HostSocket>>,
    pub logs: LogPage,
    pub preparation: RefCell<Option<Result<String, PreparationError>>>,
    pub removed: RefCell<Vec<String>>,
}

impl Fake {
    pub fn new(outputs: Vec<HelperOutput>) -> Self {
        Self { euid: OWNER_EUID, helper: RefCell::new(outputs.into()), ..Self::default() }
    }

    pub fn helper_commands(&self) -> Vec<String> {
        self.helper_calls.borrow().iter().map(|(arguments, _)| arguments[0].clone()).collect()
    }

    fn answer(&self) -> Call<Option<hostv1::Operation>> {
        self.mutation.borrow_mut().take().expect("the test expects no mutation")
    }
}

impl Runtime for Fake {
    fn now(&self) -> SystemTime {
        now()
    }
    fn euid(&self) -> u32 {
        self.euid
    }
    fn sleep(&self, _: Duration, _: Deadline) -> bool {
        let polls = self.polls.get();
        self.polls.set(polls.saturating_sub(1));
        polls > 0
    }
    fn helper(&self, _: &str, arguments: &[String], input: Option<&[u8]>, _: Deadline) -> HelperOutput {
        self.helper_calls.borrow_mut().push((arguments.to_vec(), input.map(|input| String::from_utf8_lossy(input).into_owned())));
        self.helper.borrow_mut().pop_front().unwrap_or_else(|| panic!("unexpected helper call {arguments:?}"))
    }
    fn operation(&self, _: &str, _: &str, _: Deadline) -> Call<Option<hostv1::Operation>> {
        Ok(self.operations.borrow_mut().pop_front())
    }
    fn environment(&self, _: &str, _: &str, _: Deadline) -> Call<Option<hostv1::AgentEnvironment>> {
        Ok(self.environment.clone())
    }
    fn registration(&self, _: &str, _: &str, _: Deadline) -> Call<Option<hostv1::RemoteRegistration>> {
        Ok(self.registration.clone())
    }
    fn impact_summary(&self, _: &str, _: Deadline) -> Call<hostv1::GetVmImpactSummaryResponse> {
        self.summary.clone().ok_or_else(Failure::default)
    }
    fn vm(&self, _: &str, _: Deadline) -> Call<Option<hostv1::Vm>> {
        Ok(self.vm.clone())
    }
    fn start_vm(&self, _: &str, request: hostv1::StartVmRequest, _: Deadline) -> Call<Option<hostv1::Operation>> {
        self.fences.borrow_mut().extend(request.fence);
        self.answer()
    }
    fn stop_vm(&self, _: &str, request: hostv1::StopVmRequest, _: Deadline) -> Call<Option<hostv1::Operation>> {
        self.fences.borrow_mut().extend(request.fence);
        self.answer()
    }
    fn plan_cleanup(&self, _: &str, _: &str, _: hostv1::CleanupKind, _: Deadline) -> Call<Option<hostv1::CleanupPlan>> {
        self.plan.clone().expect("the test expects no cleanup plan")
    }
    fn execute_cleanup(&self, _: &str, request: hostv1::ExecuteCleanupRequest, _: Deadline) -> Call<Option<hostv1::Operation>> {
        self.cleanups.borrow_mut().push(request);
        self.answer()
    }
    fn set_host_socket(&self, _: &str, request: hostv1::SetHostSocketRequest, _: Deadline) -> Call<Option<hostv1::HostSocket>> {
        let socket = hostv1::HostSocket { environment_id: request.environment_id, port: request.port, path: request.path, revision: 1 };
        self.host_sockets.borrow_mut().push(socket.clone());
        Ok(Some(socket))
    }
    fn remove_host_socket(&self, _: &str, request: hostv1::RemoveHostSocketRequest, _: Deadline) -> Call<()> {
        self.host_sockets.borrow_mut().retain(|socket| socket.port != request.port);
        Ok(())
    }
    fn host_sockets(&self, _: &str, _: Option<String>, _: Deadline) -> Call<Vec<hostv1::HostSocket>> {
        Ok(self.host_sockets.borrow().clone())
    }
    fn container_logs(&self, _: &str, _: &Options, _: Deadline) -> Result<LogPage, String> {
        Ok(self.logs.clone())
    }
    fn local_logs(&self, _: &Facts, _: &Options) -> Result<LogPage, String> {
        Ok(self.logs.clone())
    }
    fn prepare_update(&self, _: &Options, _: &Facts) -> Result<String, PreparationError> {
        self.preparation.borrow_mut().take().expect("the test expects no preparation")
    }
    fn remove_prepared(&self, path: &str) -> std::io::Result<()> {
        self.removed.borrow_mut().push(path.to_string());
        Ok(())
    }
}

pub fn printed(value: Value) -> HelperOutput {
    HelperOutput { stdout: value.to_string().into_bytes(), ..HelperOutput::default() }
}

pub fn failed(stdout: &str, stderr: &str) -> HelperOutput {
    HelperOutput { stdout: stdout.into(), stderr: stderr.into(), error: Some("exit status 1".into()) }
}

/// A status report that identifies the service, with more facts.
pub fn status(facts: Value) -> HelperOutput {
    let mut status = json!({"serviceUser": "worker", "serviceUid": 501, "hostJob": "gui/501/com.afk-surf.agent-vmm-host", "serviceApp": "/Users/worker/Applications/Agent VMM Host.app", "operatorSocket": SOCKET});
    status.as_object_mut().unwrap().extend(facts.as_object().unwrap().clone());
    printed(status)
}

pub fn healthy() -> HelperOutput {
    status(json!({"hostInstalled": true, "hostLoaded": true, "hostRunning": true, "hostReadable": true, "hostHealthy": true, "snapshotObservedAt": "2026-11-18T11:06:35Z"}))
}

pub fn rejection(code: RpcCode, reason: &str) -> Failure {
    Failure { code: Some(code), reason: reason.into(), message: format!("rpc error: code = {} desc = private Host text", code.name()), ..Failure::default() }
}

/// Runs the command line and returns the exit status and both streams.
pub fn run(fake: &Fake, arguments: &[&str]) -> (i32, String, String) {
    let arguments: Vec<String> = arguments.iter().map(|argument| argument.to_string()).collect();
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let code = crate::cli::run(&arguments, &mut stdout, &mut stderr, fake);
    (code, String::from_utf8(stdout).unwrap(), String::from_utf8(stderr).unwrap())
}

/// Runs a command that must succeed and returns its JSON envelope.
pub fn envelope(fake: &Fake, arguments: &[&str]) -> Value {
    let (code, stdout, stderr) = run(fake, arguments);
    assert_eq!((code, stderr.as_str()), (0, ""), "{stdout}");
    serde_json::from_str(&stdout).unwrap()
}

/// Runs a command that must fail and returns the envelope on standard error.
pub fn refusal(fake: &Fake, arguments: &[&str], code: i32) -> Value {
    let (actual, stdout, stderr) = run(fake, arguments);
    assert_eq!((actual, stdout.as_str()), (code, ""), "{stderr}");
    serde_json::from_str(&stderr).unwrap()
}

const INSPECT: [&str; 4] = ["inspect", "--json", "--lifecycle-helper", "/helpers/agent-vmm-lifecycle"];

#[test]
fn inspect_reports_the_facts_of_the_lifecycle_owner() {
    let fake = Fake::new(vec![healthy(), healthy()]);
    let report = envelope(&fake, &[&INSPECT[..], &["--service-type", "daemon", "--service-user", "worker"]].concat());
    assert_eq!(
        (report["version"].as_i64(), report["command"].as_str(), report["state"].as_str(), report["stage"].as_str(), report["freshness"].as_str(), report["partial"].as_bool()),
        (Some(1), Some("inspect"), Some("healthy"), Some("healthy"), Some("current"), Some(false))
    );
    assert_eq!(report["source"], json!(["lifecycle", SERVICE_SOURCE]));
    assert_eq!(report["target"], json!({"service_user": "worker", "service_uid": 501, "service_job": "gui/501/com.afk-surf.agent-vmm-host", "install_location": "/Users/worker/Applications/Agent VMM Host.app", "scope_status": "resolved"}));
    assert_eq!((&report["issue"], &report["operation_ref"], &report["next_action"], &report["facts"]["hostHealthy"]), (&Value::Null, &Value::Null, &json!([]), &json!(true)));
    assert_eq!(fake.helper_calls.borrow()[0].0, ["status", "--service-type", "daemon", "--service-user", "worker"]);
    assert_eq!(run(&fake, &["inspect", "--lifecycle-helper", "/h"]), (0, "inspect: healthy (healthy)\n".into(), String::new()));
}

#[test]
fn diagnose_keeps_the_facts_that_it_could_read() {
    let unreadable = Fake::new(vec![status(json!({"hostInstalled": true, "hostLoaded": true, "hostRunning": true, "operation": {"operationId": "lifecycle-1", "updatedAt": 1}}))]);
    let report = envelope(&unreadable, &["diagnose", "--json", "--lifecycle-helper", "/h"]);
    assert_eq!((report["state"].as_str(), report["partial"].as_bool(), report["issue"]["code"].as_str(), report["freshness"].as_str()), (Some("host_unreadable"), Some(true), Some("host_unreadable"), Some("stale")));
    assert_eq!((&report["next_action"], &report["operation_ref"]), (&json!([{"action": "repair", "permission": "administrator"}]), &json!({"owner": "lifecycle", "id": "lifecycle-1"})));
    assert!(report["facts"].get("resources").is_none());

    let readable = Fake::new(vec![status(json!({"hostReadable": true, "hostResources": {"pids": 7}, "guestResources": {"pids": 3}, "snapshotObservedAt": "2026-11-18T11:06:35Z"}))]);
    let report = envelope(&readable, &["diagnose", "--json", "--lifecycle-helper", "/h"]);
    assert_eq!(report["facts"]["resources"], json!({"host": {"pids": 7}, "guest": {"pids": 3}, "observed_at": "2026-11-18T11:06:35Z"}));
    assert_eq!((report["state"].as_str(), report["partial"].as_bool(), &report["source"]), (Some("unhealthy"), Some(false), &json!(["lifecycle", SERVICE_SOURCE, "host_operator"])));
}

#[test]
fn an_observation_ends_with_its_budget_or_with_a_failure() {
    let options = Options { lifecycle: "/h".into(), registration_id: "registration-1".into(), ..Options::default() };
    let late = Fake::new(vec![failed("", "killed")]);
    let report = crate::observe::inspect(Session { runtime: &late, budget: Deadline::after(Duration::ZERO) }, "inspect", &options).unwrap();
    assert_eq!((report.partial, report.issue.map(|issue| issue.code), report.target.get("registration_id")), (true, Some("observation_timeout".into()), Some(&json!("registration-1"))));

    let broken = Fake::new(vec![failed("", "token=abc cannot read status")]);
    let report = refusal(&broken, &INSPECT, 1);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["issue"]["code"].as_str(), report["issue"]["message"].as_str()), (Some("failed"), Some("unknown"), Some("observation_failed"), Some("lifecycle status failed: token=[REDACTED] cannot read status")));
    let anonymous = Fake::new(vec![printed(json!({"hostHealthy": true}))]);
    assert_eq!(refusal(&anonymous, &INSPECT, 1)["issue"]["message"], "lifecycle status did not identify one service UID, job, and installation location");
}

fn registration(allocations: &[(&str, &str)]) -> hostv1::RemoteRegistration {
    let allocation = |(id, environment): &(&str, &str)| hostv1::RemoteAllocation { allocation_id: id.to_string(), environment_id: environment.to_string(), generation: 4, state: hostv1::AllocationState::Ready as i32, ..Default::default() };
    hostv1::RemoteRegistration { id: "registration-1".into(), state: hostv1::RemoteRegistrationState::Enabled as i32, connection_state: hostv1::RemoteConnectionState::Connected as i32, allocations: allocations.iter().map(allocation).collect(), ..Default::default() }
}

fn environment(owner: &str, containers: &[&str]) -> hostv1::AgentEnvironment {
    hostv1::AgentEnvironment {
        id: "environment-1".into(),
        owner: Some(hostv1::EnvironmentOwner { registration_id: owner.into(), ..Default::default() }),
        origin: hostv1::EnvironmentOrigin::Remote as i32,
        inventory_state: hostv1::InventoryState::Available as i32,
        containers: containers.iter().map(|id| hostv1::Container { id: id.to_string(), ..Default::default() }).collect(),
        ..Default::default()
    }
}

#[test]
fn a_workload_is_resolved_through_its_registration_and_environment() {
    let scoped = [&INSPECT[..], &["--registration-id", "registration-1", "--environment-id", "environment-1", "--workload-id", "workload-1"]].concat();
    let mut fake = Fake::new(vec![healthy()]);
    (fake.registration, fake.environment) = (Some(registration(&[("workload-1", "environment-1"), ("workload-2", "environment-2")])), Some(environment("registration-1", &[])));
    let target = envelope(&fake, &scoped)["target"].clone();
    assert_eq!((target["scope_status"].as_str(), target["allocation_state"].as_str(), target["allocation_generation"].as_u64()), (Some("resolved"), Some("ready"), Some(4)));
    assert_eq!((target["registration_state"].as_str(), target["connection_state"].as_str(), target["environment_origin"].as_str(), target["environment_inventory"].as_str()), (Some("enabled"), Some("connected"), Some("remote"), Some("available")));

    let cases = [
        (registration(&[("workload-1", "environment-1")]), environment("registration-2", &[]), "the requested environment does not belong to the registration"),
        (registration(&[("workload-1", "environment-1"), ("workload-1", "environment-1")]), environment("registration-1", &[]), "the workload maps to multiple local allocations"),
        (registration(&[("workload-1", "environment-2")]), environment("registration-1", &[]), "the requested workload does not map to the environment"),
        (registration(&[]), environment("registration-1", &[]), "the requested workload has no local allocation"),
    ];
    for (registration, environment, message) in cases {
        let mut fake = Fake::new(vec![healthy()]);
        (fake.registration, fake.environment) = (Some(registration), Some(environment));
        let report = envelope(&fake, &scoped);
        assert_eq!((report["partial"].as_bool(), report["target"]["scope_status"].as_str(), report["issue"]["code"].as_str(), report["issue"]["message"].as_str()), (Some(true), Some("unavailable"), Some("target_unavailable"), Some(message)));
        assert_eq!(report["state"], "healthy", "the service facts stay in the report");
    }
}

pub fn host_operation(state: hostv1::OperationState, stage: &str) -> hostv1::Operation {
    let mut operation = hostv1::Operation { id: "operation-1".into(), kind: "vm_stop".into(), state: state as i32, stage: stage.into(), request_id: "request-1".into(), ..Default::default() };
    // Updated ten seconds before the observation.
    operation.updated_at.get_or_insert_default().seconds = 1_794_999_990;
    operation
}

#[test]
fn a_followed_operation_is_observed_until_it_ends() {
    use hostv1::OperationState::{Running, Succeeded};
    let follow = ["operation", "follow", "--owner", "host", "--operation-id", "operation-1", "--lifecycle-helper", "/h"];
    let fake = Fake::new(vec![healthy()]);
    fake.polls.set(5);
    fake.operations.borrow_mut().extend([host_operation(Running, "stopping"), host_operation(Running, "stopping"), host_operation(Succeeded, "stopped")]);
    let (code, stdout, _) = run(&fake, &follow);
    let lines: Vec<Value> = stdout.lines().map(|line| serde_json::from_str(line).unwrap()).collect();
    assert_eq!((code, lines.len(), fake.polls.get()), (0, 3, 3));
    assert_eq!((lines[0]["state"].as_str(), lines[2]["state"].as_str(), lines[2]["stage"].as_str(), &lines[2]["operation_ref"]), (Some("running"), Some("succeeded"), Some("stopped"), &json!({"owner": "host", "id": "operation-1"})));
    assert_eq!(lines[2]["facts"], json!({"kind": "vm_stop", "request_id": "request-1", "resource": null}));

    // The budget ends first: the last observation is repeated as partial.
    let fake = Fake::new(vec![healthy()]);
    fake.operations.borrow_mut().push_back(host_operation(Running, "stopping"));
    let (code, stdout, _) = run(&fake, &follow);
    let last: Value = serde_json::from_str(stdout.lines().last().unwrap()).unwrap();
    assert_eq!((code, stdout.lines().count(), last["partial"].as_bool(), last["issue"]["code"].as_str(), &last["next_action"]), (0, 2, Some(true), Some("follow_timeout"), &json!([{"action": "operation_follow", "permission": "service_user"}])));

    let absent = Fake::new(vec![healthy()]);
    assert_eq!(refusal(&absent, &["operation", "get", "--owner", "host", "--operation-id", "operation-1", "--lifecycle-helper", "/h"], 1)["issue"]["message"], "host operation was not found");
}

#[test]
fn a_failed_lifecycle_operation_reports_its_recorded_issue() {
    let operation = json!({"operationId": "lifecycle-1", "kind": "repair", "outcome": "failed", "failureStage": "host_start", "failureCode": "host_start_failed", "failureMessage": "password=hunter2", "retryable": true, "targetRegistrationId": "registration-1", "updatedAt": 1_794_999_995_000i64});
    let fake = Fake::new(vec![healthy(), printed(operation)]);
    let report = envelope(&fake, &["operation", "get", "--owner", "lifecycle", "--operation-id", "lifecycle-1", "--json", "--lifecycle-helper", "/h"]);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["freshness"].as_str(), report["target"]["registration_id"].as_str()), (Some("failed"), Some("host_start"), Some("current"), Some("registration-1")));
    assert_eq!(report["issue"], json!({"code": "host_start_failed", "message": "password=[REDACTED]", "layer": "lifecycle", "retryable": true}));
    assert_eq!(fake.helper_calls.borrow()[1].0, ["operation", "--operation-id", "lifecycle-1"]);
    let other = Fake::new(vec![healthy(), printed(json!({"operationId": "lifecycle-2"}))]);
    assert_eq!(refusal(&other, &["operation", "get", "--owner", "lifecycle", "--operation-id", "lifecycle-1", "--lifecycle-helper", "/h"], 1)["issue"]["code"], "operation_observation_failed");
}

#[test]
fn container_output_is_printed_only_on_request() {
    let logs = ["logs", "--json", "--lifecycle-helper", "/h", "--environment-id", "environment-1", "--container-id", "container-1"];
    let machine = |containers: &[&str]| {
        let mut fake = Fake::new(vec![healthy()]);
        fake.environment = Some(environment("", containers));
        fake.logs = LogPage { records: vec![LogRecord { cursor: "c1".into(), stream: "stdout".into(), data: "task output api_key=abc".into(), ..LogRecord::default() }], cursor: "c1".into(), gap: false };
        fake
    };
    let hidden = envelope(&machine(&["container-1"]), &logs);
    assert_eq!((hidden["logs"][0]["data"].as_str(), hidden["cursor"].as_str(), &hidden["source"]), (Some("[REDACTED: pass --include-container-output to read container output]"), Some("c1"), &json!(["guest_container_log"])));
    let shown = envelope(&machine(&["container-1"]), &[&logs[..], &["--include-container-output"]].concat());
    assert_eq!((shown["logs"][0]["data"].as_str(), shown["target"]["scope_status"].as_str()), (Some("task output api_key=[REDACTED]"), Some("resolved")));
    let (none, twice): (&[&str], &[&str]) = (&[], &["container-1", "container-1"]);
    for containers in [none, twice] {
        assert_eq!(refusal(&machine(containers), &logs, 1)["issue"]["code"], "log_read_failed");
    }
    let local = envelope(&machine(&[]), &["logs", "--json", "--lifecycle-helper", "/h", "--component", "host", "--operation-id", "operation-1"]);
    assert_eq!((&local["source"], &local["operation_ref"], local["state"].as_str()), (&json!(["host_log"]), &json!({"owner": "host", "id": "operation-1"}), Some("available")));
}

#[test]
fn a_command_line_that_is_not_understood_changes_nothing() {
    let fake = Fake::new(Vec::new());
    assert_eq!(run(&fake, &[]), (2, String::new(), format!("{}\n", crate::cli::USAGE)));
    assert_eq!(run(&fake, &["host"]).0, 2);
    for arguments in [&["vm"][..], &["vm", "restart"], &["operation"], &["cleanup", "preview"], &["inspect", "--request-id", "r1"], &["update", "--json"], &["repair"], &["logs", "--component", "kernel"]] {
        let report = refusal(&fake, arguments, 2);
        assert_eq!((report["issue"]["code"].as_str(), report["stage"].as_str(), report["state"].as_str(), report["partial"].as_bool()), (Some("invalid_arguments"), Some("validation"), Some("failed"), Some(true)), "{arguments:?}");
    }
    for arguments in [&["inspect", "--help"][..], &["operation", "get", "-h"], &["vm", "--help"]] {
        let (code, stdout, stderr) = run(&fake, arguments);
        assert!(code == 0 && stdout.starts_with("usage: agent-vmm ") && stderr.is_empty(), "{arguments:?}");
    }
    assert!(fake.helper_calls.borrow().is_empty());
}
