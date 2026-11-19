//! `vm`, `cleanup`, `repair` and `host-socket` against a scripted Host owner.

use super::*;
use hostv1::OperationState::{Failed, Running, Succeeded};
use hostv1::VmState;

fn workload(id: &str) -> hostv1::ResourceReference {
    hostv1::ResourceReference { kind: "container".into(), environment_id: "environment-1".into(), resource_id: id.into(), ..Default::default() }
}

/// A Host whose VM runs two known workloads.
fn running_vm(answer: Call<Option<hostv1::Operation>>) -> Fake {
    let vm = hostv1::Vm { state: VmState::Running as i32, boot_id: "boot-1".into(), ..Default::default() };
    let summary = hostv1::GetVmImpactSummaryResponse { vm: Some(vm.clone()), complete: true, known_workloads: vec![workload("container-1"), workload("container-2")], ..Default::default() };
    Fake { summary: Some(summary), vm: Some(vm), mutation: RefCell::new(Some(answer)), ..Fake::new(Vec::new()) }
}

const STOP: [&str; 7] = ["vm", "stop", "--json", "--operator-socket", SOCKET, "--request-id", "request-1"];

#[test]
fn a_vm_stop_acknowledges_every_workload_of_the_boot_that_it_observed() {
    let fake = running_vm(Ok(Some(host_operation(Running, "stopping"))));
    let report = envelope(&fake, &[&STOP[..], &["--force"]].concat());
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), &report["operation_ref"], &report["next_action"]), (Some("running"), Some("stopping"), &json!({"owner": "host", "id": "operation-1"}), &json!([{"action": "operation_follow", "permission": "service_user"}])));
    assert_eq!((report["facts"]["impact_scope"].as_str(), report["facts"]["known_workload_count"].as_u64(), &report["facts"]["known_workloads"][0]), (Some("whole_vm"), Some(2), &json!({"kind": "container", "environment_id": "environment-1", "resource_id": "container-1"})));
    let fences = fake.fences.borrow();
    assert_eq!((fences.len(), fences[0].state(), fences[0].boot_id.as_str(), fences[0].observed_workloads.len(), fences[0].whole_vm_impact_acknowledged), (1, VmState::Running, "boot-1", 2, true));
}

#[test]
fn a_vm_that_changed_after_the_observation_is_not_mutated() {
    let mut fake = running_vm(Ok(None));
    fake.vm = Some(hostv1::Vm { state: VmState::Running as i32, boot_id: "boot-2".into(), ..Default::default() });
    let report = refusal(&fake, &STOP, 1);
    assert_eq!((report["issue"]["code"].as_str(), report["issue"]["message"].as_str().map(|message| message.starts_with("VM changed after observation"))), (Some("vm_operation_failed"), Some(true)));
    assert!(fake.fences.borrow().is_empty() && fake.mutation.borrow().is_some(), "the owner received no request");
}

#[test]
fn a_vm_mutation_reports_what_is_known_about_its_outcome() {
    // No typed reason: the request may have been accepted.
    for failure in [Failure { message: "transport is closing".into(), ..Failure::default() }, rejection(RpcCode::DeadlineExceeded, ""), rejection(RpcCode::Unavailable, "VM_ADMISSION_OUTCOME_UNKNOWN")] {
        let report = envelope(&running_vm(Err(failure)), &STOP);
        assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["partial"].as_bool(), report["facts"]["request_id"].as_str()), (Some("unknown_outcome"), Some("owner_response_pending"), Some(true), Some("request-1")));
        assert_eq!(report["next_action"], json!([{"action": "retry_same_request_id", "permission": "service_user"}]));
    }
    // A typed rejection is definite.
    let report = refusal(&running_vm(Err(rejection(RpcCode::FailedPrecondition, "VM_NOT_RUNNING"))), &STOP, 1);
    assert_eq!(report["issue"]["message"], "Host owner rejected vm stop: rpc error: code = FailedPrecondition desc = private Host text");

    let mut unsaved = rejection(RpcCode::Internal, "VM_TERMINAL_PERSIST_FAILED");
    unsaved.metadata.insert("operation_id".into(), "operation-9".into());
    let report = envelope(&running_vm(Err(unsaved)), &STOP);
    assert_eq!((report["stage"].as_str(), report["issue"]["code"].as_str(), &report["operation_ref"]), (Some("terminal_result_not_saved"), Some("vm_terminal_persist_failed"), &json!({"owner": "host", "id": "operation-9"})));
    assert_eq!(report["next_action"], json!([{"action": "retry_same_request_id", "permission": "service_user"}, {"action": "service_restart", "permission": "administrator"}]));

    // A replayed request answers with its stored terminal state.
    let mut stored = host_operation(Failed, "stop_failed");
    stored.error = Some(hostv1::OperationError { reason: "VM_STOP_TIMEOUT".into(), message: "token=abc".into(), ..Default::default() });
    let report = envelope(&running_vm(Ok(Some(stored))), &STOP);
    assert_eq!((report["state"].as_str(), report["partial"].as_bool(), &report["issue"], &report["next_action"]), (Some("failed"), Some(true), &json!({"code": "VM_STOP_TIMEOUT", "message": "token=[REDACTED]", "layer": "host", "retryable": null}), &json!([{"action": "vm_status", "permission": "service_user"}])));
    assert_eq!(envelope(&running_vm(Ok(Some(host_operation(Succeeded, "stopped")))), &STOP)["next_action"], json!([]));

    let mut other = host_operation(Succeeded, "stopped");
    other.request_id = "request-2".into();
    assert_eq!(refusal(&running_vm(Ok(Some(other))), &STOP, 1)["issue"]["message"], "Host owner returned an unbound operation");
}

#[test]
fn a_partial_impact_summary_is_reported_and_does_not_narrow_the_action() {
    let mut fake = running_vm(Ok(Some(host_operation(Running, "stopping"))));
    let summary = fake.summary.as_mut().unwrap();
    (summary.complete, summary.overflow) = (false, true);
    let status = envelope(&fake, &["vm", "status", "--json", "--operator-socket", SOCKET]);
    assert_eq!((status["state"].as_str(), status["partial"].as_bool(), &status["issue"]), (Some("running"), Some(true), &json!({"code": "impact_summary_partial", "message": "the bounded VM impact summary is incomplete", "layer": "host", "retryable": null})));
    assert_eq!((status["facts"]["impact_summary_overflow"].as_bool(), status["target"]["vm"].as_str()), (Some(true), Some("local")));
    assert!(fake.fences.borrow().is_empty(), "a status mutates nothing");
    envelope(&fake, &STOP);
    assert!(fake.fences.borrow()[0].whole_vm_impact_acknowledged);

    let unreadable = Fake::new(Vec::new());
    assert_eq!(refusal(&unreadable, &["vm", "status", "--operator-socket", SOCKET], 1)["issue"]["message"], "current Host VM impact summary is not readable");
}

fn plan(impacts: usize) -> hostv1::CleanupPlan {
    let impact = hostv1::CleanupImpact { resource: Some(hostv1::ResourceReference { kind: "volume".into(), environment_id: "environment-1".into(), resource_id: "volume-1".into(), ..Default::default() }), effect: "delete".into(), irreversible: true, blocker: false };
    hostv1::CleanupPlan { environment_id: "environment-1".into(), kind: hostv1::CleanupKind::Discard as i32, revision: 7, impacts: vec![impact; impacts] }
}

const PREVIEW: [&str; 8] = ["cleanup", "preview", "--json", "--operator-socket", SOCKET, "--environment-id", "environment-1", "--kind"];
const EXECUTE: [&str; 17] = ["cleanup", "execute", "--json", "--operator-socket", SOCKET, "--environment-id", "environment-1", "--kind", "discard", "--plan-revision", "7", "--request-id", "request-1", "--confirm-irreversible", "--confirm-volume", "volume-b", "--confirm-volume=volume-a"];

#[test]
fn a_cleanup_preview_lists_the_impacts_of_the_bound_plan() {
    let preview = |plan| Fake { plan: Some(plan), ..Fake::new(Vec::new()) };
    let report = envelope(&preview(Ok(Some(plan(1)))), &[&PREVIEW[..], &["discard"]].concat());
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["facts"]["plan_revision"].as_u64(), report["facts"]["impact_count"].as_u64(), &report["facts"]["reclaimable_bytes"]), (Some("planned"), Some("preview"), Some(7), Some(1), &Value::Null));
    assert_eq!(report["facts"]["impacts"][0], json!({"kind": "volume", "environment_id": "environment-1", "resource_id": "volume-1", "generation": 0, "generation_id": "", "effect": "delete", "irreversible": true, "blocker": false}));

    // A plan of another kind, an unbounded plan, and a refusal are not printed.
    assert_eq!(refusal(&preview(Ok(Some(plan(1)))), &[&PREVIEW[..], &["reset"]].concat(), 1)["issue"]["message"], "Host returned an unbound cleanup plan");
    assert_eq!(refusal(&preview(Ok(Some(plan(101)))), &[&PREVIEW[..], &["discard"]].concat(), 1)["issue"]["message"], "cleanup plan has 101 impacts; the managed CLI limit is 100");
    assert_eq!(refusal(&preview(Ok(None)), &[&PREVIEW[..], &["discard"]].concat(), 1)["issue"]["code"], "cleanup_failed");
    let refused = refusal(&preview(Err(rejection(RpcCode::FailedPrecondition, "AGENT_ACCESS_ACTIVE"))), &[&PREVIEW[..], &["discard"]].concat(), 1);
    assert_eq!(refused["issue"]["message"], "Host cleanup preview rejected: rpc_code=FailedPrecondition reason=AGENT_ACCESS_ACTIVE; complete removal of the exact agent integration before environment discard");
    let unknown = refusal(&preview(Err(Failure { message: "private Host text".into(), ..Failure::default() })), &[&PREVIEW[..], &["discard"]].concat(), 1);
    assert_eq!(unknown["issue"]["message"], "Host cleanup preview rejected: rpc_code=Unknown reason=UNKNOWN");
}

#[test]
fn a_cleanup_executes_the_exact_plan_and_volumes_that_were_confirmed() {
    let execute = |answer| Fake { mutation: RefCell::new(Some(answer)), ..Fake::new(Vec::new()) };
    let fake = execute(Ok(Some(host_operation(Succeeded, "complete"))));
    let report = envelope(&fake, &EXECUTE);
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), &report["facts"]["confirmed_volume_ids"], &report["operation_ref"]["id"]), (Some("succeeded"), Some("complete"), &json!(["volume-a", "volume-b"]), &json!("operation-1")));
    let sent = fake.cleanups.borrow();
    let sent_plan = sent[0].plan.as_ref().unwrap();
    assert_eq!((sent[0].request_id.as_str(), sent_plan.revision, sent_plan.kind(), sent[0].confirmed_volume_ids.join(",")), ("request-1", 7, hostv1::CleanupKind::Discard, "volume-a,volume-b".into()));

    // A failed terminal replay needs a new plan and a new request.
    let report = envelope(&execute(Ok(Some(host_operation(Failed, "delete_volumes")))), &EXECUTE);
    assert_eq!((report["partial"].as_bool(), &report["next_action"]), (Some(true), &json!([{"action": "cleanup_preview_new_request", "permission": "service_user"}])));
}

#[test]
fn a_cleanup_failure_is_definite_only_when_the_host_says_so() {
    let execute = |failure| Fake { mutation: RefCell::new(Some(Err(failure))), ..Fake::new(Vec::new()) };
    let definite = [
        ("CLEANUP_PLAN_STALE", "cleanup references changed during execution; request a new plan"),
        ("CLEANUP_ADMISSION_TIMEOUT", "cleanup admission timed out before the Host accepted an operation; wait for the competing mutation and request a new plan"),
        ("CLEANUP_ADMISSION_CANCELED", "the Host canceled cleanup admission before it accepted an operation; inspect the competing mutation and request a new plan"),
        ("VOLUME_IN_USE", "Host cleanup execute rejected: rpc_code=Aborted reason=VOLUME_IN_USE"),
    ];
    for (reason, message) in definite {
        assert_eq!(refusal(&execute(rejection(RpcCode::Aborted, reason)), &EXECUTE, 1)["issue"]["message"], message);
    }
    for failure in [Failure { message: "connection reset".into(), ..Failure::default() }, rejection(RpcCode::Unavailable, "CLEANUP_ADMISSION_OUTCOME_UNKNOWN")] {
        let report = envelope(&execute(failure), &EXECUTE);
        assert_eq!((report["state"].as_str(), report["stage"].as_str(), report["issue"]["code"].as_str(), report["facts"]["plan_revision"].as_u64()), (Some("unknown_outcome"), Some("owner_response_pending"), Some("unknown_outcome"), Some(7)));
    }
    let mut unsaved = rejection(RpcCode::Internal, "CLEANUP_TERMINAL_PERSIST_FAILED");
    unsaved.metadata.insert("operation_id".into(), "operation-9".into());
    let report = envelope(&execute(unsaved), &EXECUTE);
    assert_eq!((report["issue"]["code"].as_str(), report["operation_ref"]["id"].as_str(), report["next_action"].as_array().map(Vec::len)), (Some("cleanup_terminal_persist_failed"), Some("operation-9"), Some(2)));
}

const REPAIR: [&str; 7] = ["repair", "--json", "--lifecycle-helper", "/h", "--request-id", "request-1", "--registration-id"];

#[test]
fn a_repair_reports_the_operation_of_the_lifecycle_owner() {
    let operation = json!({"operationId": "lifecycle-1", "requestId": "request-1", "targetRegistrationId": "registration-1", "outcome": "succeeded", "stage": "complete"});
    let fake = Fake::new(vec![healthy(), printed(operation)]);
    let report = envelope(&fake, &[&REPAIR[..], &["registration-1"]].concat());
    assert_eq!((report["state"].as_str(), report["stage"].as_str(), &report["operation_ref"], &report["issue"]), (Some("succeeded"), Some("complete"), &json!({"owner": "lifecycle", "id": "lifecycle-1"}), &Value::Null));
    assert_eq!(fake.helper_calls.borrow()[1].0, ["host-repair", "--request-id", "request-1", "--registration-id", "registration-1"]);

    let failure = json!({"operationId": "lifecycle-1", "requestId": "request-1", "targetRegistrationId": "registration-1", "outcome": "failed", "stage": "host_start", "failureCode": "host_start_failed", "failureMessage": "the Host did not start", "retryable": true});
    let report = envelope(&Fake::new(vec![healthy(), failed(&failure.to_string(), "")]), &[&REPAIR[..], &["registration-1"]].concat());
    assert_eq!((report["state"].as_str(), &report["issue"]), (Some("failed"), &json!({"code": "host_start_failed", "message": "the Host did not start", "layer": "lifecycle", "retryable": true})));

    // The response of another target is not this request's result.
    let other = json!({"operationId": "lifecycle-1", "requestId": "request-1", "targetRegistrationId": "registration-2", "outcome": "succeeded"});
    assert_eq!(refusal(&Fake::new(vec![healthy(), printed(other)]), &[&REPAIR[..], &["registration-1"]].concat(), 1)["issue"]["code"], "repair_failed");
}

#[test]
fn a_repair_without_an_answer_keeps_the_request_identity() {
    let busy = json!({"error": {"code": "exclusive_transition_in_progress"}, "requestId": "request-0"});
    for (helper, active) in [(failed("", "signal: killed"), None), (failed(&busy.to_string(), ""), Some("request-0"))] {
        let report = envelope(&Fake::new(vec![healthy(), helper]), &[&REPAIR[..], &["registration-1"]].concat());
        assert_eq!((report["state"].as_str(), report["disposition"].as_str(), report["stage"].as_str(), report["partial"].as_bool()), (Some("transition_in_progress"), Some("exclusive_transition_in_progress"), Some("lifecycle_response_pending"), Some(true)));
        assert_eq!((report["facts"]["request_id"].as_str(), report["facts"]["active_request_id"].as_str(), report["issue"]["code"].as_str()), (Some("request-1"), active, Some("unknown_outcome")));
    }
}

#[test]
fn a_repair_is_refused_for_a_user_that_must_not_run_it() {
    let exited = status(json!({"hostInstalled": true, "hostLoaded": true, "hostJob": "system/com.afk-surf.agent-vmm-host"}));
    let arguments = [&REPAIR[..6], &["--service-type", "daemon"]].concat();
    if cfg!(target_os = "macos") {
        // The service user cannot start a system job that exited.
        let report = envelope(&Fake::new(vec![exited.clone()]), &arguments);
        assert_eq!((report["state"].as_str(), &report["next_action"]), (Some("needs_administrator"), &json!([{"action": "service_start", "permission": "administrator"}])));
        let root = Fake { euid: 0, ..Fake::new(vec![exited]) };
        let report = envelope(&root, &arguments);
        assert_eq!((report["state"].as_str(), &report["next_action"]), (Some("needs_service_user"), &json!([{"action": "repair", "permission": "service_user"}])));
    } else {
        let user = Fake { euid: 1000, ..Fake::new(vec![exited]) };
        let report = envelope(&user, &arguments);
        assert_eq!((report["state"].as_str(), report["issue"]["layer"].as_str(), &report["next_action"]), (Some("needs_administrator"), Some("service"), &json!([{"action": "repair", "permission": "administrator"}])));
    }
}

#[test]
fn host_sockets_are_approved_listed_and_removed() {
    let fake = Fake::new(Vec::new());
    let set = envelope(&fake, &["host-socket", "set", "--json", "--operator-socket", SOCKET, "--environment-id", "environment-1", "--port", "8080", "--path", "/run/service.sock", "--request-id", "request-1"]);
    assert_eq!((set["state"].as_str(), set["stage"].as_str(), &set["facts"]["host_sockets"]), (Some("approved"), Some("set"), &json!([{"environment_id": "environment-1", "port": 8080, "path": "/run/service.sock", "revision": 1}])));
    let listed = envelope(&fake, &["host-socket", "list", "--json", "--operator-socket", SOCKET]);
    assert_eq!((listed["state"].as_str(), listed["facts"]["host_sockets"].as_array().map(Vec::len), &listed["target"]), (Some("observed"), Some(1), &json!({"environment_id": ""})));
    let removed = envelope(&fake, &["host-socket", "remove", "--json", "--operator-socket", SOCKET, "--port", "8080", "--request-id", "request-2"]);
    assert_eq!((removed["state"].as_str(), &removed["facts"]), (Some("removed"), &json!({"port": 8080})));
    assert_eq!(envelope(&fake, &["host-socket", "list", "--json", "--operator-socket", SOCKET])["facts"]["host_sockets"], json!([]));
}
