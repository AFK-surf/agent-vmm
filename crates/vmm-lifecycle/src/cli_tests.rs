use super::*;
use crate::journal::{self, LifecycleOperation};
use crate::testing::{Fake, installed};

fn invoke(arguments: &[&str]) -> (i32, String, String) {
    let arguments: Vec<String> = arguments.iter().map(|argument| argument.to_string()).collect();
    let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
    let code = run(&Fake::new().context(), &arguments, &mut std::io::empty(), &mut output, &mut diagnostics);
    (code, String::from_utf8(output).unwrap(), String::from_utf8(diagnostics).unwrap())
}

#[test]
fn an_unknown_command_tells_comma_to_update_the_host_on_both_streams() {
    let (code, output, diagnostics) = invoke(&["comma-contract-unknown-command"]);
    assert_eq!(code, 1);
    assert_eq!(output, "{\"error\":{\"code\":\"upgrade_required\",\"reason\":\"unknown_command\"}}\n");
    assert!(diagnostics.contains("unknown command"), "{diagnostics}");
    let (code, output, _) = invoke(&[]);
    assert_eq!((code, output.as_str()), (1, ""));
}

#[test]
fn version_reports_the_contract_and_the_injected_release() {
    let (code, output, diagnostics) = invoke(&["version", "--json"]);
    assert_eq!((code, diagnostics.as_str()), (0, ""));
    let report: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(report["contractVersion"], json!(1));
    assert_eq!(report["component"], json!("agent-vmm-host"));
    assert_eq!(report["release_id"], json!(crate::release_id()));
    assert_eq!(report["version"], report["release_id"]);
    assert!(report["currentSchema"].as_u64() >= report["minimumSchema"].as_u64() && report["minimumSchema"].as_u64() >= Some(1));
    assert_eq!(invoke(&["version"]), (1, String::new(), "lifecycle failed: version requires --json\n".into()));
    assert_eq!(invoke(&["preflight"]).0, 1);
}

#[test]
fn status_always_names_the_facts_that_comma_reads() {
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    for (running, registration) in [(true, Some("registration-1")), (false, None)] {
        let fake = Fake::with(crate::testing::service_manager(running));
        let observed = serde_json::to_value(status::read(&fake.context(), &paths, registration, Deadline::after(STATUS_BUDGET))).unwrap();
        assert_eq!(observed["contractVersion"], json!(1));
        for key in ["hostInstalled", "hostLoaded", "hostReadable", "hostHealthy", "connectorInstalled", "connectorLoaded"] {
            assert!(observed[key].is_boolean(), "{key} must be explicit: {observed}");
        }
        assert!(observed.as_object().unwrap().values().all(|value| !value.is_null()), "{observed}");
        assert_eq!(observed["hostHealthy"], json!(running));
        if registration.is_some() {
            assert_eq!((observed["registrationRead"].as_str(), observed.get("registrationState")), (Some("absent"), None));
        }
    }
}

#[test]
fn status_projects_a_registration_and_its_protocol_window() {
    use agent_vmm_proto::host::v1 as hostv1;
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    let fake = Fake::new();
    let mut registration = crate::testing::Host::registration("registration-1", hostv1::RemoteRegistrationState::Draining, true);
    let protocol = hostv1::RemoteProtocolStatus { host_protocol: "4".into(), min_protocol: 5, max_protocol: 6, host_side: "too_old".into() };
    registration.connector_status = Some(hostv1::ConnectorStatus { remote_protocol: Some(protocol), ..Default::default() });
    fake.host.lock().unwrap().registrations.insert("registration-1".into(), registration);
    let observed = serde_json::to_value(status::read(&fake.context(), &paths, Some("registration-1"), Deadline::after(STATUS_BUDGET))).unwrap();
    assert_eq!((observed["registrationRead"].as_str(), observed["registrationState"].as_str(), observed["connectorLoaded"].as_bool()), (Some("present"), Some("draining"), Some(true)));
    assert_eq!(observed["remoteProtocol"], json!({"hostProtocol": "4", "minProtocol": 5, "maxProtocol": 6, "hostSide": "too_old"}));
    assert_eq!(observed["hostHealth"], json!({"state": 1}), "Host facts keep the encoding of the Go Host");
}

#[test]
fn operation_reads_null_a_receipt_or_the_owner_fence() {
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    let context = Fake::new().context();
    let read = |arguments: &[&str]| report::operation(&context, &paths, &arguments.iter().map(|argument| argument.to_string()).collect::<Vec<_>>());
    assert_eq!(read(&["--request-id", "request-1"]).unwrap(), Value::Null);

    let receipt = LifecycleOperation { operation_id: "op-1".into(), outcome: "succeeded".into(), target_registration_id: "registration-1".into(), correlation_request_id: "request-2".into(), ..LifecycleOperation::unknown("request-1", "registration-recover") };
    journal::persist(&context, &paths, &receipt).unwrap();
    let report = read(&["--request-id", "request-1"]).unwrap();
    assert_eq!((report["requestId"].as_str(), report["kind"].as_str(), report["contractVersion"].as_u64()), (Some("request-1"), Some("registration-recover"), Some(1)));
    assert_eq!(read(&["--operation-id", "op-1"]).unwrap(), report);
    // The request that asked for the recovery reads the receipt of the request that it continued.
    assert_eq!(read(&["--request-id", "request-2", "--registration-id", "registration-1"]).unwrap(), report);
    assert_eq!(read(&["--request-id", "request-3", "--registration-id", "registration-1"]).unwrap(), Value::Null);
    assert!(read(&["--request-id", "request-1", "--registration-id", "registration-2"]).is_err());
    assert!(read(&["--request-id", "a.b"]).is_err());
}

/// Every argument list that a Comma consumer gives the helper is one that this parser accepts.
#[test]
fn the_invocations_of_comma_are_accepted() {
    let vectors: Value = serde_json::from_str(include_str!("../../vmm-comma-contract-tests/golden_vectors/lifecycle_cli/invocations.json")).unwrap();
    let mut checked = 0;
    for invocation in vectors["invocations"].as_array().unwrap() {
        let id = invocation["id"].as_str().unwrap();
        let binary = invocation["binary"].as_str().unwrap();
        let mut argv: Vec<String> = invocation["argv"].as_array().unwrap().iter().map(|argument| {
            let argument = argument.as_str().unwrap();
            match argument {
                "<REQ>" => "request-1".to_string(),
                "<REG>" => "registration-1".to_string(),
                "<APP>" => "/tmp/staged/Agent VMM Host.app".to_string(),
                "<USER>" => "worker".to_string(),
                choice if choice.starts_with('<') => choice.trim_matches(['<', '>']).split('|').next().unwrap().to_string(),
                plain => plain.to_string(),
            }
        }).collect();
        if argv[0] == "lifecycle" {
            argv.remove(0);
        } else if !binary.contains("agent-vmm-lifecycle") {
            continue;
        }
        let (command, rest) = argv.split_first().unwrap();
        assert!(known(command), "{id}: {command}");
        let (_, arguments) = ServiceRequest::extract(command, rest).unwrap_or_else(|error| panic!("{id}: {error}"));
        let parsed = match command.as_str() {
            "version" => (arguments == ["--json"]).then_some(()).ok_or_else(|| err("version")),
            "status" => args::parse_status(&arguments).map(|_| ()),
            "operation" => matches!(arguments.len(), 2 | 4).then_some(()).ok_or_else(|| err("operation")),
            "registration-claim" | "registration-recover" => args::parse_install("registration-claim", &arguments).map(|_| ()),
            "install" => args::parse_install("install", &arguments).map(|_| ()),
            "repair" => args::parse_install("repair", &arguments).map(|_| ()),
            "registration-enable" | "registration-drain" | "registration-remove" => args::parse_registration_state(&arguments, Some(RemoteRegistrationState::Enabled)).map(|_| ()),
            "drain" => args::parse_request_id(&arguments).map(|_| ()),
            "uninstall" => args::parse_uninstall(&arguments).map(|_| ()),
            "publish-host" => (arguments.len() == 2 && arguments[0] == "--source-app").then_some(()).ok_or_else(|| err("publish-host")),
            _ => arguments.is_empty().then_some(()).ok_or_else(|| err("the request is on standard input")),
        };
        parsed.unwrap_or_else(|error| panic!("{id}: {argv:?}: {error}"));
        checked += 1;
    }
    assert!(checked >= 30, "only {checked} invocations were checked");
}
