//! Runs every golden vector through the consumer models. These tests need no
//! agent-vmm build: they prove that the vectors and the models agree, and
//! they pin the protocol surface. `tests/live.rs` applies the same models to
//! a real build.

use serde_json::{Value, json};
use vmm_comma_contract_tests::{consumer, descriptor, golden, golden_dir, protolock, signing};

fn cases<'a>(file: &'a Value, section: &str) -> &'a Vec<Value> {
    file[section]
        .as_array()
        .unwrap_or_else(|| panic!("section {section} must be an array"))
}

fn name(case: &Value) -> &str {
    case["name"].as_str().unwrap_or("<unnamed>")
}

#[test]
fn signing_vectors_are_consistent_low_s_p256() {
    let vectors = signing::load_vectors();
    let mut expected: Vec<&str> = signing::SIGNED_TYPES
        .iter()
        .map(|(name, _, _)| *name)
        .collect();
    let mut found: Vec<&str> = vectors
        .iter()
        .map(|vector| vector.type_name.as_str())
        .collect();
    expected.sort_unstable();
    found.sort_unstable();
    assert_eq!(
        found, expected,
        "the vector file must cover exactly the nine signed types"
    );
    for salix in signing::SALIX_VERIFIED_TYPES {
        assert!(found.contains(salix), "Salix verifies {salix}");
    }
    for vector in &vectors {
        signing::check_vector(vector).unwrap_or_else(|error| panic!("{error}"));
    }
    signing::assert_verifier(|key, message, signature| {
        signing::verify(key, message, signature).is_ok()
    });
}

#[test]
fn signing_vector_file_is_the_one_cue_pins() {
    use sha2::{Digest, Sha256};
    let bytes =
        std::fs::read(golden_dir().join("agent_vmm_signing_vectors.json")).expect("read vectors");
    assert_eq!(
        hex::encode(Sha256::digest(&bytes)),
        "0e066cebbd7e2ba692d2cf9e8fa622bcef2f5e77af2e8470a8bc54ea417ca1f8",
        "must stay byte-identical to Cue systems/apps/salix_store/test/testdata/agent_vmm_signing_vectors.json"
    );
}

#[test]
fn canonical_payloads_are_deterministic_and_unsigned() {
    let tree = proto_tree();
    for vector in signing::load_vectors() {
        let (_, message, field) = signing::SIGNED_TYPES
            .iter()
            .find(|(name, _, _)| *name == vector.type_name)
            .expect("known signed type");
        let (package, short) = message.rsplit_once('.').expect("qualified message name");
        let number = tree[package]["messages"][short][*field]["number"]
            .as_u64()
            .unwrap_or_else(|| panic!("{message}.{field} must exist"));
        let payload = signing::canonical_payload(&vector).unwrap_or_else(|error| panic!("{error}"));
        let fields = signing::top_level_fields(payload)
            .unwrap_or_else(|error| panic!("{}: {error}", vector.type_name));
        assert!(!fields.is_empty(), "{}: empty payload", vector.type_name);
        assert!(
            fields.iter().all(|(found, _, _)| *found != number),
            "{}: signature field {number} must be cleared",
            vector.type_name
        );
        assert!(
            fields.windows(2).all(|pair| pair[0].0 <= pair[1].0),
            "{}: fields must be in field-number order",
            vector.type_name
        );
        let known: Vec<u64> = tree[package]["messages"][short]
            .as_object()
            .expect("message")
            .values()
            .filter_map(|field| field["number"].as_u64())
            .collect();
        assert!(
            fields.iter().all(|(found, _, _)| known.contains(found)),
            "{}: unknown field in canonical payload",
            vector.type_name
        );
    }
}

#[test]
fn version_reports_keep_the_contract_electron_reads() {
    let file = golden("lifecycle_cli/consumer_outputs.json");
    for case in cases(&file, "version_report") {
        assert_eq!(
            consumer::version_report(&case["value"]),
            case["expect"],
            "{}",
            name(case)
        );
    }
}

#[test]
fn status_reports_project_to_the_expected_observation() {
    let file = golden("lifecycle_cli/consumer_outputs.json");
    for case in cases(&file, "status") {
        assert_eq!(
            consumer::project_status(&case["value"], case["registration_id"].as_str()),
            case["expect"],
            "{}",
            name(case)
        );
    }
}

#[test]
fn maintenance_receipts_follow_the_native_receipt_schema() {
    let file = golden("lifecycle_cli/consumer_outputs.json");
    for case in cases(&file, "maintenance_status") {
        let actual = match consumer::maintenance_status(
            &case["value"],
            case["intent_request_id"].as_str(),
        ) {
            Ok(None) => json!({"result": "none"}),
            Ok(Some(mut operation)) => {
                operation["result"] = json!("receipt");
                operation
            }
            Err(code) => json!({"result": "error", "error": code}),
        };
        assert_eq!(actual, case["expect"], "{}", name(case));
    }
}

#[test]
fn request_receipts_acknowledge_the_exact_request() {
    let file = golden("lifecycle_cli/consumer_outputs.json");
    for case in cases(&file, "request_receipt") {
        let text = |key: &str| {
            case[key]
                .as_str()
                .unwrap_or_else(|| panic!("{key} is required"))
        };
        let actual = match consumer::request_receipt(
            &case["value"],
            text("command"),
            text("request_id"),
            text("registration_id"),
        ) {
            Ok(mut receipt) => {
                receipt["result"] = json!("receipt");
                receipt
            }
            Err(code) => json!({"result": "error", "error": code}),
        };
        assert_eq!(actual, case["expect"], "{}", name(case));
    }
}

#[test]
fn native_command_errors_are_read_from_stdout() {
    let file = golden("lifecycle_cli/consumer_outputs.json");
    for case in cases(&file, "native_command_error") {
        let actual = consumer::native_command_error(case["stdout"].as_str().expect("stdout"))
            .unwrap_or(Value::Null);
        assert_eq!(actual, case["expect"], "{}", name(case));
    }
}

#[test]
fn decimal_strings_are_canonical_uint64() {
    let file = golden("lifecycle_cli/local_operator.json");
    for case in cases(&file, "decimal") {
        assert_eq!(
            consumer::decimal(&case["value"]),
            case["accept"],
            "decimal {}",
            case["value"]
        );
    }
}

fn expand(value: &Value) -> Value {
    // "@repeat33" stands for 33 valid environments: one above the page bound.
    let mut value = value.clone();
    if value["environments"] == json!("@repeat33") {
        let rows: Vec<Value> = (0..33).map(|index| json!({"id": format!("env-{index}"), "origin": "local", "state": "unknown", "revision": "1"})).collect();
        value["environments"] = Value::Array(rows);
    }
    value
}

#[test]
fn local_operator_pages_match_the_zod_schemas() {
    let file = golden("lifecycle_cli/local_operator.json");
    for case in cases(&file, "local_environments") {
        let verdict = consumer::local_environments(&expand(&case["value"]));
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "local-environments {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file, "local_disk") {
        let verdict = consumer::local_disk(&case["value"]);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "local-disk {}: {verdict:?}",
            name(case)
        );
        if let Ok(disk) = verdict {
            assert_eq!(
                disk.unwrap_or(Value::Null),
                case["expect_disk"],
                "local-disk {}",
                name(case)
            );
        }
    }
    for case in cases(&file, "local_registrations") {
        let verdict = consumer::local_registrations(&case["value"]);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "local-registrations {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file, "local_plan") {
        let target = &case["target"];
        let verdict = consumer::local_plan(
            &case["value"],
            target["kind"].as_str().expect("kind"),
            target["id"].as_str().expect("id"),
        );
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "local-plan {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file, "local_operation") {
        let verdict = consumer::local_operation(
            &case["value"],
            case["plan_target_id"].as_str().expect("plan_target_id"),
        );
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "local-operation {}: {verdict:?}",
            name(case)
        );
        if let Ok(outcome) = verdict {
            assert_eq!(
                json!(outcome),
                case["expect_outcome"],
                "local-operation {}",
                name(case)
            );
        }
    }
}

#[test]
fn invocations_use_known_commands_and_bounded_stdin() {
    let file = golden("lifecycle_cli/invocations.json");
    // agent-vmm internal/lifecycle/main.go:348-363 and local_operator.go:21-27, plus the
    // managed CLI verbs and the Linux `lifecycle` prefix that Cue invokes.
    let known = [
        "version",
        "status",
        "operation",
        "maintenance-status",
        "publish-host",
        "host-maintain",
        "install",
        "repair",
        "drain",
        "uninstall",
        "registration-claim",
        "registration-recover",
        "registration-enable",
        "registration-drain",
        "registration-remove",
        "local-plan",
        "local-execute",
        "local-operation",
        "local-disk",
        "local-environments",
        "local-registrations",
        "inspect",
        "update",
        "lifecycle",
    ];
    let mut ids = std::collections::BTreeSet::new();
    for invocation in cases(&file, "invocations") {
        let id = invocation["id"].as_str().expect("id");
        assert!(ids.insert(id.to_string()), "duplicate invocation {id}");
        let argv = invocation["argv"].as_array().expect("argv");
        let verb = argv[0].as_str().expect("verb");
        let alternatives: Vec<&str> = verb
            .trim_matches(|character| character == '<' || character == '>')
            .split('|')
            .collect();
        assert!(
            alternatives.iter().all(|verb| known.contains(verb)),
            "{id}: unknown command {verb}"
        );
        assert!(
            invocation["source"]
                .as_str()
                .is_some_and(|source| source.contains("Cue ")),
            "{id}: a Cue source is required"
        );
        let stdin = invocation["stdin"].as_str().expect("stdin");
        let builtin = [
            "none",
            "empty",
            "inherited",
            "claim_descriptor",
            "recovery_descriptor",
        ];
        assert!(
            builtin.contains(&stdin) || file["stdin"].get(stdin).is_some(),
            "{id}: unknown stdin kind {stdin}"
        );
    }
    for (kind, schema) in file["stdin"].as_object().expect("stdin schemas") {
        let allowed: Vec<&str> = schema["allowed_keys"]
            .as_array()
            .expect("allowed_keys")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        for sample in schema["samples"].as_array().expect("samples") {
            let value = sample["value"]
                .as_object()
                .unwrap_or_else(|| panic!("{kind}: sample must be an object"));
            for key in value.keys() {
                assert!(
                    allowed.contains(&key.as_str()),
                    "{kind} / {}: key {key} is outside the accepted set",
                    name(sample)
                );
            }
            let bytes = serde_json::to_vec(&sample["value"]).expect("encode").len() as u64;
            if let Some(limit) = schema["max_bytes"].as_u64() {
                assert!(
                    bytes <= limit,
                    "{kind} / {}: {bytes} bytes exceed {limit}",
                    name(sample)
                );
            }
        }
    }
    // The maintenance request ids Cue sends satisfy the helper's id pattern.
    for sample in file["stdin"]["maintenance_request"]["samples"]
        .as_array()
        .expect("samples")
    {
        let request = sample["value"]["requestId"].as_str().expect("requestId");
        assert!(
            descriptor::operation_id(request),
            "{}: requestId {request}",
            name(sample)
        );
        assert_eq!(sample["value"]["version"], json!(1), "{}", name(sample));
        assert_eq!(
            sample["value"]["confirmSharedHost"],
            json!(true),
            "{}",
            name(sample)
        );
    }
}

#[test]
fn registration_descriptors_and_bodies_match_both_sides() {
    let file = golden("salix_http/registration.json");
    for case in cases(&file["claim_descriptor"], "cases") {
        let verdict = descriptor::claim_descriptor(&case["value"]);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "claim descriptor {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file["recovery_descriptor"], "cases") {
        let verdict = descriptor::recovery_descriptor(&case["value"]);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "recovery descriptor {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file["claim_request"], "cases") {
        let verdict = descriptor::claim_request(&case["value"]);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "claim request {}: {verdict:?}",
            name(case)
        );
    }
    for case in cases(&file["claim_response"], "cases") {
        let registration = case["value"]["registration_id"]
            .as_str()
            .unwrap_or_default();
        let verdict = descriptor::claim_response(&case["value"], registration);
        assert_eq!(
            verdict.is_ok(),
            case["accept"],
            "claim response {}: {verdict:?}",
            name(case)
        );
    }
    let errors = &file["claim_errors"];
    let recognized: Vec<&str> = errors["host_recognized_codes"]
        .as_array()
        .expect("codes")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for (code, status) in errors["status_by_code"]
        .as_object()
        .expect("status_by_code")
    {
        assert!(
            recognized.contains(&code.as_str()),
            "the Host must keep recognizing Salix code {code}"
        );
        assert!(
            matches!(status.as_u64(), Some(400 | 401 | 403 | 409 | 410 | 503)),
            "{code}: unexpected status {status}"
        );
    }
    let recovery = &file["recovery"];
    let fields: Vec<&str> = recovery["wire_fields_in_order"]
        .as_array()
        .expect("fields")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let wire =
        descriptor::recovery_wire(&recovery["wire_case"]["challenge"], &fields).expect("wire");
    assert_eq!(json!(wire), recovery["wire_case"]["wire"]);
}

/// The packages that Comma reads, from `$AGENT_VMM_PROTO_DIR` or from `api` of this repository.
fn proto_tree() -> Value {
    let root = match std::env::var_os("AGENT_VMM_PROTO_DIR") {
        Some(path) => std::path::PathBuf::from(path),
        None => std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../api"),
    };
    let mut packages = serde_json::Map::new();
    for directory in ["appliance", "host", "remote", "service", "trust"] {
        let tree =
            protolock::parse_tree(&root.join(directory)).unwrap_or_else(|error| panic!("{error}"));
        packages.extend(tree.as_object().expect("packages").clone());
    }
    Value::Object(packages)
}

#[test]
fn protocol_surface_keeps_every_pinned_number_and_name() {
    let pinned = protolock::parse_tree(&golden_dir().join("proto_pinned"))
        .expect("parse pinned proto files");
    let lock = json!({ "packages": pinned });
    let violations = protolock::compare(&lock, &proto_tree());
    assert!(
        violations.is_empty(),
        "{} incompatible protocol changes:\n{}",
        violations.len(),
        violations.join("\n")
    );
}

#[test]
fn protocol_surface_rejects_incompatible_changes() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("api.proto");
    let proto = "package example; message Item { string name = 1; } message Empty {} service Api { rpc Get(Empty) returns (Item); }";
    std::fs::write(&file, proto).unwrap();
    let lock = json!({ "packages": protolock::parse_tree(dir.path()).unwrap() });
    for (original, replacement, violation) in [
        (
            "rpc Get(Empty) returns (Item);",
            "",
            "example.Api.Get is missing",
        ),
        (
            "message Empty {}",
            "",
            "example.Empty (messages) is missing",
        ),
        ("name = 1", "name = 2", "example.Item.name changed"),
        ("string name", "bytes name", "example.Item.name changed"),
    ] {
        std::fs::write(&file, proto.replace(original, replacement)).unwrap();
        let errors = protolock::compare(&lock, &protolock::parse_tree(dir.path()).unwrap());
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].starts_with(violation), "{errors:?}");
    }
}

#[test]
fn every_proto_name_cue_uses_exists() {
    let usage = golden("remote_v1/cue_proto_usage.json");
    let problems = protolock::check_usage(&usage, &proto_tree());
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    // The stored controller commands use only JSON names of ControllerCommand.
    let tree = proto_tree();
    let fields = tree["agentvmm.remote.v1"]["messages"]["ControllerCommand"]
        .as_object()
        .expect("ControllerCommand");
    let names: Vec<String> = fields
        .keys()
        .map(|field| protolock::json_name(field))
        .collect();
    for sample in usage["controller_commands"]["samples"]
        .as_array()
        .expect("samples")
    {
        for key in sample["value"].as_object().expect("command").keys() {
            assert!(
                names.contains(key),
                "{}: {key} is not a ControllerCommand field",
                name(sample)
            );
        }
        assert_eq!(
            sample["value"]["timeoutMillis"],
            json!(60_000),
            "{}",
            name(sample)
        );
    }
}

#[test]
fn bundle_layout_names_the_four_checked_executables() {
    let layout = golden("bundle_layout.json");
    let paths: Vec<&str> = layout["macos"]["required_executables"]
        .as_array()
        .expect("executables")
        .iter()
        .filter_map(|entry| entry["path"].as_str())
        .collect();
    assert_eq!(
        paths,
        [
            "Contents/MacOS/agent-vmm-host",
            "Contents/Helpers/agent-vmm-lifecycle",
            "Contents/Helpers/agent-vmm",
            "Contents/Helpers/agent-vmm-service-executor"
        ]
    );
    assert_eq!(
        layout["linux"]["required_executables"][0]["path"],
        json!("agent-vmm")
    );
}
