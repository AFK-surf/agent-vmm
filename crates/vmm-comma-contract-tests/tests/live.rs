//! Applies the consumer models to a real agent-vmm build. Each probe reads one
//! environment variable and is skipped when it is unset, unless
//! `VMM_CONTRACT_REQUIRE_LIVE=1` makes a missing input a failure.
//!
//! | Variable | Probe |
//! | --- | --- |
//! | `AGENT_VMM_LIFECYCLE_BIN` | runs `version --json`; unknown commands must fail |
//! | `AGENT_VMM_HOST_APP` | the four executables of `Agent VMM Host.app` |
//! | `AGENT_VMM_LINUX_RELEASE_DIR` | executable `agent-vmm` at the release root |
//! | `AGENT_VMM_CONTRACT_OUTPUT_DIR` | captured outputs: `hello.json`, `version.json`, `status.json`, `maintenance-status.json`, `local-environments.json`, `local-disk.json`, `local-registrations.json` |
//! | `AGENT_VMM_SIGNING_PROBE` | executable: stdin `{"type","message"}`, stdout canonical input in hex |

use serde_json::{json, Value};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use vmm_comma_contract_tests::{consumer, golden, signing};

fn input(variable: &str) -> Option<PathBuf> {
    match std::env::var_os(variable) {
        Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
        _ if std::env::var("VMM_CONTRACT_REQUIRE_LIVE").as_deref() == Ok("1") => panic!("{variable} is required when VMM_CONTRACT_REQUIRE_LIVE=1"),
        _ => {
            eprintln!("skipped: {variable} is not set");
            None
        }
    }
}

fn executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

#[test]
fn lifecycle_helper_reports_a_readable_version() {
    let Some(helper) = input("AGENT_VMM_LIFECYCLE_BIN") else { return };
    let output = Command::new(&helper).args(["version", "--json"]).stdin(Stdio::null()).output().expect("run version --json");
    assert!(output.status.success(), "version --json exited with {}", output.status);
    let report: Value = serde_json::from_slice(&output.stdout).expect("version --json must print one JSON value");
    let verdict = consumer::version_report(&report);
    assert_eq!(verdict["contract"], json!("ok"), "contractVersion must be 1: {report}");
    let release = verdict["installed_release_id"].as_str().expect("release_id must be a string");
    // Cue scripts/lib/server-release-descriptor.mjs and server_release_descriptor.ex:11.
    assert!(!release.is_empty() && release.len() <= 128, "release_id {release}");
    assert!(release.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')), "release_id {release}");

    let unknown = Command::new(&helper).arg("comma-contract-unknown-command").stdin(Stdio::null()).output().expect("run unknown command");
    assert!(!unknown.status.success(), "an unknown command must exit non-zero");
}

#[test]
fn macos_bundle_has_the_executables_cue_checks() {
    let Some(app) = input("AGENT_VMM_HOST_APP") else { return };
    assert_eq!(app.file_name().and_then(|name| name.to_str()), Some("Agent VMM Host.app"));
    assert!(!std::fs::symlink_metadata(&app).expect("bundle").file_type().is_symlink(), "the bundle cannot be a symbolic link");
    let layout = golden("bundle_layout.json");
    for entry in layout["macos"]["required_executables"].as_array().expect("executables") {
        let path = app.join(entry["path"].as_str().expect("path"));
        assert!(executable(&path), "{} must be executable ({})", path.display(), entry["sources"]);
    }
}

#[test]
fn linux_release_has_the_installer_at_its_root() {
    let Some(release) = input("AGENT_VMM_LINUX_RELEASE_DIR") else { return };
    assert!(executable(&release.join("agent-vmm")), "agent-vmm must be executable at the release root");
}

#[test]
fn captured_outputs_satisfy_their_consumers() {
    let Some(directory) = input("AGENT_VMM_CONTRACT_OUTPUT_DIR") else { return };
    let read = |file: &str| -> Option<Value> {
        let text = std::fs::read_to_string(directory.join(file)).ok()?;
        Some(serde_json::from_str(&text).unwrap_or_else(|error| panic!("{file}: {error}")))
    };
    let mut checked = 0;
    if let Some(hello) = read("hello.json") {
        // Salix reads these protojson keys (salix_web/router.ex:286-294, salix_store/agent_vmm.ex:265-266).
        assert!(hello["registrationId"].as_str().is_some_and(|id| !id.is_empty()), "hello.registrationId");
        assert!(consumer::decimal(&hello["connectionEpoch"]) && hello["connectionEpoch"] != json!("0"), "hello.connectionEpoch must be a non-zero decimal string");
        assert!(consumer::decimal(&hello["inventoryWatermark"]) && hello["inventoryWatermark"] != json!("0"), "hello.inventoryWatermark is required");
        // The multiplexed controller connection negotiates protocol 5.
        assert_eq!(hello["protocolVersion"], json!("5"), "hello.protocolVersion");
        // The controller requires non-empty, unique allocation ids.
        let mut seen = std::collections::BTreeSet::new();
        for allocation in hello["inventory"].as_array().cloned().unwrap_or_default() {
            let id = allocation["allocationId"].as_str().unwrap_or_default().to_string();
            assert!(!id.is_empty() && seen.insert(id), "hello.inventory allocation ids must be non-empty and unique");
        }
        checked += 1;
    }
    if let Some(version) = read("version.json") {
        assert_eq!(consumer::version_report(&version)["contract"], json!("ok"));
        checked += 1;
    }
    if let Some(status) = read("status.json") {
        assert_eq!(consumer::helper_contract_failure(&status), None, "status.contractVersion");
        for key in ["hostInstalled", "hostLoaded", "hostReadable", "hostHealthy", "connectorInstalled", "connectorLoaded"] {
            assert!(status[key].is_boolean(), "status.{key} must be a boolean");
        }
        checked += 1;
    }
    if let Some(receipt) = read("maintenance-status.json") {
        consumer::maintenance_status(&receipt, None).unwrap_or_else(|error| panic!("maintenance-status.json: {error}"));
        checked += 1;
    }
    if let Some(page) = read("local-environments.json") {
        consumer::local_environments(&page).unwrap_or_else(|error| panic!("local-environments.json: {error}"));
        checked += 1;
    }
    if let Some(disk) = read("local-disk.json") {
        consumer::local_disk(&disk).unwrap_or_else(|error| panic!("local-disk.json: {error}"));
        checked += 1;
    }
    if let Some(page) = read("local-registrations.json") {
        consumer::local_registrations(&page).unwrap_or_else(|error| panic!("local-registrations.json: {error}"));
        checked += 1;
    }
    assert!(checked > 0, "{} holds none of the known capture files", directory.display());
}

#[test]
fn signing_probe_reproduces_every_canonical_input() {
    let Some(probe) = input("AGENT_VMM_SIGNING_PROBE") else { return };
    signing::assert_canonical_encoder(|type_name, message| {
        let mut child = Command::new(&probe).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().map_err(|error| error.to_string())?;
        let request = json!({"type": type_name, "message": message}).to_string();
        child.stdin.take().ok_or("no stdin")?.write_all(request.as_bytes()).map_err(|error| error.to_string())?;
        let output = child.wait_with_output().map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(format!("probe exited with {}", output.status));
        }
        hex::decode(String::from_utf8_lossy(&output.stdout).trim()).map_err(|error| error.to_string())
    });
}
