//! Runs the built program: each entry is reached, and a command without a Host fails with one envelope.

use serde_json::{Value, json};
use std::process::{Command, Stdio};

/// Returns the exit status, standard output and standard error of `agent-vmm ARGUMENTS`.
fn run(arguments: &[&str]) -> (Option<i32>, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-vmm")).args(arguments).stdin(Stdio::null()).output().unwrap();
    (output.status.code(), String::from_utf8(output.stdout).unwrap(), String::from_utf8(output.stderr).unwrap())
}

#[test]
fn each_entry_answers_on_the_stream_that_its_caller_reads() {
    let (code, stdout, stderr) = run(&[]);
    assert!(code == Some(2) && stdout.is_empty() && stderr.starts_with("usage: agent-vmm inspect|diagnose|logs|operation|repair|update"), "{stderr}");
    assert_eq!(run(&["appliance", "--help"]), (Some(0), "usage: agent-vmm appliance install|update|status|recover|stop|start|activate|restart|revoke|remove ...\n".into(), String::new()));
    assert_eq!(run(&["appliance", "build"]).0, Some(2), "the packaged program has no artifact builder");
    assert_eq!(run(&["mcp"]), (Some(2), String::new(), "usage: agent-vmm mcp token create|rotate [--operator-socket PATH]\n".into()));
    assert_eq!(run(&["appliance", "remove", "--installation", "mcp-1", "--confirm-resource", "volume-1"]).0, Some(1), "a confirmation without --execute is refused before any call");
    assert_eq!(run(&["appliance", "stop", "--unknown-flag"]).0, Some(2));

    // No Host listens on the socket: the command fails with one envelope on standard error.
    let (code, stdout, stderr) = run(&["vm", "status", "--json", "--operator-socket", "/nonexistent/agent-vmm/operator.sock"]);
    let report: Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!((code, stdout.as_str(), stderr.matches('\n').count()), (Some(1), "", 1));
    assert_eq!((report["version"].as_i64(), report["command"].as_str(), report["state"].as_str(), report["issue"]["code"].as_str(), report["partial"].as_bool()), (Some(1), Some("vm"), Some("failed"), Some("vm_operation_failed"), Some(true)));
    let (code, stdout, stderr) = run(&["mcp", "token", "create", "--operator-socket", "/nonexistent/agent-vmm/operator.sock"]);
    assert!(code == Some(1) && stdout.is_empty() && stderr.starts_with("rpc error: code = Unavailable"), "{stderr}");
}

#[test]
fn the_lifecycle_helper_is_this_program_on_linux_only() {
    let (code, stdout, stderr) = run(&["lifecycle", "version", "--json"]);
    if cfg!(target_os = "macos") {
        // The macOS bundle has a separate helper next to this program.
        assert!(code == Some(2) && stdout.is_empty() && stderr.starts_with("usage: agent-vmm "), "{stderr}");
        return;
    }
    let version: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!((code, version["contractVersion"].as_u64(), version["component"].as_str(), version["release_id"].is_string()), (Some(0), Some(1), Some("agent-vmm-host"), true), "{stderr}");
    // The installation script of Comma reads this error to tell an old Host from a failed command.
    let (code, stdout, stderr) = run(&["lifecycle", "comma-contract-unknown-command"]);
    assert_eq!(serde_json::from_str::<Value>(&stdout).unwrap(), json!({"error": {"code": "upgrade_required", "reason": "unknown_command"}}));
    assert!(code != Some(0) && stderr.contains("unknown command"), "{stderr}");
}
