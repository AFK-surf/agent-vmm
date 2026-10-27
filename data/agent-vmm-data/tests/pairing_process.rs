#![cfg(unix)]

use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Worker {
    child: Child,
    socket: PathBuf,
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_worker(root: &Path, name: &str) -> Worker {
    let socket = root.join(format!("{name}-control.sock"));
    let service = root.join(format!("{name}-service.sock"));
    let mesh = root.join(format!("{name}-mesh.sock"));
    let child = Command::new(env!("CARGO_BIN_EXE_agent-vmm-data"))
        .args([
            "--socket",
            socket.to_str().unwrap(),
            "--service-socket",
            service.to_str().unwrap(),
            "--mesh-control-socket",
            mesh.to_str().unwrap(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !socket.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    assert!(socket.exists(), "worker control socket did not appear");
    Worker { child, socket }
}

fn request(socket: &Path, value: Value) -> Value {
    let mut connection = UnixStream::connect(socket).unwrap();
    connection
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    serde_json::to_writer(&mut connection, &value).unwrap();
    connection.write_all(b"\n").unwrap();
    let mut line = String::new();
    BufReader::new(connection).read_line(&mut line).unwrap();
    assert!(!line.is_empty(), "worker returned no response for {value}");
    serde_json::from_str(&line).unwrap()
}

fn bytes(value: &[u8]) -> Value {
    Value::Array(value.iter().map(|byte| Value::from(*byte)).collect())
}

fn context(joiner_bound: bool) -> Value {
    json!({
        "session_id": bytes(&[9; 16]),
        "mesh_id": bytes(b"mesh-1"),
        "invite_revision": 7,
        "manager_identity": bytes(b"manager-device"),
        "joiner_identity": bytes(if joiner_bound { b"joiner-device" } else { b"" }),
        "manager_root_key": bytes(&[2; 33]),
        "joiner_root_key": bytes(if joiner_bound { &[3; 33] } else { &[] }),
        "roles_and_features": bytes(b"use-services")
    })
}

#[test]
fn worker_exits_when_host_lifeline_closes() {
    let root = tempfile::tempdir().unwrap();
    let mut worker = start_worker(root.path(), "lifeline");
    drop(worker.child.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker.child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "worker outlived its Host lifeline"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert!(!worker.socket.exists(), "worker left its control socket");
}

#[test]
fn two_worker_processes_complete_real_pairing_alpn_exchange() {
    let root = tempfile::tempdir().unwrap();
    let manager = start_worker(root.path(), "manager");
    let joiner = start_worker(root.path(), "joiner");
    let manager_status = request(&manager.socket, json!({"command": "status"}));

    let prepared = request(
        &manager.socket,
        json!({
            "command": "prepare_pairing_manager",
            "context": context(false),
            "secret_kind": "pin",
            "secret": bytes(b"12345678"),
            "acceptance": bytes(b"manager-signed-acceptance")
        }),
    );
    assert_eq!(prepared["state"], "invited");
    assert_eq!(prepared["attempts_remaining"], 5);

    let joined = request(
        &joiner.socket,
        json!({
            "command": "join_pairing",
            "endpoint_addr": manager_status["endpoint_addr"].clone(),
            "context": context(true),
            "secret_kind": "pin",
            "secret": bytes(b"12345678"),
            "proposal": bytes(b"joiner-signed-identity-permission-proposal")
        }),
    );
    assert_eq!(joined["state"], "awaiting_confirm");
    assert_eq!(joined["peer_payload"], bytes(b"manager-signed-acceptance"));
    assert_eq!(joined["sas"].as_str().unwrap().len(), 6);

    let manager_pairing = request(
        &manager.socket,
        json!({"command": "pairing_status", "session_id": bytes(&[9; 16])}),
    );
    assert_eq!(manager_pairing["state"], "awaiting_confirm");
    assert_eq!(
        manager_pairing["peer_payload"],
        bytes(b"joiner-signed-identity-permission-proposal")
    );
    assert_eq!(manager_pairing["sas"], joined["sas"]);

    let manager_confirmed = request(
        &manager.socket,
        json!({"command": "confirm_pairing", "session_id": bytes(&[9; 16])}),
    );
    assert_eq!(manager_confirmed["state"], "awaiting_confirm");
    request(
        &joiner.socket,
        json!({"command":"confirm_pairing", "session_id":bytes(&[9;16])}),
    );
    for worker in [&manager, &joiner] {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = request(
                &worker.socket,
                json!({"command":"pairing_status", "session_id":bytes(&[9;16])}),
            );
            if status["state"] == "ready_to_commit" {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pairing did not receive peer confirmation: {status}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    let committed = request(
        &manager.socket,
        json!({"command":"commit_pairing", "session_id":bytes(&[9;16]), "registry_cas_succeeded":true}),
    );
    assert_eq!(committed["state"], "committed");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status = request(
            &joiner.socket,
            json!({"command":"pairing_status", "session_id":bytes(&[9;16])}),
        );
        if status["state"] == "committed" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "joiner did not receive manager commit: {status}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}
