use std::{fs, process::Command};

#[cfg(target_os = "linux")]
#[test]
fn invalid_dom0_config_fails_before_binding_the_operator_socket() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let config = dir.path().join("backend.json");
    let key = dir.path().join("test.key");
    fs::write(&key, [0; 31]).unwrap();
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(
        &config,
        serde_json::to_vec(&serde_json::json!({
            "state_dir": dir.path().join("dom0"),
            "backing_filesystem": dir.path(), "disk": 1, "first_sector": 0, "sectors": 8,
            "master_key": key, "initrd": "initrd", "interface": "guest0", "switch": 1,
            "helper_root": "helper", "helper_digest": "00".repeat(32),
        }))
        .unwrap(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"))
        .arg("--database")
        .arg(dir.path().join("host.sqlite"))
        .arg("--dom0-config")
        .arg(config)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(dir.path().join("host.sqlite").exists());
    assert!(dir.path().join("dom0").is_dir());
    assert!(!dir.path().join("dom0/pool.db").exists());
    assert!(!dir.path().join("operator.sock").exists());
    assert!(!dir.path().join("host-start-failure.json").exists());
}

#[tokio::test]
async fn offline_host_accepts_connections_and_releases_the_socket_on_sigterm() {
    use rustix::process::{Signal, kill_process};
    use std::{
        os::unix::net::UnixStream,
        process::Child,
        time::{Duration, Instant},
    };

    struct Host(Child);
    impl Drop for Host {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let database = dir.path().join("state/host.sqlite");
    let socket = dir.path().join("state/operator.sock");
    use agent_vmm_proto::{appliance::v1 as a, host::v1 as h};
    let mut receipt: Option<String> = None;
    for _ in 0..2 {
        let mut host = Host(
            Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"))
                .arg("--database")
                .arg(&database)
                .arg("--carrier-socket")
                .arg(dir.path().join("Application Support/AgentVMM/carrier.sock"))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while UnixStream::connect(&socket).is_err() {
            assert!(
                host.0.try_wait().unwrap().is_none(),
                "host exited at startup"
            );
            assert!(Instant::now() < deadline, "operator socket unavailable");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let channel = vmm_rpc::Client::unix(&socket);
        let mut operator = h::operator_service_client::OperatorServiceClient::new(channel.clone());
        let mut appliances =
            a::appliance_admin_service_client::ApplianceAdminServiceClient::new(channel);
        let verify = vmm_mcp::TokenAuthority::new(std::sync::Arc::new(
            vmm_mcp::FileSigningKeys::open(database.parent().unwrap()).unwrap(),
        ));
        let token = operator
            .create_mcp_token(h::CreateMcpTokenRequest {})
            .await
            .unwrap()
            .into_inner()
            .token;
        assert!(verify.verify(&token).unwrap().is_some());
        if receipt.is_none() {
            receipt = Some(
                operator
                    .rotate_mcp_token_key(h::RotateMcpTokenKeyRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .operation_id,
            );
            assert!(verify.verify(&token).unwrap().is_none());
        }
        let mut watched = appliances
            .watch_appliance_operation(a::WatchApplianceOperationRequest {
                installation_id: "agent-vmm-mcp".into(),
                operation_id: receipt.clone().unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        let operation = watched.message().await.unwrap().unwrap().operation.unwrap();
        assert_eq!(operation.state(), a::ApplianceOperationState::Succeeded);
        assert_eq!(operation.phase(), a::ApplianceOperationPhase::Terminal);
        assert_eq!(operation.kind(), a::ApplianceOperationKind::Update);
        assert_eq!(
            operation.terminal_reason(),
            a::ApplianceTerminalReason::Succeeded
        );
        assert_eq!(Some(&operation.operation_id), receipt.as_ref());
        assert!(watched.message().await.unwrap().is_none());
        drop(watched);
        // Keep the channels open while the daemon drains.
        kill_process(rustix::process::Pid::from_child(&host.0), Signal::TERM).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = host.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(Instant::now() < deadline, "host failed to stop");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(!socket.exists());
    }
}

#[test]
fn launchd_database_argument_reports_foreign_state_without_acquiring_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("host.sqlite");
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE preserved(value); INSERT INTO preserved VALUES('original')")
        .unwrap();
    let before = fs::read(&path).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"));
    #[cfg(target_os = "linux")]
    command
        .arg("--dom0-config")
        .arg(dir.path().join("missing.json"));
    let status = command
        .arg("--database")
        .arg(&path)
        .arg("--operator-socket")
        .arg(dir.path().join("operator.sock"))
        .status()
        .unwrap();
    assert!(!status.success());
    assert_eq!(before, fs::read(path).unwrap());
    assert_eq!(
        fs::read_to_string(dir.path().join("host-start-failure.json")).unwrap(),
        r#"{"code":"STATE_SCHEMA_INCOMPATIBLE","dataSchema":0,"currentSchema":2}"#
    );
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[test]
fn online_wal_is_observed_before_rejecting_startup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("host.sqlite");
    let owner = rusqlite::Connection::open(&path).unwrap();
    owner.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE present_only_in_wal(v); INSERT INTO present_only_in_wal VALUES(42);").unwrap();
    let main = fs::read(&path).unwrap();
    let wal = fs::read(dir.path().join("host.sqlite-wal")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"))
        .arg("--database")
        .arg(&path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(path).unwrap(), main);
    assert_eq!(fs::read(dir.path().join("host.sqlite-wal")).unwrap(), wal);
    assert!(!dir.path().join("host.sqlite.owner").exists());
    assert_eq!(
        owner
            .query_row("SELECT v FROM present_only_in_wal", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        42
    );
}
