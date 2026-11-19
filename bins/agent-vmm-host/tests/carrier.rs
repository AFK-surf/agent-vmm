#![cfg(target_os = "macos")]
use agent_vmm_proto::host::v1::{self as h, operator_service_client::OperatorServiceClient};
use std::{
    collections::BTreeMap,
    os::unix::fs::PermissionsExt,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};
use tokio::{net::UnixListener, sync::Mutex};
use vmm_carrier_macos::{mock::MockCarrierHypervisor, protocol::CONTROL_PORT, server::serve};
use vmm_core::fake::FakeDom0Backend;

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn host_process_drives_carrier_and_adopts_its_running_boot() {
    let d = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let path = |name: &str| d.path().join(name);
    let control = UnixListener::bind(path("dom0")).unwrap();
    let daemon = tokio::spawn(async move {
        let backend = FakeDom0Backend::default();
        loop {
            let (sock, _) = control.accept().await.unwrap();
            vmm_dom0::remote::serve_connection(sock, &backend)
                .await
                .unwrap();
        }
    });
    let machine = MockCarrierHypervisor::new(BTreeMap::from([(CONTROL_PORT, path("dom0"))]));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let carrier = tokio::spawn(serve(
        UnixListener::bind(path("carrier")).unwrap(),
        Arc::new(Mutex::new(machine)),
        async {
            let _ = stopped.await;
        },
    ));
    let mut boot = String::new();
    let database = path("state/host.db");
    for pass in 0..2 {
        let mut proc = Process(
            Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"))
                .arg("--database")
                .arg(&database)
                .args(["--mcp-port", "0"])
                .arg("--carrier-socket")
                .arg(path("carrier"))
                .spawn()
                .unwrap(),
        );
        let ep = tonic::transport::Endpoint::from_shared(format!(
            "unix:{}",
            path("state/operator.sock").display()
        ))
        .unwrap()
        .timeout(Duration::from_secs(5));
        let channel = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Ok(c) = ep.connect().await {
                    break c;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        use agent_vmm_proto::appliance::v1::{
            self as a, appliance_admin_service_client::ApplianceAdminServiceClient,
        };
        let mut admin = ApplianceAdminServiceClient::new(channel.clone());
        let listed = admin
            .list_appliances(a::ListAppliancesRequest {})
            .await
            .unwrap()
            .into_inner();
        let builtin = admin
            .get_appliance_status(a::GetApplianceStatusRequest {
                installation_id: "agent-vmm-mcp".into(),
            })
            .await
            .unwrap()
            .into_inner()
            .status
            .unwrap();
        assert!(listed.appliances.contains(&builtin));
        let frontend = &builtin.frontends[0];
        let addr: std::net::SocketAddr = frontend.endpoint.parse().unwrap();
        assert!(frontend.available && addr.ip().is_loopback() && addr.port() != 0);
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut http = tokio::net::TcpStream::connect(addr).await.unwrap();
        http.write_all(b"GET /mcp HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        http.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 401"));
        drop(admin);
        let mut client = OperatorServiceClient::new(channel);
        let vm = client
            .get_vm(h::GetVmRequest {})
            .await
            .unwrap()
            .into_inner()
            .vm
            .unwrap();
        if pass == 0 {
            assert_eq!(vm.state(), h::VmState::Stopped);
            client
                .start_vm(h::StartVmRequest {
                    request_id: "start".into(),
                    fence: Some(h::VmMutationFence {
                        state: vm.state,
                        boot_id: vm.boot_id,
                        whole_vm_impact_acknowledged: true,
                        ..Default::default()
                    }),
                })
                .await
                .unwrap();
            let vm = client
                .get_vm(h::GetVmRequest {})
                .await
                .unwrap()
                .into_inner()
                .vm
                .unwrap();
            assert_eq!(vm.state(), h::VmState::Running);
            boot = vm.boot_id;
            assert!(!boot.is_empty());
        } else {
            assert_eq!(vm.boot_id, boot);
            let receipt = client
                .stop_vm(h::StopVmRequest {
                    request_id: "stop".into(),
                    force: false,
                    fence: Some(h::VmMutationFence {
                        state: vm.state,
                        boot_id: vm.boot_id,
                        whole_vm_impact_acknowledged: true,
                        ..Default::default()
                    }),
                })
                .await
                .unwrap()
                .into_inner()
                .operation
                .unwrap();
            assert_eq!(receipt.state(), h::OperationState::Succeeded);
            assert_eq!(
                client
                    .get_vm(h::GetVmRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .vm
                    .unwrap()
                    .state(),
                h::VmState::Stopped
            );
        }
        drop(client);
        rustix::process::kill_process(
            rustix::process::Pid::from_child(&proc.0),
            rustix::process::Signal::TERM,
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                if let Some(status) = proc.0.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    stop.send(()).unwrap();
    carrier.await.unwrap().unwrap();
    daemon.abort();
}

#[path = "carrier/observation.rs"]
mod observation;
