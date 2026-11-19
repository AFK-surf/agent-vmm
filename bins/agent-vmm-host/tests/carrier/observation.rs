use super::Process;
use agent_vmm_host::host::HostState;
use agent_vmm_proto::{host::v1 as h, remote::v1 as r};
use r::remote_controller_service_server::{RemoteControllerService, RemoteControllerServiceServer};
use std::{
    collections::BTreeMap, os::unix::fs::PermissionsExt, process::Command, sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{Mutex, mpsc},
};
use tokio_stream::wrappers::ReceiverStream;
use vmm_carrier_macos::{
    mock::MockCarrierHypervisor,
    protocol::CONTROL_PORT,
    server::{CarrierHypervisor, serve},
};
use vmm_core::fake::FakeDom0Backend;
use vmm_rpc::{Request, Response, Router, ServerTls, Status, Streaming};

struct Controller(mpsc::Sender<r::RemoteObservationSnapshot>);

#[vmm_rpc::async_trait]
impl RemoteControllerService for Controller {
    type ConnectStream = ReceiverStream<Result<r::ConnectResponse, Status>>;

    async fn connect(
        &self,
        request: Request<Streaming<r::ConnectRequest>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let mut frames = request.into_inner();
        let Some(r::connect_request::Payload::Hello(hello)) =
            frames.message().await?.and_then(|frame| frame.payload)
        else {
            return Err(Status::invalid_argument("expected HostHello"));
        };
        let ready = r::ControllerReady {
            registration_id: hello.registration_id,
            connection_epoch: hello.connection_epoch,
            acknowledged_inventory_watermark: hello.inventory_watermark,
            enabled_features: vec!["remote-observation-v1".into()],
        };
        self.0
            .send(hello.observation.expect("HostHello observation"))
            .await
            .unwrap();
        let (commands, responses) = mpsc::channel(1);
        commands
            .send(Ok(r::ConnectResponse {
                payload: Some(r::connect_response::Payload::ControllerReady(ready)),
            }))
            .await
            .unwrap();
        let observations = self.0.clone();
        tokio::spawn(async move {
            while let Ok(Some(frame)) = frames.message().await {
                if let Some(r::connect_request::Payload::ObservationRenewal(renewal)) =
                    frame.payload
                    && observations
                        .send(renewal.observation.expect("renewal observation"))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            drop(commands);
        });
        Ok(Response::new(ReceiverStream::new(responses)))
    }
}

#[tokio::test]
async fn enrolled_host_reports_carrier_health_in_hello_and_after_stop() {
    let dir = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let root = dir.path();
    let database = root.join("host.db");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let pem = cert.cert.pem();
    let tls = ServerTls::new(
        vec![cert.cert.der().clone()],
        cert.signing_key.serialize_der().try_into().unwrap(),
    )
    .unwrap();
    let (observations, mut report) = mpsc::channel(4);
    let gateway = tokio::spawn(async move {
        let router = Router::new(RemoteControllerServiceServer::new(Controller(observations)));
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tls.accept(socket, router.clone()).await.unwrap();
        }
    });

    // Restore an existing registration so the connector can send Hello immediately at startup.
    let host = HostState::open(&database).unwrap();
    host.local_operator(root, None)
        .unwrap()
        .management
        .unwrap()
        .apply(h::ApplyRemoteRegistrationRequest {
            registration: Some(h::RemoteRegistrationConnection {
                request_id: "source:registration:managed-enrollment".into(),
                endpoint: format!("localhost:{port}"),
                display_name: "work".into(),
                registration_id: "one".into(),
                source_policy_revision: 1,
                trust_bundle: pem.into_bytes(),
                pool: Some(h::RemotePoolPolicy {
                    per_environment_limits: Some(h::ResourceLimits {
                        pids: Some(80),
                        disk_bytes: Some(1 << 20),
                    }),
                    max_egress_mode: h::EgressMode::DenyAll.into(),
                    stop_grace: Some(Duration::from_secs(15).try_into().unwrap()),
                }),
            }),
            credential: b"test-registration-credential".to_vec(),
        })
        .unwrap();
    drop(host);
    let dom0 = root.join("dom0");
    let control = UnixListener::bind(&dom0).unwrap();
    let daemon = tokio::spawn(async move {
        let backend = FakeDom0Backend::default();
        loop {
            let (stream, _) = control.accept().await.unwrap();
            vmm_dom0::remote::serve_connection(stream, &backend)
                .await
                .unwrap();
        }
    });
    let mut machine = MockCarrierHypervisor::new(BTreeMap::from([(CONTROL_PORT, dom0)]));
    machine.ensure_started("start".into()).await.unwrap();
    let disk_bytes = machine.status().await.unwrap().pool_allocated_bytes;
    let machine = Arc::new(Mutex::new(machine));
    let socket = root.join("carrier");
    let carrier = tokio::spawn(serve(
        UnixListener::bind(&socket).unwrap(),
        machine.clone(),
        std::future::pending(),
    ));
    let _host = Process(
        Command::new(env!("CARGO_BIN_EXE_agent-vmm-host"))
            .arg("--database")
            .arg(database)
            .args(["--mcp-port", "0"])
            .arg("--carrier-socket")
            .arg(socket)
            .spawn()
            .unwrap(),
    );
    for (sequence, status, issue, message) in [
        (1, "healthy", "", ""),
        (
            2,
            "unavailable",
            "host_unavailable",
            "Host health is unavailable.",
        ),
    ] {
        let observation = tokio::time::timeout(Duration::from_secs(25), report.recv())
            .await
            .expect("controller observation deadline")
            .unwrap();
        assert_eq!(observation.sequence, sequence);
        let health = observation.health.unwrap();
        assert_eq!(
            (
                health.status.as_str(),
                health.issue.as_str(),
                health.message.as_str()
            ),
            (status, issue, message)
        );
        let components: Vec<_> = health
            .components
            .iter()
            .map(|c| (c.component.as_str(), c.status.as_str()))
            .collect();
        assert_eq!(components, [("vmmd", status), ("state-disk", status)]);
        let usage = observation.usage.unwrap();
        let age = vmm_remote_salix::unix_millis() - usage.sampled_unix_millis;
        assert!(
            (-30_000..=120_000).contains(&age),
            "usage sample age {age}ms"
        );
        if sequence == 1 {
            assert!(!usage.stale);
            assert!(usage.issue.is_empty());
            assert_eq!(usage.disk_bytes, disk_bytes);
            machine.lock().await.request_stop().unwrap();
        } else {
            assert!(usage.stale);
            assert_eq!(usage.issue, "usage_unavailable");
        }
    }
    carrier.abort();
    daemon.abort();
    gateway.abort();
}
