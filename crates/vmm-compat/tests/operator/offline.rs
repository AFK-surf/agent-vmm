use super::{disposal::Connectors, *};
use h::operator_service_client::OperatorServiceClient;
use vmm_rpc::Status;
use vmm_compat::operator::{LocalOperator, LocalOperatorServer, VmControl};

struct OfflineCarrier;
#[async_trait::async_trait]
impl VmControl for OfflineCarrier {
    async fn observe(&self) -> Result<h::Vm, Status> {
        Err(Status::unavailable("carrier status unavailable"))
    }
    async fn change(&self, _: bool, _: bool, _: &str) -> Result<(), Status> {
        panic!("read-only request changed the VM")
    }
}

#[tokio::test]
async fn carrier_observation_failure_preserves_vm_fences_and_local_inventory() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host")).unwrap());
    store
        .create_scope(&scope("owned", Origin::Local), None)
        .unwrap();
    let sampled = Arc::new(std::sync::atomic::AtomicBool::default());
    let monitor = sampled.clone();
    let mut api = LocalOperator::open(
        &dir.path().join("compat"),
        store,
        Arc::new(Connectors::default()),
        Some(Arc::new(vmm_core::fake::FakeDom0Backend::default())),
    )
    .unwrap()
    .with_disk_sample(move || {
        monitor.store(true, std::sync::atomic::Ordering::Relaxed);
        Err(vmm_core::Error::Interrupted)
    });
    api.vm_control = Some(Arc::new(OfflineCarrier));
    let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
    let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
    let server = tokio::spawn(
        vmm_rpc::Router::new(LocalOperatorServer(Arc::new(api)))
            .serve_unix(listener, 128, std::future::pending()),
    );
    let mut client = OperatorServiceClient::new(
        vmm_rpc::Client::unix(&endpoint),
    );
    for error in [
        client.get_vm(h::GetVmRequest {}).await.unwrap_err(),
        client
            .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.code(), Code::Unavailable);
        assert_eq!(error.message(), "carrier status unavailable");
    }
    let health = client
        .get_system_health_summary(h::GetSystemHealthSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    let vm = health.vm.unwrap();
    assert_eq!(vm.state(), h::VmState::Stopped);
    assert_eq!(vm.error, "carrier status unavailable");
    let health = health.health.unwrap();
    assert_eq!(health.state(), h::HealthState::Unavailable);
    assert_eq!(health.components[0].reason, vm.error);
    let page = client
        .list_local_environment_page(h::ListLocalEnvironmentPageRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.environments[0].id, "owned");
    assert_eq!(page.environments[0].state, "vm_stopped");
    let disk = client
        .get_local_disk_summary(h::GetLocalDiskSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disk.boot_id, page.boot_id);
    assert_eq!(disk.vm_state(), h::VmState::Stopped);
    assert!(disk.guest_state_fs.is_none());
    assert_eq!(disk.unavailable_reason, "guest state disk is unavailable");
    assert!(!sampled.load(std::sync::atomic::Ordering::Relaxed));
    let env = client
        .get_environment(h::OperatorServiceGetEnvironmentRequest {
            environment_id: "owned".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .environment
        .unwrap();
    assert_eq!(env.id, "owned");
    assert_eq!(env.inventory_state(), h::InventoryState::Unavailable);
    assert_eq!(
        env.inventory_message,
        "Runtime inventory is temporarily unavailable."
    );
    assert!(env.usage.unwrap().stale);
    server.abort();
}
