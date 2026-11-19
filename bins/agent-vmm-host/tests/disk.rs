use agent_vmm_host::{control::serve_local, host::HostState};
use agent_vmm_proto::host::v1::{
    GetLocalDiskSummaryRequest, GetVmRequest, operator_service_client::OperatorServiceClient,
};
use std::{
    fs,
    sync::{Arc, Mutex},
    time::Duration,
};
use vmm_dom0::{
    backend::{LocalDom0Backend, Options},
    boot_server::BootServer,
    extent::{BLOCK_BYTES, BLOCK_SECTORS, Control, FilesystemHeadroom, Pool},
    keys::MasterKey,
    network::MockFirewallDriver,
};
use vmm_l4hctl::{DiskExtent, DiskInfo, MockHypervisorCtl};

#[tokio::test]
async fn operator_socket_reports_pool_and_stage_changes_and_sampling_failure() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let root = dir.path();
    let sectors = 4 * BLOCK_SECTORS;
    let ctl: Control = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 1,
        owned: false,
        failed: false,
        sectors,
    }])));
    let pool = Arc::new(
        Pool::open(
            &root.join("pool.db"),
            ctl.clone(),
            1,
            DiskExtent { sector: 0, sectors },
            Arc::new(FilesystemHeadroom(root.into())),
            0,
        )
        .unwrap(),
    );
    let staging = root.join("archives");
    let backend = Arc::new(
        LocalDom0Backend::open(
            pool.clone(),
            ctl,
            Options::new(vec![].into(), staging.clone()),
            Arc::new(MasterKey([1; 32].into())),
            Arc::new(MockFirewallDriver::default()),
            BootServer::default(),
        )
        .unwrap(),
    );
    let host = HostState::open(&root.join("host.db")).unwrap();
    let (core, _) = host.attach(backend.clone()).await.unwrap();
    let api = host.local_operator(root, Some(backend)).unwrap();
    let socket = root.join("operator.sock");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = serve_local(&socket, api, Some(core), async {
        let _ = stopped.await;
    });
    let client = async {
        let endpoint =
            tonic::transport::Endpoint::from_shared(format!("unix:{}", socket.display())).unwrap();
        let channel = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(channel) = endpoint.connect().await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let mut client = OperatorServiceClient::new(channel);
        let boot = client
            .get_vm(GetVmRequest {})
            .await
            .unwrap()
            .into_inner()
            .vm
            .unwrap()
            .boot_id;
        let token = staging.join("11111111111111111111111111111111");
        assert_sample(&mut client, &boot, 0, 0).await;
        pool.allocate("data", 1).unwrap();
        fs::write(&token, [0; 19]).unwrap();
        assert_sample(&mut client, &boot, BLOCK_BYTES, 19).await;
        pool.discard("data").unwrap();
        fs::remove_file(token).unwrap();
        assert_sample(&mut client, &boot, 0, 0).await;
        fs::rename(&staging, root.join("moved")).unwrap();
        let summary = client
            .get_local_disk_summary(GetLocalDiskSummaryRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(boot, summary.boot_id);
        assert!(!summary.unavailable_reason.is_empty());
        assert!(summary.guest_state_fs.is_none());
        drop(client);
        stop.send(()).unwrap();
    };
    let (served, ()) = tokio::join!(server, client);
    served.unwrap();
}

async fn assert_sample(
    client: &mut OperatorServiceClient<tonic::transport::Channel>,
    boot: &str,
    used: u64,
    staged: u64,
) {
    let summary = client
        .get_local_disk_summary(GetLocalDiskSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(boot, summary.boot_id);
    assert!(summary.unavailable_reason.is_empty());
    let counters = summary.guest_state_fs.unwrap();
    assert_eq!(
        (Some(used), Some(4 * BLOCK_BYTES), Some(staged)),
        (
            counters.used_bytes,
            counters.capacity_bytes,
            counters.import_reservation_bytes
        )
    );
}
