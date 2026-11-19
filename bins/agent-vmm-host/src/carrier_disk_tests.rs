use super::{HostState, run_carrier};
use crate::carrier::CarrierImageStaging;
use agent_vmm_proto::host::v1::{self as h, operator_service_client::OperatorServiceClient};
use serde_json::json;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Mutex, oneshot},
};
use vmm_carrier_macos::{
    client::CarrierClient,
    mock::MockCarrierHypervisor,
    protocol::{CONTROL_PORT, Request},
    server::serve,
};
use vmm_compat::image::ImageStaging;

// Dom0 returns the formatted extent size, which can differ from the uploaded archive size.
async fn stage_reply(stream: &mut UnixStream, token: &str, physical_bytes: u64) {
    let size = stream.read_u32().await.unwrap() as usize;
    assert!(size < 1024);
    let mut frame = vec![0; size];
    stream.read_exact(&mut frame).await.unwrap();
    let digest = [7u8; 32];
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&frame).unwrap(),
        json!([1, {"StageImage":[digest, 3]}])
    );
    let admitted = br#"{"Ok":null}"#;
    stream.write_u32(admitted.len() as u32).await.unwrap();
    stream.write_all(admitted).await.unwrap();
    let mut data = [0; 3];
    stream.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"oci");
    let result = serde_json::to_vec(&json!({"Ok":[token, physical_bytes]})).unwrap();
    stream.write_u32(result.len() as u32).await.unwrap();
    stream.write_all(&result).await.unwrap();
}

#[tokio::test]
async fn carrier_disk_summary_follows_offline_start_and_active_image_leases() {
    let dir = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let root = dir.path();
    let host = HostState::open(&root.join("host.db")).unwrap();
    let socket = root.join("operator.sock");
    let carrier_socket = root.join("carrier");
    let client = CarrierClient::new(&carrier_socket);
    let staging = Arc::new(CarrierImageStaging::new(client.clone()));
    let (stop, stopped) = oneshot::channel();
    let host = run_carrier(host, root, &socket, 0, staging.clone(), async {
        stopped.await.unwrap();
    });
    let check = async {
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
        let mut operator = OperatorServiceClient::new(channel);
        let page = operator
            .list_local_environment_page(h::ListLocalEnvironmentPageRequest::default())
            .await
            .unwrap()
            .into_inner();
        let health = operator
            .get_system_health_summary(h::GetSystemHealthSummaryRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.health.unwrap().state(), h::HealthState::Unavailable);
        assert_eq!(health.vm.unwrap().error, "carrier status unavailable");
        let disk = operator
            .get_local_disk_summary(h::GetLocalDiskSummaryRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.boot_id, disk.boot_id);
        assert!(disk.guest_state_fs.is_none());
        assert_eq!(disk.unavailable_reason, "guest state disk is unavailable");

        let dom0 = root.join("dom0");
        let control = UnixListener::bind(&dom0).unwrap();
        let machine = MockCarrierHypervisor::new(BTreeMap::from([(CONTROL_PORT, dom0)]));
        let (stop_carrier, stopped_carrier) = oneshot::channel();
        let carrier = tokio::spawn(serve(
            UnixListener::bind(carrier_socket).unwrap(),
            Arc::new(Mutex::new(machine)),
            async {
                stopped_carrier.await.unwrap();
            },
        ));
        let status = client
            .control(Request::EnsureStarted {
                operation_id: "start".into(),
            })
            .await
            .unwrap();
        let archive = root.join("upload");
        std::fs::write(&archive, b"oci").unwrap();
        let mut peers = Vec::new();
        for (token, bytes) in [("first", 4096), ("second", 8192)] {
            let stage = staging.clone();
            let file = archive.clone();
            let upload = tokio::task::spawn_blocking(move || stage.stage_image(&file, [7; 32]));
            let (mut peer, _) = control.accept().await.unwrap();
            stage_reply(&mut peer, token, bytes).await;
            assert_eq!(upload.await.unwrap().unwrap().staged_extent, token);
            peers.push(peer);
        }
        for (released, expected) in [(None, 12288), (Some("first"), 8192), (Some("second"), 0)] {
            if let Some(token) = released {
                staging.discard_staged_image(token).unwrap();
            }
            let disk = operator
                .get_local_disk_summary(h::GetLocalDiskSummaryRequest {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(disk.vm_state(), h::VmState::Running);
            assert!(disk.unavailable_reason.is_empty());
            let fs = disk.guest_state_fs.unwrap();
            assert_eq!(fs.used_bytes, Some(status.pool_allocated_bytes));
            assert_eq!(fs.capacity_bytes, Some(status.pool_logical_bytes));
            assert_eq!(fs.import_reservation_bytes, Some(expected));
        }
        drop(operator);
        stop_carrier.send(()).unwrap();
        carrier.await.unwrap().unwrap();
        stop.send(()).unwrap();
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(host, check) })
            .await
            .unwrap();
    result.unwrap();
}
