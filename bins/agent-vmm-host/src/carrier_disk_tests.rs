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

// Dom0 receives pre-built EROFS over StageErofs and allocates it directly in the bare pool.
async fn stage_erofs_reply(
    stream: &mut UnixStream,
    expected_digest: [u8; 32],
    token: &str,
) -> u64 {
    let size = stream.read_u32().await.unwrap() as usize;
    assert!(size < 8192);
    let mut frame = vec![0; size];
    stream.read_exact(&mut frame).await.unwrap();
    let req: serde_json::Value = serde_json::from_slice(&frame).unwrap();
    assert_eq!(req[0], json!(1));
    let stage = &req[1]["StageErofs"];
    assert_eq!(stage["manifest"], json!(expected_digest));
    let physical_bytes = stage["physical_bytes"].as_u64().unwrap();
    assert!(physical_bytes > 0 && physical_bytes.is_multiple_of(4 << 20));
    let admitted = br#"{"Ok":"Upload"}"#;
    stream.write_u32(admitted.len() as u32).await.unwrap();
    stream.write_all(admitted).await.unwrap();
    let mut remaining = physical_bytes as usize;
    let mut buf = vec![0u8; 64 << 10];
    while remaining > 0 {
        let n = remaining.min(buf.len());
        stream.read_exact(&mut buf[..n]).await.unwrap();
        remaining -= n;
    }
    let result = serde_json::to_vec(&json!({"Ok":[token, physical_bytes]})).unwrap();
    stream.write_u32(result.len() as u32).await.unwrap();
    stream.write_all(&result).await.unwrap();
    physical_bytes
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    use std::io::Write;
    let mut f = tempfile::tempfile().unwrap();
    f.write_all(data).unwrap();
    vmm_dom0::image::file_sha256(&mut f).unwrap()
}

fn append_tar_file(out: &mut Vec<u8>, name: &str, data: &[u8]) {
    let mut hdr = [0u8; 512];
    hdr[..name.len()].copy_from_slice(name.as_bytes());
    hdr[100..108].copy_from_slice(b"0000644\0");
    hdr[108..116].copy_from_slice(b"0000000\0");
    hdr[116..124].copy_from_slice(b"0000000\0");
    let size = format!("{:011o}\0", data.len());
    hdr[124..136].copy_from_slice(size.as_bytes());
    hdr[136..148].copy_from_slice(b"00000000000\0");
    hdr[148..156].copy_from_slice(b"        ");
    hdr[156] = b'0';
    hdr[257..263].copy_from_slice(b"ustar\0");
    hdr[263..265].copy_from_slice(b"00");
    let sum: u32 = hdr.iter().map(|&b| u32::from(b)).sum();
    let chk = format!("{sum:06o}\0 ");
    hdr[148..156].copy_from_slice(chk.as_bytes());
    out.extend_from_slice(&hdr);
    out.extend_from_slice(data);
    let pad = (512 - (data.len() % 512)) % 512;
    out.resize(out.len() + pad, 0);
}

fn minimal_oci_archive() -> (Vec<u8>, [u8; 32]) {
    let mut layer = Vec::new();
    append_tar_file(&mut layer, "hi", b"ok");
    layer.resize(layer.len() + 1024, 0);
    let layer_hash = hex_lower(&sha256_bytes(&layer));
    let config = serde_json::to_vec(&json!({
        "rootfs": { "diff_ids": [format!("sha256:{layer_hash}")] }
    }))
    .unwrap();
    let config_hash = hex_lower(&sha256_bytes(&config));
    let manifest = serde_json::to_vec(&json!({
        "config": { "digest": format!("sha256:{config_hash}"), "size": config.len() },
        "layers": [{ "digest": format!("sha256:{layer_hash}"), "size": layer.len() }]
    }))
    .unwrap();
    let digest = sha256_bytes(&manifest);
    let manifest_hash = hex_lower(&digest);
    let index = serde_json::to_vec(&json!({
        "manifests": [{ "digest": format!("sha256:{manifest_hash}"), "size": manifest.len() }]
    }))
    .unwrap();
    let mut outer = Vec::new();
    for (name, data) in [
        ("oci-layout".to_string(), br#"{"imageLayoutVersion":"1.0.0"}"#.as_slice()),
        ("index.json".to_string(), index.as_slice()),
        (format!("blobs/sha256/{manifest_hash}"), manifest.as_slice()),
        (format!("blobs/sha256/{config_hash}"), config.as_slice()),
        (format!("blobs/sha256/{layer_hash}"), layer.as_slice()),
    ] {
        append_tar_file(&mut outer, &name, data);
    }
    outer.resize(outer.len() + 1024, 0);
    (outer, digest)
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
    let staging = Arc::new(CarrierImageStaging::new(
        client.clone(),
        host.staging_dir().to_path_buf(),
    ));
    let (stop, stopped) = oneshot::channel();
    let host = run_carrier(host, root, &socket, 0, staging.clone(), async {
        stopped.await.unwrap();
    });
    let check = async {
        let rpc_socket = socket.clone();
        let channel = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::net::UnixStream::connect(&rpc_socket).await.is_ok() {
                    break vmm_rpc::Client::unix(&rpc_socket);
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
        let (oci_bytes, oci_digest) = minimal_oci_archive();
        std::fs::write(&archive, &oci_bytes).unwrap();
        let mut peers = Vec::new();
        for token in ["first", "second"] {
            let stage = staging.clone();
            let file = archive.clone();
            let upload = tokio::task::spawn_blocking(move || stage.stage_image(&file, oci_digest));
            let (mut peer, _) = control.accept().await.unwrap();
            stage_erofs_reply(&mut peer, oci_digest, token).await;
            assert_eq!(upload.await.unwrap().unwrap().staged_extent, token);
            peers.push(peer);
        }
        for released in [None, Some("first"), Some("second")] {
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
            // Pool-allocated `stage/*` extents are already counted in `pool_allocated_bytes`.
            assert_eq!(fs.import_reservation_bytes, Some(0));
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
