use std::{collections::BTreeMap, io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UnixListener},
    sync::Mutex,
};
use vmm_carrier_macos::{
    client::CarrierClient, mock::MockCarrierHypervisor, protocol::*, server::serve,
};
use vmm_core::{fake::FakeDom0Backend, *};
use vmm_dom0::remote::serve_connection;

#[tokio::test]
async fn replacement_host_adopts_running_workload_and_reconnects_guest() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let path = |s| tmp.path().join(s);
    let control = UnixListener::bind(path("control"))?;
    let guest = UnixListener::bind(path("guest"))?;
    let listener = UnixListener::bind(path("carrier"))?;
    let machine = MockCarrierHypervisor::new(BTreeMap::from([
        (CONTROL_PORT, path("control")),
        (GUEST_PORT, path("guest")),
    ]));
    let machine = Arc::new(Mutex::new(machine));
    let (stop, done) = tokio::sync::oneshot::channel();
    let carrier = tokio::spawn(serve(listener, machine.clone(), async {
        let _ = done.await;
    }));
    let dom0 = tokio::spawn(async move {
        let backend = FakeDom0Backend::default();
        loop {
            let (s, _) = control.accept().await.unwrap();
            serve_connection(s, &backend).await.unwrap();
        }
    });
    let open = || {
        let client = CarrierClient::new(path("carrier"));
        let core = Core::new(
            Arc::new(Store::open(path("host.db")).unwrap()),
            Arc::new(client.dom0()),
        );
        (client, core)
    };
    let ensure = || Request::EnsureStarted {
        operation_id: "resume".into(),
    };
    let (boot, workload) = {
        let (client, core) = open();
        let boot = client.control(ensure()).await?.boot_id.unwrap();
        core.store.create_scope(
            &Scope {
                id: "local".into(),
                origin: Origin::Local,
                generation: 1,
                revision: 1,
                limits: Limits {
                    pids_max: 64,
                    disk_quota_bytes: 1 << 30,
                    vcpus: 1,
                    boot_ram_mb: 128,
                    max_ram_mb: 128,
                },
                egress_mode: EgressMode::DenyAll,
                host_socket_rules: vec![],
                state: ScopeState::Active,
            },
            None,
        )?;
        let def = SandboxDefinition {
            key: SandboxKey {
                scope_id: "local".into(),
                sandbox_id: "worker".into(),
                generation_id: "1".into(),
            },
            image_reference: "base".into(),
            manifest_digest: [1; 32],
            read_only_root: true,
            root_overlay: None,
            mounts: vec![],
            host_shares: vec![],
            entrypoint: None,
            command: Some(vec!["worker".into()]),
            workdir: None,
            user: None,
            pids_max: None,
            log_limit_bytes: 4096,
            env: Default::default(),
        };
        let image = ImageSource {
            manifest_digest: def.manifest_digest,
            staged_extent: "stage".into(),
            physical_bytes: 4096,
        };
        let reserve = vmm_carrier_macos::sparse::EMERGENCY_BYTES;
        let volume = Volume {
            scope_id: "local".into(),
            volume_id: "data".into(),
            quota_bytes: 4 << 20,
            used_bytes: 0,
            owner_uid: 1000,
            owner_gid: 1000,
            mode: 0o700,
            key_id: Some("key".into()),
        };
        machine.lock().await.set_host_free_bytes(reserve - 1);
        assert!(matches!(
            core.create_volume(&volume).await,
            Err(Error::Capacity(CapacityDimension::StorageHeadroom))
        ));
        assert!(matches!(
            core.import_image("local", "base", &image).await,
            Err(Error::Capacity(CapacityDimension::StorageHeadroom))
        ));
        machine
            .lock()
            .await
            .set_host_free_bytes(reserve + image.physical_bytes - 1);
        assert!(matches!(
            core.import_image("local", "base", &image).await,
            Err(Error::Capacity(CapacityDimension::StorageHeadroom))
        ));
        machine
            .lock()
            .await
            .set_host_free_bytes(reserve + image.physical_bytes);
        core.create_volume(&volume).await?;
        core.import_image("local", "base", &image).await?;
        core.define_sandbox(&def).await?;
        (boot, core.start(&def.key).await?)
    };
    let (client, core) = open();
    assert_eq!(
        client.control(ensure()).await?.boot_id.as_deref(),
        Some(boot.as_str())
    );
    assert_eq!(
        core.reconcile_after_restart().await?.adopted,
        vec![workload.clone()]
    );
    let ip = workload.slot_ip;
    let guest_service = tokio::spawn(async move {
        let (mut s, _) = guest.accept().await?;
        let mut bytes = [0; 4];
        s.read_exact(&mut bytes).await?;
        assert_eq!(bytes, ip.octets());
        assert_eq!(s.read_u16().await?, 8181);
        s.write_u8(0).await?;
        let mut payload = Vec::new();
        s.read_to_end(&mut payload).await?;
        assert_eq!(payload, b"request");
        s.write_all(b"response").await?;
        io::Result::Ok(())
    });
    let bridge = client.bridge_guest_tcp(ip, 8181).await?;
    let mut stream = TcpStream::connect(bridge.local_addr).await?;
    stream.write_all(b"request").await?;
    stream.shutdown().await?;
    let mut data = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_end(&mut data),
    )
    .await
    .map_err(io::Error::from)??;
    assert_eq!(data, b"response");
    guest_service.await.unwrap()?;
    drop(bridge);
    stop.send(()).unwrap();
    carrier.await.unwrap()?;
    dom0.abort();
    Ok(())
}
