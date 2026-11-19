#![cfg(target_os = "linux")]
#[path = "support/guest.rs"]
mod guest;
mod support;

use guest::SLOT;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use vmm_core::{
    backend::{Dom0Backend, WorkspaceRequest},
    *,
};
use vmm_dom0::{
    backend::{LocalDom0Backend, Options},
    boot_server::BootServer,
    extent::{BLOCK_SECTORS, FilesystemHeadroom, Pool},
    keys::MasterKey,
    network::MockFirewallDriver,
};
use vmm_guest_proto::rpc::{WorkspaceTarget, WorkspaceWriteRequest};
use vmm_l4hctl::{DiskExtent, DiskInfo, HypervisorCtl, MockHypervisorCtl};

struct HostControl(std::path::PathBuf);
#[async_trait::async_trait]
impl vmm_dom0::remote::Connect for HostControl {
    async fn connect(&self) -> std::io::Result<tokio::net::UnixStream> {
        tokio::net::UnixStream::connect(&self.0).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_guest_workspace_helper_and_reclamation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let root = dir.path();
    let ctl = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 1,
        owned: false,
        failed: false,
        sectors: 24 * BLOCK_SECTORS,
    }])));
    let pool = Arc::new(Pool::open(
        &root.join("pool.db"),
        ctl.clone(),
        1,
        DiskExtent {
            sector: 0,
            sectors: 24 * BLOCK_SECTORS,
        },
        Arc::new(FilesystemHeadroom(root.into())),
        0,
    )?);
    let listen = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listen.local_addr()?.port();
    let mut cfg = Options::new(vec![].into(), root.join("staging"));
    cfg.slot_address = |s| (s == 0).then_some(SLOT);
    cfg.gateway = Ipv4Addr::LOCALHOST;
    cfg.rpc_port = std::net::TcpListener::bind((SLOT, 0))?.local_addr()?.port();
    cfg.boot_port = port;
    cfg.boot_timeout = Duration::from_secs(8);
    cfg.helper_image = Some([7; 32]);
    cfg.builder_image = Some([7; 32]);
    let dns = vmm_dom0::dns::DnsServer::bind(SocketAddrV4::new(cfg.gateway, 0)).await?;
    cfg.dns_port = dns.port()?;
    let dns_endpoint = SocketAddrV4::new(cfg.gateway, cfg.dns_port);
    let boot = BootServer::default();
    let firewall = Arc::new(MockFirewallDriver::default());
    let backend = Arc::new(LocalDom0Backend::open(
        pool,
        ctl.clone(),
        cfg,
        Arc::new(MasterKey(zeroize::Zeroizing::new([11; 32]))),
        firewall.clone(),
        boot.clone(),
    )?);
    let dns_task = tokio::spawn(dns.serve(backend.clone()));
    let http = tokio::spawn(async move { boot.serve(listen).await });
    let observed = Arc::new(Mutex::new(guest::Observed::default()));
    let guests = tokio::spawn(guest::guests(
        ctl.clone(),
        port,
        root.into(),
        observed.clone(),
    ));
    let core = Core::new(
        Arc::new(Store::open(root.join("core.db"))?),
        backend.clone(),
    );
    core.store.create_scope(&support::scope("a"), None)?;
    let image_dir = root.join("image");
    std::fs::create_dir(&image_dir)?;
    std::fs::write(image_dir.join("version"), "example")?;
    let staged = backend.stage_directory(&image_dir, [7; 32])?;
    core.import_image("a", "cue", &staged).await?;
    let vol = support::volume("a");
    core.create_volume(&vol).await?;
    let def = support::definition("a", "work", AccessMode::ReadWrite);
    core.define_sandbox(&def).await?;
    let vm = core.start(&def.key).await?;
    assert_eq!(
        core.reconcile_after_restart().await?.adopted,
        vec![vm.clone()]
    );
    let control_path = root.join("control.sock");
    let listener = tokio::net::UnixListener::bind(&control_path)?;
    let authority = backend.clone();
    let control = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let backend = authority.clone();
            tokio::spawn(async move {
                vmm_dom0::remote::serve_host_connection(stream, &backend)
                    .await
                    .unwrap();
            });
        }
    });
    let remote = vmm_dom0::remote::RemoteDom0Client::unix(&control_path);
    let endpoint = remote.open_guest_runtime(&vm.boot).await?;
    assert_eq!(endpoint.address, remote.guest_address(&vm.boot).await?);
    let client = vmm_guest_proto::client::Client::new(endpoint.address.into(), endpoint.token);
    let health = tokio::task::spawn_blocking(move || client.health([7; 16]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(health.instance_id, vm.boot.instance_id);
    let mut stale = vm.boot.clone();
    stale.boot_token_hash[0] ^= 1;
    assert!(matches!(
        remote.open_guest_runtime(&stale).await,
        Err(Error::Fenced)
    ));
    let exec = backend
        .runtime(&vm.boot)?
        .call(|client| {
            client.exec(
                vmm_guest_proto::rpc::ExecRequest {
                    command: vmm_guest_proto::rpc::CommandSpec {
                        argv: vec!["/bin/echo".into(), "exec".into()],
                        env: vec![],
                        user: None,
                        workdir: None,
                    },
                    stdin: false,
                    tty: None,
                    timeout_ms: 3000,
                    max_output_bytes: 4096,
                },
                &[],
            )
        })
        .await?;
    assert_eq!(exec.stdout, b"exec\n");
    let process = backend
        .exec(&vm.boot, &["/bin/echo".into(), "hello".into()])
        .await?;
    for _ in 0..100 {
        if backend.logs(&vm.boot, &process).await? == b"hello\n" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(backend.logs(&vm.boot, &process).await?, b"hello\n");
    let ws = backend
        .resolve_workspace("a", Some("data"), AccessMode::ReadWrite)
        .await?;
    let reply = ws
        .write(
            WorkspaceWriteRequest {
                target: WorkspaceTarget {
                    volume: None,
                    path: "x".into(),
                },
                request_id: "write-1".into(),
                offset: 0,
                expected_prefix_sha256: None,
                mode: 0o600,
            },
            std::io::Cursor::new(b"hello".to_vec()),
        )
        .await?;
    assert!(reply.root_used_bytes.is_some_and(|n| n > 0));
    assert!(ws.used_bytes().await?.is_some());
    ws.close().await?;
    assert_eq!(
        backend
            .workspace(&vm.boot, WorkspaceRequest::Read("/data/x".into()))
            .await?,
        b"hello"
    );
    let active = if std::env::var_os("VMM_TEST_SERVICE_VIP").is_some() {
        Some(verify_route(&backend, &vm.boot, dns_endpoint, &firewall).await?)
    } else {
        None
    };
    core.stop(&vm.boot, true).await?;
    assert!(matches!(
        remote.open_guest_runtime(&vm.boot).await,
        Err(Error::Fenced)
    ));
    if let Some(mut active) = active {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), active.read(&mut [0]))
                .await
                .unwrap()?,
            0
        );
    }
    let lease = vmm_dom0::remote::RemoteWorkspace::acquire(
        &HostControl(control_path.clone()),
        "a",
        Some("data"),
        AccessMode::ReadOnly,
    )
    .await?;
    assert_ne!(lease.boot, vm.boot);
    let ep = remote.open_guest_runtime(&lease.boot).await?;
    let client = vmm_guest_proto::client::Client::new(ep.address.into(), ep.token);
    let alias = lease.alias.clone();
    let entry = tokio::task::spawn_blocking(move || {
        client.workspace_stat(WorkspaceTarget {
            volume: Some(alias),
            path: "x".into(),
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(entry.size, 5);
    assert_eq!(firewall.0.lock().unwrap()[&SLOT].mode, EgressMode::DenyAll);
    let helper = lease.boot.clone();
    drop(lease);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(remote.open_guest_runtime(&helper).await, Err(Error::Fenced)) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected host lease retained its helper");
    // The boot fence precedes guest shutdown. Wait for the hypervisor to
    // finish reclaiming the helper before inspecting its shutdown record.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ctl.lock().unwrap().list_vms().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnected host lease retained its VM");

    {
        let record = observed.lock().unwrap();
        assert_eq!(record.boots.len(), 3);
        assert_eq!(
            record.boots[0],
            ("data".into(), true, false, true, Some(23))
        );
        assert!(!record.boots[1].1 && !record.boots[1].2);
        assert!(!record.boots[2].1 && record.boots[2].2);
        assert_eq!(record.prepared.len(), 2);
    }
    let second = core.start(&def.key).await?;
    assert!(!observed.lock().unwrap().boots.last().unwrap().3);
    core.stop(&second.boot, false).await?;
    core.delete_sandbox(&def.key).await?;
    core.discard_volume("a", "data").await?;
    let mut default_volume = vol;
    default_volume.volume_id = "__default_workspace".into();
    core.create_volume(&default_volume).await?;
    let default = backend
        .resolve_workspace("a", None, AccessMode::ReadWrite)
        .await?;
    assert!(default.used_bytes().await?.is_some());
    default.close().await?;
    assert_eq!(
        observed.lock().unwrap().boots.last().unwrap().0,
        "workspace"
    );
    core.discard_volume("a", "__default_workspace").await?;
    let build = backend
        .begin_build(&support::scope("a"), EgressMode::PublicInternet, 4 << 20)
        .await?;
    assert_eq!(firewall.0.lock().unwrap()[&SLOT].mode, EgressMode::DenyAll);
    assert_eq!(
        build.guest.health().await?.instance_id,
        build.boot.instance_id
    );
    build.close().await?;
    let discarded = ctl.lock().unwrap().discards().len();
    let build = vmm_dom0::remote::RemoteBuild::acquire(
        &HostControl(control_path.clone()),
        &support::scope("a"),
        EgressMode::DenyAll,
        4 << 20,
    )
    .await?;
    assert!(remote.open_guest_runtime(&build.boot).await.is_ok());
    let retired = build.boot.clone();
    build.close().await?;
    assert!(remote.open_guest_runtime(&retired).await.is_err());
    control.abort();
    core.remove_image_reference("a", "cue", [7; 32]).await?;
    assert!(backend.image_installed([7; 32])?);
    assert_eq!(ctl.lock().unwrap().discards().len(), discarded + 2);
    assert!(ctl.lock().unwrap().list_vms().unwrap().is_empty());
    dns_task.abort();
    guests.abort();
    http.abort();
    Ok(())
}
async fn verify_route(
    backend: &LocalDom0Backend,
    boot: &BootIdentity,
    dns: SocketAddrV4,
    firewall: &MockFirewallDriver,
) -> Result<tokio::net::TcpStream> {
    use vmm_dom0::proxy::{AsyncStream, DialFuture, ExternalServiceRoute, Target, UpstreamDialer};
    struct ViaHost(SocketAddrV4);
    impl UpstreamDialer for ViaHost {
        fn connect(&self) -> DialFuture {
            let addr = self.0;
            Box::pin(async move {
                Ok(Box::new(tokio::net::TcpStream::connect(addr).await?) as Box<dyn AsyncStream>)
            })
        }
    }
    let echo = TcpListener::bind((SLOT, 0)).await?;
    let port = echo.local_addr()?.port();
    let vip = Ipv4Addr::new(100, 127, 254, 9);
    let route = ServiceRoute {
        revision: 2,
        binding: RouteBinding {
            expires_at_unix_seconds: None,
            virtual_service_name: "local-api".into(),
            id: "connection".into(),
            source: boot.clone(),
            export: ServiceExport {
                id: "service".into(),
                boot: boot.clone(),
                port,
            },
            vip,
        },
    };
    backend.bind_route(&route).await?;
    let mut external = ExternalServiceRoute::from(&route);
    external.id = "cross-device".into();
    let host = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let dialer = Target::Dialer(Arc::new(ViaHost(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        host.local_addr()?.port(),
    ))));
    external.virtual_service_name = "remote-api".into();
    assert!(matches!(
        backend
            .bind_external_route(external.clone(), dialer.clone())
            .await,
        Err(Error::Conflict(_))
    ));
    let reservation = TcpListener::bind((vip, 0)).await?;
    external.port = reservation.local_addr()?.port();
    drop(reservation);
    external.virtual_service_name = "LOCAL-API".into();
    assert!(matches!(
        backend
            .bind_external_route(external.clone(), dialer.clone())
            .await,
        Err(Error::Conflict(_))
    ));
    external.virtual_service_name = "remote-api".into();
    backend
        .bind_external_route(external.clone(), dialer.clone())
        .await?;
    backend
        .bind_external_route(external.clone(), dialer.clone())
        .await?;
    let endpoint = SocketAddrV4::new(vip, external.port);
    {
        let fw = firewall.0.lock().unwrap();
        assert!(fw[&SLOT].local_ports.contains(&endpoint));
        assert_eq!(fw[&SLOT].dns, Some(dns));
    }
    for alias in ["local-api", "REMOTE-api.services.agent"] {
        assert_eq!(resolve(dns, alias).await, Some(vip));
    }
    let stream = guest::connect_from_slot(endpoint).await;
    let mut host_stream = host.accept().await?.0;
    host_stream.write_all(b"H").await?;
    let mut stream = stream;
    assert_eq!(stream.read_u8().await?, b'H');
    backend
        .revoke_route(&external.id, external.revision)
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut [0]))
            .await
            .unwrap()?,
        0
    );
    assert_eq!(resolve(dns, "remote-api").await, None);
    assert!(
        !firewall.0.lock().unwrap()[&SLOT]
            .local_ports
            .contains(&endpoint)
    );
    assert!(matches!(
        backend
            .bind_external_route(external.clone(), dialer.clone())
            .await,
        Err(Error::Fenced)
    ));
    external.revision += 1;
    backend.bind_external_route(external, dialer).await?;
    let mut active = guest::connect_from_slot(endpoint).await;
    let mut remote = host.accept().await?.0;
    remote.write_all(b"A").await?;
    assert_eq!(active.read_u8().await?, b'A');
    let echo_task = tokio::spawn(async move {
        let (mut stream, _) = echo.accept().await.unwrap();
        let (mut r, mut w) = stream.split();
        tokio::io::copy(&mut r, &mut w).await
    });
    let mut connection = guest::connect_from_slot(SocketAddrV4::new(vip, port)).await;
    connection.write_all(b"echo").await?;
    let mut bytes = [0; 4];
    connection.read_exact(&mut bytes).await?;
    assert_eq!(&bytes, b"echo");
    backend.revoke_route("connection", 2).await?;
    assert!(matches!(
        backend.bind_route(&route).await,
        Err(Error::Fenced)
    ));
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), connection.read(&mut bytes))
            .await
            .unwrap()?,
        0
    );
    echo_task.await.unwrap()?;
    // Keep the tunnel open until the source VM loses its network grants.
    tokio::spawn(async move {
        let _ = remote.read_to_end(&mut Vec::new()).await;
    });
    Ok(active)
}

async fn resolve(dns: SocketAddrV4, alias: &str) -> Option<Ipv4Addr> {
    use simple_dns::{CLASS, Name, Packet, Question, RCODE, TYPE, rdata::RData};
    let mut q = Packet::new_query(51);
    q.questions.push(Question::new(
        Name::new(alias).unwrap(),
        TYPE::A.into(),
        CLASS::IN.into(),
        false,
    ));
    let s = tokio::net::UdpSocket::bind((SLOT, 0)).await.unwrap();
    s.connect(dns).await.unwrap();
    s.send(&q.build_bytes_vec().unwrap()).await.unwrap();
    let mut buf = [0; 512];
    let n = tokio::time::timeout(Duration::from_secs(2), s.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let reply = Packet::parse(&buf[..n]).unwrap();
    if reply.rcode() == RCODE::NameError {
        return None;
    }
    let RData::A(a) = &reply.answers[0].rdata else {
        panic!("missing VIP")
    };
    Some(a.address.into())
}
