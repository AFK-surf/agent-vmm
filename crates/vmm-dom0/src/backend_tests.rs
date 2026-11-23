use crate::{
    backend::{LocalDom0Backend, Options},
    boot_server::BootServer,
    catalog::{SavedBoot, volume_id},
    dns::{DnsServer, public_resolver},
    extent::{BLOCK_SECTORS, Control, FilesystemHeadroom, Pool},
    keys::MasterKey,
    network::MockFirewallDriver,
    proxy::{AsyncStream, DialFuture, ExternalServiceRoute, Proxies, Target, UpstreamDialer},
};
use simple_dns::{CLASS, Name, Packet, PacketFlag, QTYPE, Question, RCODE, TYPE, rdata::RData};
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use vmm_core::{backend::Dom0Backend, *};
use vmm_l4hctl::{DiskExtent, DiskInfo, MockHypervisorCtl, VmSpec};

mod support {
    include!("../tests/support/mod.rs");
}

fn setup() -> Result<(tempfile::TempDir, Arc<Pool>, Control)> {
    let dir = tempfile::tempdir()?;
    let sectors = 8 * BLOCK_SECTORS;
    let ctl: Control = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 1,
        owned: false,
        failed: false,
        sectors,
    }])));
    let pool = Pool::open(
        &dir.path().join("pool.db"),
        ctl.clone(),
        1,
        DiskExtent { sector: 0, sectors },
        Arc::new(FilesystemHeadroom(dir.path().into())),
        0,
    )?;
    Ok((dir, Arc::new(pool), ctl))
}
fn open(pool: &Arc<Pool>, ctl: &Control, cfg: Options) -> Result<LocalDom0Backend> {
    LocalDom0Backend::open(
        pool.clone(),
        ctl.clone(),
        cfg,
        Arc::new(MasterKey(zeroize::Zeroizing::new([3; 32]))),
        Arc::new(MockFirewallDriver::default()),
        BootServer::default(),
    )
}
fn options(dir: &Path) -> Options {
    Options::new(vec![].into(), dir.join("staging"))
}

#[test]
fn disk_accounting_measures_extents_and_only_token_named_stage_files() -> Result<()> {
    use crate::extent::BLOCK_BYTES;
    let (dir, pool, ctl) = setup()?;
    let backend = open(&pool, &ctl, options(dir.path()))?;
    let capacity = 8 * BLOCK_BYTES;
    assert_eq!(backend.disk_accounting()?, (0, capacity, 0));
    pool.allocate("small", 1)?;
    pool.allocate("large", BLOCK_BYTES + 1)?;
    let token = "0123456789abcdef0123456789abcdef";
    let other = "ffffffffffffffffffffffffffffffff";
    std::fs::write(backend.cfg.staging.join(token), [8; 91])?;
    std::fs::write(backend.cfg.staging.join(other), [2; 9])?;
    std::fs::write(backend.cfg.staging.join("temporary"), [0; 16])?;
    std::fs::create_dir(backend.cfg.staging.join("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"))?;
    std::os::unix::fs::symlink(
        dir.path().join("pool.db"),
        backend.cfg.staging.join("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )?;
    assert_eq!(backend.disk_accounting()?, (3 * BLOCK_BYTES, capacity, 100));
    std::fs::remove_file(backend.cfg.staging.join(token))?;
    pool.discard("large")?;
    assert_eq!(backend.disk_accounting()?, (BLOCK_BYTES, capacity, 9));
    Ok(())
}

#[tokio::test]
async fn staged_files_and_properties_follow_install_discard_and_restart() -> Result<()> {
    let (dir, pool, ctl) = setup()?;
    let root = dir.path().join("root");
    std::fs::create_dir(&root)?;
    let b = open(&pool, &ctl, options(dir.path()))?;
    assert!(matches!(
        b.stage_image(&root, [2; 32]),
        Err(Error::Invalid("OCI layout directory"))
    ));
    assert!(b.discard_staged_image("../pool.db").is_err());
    for _ in 0..2 {
        let image = b.stage_directory(&root, [2; 32])?;
        let token = &image.staged_extent;
        b.install_image_artifact(&image).await?;
        assert!(!b.cfg.staging.join(token).exists() && b.catalog().staged_tokens()?.is_empty());
        b.discard_staged_image(token)?;
    }
    let volume = support::volume("scope");
    b.allocate_volume(&volume).await?;
    b.discard_volume("scope", "data").await?;
    assert!(
        b.catalog()
            .get::<Volume>(&format!("volume/{}", volume_id("scope", "data")?))?
            .is_none()
    );
    let stale = b.stage_directory(&root, [3; 32])?.staged_extent;
    let orphan = "abcdabcdabcdabcdabcdabcdabcdabcd";
    std::fs::write(b.cfg.staging.join(orphan), b"orphan")?;
    drop(b);
    let b = open(&pool, &ctl, options(dir.path()))?;
    let fresh = b.stage_directory(&root, [4; 32])?.staged_extent;
    b.recover_transients().await?;
    b.recover_transients().await?;
    assert_eq!(
        b.catalog().staged_tokens()?.as_slice(),
        std::slice::from_ref(&fresh)
    );
    assert!(b.cfg.staging.join(&fresh).exists());
    assert!(!b.cfg.staging.join(stale).exists() && !b.cfg.staging.join(orphan).exists());
    b.discard_staged_image(&fresh)?;
    b.discard_staged_image(&fresh)?;
    assert!(b.catalog().staged_tokens()?.is_empty());
    Ok(())
}

struct Echo;
impl UpstreamDialer for Echo {
    fn connect(&self) -> DialFuture {
        Box::pin(async {
            let (socket, peer) = tokio::io::duplex(256);
            tokio::spawn(async move {
                let (mut a, mut b) = tokio::io::split(peer);
                let _ = tokio::io::copy(&mut a, &mut b).await;
            });
            Ok(Box::new(socket) as Box<dyn AsyncStream>)
        })
    }
}
#[tokio::test]
async fn external_dialer_disconnects_an_active_stream_on_revoke() -> Result<()> {
    let ip = Ipv4Addr::LOCALHOST;
    let port = std::net::TcpListener::bind((ip, 0))?.local_addr()?.port();
    let endpoint = SocketAddrV4::new(ip, port);
    let proxies = Proxies::default();
    let dialer = Target::Dialer(Arc::new(Echo));
    proxies.bind(endpoint, ip, dialer.clone(), 5).await?;
    proxies.bind(endpoint, ip, dialer, 5).await?;
    assert!(
        proxies
            .bind(endpoint, ip, Target::Dialer(Arc::new(Echo)), 5)
            .await
            .is_err()
    );
    let mut stream = TcpStream::connect(endpoint).await?;
    stream.write_all(b"x").await?;
    assert_eq!(stream.read_u8().await?, b'x');
    proxies.revoke(endpoint, ip, 5).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.read(&mut [0]))
            .await
            .unwrap()?,
        0
    );
    Ok(())
}

#[tokio::test]
async fn expiring_grant_closes_current_and_future_connections() -> Result<()> {
    let ip = Ipv4Addr::LOCALHOST;
    let port = std::net::TcpListener::bind((ip, 0))?.local_addr()?.port();
    let endpoint = SocketAddrV4::new(ip, port);
    let proxies = Proxies::default();
    let deadline = std::time::SystemTime::now() + Duration::from_millis(500);
    proxies
        .bind_until(
            endpoint,
            ip,
            Target::Dialer(Arc::new(Echo)),
            7,
            Some(deadline),
        )
        .await?;
    let mut current = TcpStream::connect(endpoint).await?;
    current.write_all(b"x").await?;
    assert_eq!(current.read_u8().await?, b'x');
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), current.read(&mut [0]))
            .await
            .unwrap()?,
        0
    );
    let mut later = TcpStream::connect(endpoint).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), later.read(&mut [0]))
            .await
            .unwrap()?,
        0
    );
    Ok(())
}

async fn query(addr: SocketAddrV4, name: &str, typ: QTYPE, tcp: bool) -> Packet<'static> {
    let mut q = Packet::new_query(37);
    q.questions.push(Question::new(
        Name::new(name).unwrap(),
        typ,
        CLASS::IN.into(),
        false,
    ));
    query_wire(addr, &q.build_bytes_vec().unwrap(), tcp).await
}

async fn query_wire(addr: SocketAddrV4, wire: &[u8], tcp: bool) -> Packet<'static> {
    let bytes = tokio::time::timeout(Duration::from_secs(3), async {
        if tcp {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_u16(wire.len() as u16).await.unwrap();
            s.write_all(wire).await.unwrap();
            let mut b = vec![0; s.read_u16().await.unwrap() as usize];
            s.read_exact(&mut b).await.unwrap();
            b
        } else {
            let s = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            s.connect(addr).await.unwrap();
            s.send(wire).await.unwrap();
            let mut b = vec![0; 4096];
            let n = s.recv(&mut b).await.unwrap();
            b.truncate(n);
            b
        }
    })
    .await
    .unwrap();
    let packet = Packet::parse(&bytes).unwrap();
    assert_eq!(packet.id(), 37);
    // Retain only the fields these assertions consume.
    let mut result = Packet::new_reply(packet.id());
    *result.rcode_mut() = packet.rcode();
    if packet.has_flags(PacketFlag::AUTHORITATIVE_ANSWER) {
        result.set_flags(PacketFlag::AUTHORITATIVE_ANSWER);
    }
    result.answers = packet.answers.into_iter().map(|r| r.into_owned()).collect();
    result
}

#[tokio::test]
async fn gateway_dns_scopes_aliases_and_upstream_by_live_source_over_udp_and_tcp() -> Result<()> {
    let (dir, pool, ctl) = setup()?;
    let ip = Ipv4Addr::LOCALHOST;
    let server = DnsServer::bind(SocketAddrV4::new(ip, 0)).await?;
    let dns = SocketAddrV4::new(ip, server.port()?);
    let upstream_tcp = TcpListener::bind((ip, 0)).await?;
    let upstream = SocketAddrV4::new(ip, upstream_tcp.local_addr()?.port());
    let upstream_udp = UdpSocket::bind(upstream).await?;
    let mut cfg = options(dir.path());
    cfg.dns_port = dns.port();
    cfg.upstream_dns = Some(upstream);
    cfg.slot_address = |slot| (slot == 0).then_some(Ipv4Addr::LOCALHOST);
    let b = Arc::new(open(&pool, &ctl, cfg)?);
    let task = tokio::spawn(server.serve(b.clone()));
    assert_eq!(
        query(dns, "db", TYPE::A.into(), true).await.rcode(),
        RCODE::Refused
    );
    let def = support::definition("scope", "box", AccessMode::ReadWrite);
    let mut boot = SavedBoot {
        identity: BootIdentity {
            sandbox: def.key.clone(),
            instance_id: "instance".into(),
            boot_token_hash: [1; 32],
        },
        name: "vm".into(),
        slot: 0,
        vm: None,
        scope: support::scope("scope"),
        definition: def,
        encrypted_token: vec![],
        disks: vec![],
        writers: vec![],
        retiring: false,
        retired: false,
        helper: false,
        share_ports: Default::default(),
    };
    ctl.lock()
        .unwrap()
        .create_vm(&VmSpec {
            name: "vm",
            memory_bytes: 128 << 20,
            vcpus: 1,
            initrd: &[],
            cmdline: "",
            volumes: &[],
        })
        .unwrap();
    b.catalog().save(&boot)?;
    b.prepare_network(&mut boot).await?;
    let vip = Ipv4Addr::new(100, 127, 254, 9);
    let route = ExternalServiceRoute {
        id: "remote".into(),
        source: boot.identity.clone(),
        port: 80,
        vip,
        virtual_service_name: "db".into(),
        revision: 1,
    };
    b.routes
        .lock()
        .await
        .external
        .insert(route.id.clone(), route);
    for tcp in [false, true] {
        let edns = query_wire(dns, EDNS_QUERY, tcp).await;
        assert_eq!(edns.rcode(), RCODE::NoError);
        assert_eq!(edns.answers[0].rdata, RData::A(vip.into()));
        for (name, t) in [
            ("DB", QTYPE::ANY),
            ("dB.SeRviCes.AGENT", TYPE::A.into()),
            ("DB.SCOPE.AGENT", TYPE::A.into()),
        ] {
            let p = query(dns, name, t, tcp).await;
            assert_eq!(p.rcode(), RCODE::NoError);
            assert!(p.has_flags(PacketFlag::AUTHORITATIVE_ANSWER));
            assert_eq!(p.answers.len(), 1);
            assert_eq!(p.answers[0].ttl, 1);
            assert_eq!(p.answers[0].rdata, RData::A(vip.into()));
        }
        let p = query(dns, "db", TYPE::AAAA.into(), tcp).await;
        assert!(p.answers.is_empty() && p.rcode() == RCODE::NoError);
        for name in ["unknown", "db.other.agent"] {
            assert_eq!(
                query(dns, name, TYPE::A.into(), tcp).await.rcode(),
                RCODE::NameError
            );
        }
        assert_eq!(
            query(dns, "www.example.org", TYPE::A.into(), tcp)
                .await
                .rcode(),
            RCODE::Refused
        );
    }
    boot.scope.egress_mode = EgressMode::PublicInternet;
    b.catalog().save(&boot)?;
    let upstream_task = tokio::spawn(async move {
        let mut buf = [0; 512];
        let (n, peer) = upstream_udp.recv_from(&mut buf).await.unwrap();
        let answer = Packet::parse(&buf[..n])
            .unwrap()
            .into_reply()
            .build_bytes_vec()
            .unwrap();
        upstream_udp.send_to(&answer, peer).await.unwrap();
        let (mut stream, _) = upstream_tcp.accept().await.unwrap();
        let n = stream.read_u16().await.unwrap() as usize;
        stream.read_exact(&mut buf[..n]).await.unwrap();
        let answer = Packet::parse(&buf[..n])
            .unwrap()
            .into_reply()
            .build_bytes_vec()
            .unwrap();
        stream.write_u16(answer.len() as u16).await.unwrap();
        stream.write_all(&answer).await.unwrap();
    });
    for tcp in [false, true] {
        assert_eq!(
            query(dns, "www.example.org", TYPE::A.into(), tcp)
                .await
                .rcode(),
            RCODE::NoError
        );
    }
    upstream_task.await.unwrap();
    boot.retiring = true;
    b.catalog().save(&boot)?;
    assert_eq!(
        query(dns, "db", TYPE::A.into(), false).await.rcode(),
        RCODE::Refused
    );
    task.abort();
    assert_eq!(
        *public_resolver(
            "nameserver ::1\nnameserver 127.0.0.53\nnameserver 100.64.10.1\nnameserver 9.9.9.9"
        )
        .ip(),
        Ipv4Addr::new(9, 9, 9, 9)
    );
    assert_eq!(
        *public_resolver("nameserver 10.0.0.1").ip(),
        Ipv4Addr::new(1, 1, 1, 1)
    );
    assert_eq!(
        crate::dns::fallback_resolvers(
            "nameserver 127.0.0.53\nnameserver 192.168.64.1\nnameserver 192.168.64.1"
        ),
        vec![SocketAddrV4::new(Ipv4Addr::new(192, 168, 64, 1), 53)]
    );
    Ok(())
}

#[tokio::test]
async fn dns_registers_resolved_fake_ips_and_blocks_reserved_fake_ips() -> Result<()> {
    let (dir, pool, ctl) = setup()?;
    let upstream_udp = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let mut cfg = options(dir.path());
    cfg.upstream_dns = Some(match upstream_udp.local_addr()? {
        std::net::SocketAddr::V4(a) => a,
        _ => unreachable!(),
    });
    let fw = Arc::new(MockFirewallDriver::default());
    cfg.slot_address = |slot| (slot == 0).then_some(Ipv4Addr::LOCALHOST);
    let b = Arc::new(LocalDom0Backend::open(
        pool,
        ctl.clone(),
        cfg,
        Arc::new(crate::keys::MasterKey(zeroize::Zeroizing::new([7; 32]))),
        fw.clone(),
        BootServer::default(),
    )?);
    let def = support::definition("scope", "box", AccessMode::ReadWrite);
    let mut scope = support::scope("scope");
    scope.egress_mode = EgressMode::PublicInternet;
    let mut boot = SavedBoot {
        identity: BootIdentity {
            sandbox: def.key.clone(),
            instance_id: "instance-fakeip".into(),
            boot_token_hash: [2; 32],
        },
        name: "vm-fakeip".into(),
        slot: 0,
        vm: None,
        scope,
        definition: def,
        encrypted_token: vec![],
        disks: vec![],
        writers: vec![],
        retiring: false,
        retired: false,
        helper: false,
        share_ports: Default::default(),
    };
    ctl.lock()
        .unwrap()
        .create_vm(&VmSpec {
            name: "vm-fakeip",
            memory_bytes: 128 << 20,
            vcpus: 1,
            initrd: &[],
            cmdline: "",
            volumes: &[],
        })
        .unwrap();
    b.catalog().save(&boot)?;
    b.prepare_network(&mut boot).await?;
    let server = DnsServer::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await?;
    let dns = SocketAddrV4::new(Ipv4Addr::LOCALHOST, server.port()?);
    let task = tokio::spawn(server.serve(b.clone()));

    let upstream_task = tokio::spawn(async move {
        let mut buf = [0u8; 512];
        for (ip, ttl) in [
            (Ipv4Addr::new(198, 18, 14, 184), 30),
            (Ipv4Addr::new(198, 18, 0, 1), 300),
        ] {
            let (n, peer) = upstream_udp.recv_from(&mut buf).await.unwrap();
            let q = Packet::parse(&buf[..n]).unwrap();
            let qname = q.questions[0].qname.clone();
            let mut reply = q.into_reply();
            reply.answers.push(simple_dns::ResourceRecord::new(
                qname,
                simple_dns::CLASS::IN,
                ttl,
                simple_dns::rdata::RData::A(ip.into()),
            ));
            let wire = reply.build_bytes_vec().unwrap();
            upstream_udp.send_to(&wire, peer).await.unwrap();
        }
    });

    assert_eq!(
        query(dns, "proxy.example.com", TYPE::A.into(), false)
            .await
            .rcode(),
        RCODE::NoError
    );
    assert_eq!(
        query(dns, "reserved.example.com", TYPE::A.into(), false)
            .await
            .rcode(),
        RCODE::NoError
    );
    upstream_task.await.unwrap();
    task.abort();

    let registered = fw.1.lock().unwrap();
    assert_eq!(
        registered.get(&Ipv4Addr::new(198, 18, 14, 184)),
        Some(&Duration::from_secs(60))
    );
    assert!(!registered.contains_key(&Ipv4Addr::new(198, 18, 0, 1)));
    Ok(())
}

// ID 37, RD, one question for db IN A, one root OPT (type 41, 1232-byte UDP payload).
const EDNS_QUERY: &[u8] = &[
    0, 37, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 2, b'd', b'b', 0, 0, 1, 0, 1, 0, 0, 41, 4, 208, 0, 0, 0,
    0, 0, 0,
];

#[tokio::test]
async fn dns_edns0_preserves_source_authorization_and_rejects_other_additionals() -> Result<()> {
    let (dir, pool, ctl) = setup()?;
    let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
    let dns = DnsServer::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).await?;
    let addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, dns.port()?);
    let server = tokio::spawn(dns.serve(b));
    assert_eq!(
        query_wire(addr, EDNS_QUERY, false).await.rcode(),
        RCODE::Refused
    );
    let mut other = EDNS_QUERY[..20].to_vec();
    other.extend_from_slice(&[0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 4, 127, 0, 0, 1]);
    assert_eq!(
        query_wire(addr, &other, false).await.rcode(),
        RCODE::FormatError
    );
    server.abort();
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn embedded_handle_drop_releases_boot_and_dns_listeners() -> Result<()> {
    let (dir, pool, ctl) = setup()?;
    let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
    let ip = Ipv4Addr::LOCALHOST;
    let listener = TcpListener::bind((ip, 0)).await?;
    let http = listener.local_addr()?;
    let dns = DnsServer::bind(SocketAddrV4::new(ip, 0)).await?;
    let dns_addr = SocketAddrV4::new(ip, dns.port()?);
    let mut handle = crate::local::LocalDom0Handle::serve(b, listener, dns);
    assert_eq!(
        query_wire(dns_addr, EDNS_QUERY, false).await.rcode(),
        RCODE::Refused
    );
    let mut waiting = TcpStream::connect(http).await?;
    waiting.write_all(b"GET /v1/boot HTTP/1.1\r\n").await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), handle.wait())
            .await
            .is_err()
    );
    drop(handle);
    tokio::time::timeout(Duration::from_secs(2), async {
        let closed = waiting.read_u8().await.unwrap_err();
        assert!(matches!(
            closed.kind(),
            std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
        ));
        loop {
            if let (Ok(_http), Ok(_dns)) = (
                TcpListener::bind(http).await,
                DnsServer::bind(dns_addr).await,
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    Ok(())
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn embedded_open_accepts_config_without_control_socket_and_checks_master_key()
-> crate::local::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.json");
    let key = dir.path().join("key");
    let config = serde_json::json!({
        "state_dir":dir.path().join("state"), "backing_filesystem":dir.path(),
        "disk":1, "first_sector":0, "sectors":BLOCK_SECTORS, "master_key":key,
        "initrd":"initrd", "interface":"guest0", "switch":1, "helper_root":"helper",
        "helper_digest":hex::encode([0u8;32])
    });
    std::fs::write(&path, serde_json::to_vec(&config)?)?;
    assert!(crate::local::Dom0Config::read(&path)?.socket.is_none());
    std::fs::write(&key, [0u8; 33])?;
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o640))?;
    assert_eq!(
        crate::local::open(&path, false)
            .await
            .err()
            .unwrap()
            .to_string(),
        "master key is group- or world-accessible"
    );
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600))?;
    assert_eq!(
        crate::local::open(&path, false)
            .await
            .err()
            .unwrap()
            .to_string(),
        "master key must contain exactly 32 bytes"
    );
    Ok(())
}

#[tokio::test]
async fn memory_summary_adds_running_workload_vms_by_environment_for_the_host_only() -> Result<()> {
    use crate::memory::{Dom0MemorySummary, EnvironmentMemory};
    use crate::remote::{RemoteDom0Client, serve_connection, serve_host_connection};
    use vmm_l4hctl::{HypervisorCtl, VmMemInfo};
    const MIB: u64 = 1 << 20;

    let dir = tempfile::tempdir()?;
    let sectors = 8 * BLOCK_SECTORS;
    let mock = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 1,
        owned: false,
        failed: false,
        sectors,
    }])));
    let ctl: Control = mock.clone();
    let pool = Arc::new(Pool::open(
        &dir.path().join("pool.db"),
        ctl.clone(),
        1,
        DiskExtent { sector: 0, sectors },
        Arc::new(FilesystemHeadroom(dir.path().into())),
        0,
    )?);
    let mut cfg = options(dir.path());
    cfg.meminfo = dir.path().join("meminfo");
    std::fs::write(
        &cfg.meminfo,
        "MemTotal:         247808 kB\nMemFree:            1024 kB\nMemAvailable:      30720 kB\n",
    )?;
    let b = Arc::new(open(&pool, &ctl, cfg)?);

    // Name, environment, helper, boot, target and plugged MiB. No VM has the name `lost`.
    let vms = [
        ("a1", "alpha", false, 128, 1024, 256),
        ("a2", "alpha", false, 128, 512, 0),
        ("b1", "beta", false, 256, 256, 0),
        ("helper", "alpha", true, 64, 64, 0),
        ("lost", "gamma", false, 128, 128, 0),
    ];
    for (slot, (name, scope, helper, boot, target, plugged)) in vms.into_iter().enumerate() {
        let def = support::definition(scope, name, AccessMode::ReadWrite);
        b.catalog().save(&SavedBoot {
            identity: BootIdentity {
                sandbox: def.key.clone(),
                instance_id: name.into(),
                boot_token_hash: [1; 32],
            },
            name: name.into(),
            slot: slot as u32,
            vm: None,
            scope: support::scope(scope),
            definition: def,
            encrypted_token: vec![],
            disks: vec![],
            writers: vec![],
            retiring: false,
            retired: false,
            helper,
            share_ports: Default::default(),
        })?;
        if name == "lost" {
            continue;
        }
        let mut ctl = mock.lock().unwrap();
        let id = ctl
            .create_vm(&VmSpec {
                name,
                memory_bytes: boot * MIB,
                vcpus: 1,
                initrd: &[],
                cmdline: "",
                volumes: &[],
            })
            .unwrap();
        ctl.vm_set_mem(id, target * MIB).unwrap();
        let mut info = ctl.vm_mem_info(id).unwrap();
        info.plugged_bytes = plugged * MIB;
        ctl.set_vm_mem(id, info);
    }
    {
        let mut ctl = mock.lock().unwrap();
        ctl.set_host_mem((4096 + 1536 + 192) * MIB, 8192 * MIB);
        ctl.set_vm_mem(
            0,
            VmMemInfo {
                boot_bytes: 256 * MIB,
                reported_bytes: 1536 * MIB,
                clean_bytes: 4096 * MIB,
                ..Default::default()
            },
        );
    }
    let environment = |id: &str, boot, grant, plugged| EnvironmentMemory {
        environment_id: id.into(),
        boot_bytes: boot * MIB,
        grant_bytes: grant * MIB,
        plugged_bytes: plugged * MIB,
    };
    let expected = Dom0MemorySummary {
        clean_bytes: 4096 * MIB,
        free_bytes: 192 * MIB,
        inflated_bytes: 1536 * MIB,
        guest_plugged_bytes: (256 + 128 + 256 + 128 + 256 + 64) * MIB,
        dom0_limit_bytes: 256 * MIB,
        dom0_used_bytes: 212 * MIB,
        environments: vec![
            environment("alpha", 256, 1024 + 512, 128 + 256 + 128),
            environment("beta", 256, 256, 256),
        ],
    };
    assert_eq!(b.memory_summary()?, expected);

    // The socket path on macOS is limited to 104 bytes.
    let sockets = tempfile::tempdir_in("/tmp")?;
    let path = sockets.path().join("dom0.sock");
    let listener = tokio::net::UnixListener::bind(&path)?;
    let client = RemoteDom0Client::unix(&path);
    let (served, summary) = tokio::join!(
        async {
            let (socket, _) = listener.accept().await?;
            serve_host_connection(socket, &b).await
        },
        client.memory_summary(),
    );
    served?;
    assert_eq!(summary?, expected);
    let (served, refused) = tokio::join!(
        async {
            let (socket, _) = listener.accept().await?;
            serve_connection(socket, b.as_ref()).await
        },
        client.memory_summary(),
    );
    served?;
    assert!(matches!(refused, Err(Error::Fenced)));
    Ok(())
}

#[tokio::test]
async fn carrier_image_upload_releases_unpublished_stage_when_the_host_disconnects() -> Result<()> {
    use crate::remote::{Connect, RemoteImageStage, serve_host_connection};
    use tokio::net::{UnixListener, UnixStream};
    struct Dial(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Connect for Dial {
        async fn connect(&self) -> std::io::Result<UnixStream> {
            UnixStream::connect(&self.0).await
        }
    }

    let (dir, pool, ctl) = setup()?;
    let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
    let (bytes, digest) = crate::image::tests::fixture(false);
    let archive = dir.path().join("verified.tar");
    std::fs::write(&archive, bytes)?;
    // The socket path on macOS is limited to 104 bytes.
    let sockets = tempfile::tempdir_in("/tmp")?;
    let dial = Dial(sockets.path().join("dom0.sock"));
    let listener = UnixListener::bind(&dial.0)?;
    for install in [false, true] {
        let accept = async {
            let (socket, _) = listener.accept().await?;
            serve_host_connection(socket, &b).await
        };
        let upload = async {
            let stage = RemoteImageStage::upload(&dial, &archive, digest).await?;
            assert_eq!(b.disk_accounting()?.2, stage.source.physical_bytes);
            if install {
                b.install_image_artifact(&stage.source).await?;
            }
            drop(stage);
            Ok::<_, Error>(())
        };
        tokio::try_join!(accept, upload)?;
        assert_eq!(b.disk_accounting()?.2, 0);
        assert_eq!(b.image_installed(digest)?, install);
        assert!(b.catalog().staged_tokens()?.is_empty());
    }
    let (server, refused) = tokio::join!(
        async {
            let (socket, _) = listener.accept().await?;
            crate::remote::serve_connection(socket, b.as_ref()).await
        },
        RemoteImageStage::upload(&dial, &archive, digest),
    );
    server?;
    assert!(matches!(refused, Err(Error::Fenced)));
    Ok(())
}

#[tokio::test]
async fn stage_erofs_promotes_idempotently_and_recovers_orphaned_stage_on_restart() -> Result<()> {
    use crate::remote::{Connect, RemoteImageStage, serve_host_connection};
    use sha2::{Digest as _, Sha256};
    use std::io::Seek;
    use tokio::net::{UnixListener, UnixStream};
    struct Dial(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Connect for Dial {
        async fn connect(&self) -> std::io::Result<UnixStream> {
            UnixStream::connect(&self.0).await
        }
    }

    let (dir, pool, ctl) = setup()?;
    let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
    let (bytes, digest) = crate::image::tests::fixture(false);
    let archive = dir.path().join("verified.tar");
    std::fs::write(&archive, bytes)?;
    let mut built = crate::image::build_plain(&archive, digest, dir.path())?;
    let physical_bytes = built.file.as_file().metadata()?.len();
    built.file.as_file_mut().rewind()?;
    let mut sha = Sha256::new();
    std::io::copy(built.file.as_file_mut(), &mut sha)?;
    let erofs_sha256: Digest = sha.finalize().into();

    let sockets = tempfile::tempdir_in("/tmp")?;
    let dial = Dial(sockets.path().join("dom0.sock"));
    let listener = UnixListener::bind(&dial.0)?;

    // 1. StageErofs allocates `stage/<token>` directly in the bare pool and promotes atomically.
    let accept = async {
        let (socket, _) = listener.accept().await?;
        serve_host_connection(socket, &b).await
    };
    let upload = async {
        let stage = RemoteImageStage::upload_erofs(
            &dial,
            built.file.path(),
            digest,
            physical_bytes,
            erofs_sha256,
            built.defaults.clone(),
        )
        .await?;
        assert!(!stage.source.staged_extent.is_empty());
        assert_eq!(b.disk_accounting()?.2, 0); // zero bytes on staging filesystem
        assert!(b.pool.has_object(&format!("stage/{}", stage.source.staged_extent))?);
        let artifact = b.install_image_artifact(&stage.source).await?;
        assert_eq!(artifact.physical_bytes, physical_bytes);
        assert!(!b.pool.has_object(&format!("stage/{}", stage.source.staged_extent))?);
        assert!(b.image_installed(digest)?);
        drop(stage);
        Ok::<_, Error>(())
    };
    tokio::try_join!(accept, upload)?;

    // 2. Second StageErofs for the same manifest short-circuits with 0-byte transfer.
    let accept = async {
        let (socket, _) = listener.accept().await?;
        serve_host_connection(socket, &b).await
    };
    let short_circuit = async {
        let stage = RemoteImageStage::upload_erofs(
            &dial,
            built.file.path(),
            digest,
            physical_bytes,
            erofs_sha256,
            built.defaults.clone(),
        )
        .await?;
        assert!(stage.source.staged_extent.is_empty());
        let artifact = b.install_image_artifact(&stage.source).await?;
        assert_eq!(artifact.physical_bytes, physical_bytes);
        Ok::<_, Error>(())
    };
    tokio::try_join!(accept, short_circuit)?;

    // 3. Idempotent promotion when a duplicate `stage/<token>` exists alongside an installed image.
    let dup_token = "a".repeat(32);
    let dup_obj = format!("stage/{dup_token}");
    b.pool.allocate(&dup_obj, physical_bytes)?;
    b.catalog()
        .put(&dup_obj, &(digest, built.defaults.clone(), physical_bytes))?;
    let artifact = b
        .install_image_artifact(&ImageSource {
            manifest_digest: digest,
            staged_extent: dup_token.clone(),
            physical_bytes,
        })
        .await?;
    assert_eq!(artifact.physical_bytes, physical_bytes);
    assert!(!b.pool.has_object(&dup_obj)?);

    // 4. Crash recovery: orphaned `stage/*` object is discarded by `recover_transients` on reopen.
    let orphan_token = "b".repeat(32);
    let orphan_obj = format!("stage/{orphan_token}");
    b.pool.allocate(&orphan_obj, physical_bytes)?;
    drop(b);
    let reopened = open(&pool, &ctl, options(dir.path()))?;
    assert!(reopened.pool.has_object(&orphan_obj)?);
    reopened.recover_transients().await?;
    assert!(!reopened.pool.has_object(&orphan_obj)?);
    Ok(())
}

#[tokio::test]
async fn erofs_bundle_local_and_remote_streaming_short_circuit_and_rejects_adversarial_inputs()
-> Result<()> {
    use crate::remote::{Connect, RemoteImageStage, serve_host_connection};
    use sha2::{Digest as _, Sha256};
    use tokio::net::{UnixListener, UnixStream};
    use vmm_core::erofs_bundle::{
        EROFS_BLOCK_BYTES, EROFS_SUPER_MAGIC_LE, EROFS_SUPER_OFFSET, ErofsBundleCompression,
        ErofsBundleHeader, encode_frame, parse_and_validate_file,
    };

    struct Dial(std::path::PathBuf);
    #[async_trait::async_trait]
    impl Connect for Dial {
        async fn connect(&self) -> std::io::Result<UnixStream> {
            UnixStream::connect(&self.0).await
        }
    }

    let make_zstd_bundle = |manifest: Digest,
                            erofs_bytes: &[u8],
                            declared_erofs_bytes: u64,
                            declared_sha: Digest,
                            defaults: crate::image::Defaults|
     -> Vec<u8> {
        let compressed = zstd::stream::encode_all(erofs_bytes, 1).unwrap();
        let header = ErofsBundleHeader {
            schema_version: 1,
            manifest_digest: format!("sha256:{}", hex::encode(manifest)),
            platform: "linux/arm64".into(),
            logical_bytes: 987_654,
            erofs_bytes: declared_erofs_bytes,
            erofs_sha256: hex::encode(declared_sha),
            compression: ErofsBundleCompression::Zstd,
            defaults,
        };
        encode_frame(&header, &compressed).unwrap()
    };

    // Build a real EROFS image via built-in plain writer.
    let build_dir = tempfile::tempdir()?;
    let (oci_bytes, manifest) = crate::image::tests::fixture(false);
    let oci_path = build_dir.path().join("image.oci.tar");
    std::fs::write(&oci_path, oci_bytes)?;
    let built = crate::image::build_plain(&oci_path, manifest, build_dir.path())?;
    let real_erofs = std::fs::read(built.file.path())?;
    let real_sha: Digest = Sha256::digest(&real_erofs).into();
    let valid_bundle = make_zstd_bundle(
        manifest,
        &real_erofs,
        real_erofs.len() as u64,
        real_sha,
        built.defaults.clone(),
    );

    // 1. LocalDom0Backend::stage_image -> install -> second stage_image short-circuits.
    {
        let (dir, pool, ctl) = setup()?;
        let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
        let bundle_path = dir.path().join("valid.erofs.bundle");
        std::fs::write(&bundle_path, &valid_bundle)?;

        let staged = b.stage_image(&bundle_path, manifest)?;
        assert!(!staged.staged_extent.is_empty());
        assert_eq!(b.disk_accounting()?.2, 0);
        let artifact = b.install_image_artifact(&staged).await?;
        assert_eq!(artifact.manifest_digest, manifest);
        assert!(b.image_installed(manifest)?);
        assert_eq!(
            b.catalog().defaults(&crate::catalog::image_id(manifest))?,
            Some(built.defaults.clone())
        );

        let hit = b.stage_image(&bundle_path, manifest)?;
        assert!(hit.staged_extent.is_empty());
        b.discard_staged_image(&hit.staged_extent)?;
        let artifact2 = b.install_image_artifact(&hit).await?;
        assert_eq!(artifact2.physical_bytes, artifact.physical_bytes);
    }

    // 2. RemoteImageStage::upload_erofs_bundle -> serve_erofs -> install + AlreadyInstalled short-circuit.
    {
        let (dir, pool, ctl) = setup()?;
        let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
        let bundle_path = dir.path().join("remote.erofs.bundle");
        std::fs::write(&bundle_path, &valid_bundle)?;
        let parsed = parse_and_validate_file(&bundle_path, Some(manifest))?.unwrap();

        let sockets = tempfile::tempdir_in("/tmp")?;
        let dial = Dial(sockets.path().join("dom0.sock"));
        let listener = UnixListener::bind(&dial.0)?;

        let accept = async {
            let (socket, _) = listener.accept().await?;
            serve_host_connection(socket, &b).await
        };
        let upload = async {
            let stage = RemoteImageStage::upload_erofs_bundle(&dial, &bundle_path, &parsed).await?;
            assert!(!stage.source.staged_extent.is_empty());
            assert_eq!(b.disk_accounting()?.2, 0);
            let artifact = b.install_image_artifact(&stage.source).await?;
            assert_eq!(artifact.manifest_digest, manifest);
            assert!(b.image_installed(manifest)?);
            drop(stage);
            Ok::<_, Error>(())
        };
        tokio::try_join!(accept, upload)?;

        // Overwrite payload with garbage after header to prove AlreadyInstalled never decompresses.
        let mut corrupted = valid_bundle.clone();
        let off = parsed.payload_offset as usize;
        corrupted[off] ^= 0xff;
        std::fs::write(&bundle_path, &corrupted)?;
        let accept = async {
            let (socket, _) = listener.accept().await?;
            serve_host_connection(socket, &b).await
        };
        let hit = async {
            let stage = RemoteImageStage::upload_erofs_bundle(&dial, &bundle_path, &parsed).await?;
            assert!(stage.source.staged_extent.is_empty());
            let artifact = b.install_image_artifact(&stage.source).await?;
            assert_eq!(artifact.manifest_digest, manifest);
            Ok::<_, Error>(())
        };
        tokio::try_join!(accept, hit)?;
    }

    // 3. Adversarial inputs on both Local and Remote paths must fail and leak 0 pool bytes.
    let mut bad_sha = real_sha;
    bad_sha[0] ^= 1;
    let tampered_sha = make_zstd_bundle(
        manifest,
        &real_erofs,
        real_erofs.len() as u64,
        bad_sha,
        built.defaults.clone(),
    );

    let mut fake_super = vec![0u8; (EROFS_BLOCK_BYTES * 2) as usize];
    fake_super[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4].copy_from_slice(b"BAD!");
    let bad_super = make_zstd_bundle(
        manifest,
        &fake_super,
        fake_super.len() as u64,
        Sha256::digest(&fake_super).into(),
        built.defaults.clone(),
    );

    let mut oversized = vec![0u8; (EROFS_BLOCK_BYTES * 2) as usize];
    oversized[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4].copy_from_slice(&EROFS_SUPER_MAGIC_LE);
    let bomb = make_zstd_bundle(
        manifest,
        &oversized,
        EROFS_BLOCK_BYTES, // declares 4096, decompresses to 8192
        Sha256::digest(&oversized[..EROFS_BLOCK_BYTES as usize]).into(),
        built.defaults.clone(),
    );

    for (name, bytes) in [
        ("tampered-sha", tampered_sha),
        ("bad-superblock", bad_super),
        ("zstd-overflow", bomb),
    ] {
        // Local path
        let (dir, pool, ctl) = setup()?;
        let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
        let path = dir.path().join(format!("{name}.bundle"));
        std::fs::write(&path, &bytes)?;
        let err = b.stage_image(&path, manifest).unwrap_err();
        assert!(
            matches!(err, Error::Invalid(_)),
            "local {name} should return Error::Invalid, got {err:?}"
        );
        assert_eq!(
            b.disk_accounting()?.0,
            0,
            "local {name} leaked pool bytes"
        );
        assert!(b.catalog().staged_tokens()?.is_empty());

        // Remote path
        let parsed = parse_and_validate_file(&path, Some(manifest))?.unwrap();
        let sockets = tempfile::tempdir_in("/tmp")?;
        let dial = Dial(sockets.path().join("dom0.sock"));
        let listener = UnixListener::bind(&dial.0)?;
        let (server, client) = tokio::join!(
            async {
                let (socket, _) = listener.accept().await?;
                serve_host_connection(socket, &b).await
            },
            RemoteImageStage::upload_erofs_bundle(&dial, &path, &parsed),
        );
        let _ = server;
        let err = client.err().expect("remote adversarial bundle must fail");
        assert!(
            matches!(err, Error::Invalid(_)),
            "remote {name} should return Error::Invalid, got {err:?}"
        );
        assert_eq!(
            b.disk_accounting()?.0,
            0,
            "remote {name} leaked pool bytes"
        );
    }

    // 4. Direct serve_erofs also rejects raw EROFS upload with forged superblock magic.
    {
        let (dir, pool, ctl) = setup()?;
        let b = Arc::new(open(&pool, &ctl, options(dir.path()))?);
        let raw_bad = vec![0u8; EROFS_BLOCK_BYTES as usize];
        let raw_sha: Digest = Sha256::digest(&raw_bad).into();
        let raw_path = dir.path().join("bad.erofs");
        std::fs::write(&raw_path, &raw_bad)?;

        let sockets = tempfile::tempdir_in("/tmp")?;
        let dial = Dial(sockets.path().join("dom0.sock"));
        let listener = UnixListener::bind(&dial.0)?;
        let (server, client) = tokio::join!(
            async {
                let (socket, _) = listener.accept().await?;
                serve_host_connection(socket, &b).await
            },
            RemoteImageStage::upload_erofs(
                &dial,
                &raw_path,
                manifest,
                raw_bad.len() as u64,
                raw_sha,
                built.defaults.clone(),
            ),
        );
        server?;
        assert!(matches!(client, Err(Error::Invalid(_))));
        assert_eq!(b.disk_accounting()?.0, 0);
    }

    Ok(())
}
