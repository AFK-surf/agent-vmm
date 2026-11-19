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
