#![forbid(unsafe_code)]
//! QUIC carrier transport for macOS Apple VZ (`l4h` dom0 <-> `agent-vmm-carrier`).
//!
//! Listens on the dedicated point-to-point carrier link (`169.254.77.2:8180`)
//! and multiplexes:
//! - `0x01` (`OP_CONTROL`): carrier -> dom0 control RPC (`serve_host_connection`)
//! - `0x02` (`OP_GUEST_RELAY`): carrier -> dom0 guest TCP relay (`serve_guest_relay`)
//! - `0x03` (`OP_HOST_SHARE`): dom0 -> carrier reverse 9p2000.L stream for host shares
//! - `0x04` (`OP_SHUTDOWN`): carrier -> dom0 graceful shutdown request

use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::TcpStream,
    sync::Semaphore,
    time::timeout,
};
use vmm_dom0::{
    backend::LocalDom0Backend,
    host_share::RemoteShareDialer,
    local::Result,
    remote::serve_host_connection,
};

pub const ALPN: &[u8] = b"agent-vmm-carrier/1";
pub const OP_CONTROL: u8 = 0x01;
pub const OP_GUEST_RELAY: u8 = 0x02;
pub const OP_HOST_SHARE: u8 = 0x03;
pub const OP_SHUTDOWN: u8 = 0x04;

#[derive(Clone, Default)]
pub struct QuicShareDialer {
    conn: Arc<RwLock<Option<quinn::Connection>>>,
}

impl QuicShareDialer {
    fn set_connection(&self, conn: quinn::Connection) {
        if let Ok(mut guard) = self.conn.write() {
            *guard = Some(conn);
        }
    }

    fn connection(&self) -> io::Result<quinn::Connection> {
        self.conn
            .read()
            .ok()
            .and_then(|g| g.clone())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "carrier QUIC disconnected"))
    }
}

#[async_trait::async_trait]
impl RemoteShareDialer for QuicShareDialer {
    async fn bridge(
        &self,
        share_id: &str,
        read_only: bool,
        mut tcp: TcpStream,
    ) -> io::Result<()> {
        let conn = self.connection()?;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| io::Error::other(format!("carrier QUIC open_bi: {e}")))?;
        let name_bytes = share_id.as_bytes();
        let name_len = u16::try_from(name_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "share id too long"))?;
        send.write_u8(OP_HOST_SHARE).await?;
        send.write_u8(u8::from(read_only)).await?;
        send.write_u16(name_len).await?;
        send.write_all(name_bytes).await?;
        let status = timeout(Duration::from_secs(5), recv.read_u8())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "host share ack timed out"))??;
        if status != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "carrier rejected host share",
            ));
        }
        let mut quic_stream = tokio::io::join(recv, send);
        copy_bidirectional(&mut tcp, &mut quic_stream).await?;
        Ok(())
    }
}

fn server_config() -> Result<quinn::ServerConfig> {
    let certified = rcgen::generate_simple_self_signed(vec!["l4h-dom0".to_owned()])?;
    let cert = CertificateDer::from(certified.cert);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let mut transport = quinn::TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    transport.max_idle_timeout(Some(Duration::from_secs(30).try_into()?));
    transport.max_concurrent_bidi_streams(1024u32.into());
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    cfg.transport_config(Arc::new(transport));
    Ok(cfg)
}

pub struct Server {
    endpoint: quinn::Endpoint,
    dialer: Arc<QuicShareDialer>,
}

impl Server {
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let cfg = server_config()?;
        let endpoint = quinn::Endpoint::server(cfg, addr)?;
        Ok(Self {
            endpoint,
            dialer: Arc::new(QuicShareDialer::default()),
        })
    }

    pub fn dialer(&self) -> Arc<QuicShareDialer> {
        self.dialer.clone()
    }

    pub async fn serve(self, backend: Arc<LocalDom0Backend>) -> Result<()> {
        let control_permits = Arc::new(Semaphore::new(128));
        let relay_permits = Arc::new(Semaphore::new(1024));
        while let Some(incoming) = self.endpoint.accept().await {
            let backend = backend.clone();
            let dialer = self.dialer.clone();
            let control_permits = control_permits.clone();
            let relay_permits = relay_permits.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else {
                    return;
                };
                dialer.set_connection(conn.clone());
                loop {
                    let Ok((mut send, mut recv)) = conn.accept_bi().await else {
                        break;
                    };
                    let Ok(Ok(op)) = timeout(Duration::from_secs(5), recv.read_u8()).await else {
                        continue;
                    };
                    match op {
                        OP_CONTROL => {
                            let Ok(permit) = control_permits.clone().acquire_owned().await else {
                                break;
                            };
                            let backend = backend.clone();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let stream = tokio::io::join(recv, send);
                                let _ = serve_host_connection(stream, &backend).await;
                            });
                        }
                        OP_GUEST_RELAY => {
                            let Ok(permit) = relay_permits.clone().acquire_owned().await else {
                                break;
                            };
                            tokio::spawn(async move {
                                let _permit = permit;
                                let stream = tokio::io::join(recv, send);
                                let _ = crate::relay::serve_guest_relay(
                                    stream,
                                    tokio::net::TcpStream::connect,
                                )
                                .await;
                            });
                        }
                        OP_SHUTDOWN => {
                            // Stop active sandbox VMs, checkpoint pool.db WAL and flush
                            // backing storage, then ACK the carrier so VZVirtualMachine
                            // can stop cleanly.
                            use vmm_core::backend::Dom0Backend;
                            if let Ok(live) = backend.list_live_vms().await {
                                for vm in live {
                                    let _ = backend.shutdown_vm(&vm.incarnation.boot).await;
                                }
                            }
                            backend.flush_for_shutdown();
                            let _ = tokio::process::Command::new("/bin/sync").status().await;
                            let _ = send.write_u8(0).await;
                            let _ = send.finish();
                            break;
                        }
                        _ => {}
                    }
                }
            });
        }
        Ok(())
    }
}
