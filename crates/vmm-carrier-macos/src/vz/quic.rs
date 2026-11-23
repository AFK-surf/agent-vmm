//! QUIC client over `vz::netlink::NetlinkSocket` connecting `agent-vmm-carrier`
//! to `l4h-dom0d` (`169.254.77.2:8180`) for control RPC, guest relay, graceful
//! shutdown, and reverse 9p2000.L host-share streams.

use super::netlink::{GUEST_IP, GUEST_QUIC_PORT, NetlinkSocket};
use crate::protocol::{CONTROL_PORT, GUEST_PORT, Shares};
use quinn::{
    ClientConfig, Connection, Endpoint, EndpointConfig, TransportConfig,
    crypto::rustls::QuicClientConfig,
};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use std::{
    fs::File,
    io,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::UnixStream,
    sync::{Mutex, watch},
    task::JoinHandle,
    time::timeout,
};

const ALPN: &[u8] = b"agent-vmm-carrier/1";
const OP_CONTROL: u8 = 0x01;
const OP_GUEST_RELAY: u8 = 0x02;
const OP_HOST_SHARE: u8 = 0x03;
const OP_SHUTDOWN: u8 = 0x04;

/// Accepts the ephemeral self-signed certificate presented by `l4h-dom0d` over
/// the private point-to-point `VZFileHandleNetworkDeviceAttachment` socketpair.
#[derive(Debug)]
struct LinkLocalVerifier;

impl ServerCertVerifier for LinkLocalVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config() -> io::Result<ClientConfig> {
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| io::Error::other(e.to_string()))?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(LinkLocalVerifier))
    .with_no_client_auth();
    tls.alpn_protocols = vec![ALPN.to_vec()];
    let mut transport = TransportConfig::default();
    transport.keep_alive_interval(Some(Duration::from_secs(15)));
    transport.max_idle_timeout(Some(
        Duration::from_secs(60)
            .try_into()
            .map_err(|e| io::Error::other(format!("{e}")))?,
    ));
    transport.max_concurrent_bidi_streams(1024u32.into());
    let quic_tls = QuicClientConfig::try_from(tls).map_err(|e| io::Error::other(e.to_string()))?;
    let mut cfg = ClientConfig::new(Arc::new(quic_tls));
    cfg.transport_config(Arc::new(transport));
    Ok(cfg)
}

#[derive(Clone)]
pub(super) struct CarrierQuicClient {
    endpoint: Endpoint,
    conn: Arc<Mutex<Option<Connection>>>,
    shares: Arc<RwLock<Shares>>,
    stop_tx: Arc<watch::Sender<bool>>,
    reverse_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl CarrierQuicClient {
    pub(super) fn new(
        host_socket: std::os::unix::net::UnixDatagram,
        shares: Arc<RwLock<Shares>>,
    ) -> io::Result<Self> {
        let sock = NetlinkSocket::new(host_socket)?;
        let runtime = quinn::default_runtime()
            .ok_or_else(|| io::Error::other("missing tokio runtime for QUIC"))?;
        let mut endpoint =
            Endpoint::new_with_abstract_socket(EndpointConfig::default(), None, sock, runtime)?;
        endpoint.set_default_client_config(client_config()?);
        let (stop_tx, _) = watch::channel(false);
        Ok(Self {
            endpoint,
            conn: Arc::new(Mutex::new(None)),
            shares,
            stop_tx: Arc::new(stop_tx),
            reverse_task: Arc::new(Mutex::new(None)),
        })
    }

    async fn connection(&self) -> io::Result<Connection> {
        let mut guard = self.conn.lock().await;
        if let Some(conn) = guard.as_ref()
            && conn.close_reason().is_none()
        {
            return Ok(conn.clone());
        }
        let addr = SocketAddr::V4(SocketAddrV4::new(GUEST_IP, GUEST_QUIC_PORT));
        let connecting = self
            .endpoint
            .connect(addr, "l4h-dom0")
            .map_err(|e| io::Error::other(format!("carrier QUIC connect: {e}")))?;
        let conn = connecting
            .await
            .map_err(|e| io::Error::other(format!("carrier QUIC handshake: {e}")))?;
        *guard = Some(conn.clone());
        let shares = self.shares.clone();
        let stop = self.stop_tx.subscribe();
        let rev_conn = conn.clone();
        let mut rev_guard = self.reverse_task.lock().await;
        if let Some(old) = rev_guard.take() {
            old.abort();
        }
        *rev_guard = Some(tokio::spawn(async move {
            serve_reverse_shares(rev_conn, shares, stop).await;
        }));
        Ok(conn)
    }

    /// Waits until `l4h-dom0d` finishes booting and accepts a QUIC handshake on
    /// the carrier link, retrying short handshake attempts until `budget` elapses.
    pub(super) async fn wait_ready(&self, budget: Duration) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + budget;
        let mut last_err = None;
        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(last_err.unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::TimedOut, "timed out waiting for dom0 QUIC")
                }));
            }
            let attempt = (deadline - now).min(Duration::from_secs(2));
            match timeout(attempt, self.connection()).await {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => last_err = Some(e),
                Err(_) => {
                    last_err = Some(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "dom0 QUIC handshake attempt timed out",
                    ));
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub(super) async fn open_stream(&self, port: u32) -> io::Result<UnixStream> {
        let op = match port {
            CONTROL_PORT => OP_CONTROL,
            GUEST_PORT => OP_GUEST_RELAY,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported carrier port {port}"),
                ));
            }
        };
        let conn = self.connection().await?;
        let (mut send, recv) = conn
            .open_bi()
            .await
            .map_err(|e| io::Error::other(format!("carrier QUIC open_bi: {e}")))?;
        send.write_u8(op).await?;
        let (client_end, mut bridge_end) = UnixStream::pair()?;
        tokio::spawn(async move {
            let mut quic_stream = tokio::io::join(recv, send);
            let _ = copy_bidirectional(&mut bridge_end, &mut quic_stream).await;
        });
        Ok(client_end)
    }

    pub(super) async fn request_shutdown(&self) -> io::Result<()> {
        let conn = timeout(Duration::from_secs(5), self.connection())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "carrier QUIC shutdown connect"))??;
        let (mut send, mut recv) = conn
            .open_bi()
            .await
            .map_err(|e| io::Error::other(format!("carrier QUIC shutdown open_bi: {e}")))?;
        send.write_u8(OP_SHUTDOWN).await?;
        let _ = send.finish();
        let ack = timeout(Duration::from_secs(20), recv.read_u8())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "dom0 shutdown ack timed out"))??;
        if ack != 0 {
            return Err(io::Error::other("dom0 graceful shutdown failed"));
        }
        Ok(())
    }

    pub(super) fn close(&self) {
        self.stop_tx.send_replace(true);
        self.endpoint.close(0u32.into(), b"carrier stopped");
    }
}

async fn serve_reverse_shares(
    conn: Connection,
    shares: Arc<RwLock<Shares>>,
    stop: watch::Receiver<bool>,
) {
    loop {
        let Ok((mut send, mut recv)) = conn.accept_bi().await else {
            break;
        };
        let shares = shares.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let Ok(Ok(op)) = timeout(Duration::from_secs(5), recv.read_u8()).await else {
                return;
            };
            if op != OP_HOST_SHARE {
                let _ = send.write_u8(1).await;
                return;
            }
            let Ok(ro) = recv.read_u8().await else {
                return;
            };
            let Ok(len) = recv.read_u16().await else {
                return;
            };
            let mut name_buf = vec![0u8; len as usize];
            if recv.read_exact(&mut name_buf).await.is_err() {
                return;
            }
            let Ok(name) = String::from_utf8(name_buf) else {
                let _ = send.write_u8(1).await;
                return;
            };
            let path = shares.read().ok().and_then(|g| g.get(&name).cloned());
            let Some(path) = path else {
                let _ = send.write_u8(1).await;
                return;
            };
            let Ok(file) = File::open(&path) else {
                let _ = send.write_u8(1).await;
                return;
            };
            if !file.metadata().is_ok_and(|m| m.is_dir()) {
                let _ = send.write_u8(1).await;
                return;
            }
            if send.write_u8(0).await.is_err() {
                return;
            }
            let stream = tokio::io::join(recv, send);
            let _ = vmm_dom0::host_share::serve_stream(stream, Arc::new(file), name, ro != 0, stop)
                .await;
        });
    }
}
