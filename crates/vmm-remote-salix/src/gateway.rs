use crate::{
    Error, Result, host, registration::RemoteConnection,
    remote::remote_controller_service_client::RemoteControllerServiceClient,
    trust::personal_mesh_registry_service_client::PersonalMeshRegistryServiceClient,
};
use agent_vmm_proto::google::rpc;
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use vmm_rpc::{Client, Code, Metadata, Mode, MuxSession, Router, Status, Transport};
use vmm_signing::{CredentialStore, MAX_CREDENTIAL_BYTES};
use zeroize::Zeroizing;

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
pub type RegistryClient = PersonalMeshRegistryServiceClient;
pub(crate) type ControlClient = RemoteControllerServiceClient;

#[derive(Clone)]
pub struct Gateway {
    host: String,
    port: u16,
    tls: TlsConnector,
    peer: ServerName<'static>,
    registration: Arc<str>,
    credentials: Arc<dyn CredentialStore>,
    current: Arc<Mutex<Option<(u64, Client)>>>,
}

pub(crate) struct Connection {
    gateway: Gateway,
    epoch: u64,
    mux: MuxSession,
    pub controller: ControlClient,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let mut current = self.gateway.current.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|(epoch, _)| *epoch == self.epoch)
        {
            *current = None;
        }
        self.mux.close();
    }
}

impl Gateway {
    pub fn new(
        registration_id: &str,
        config: &RemoteConnection,
        credentials: Arc<dyn CredentialStore>,
    ) -> Result<Self> {
        if registration_id.is_empty()
            || registration_id.len() > 128
            || !registration_id.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(Error::Invalid("gateway registration"));
        }
        let authority = config
            .gateway_endpoint
            .parse::<http::uri::Authority>()
            .map_err(|_| Error::Invalid("gateway endpoint"))?;
        let port = authority
            .port_u16()
            .filter(|port| *port > 0)
            .ok_or(Error::Invalid("gateway endpoint port"))?;
        if authority.as_str().contains('@') {
            return Err(Error::Invalid("gateway endpoint"));
        }
        let host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        let peer_name = ServerName::try_from(host.clone())
            .map_err(|_| Error::Invalid("gateway endpoint host"))?;
        let pem = STANDARD
            .decode(&config.trust_bundle)
            .map_err(|_| Error::Invalid("gateway trust bundle"))?;
        let mut roots = RootCertStore::empty();
        for certificate in rustls_pemfile::certs(&mut pem.as_slice()) {
            roots
                .add(certificate.map_err(|_| Error::Invalid("gateway trust bundle"))?)
                .map_err(|_| Error::Invalid("gateway trust bundle"))?;
        }
        if roots.is_empty() {
            return Err(Error::Invalid("gateway trust bundle"));
        }
        let mut tls =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|_| Error::Invalid("gateway TLS configuration"))?
                .with_root_certificates(roots)
                .with_no_client_auth();
        tls.alpn_protocols = vec![vmm_rpc::ALPN.to_vec()];
        Ok(Self {
            host,
            port,
            tls: TlsConnector::from(Arc::new(tls)),
            peer: peer_name,
            registration: registration_id.into(),
            credentials,
            current: Default::default(),
        })
    }

    pub fn registration_id(&self) -> &str {
        &self.registration
    }

    fn authorization(
        &self,
    ) -> impl Fn(&mut Metadata) -> std::result::Result<(), Status> + Send + Sync + 'static {
        let credentials = self.credentials.clone();
        let registration = self.registration.clone();
        move |metadata| {
            let credential = credentials
                .load(&registration)
                .map_err(|_| Status::unauthenticated("registration credential unavailable"))?;
            if !(16..=MAX_CREDENTIAL_BYTES).contains(&credential.len()) {
                return Err(Status::unauthenticated("registration credential invalid"));
            }
            let mut bearer = Zeroizing::new(String::from("Bearer "));
            STANDARD.encode_string(&*credential, &mut bearer);
            metadata.insert("authorization", bearer.as_str());
            metadata.insert("x-agentvmm-registration-id", registration.as_ref());
            Ok(())
        }
    }

    pub(crate) async fn connect(&self, router: Router, epoch: u64) -> Result<Connection> {
        let io = tokio::time::timeout(CONNECT_TIMEOUT, async {
            let socket = TcpStream::connect((self.host.as_str(), self.port)).await?;
            socket.set_nodelay(true)?;
            let tls = self.tls.connect(self.peer.clone(), socket).await?;
            if tls.get_ref().1.alpn_protocol() != Some(vmm_rpc::ALPN) {
                return Err(io::Error::other("gateway did not negotiate vmm-rpc"));
            }
            Ok(tls)
        })
        .await
        .map_err(|_| Error::Deadline)??;
        let mux = MuxSession::start(io, Mode::Client, router);
        let client = mux.client().authorized(self.authorization());
        *self.current.lock().unwrap() = Some((epoch, client.clone()));
        Ok(Connection {
            gateway: self.clone(),
            epoch,
            mux,
            controller: ControlClient::new(client),
        })
    }

    pub fn registry(&self) -> RegistryClient {
        let client = self
            .current
            .lock()
            .unwrap()
            .as_ref()
            .map(|(_, client)| client.clone())
            .unwrap_or_else(|| {
                Client::new(|| {
                    Box::pin(async {
                        Err::<Transport, _>(Status::unavailable("gateway disconnected"))
                    })
                })
            });
        RegistryClient::new(client)
    }
}
#[derive(prost::Message)]
pub(crate) struct ErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: String,
    #[prost(string, tag = "2")]
    pub domain: String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata: HashMap<String, String>,
}

pub(crate) fn status_error(status: Status) -> Error {
    match status.code() {
        Code::Unauthenticated | Code::PermissionDenied => Error::Authentication(Box::new(status)),
        Code::FailedPrecondition => protocol_refusal(&status)
            .map_or_else(|| Error::Rpc(Box::new(status)), Error::ProtocolUnsupported),
        _ => Error::Rpc(Box::new(status)),
    }
}

fn protocol_refusal(status: &Status) -> Option<host::RemoteProtocolStatus> {
    let details = rpc::Status::decode(status.details()).ok()?;
    for detail in details.details {
        if detail.type_url.rsplit('/').next() != Some("google.rpc.ErrorInfo") {
            continue;
        }
        let info = ErrorInfo::decode(&*detail.value).ok()?;
        if info.domain == "agent-vmm" && info.reason == "REMOTE_PROTOCOL_UNSUPPORTED" {
            let text = |key| info.metadata.get(key).cloned().unwrap_or_default();
            let number = |key| text(key).parse().unwrap_or_default();
            return Some(host::RemoteProtocolStatus {
                host_protocol: text("host_protocol"),
                min_protocol: number("min_protocol"),
                max_protocol: number("max_protocol"),
                host_side: text("host_side"),
            });
        }
    }
    None
}
