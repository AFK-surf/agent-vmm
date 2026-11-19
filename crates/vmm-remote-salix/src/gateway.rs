use crate::{
    Error, Result, host, registration::RemoteConnection,
    remote::remote_controller_service_client::RemoteControllerServiceClient,
    trust::personal_mesh_registry_service_client::PersonalMeshRegistryServiceClient,
};
use agent_vmm_proto::google::rpc;
use base64::{Engine, engine::general_purpose::STANDARD};
use hyper_util::rt::TokioIo;
use prost::Message;
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::{
    Code, Request, Status,
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, Endpoint},
};
use vmm_signing::{CredentialStore, MAX_CREDENTIAL_BYTES};
use zeroize::Zeroizing;

const MAX_MESSAGE_BYTES: usize = 16 << 20;
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE: Duration = Duration::from_secs(30);

type AuthChannel = InterceptedService<Channel, GatewayAuthorization>;
pub type RegistryClient = PersonalMeshRegistryServiceClient<AuthChannel>;
pub(crate) type ControlClient = RemoteControllerServiceClient<AuthChannel>;

#[derive(Clone)]
pub struct Gateway {
    channel: Channel,
    authorization: GatewayAuthorization,
}

#[derive(Clone)]
pub struct GatewayAuthorization {
    registration: MetadataValue<Ascii>,
    registration_id: Arc<str>,
    credentials: Arc<dyn CredentialStore>,
}

impl Gateway {
    pub fn new(
        registration_id: &str,
        config: &RemoteConnection,
        credentials: Arc<dyn CredentialStore>,
    ) -> Result<Self> {
        let endpoint = gateway_channel(config)?;
        Self::with_channel(registration_id, endpoint, credentials)
    }

    pub(crate) fn with_channel(
        registration_id: &str,
        channel: Channel,
        credentials: Arc<dyn CredentialStore>,
    ) -> Result<Self> {
        if registration_id.is_empty()
            || registration_id.len() > 128
            || !registration_id.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::Invalid("gateway registration"));
        }
        let registration = registration_id
            .parse()
            .map_err(|_| Error::Invalid("gateway registration"))?;
        Ok(Self {
            channel,
            authorization: GatewayAuthorization {
                registration,
                registration_id: registration_id.into(),
                credentials,
            },
        })
    }

    pub fn registration_id(&self) -> &str {
        &self.authorization.registration_id
    }

    pub fn registry(&self) -> RegistryClient {
        PersonalMeshRegistryServiceClient::with_interceptor(
            self.channel.clone(),
            self.authorization.clone(),
        )
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    pub(crate) fn controller(&self) -> ControlClient {
        RemoteControllerServiceClient::with_interceptor(
            self.channel.clone(),
            self.authorization.clone(),
        )
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }
}

fn gateway_channel(config: &RemoteConnection) -> Result<Channel> {
    let authority = config
        .gateway_endpoint
        .parse::<tonic::codegen::http::uri::Authority>()
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
    let peer_name =
        ServerName::try_from(host.clone()).map_err(|_| Error::Invalid("gateway endpoint host"))?;
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
    tls.alpn_protocols = vec![b"h2".to_vec()];
    let tls = TlsConnector::from(Arc::new(tls));
    let origin = format!("https://{authority}")
        .parse()
        .map_err(|_| Error::Invalid("gateway origin"))?;
    // The connector below performs TLS. The origin still supplies HTTPS to gRPC requests.
    let endpoint = Endpoint::from_shared(format!("http://{authority}"))?
        .origin(origin)
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .initial_stream_window_size(8 << 20)
        .initial_connection_window_size(16 << 20);
    Ok(
        endpoint.connect_with_connector_lazy(tower::service_fn(move |_| {
            let host = host.clone();
            let tls = tls.clone();
            let peer = peer_name.clone();
            async move {
                let socket = TcpStream::connect((host.as_str(), port)).await?;
                socket.set_nodelay(true)?;
                let stream = tls.connect(peer, socket).await?;
                if stream.get_ref().1.alpn_protocol() != Some(b"h2") {
                    return Err(io::Error::other("gateway did not negotiate HTTP/2"));
                }
                Ok(TokioIo::new(stream))
            }
        })),
    )
}

impl Interceptor for GatewayAuthorization {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let credential = self
            .credentials
            .load(&self.registration_id)
            .map_err(|_| Status::unauthenticated("registration credential unavailable"))?;
        if !(16..=MAX_CREDENTIAL_BYTES).contains(&credential.len()) {
            return Err(Status::unauthenticated("registration credential invalid"));
        }
        let mut encoded = Zeroizing::new(String::from("Bearer "));
        STANDARD.encode_string(&*credential, &mut encoded);
        let mut bearer: MetadataValue<Ascii> = encoded
            .parse()
            .map_err(|_| Status::unauthenticated("registration credential invalid"))?;
        bearer.set_sensitive(true);
        request.metadata_mut().insert("authorization", bearer);
        request
            .metadata_mut()
            .insert("x-agent-vmm-registration", self.registration.clone());
        Ok(request)
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
            .map_or_else(|| Error::Grpc(Box::new(status)), Error::ProtocolUnsupported),
        _ => Error::Grpc(Box::new(status)),
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
