use base64::{Engine, engine::general_purpose::STANDARD};
use hyper_util::rt::TokioIo;
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex, split},
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Endpoint, Identity, Server, ServerTlsConfig},
};
use vmm_remote_salix::{
    CandidateCredential, RemoteConnection,
    remote::{
        ConnectRequest, ConnectResponse, ControllerReady, HostHello, ServeSessionRequest,
        ServeSessionResponse, SessionHeader, connect_request, connect_response,
        remote_controller_service_server::{
            RemoteControllerService, RemoteControllerServiceServer,
        },
        serve_session_request, serve_session_response,
    },
};

use super::registration::{REGISTRATION, configuration};

type Stream<T> = ReceiverStream<Result<T, Status>>;

pub struct GatewayMock {
    pub connections: mpsc::Receiver<Control>,
    pub tunnels: mpsc::Receiver<Session>,
    pub settings: RemoteConnection,
    pub mode: Arc<AtomicU8>,
    task: JoinHandle<()>,
}

pub struct Control {
    pub hello: HostHello,
    pub commands: mpsc::Sender<Result<ConnectResponse, Status>>,
    pub frames: Streaming<ConnectRequest>,
}

pub struct Session {
    pub header: SessionHeader,
    pub transport: DuplexStream,
}

#[derive(Clone)]
struct GatewayService {
    auth: String,
    connections: mpsc::Sender<Control>,
    tunnels: mpsc::Sender<Session>,
    mode: Arc<AtomicU8>,
}

impl Drop for GatewayMock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl GatewayMock {
    pub async fn start(candidate: &CandidateCredential) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let signing = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let pem = signing.cert.pem();
        let identity = Identity::from_pem(&pem, signing.signing_key.serialize_pem());
        let settings = configuration(address.to_string(), &pem);
        let (connection_tx, connections) = mpsc::channel(8);
        let (tunnel_tx, tunnels) = mpsc::channel(8);
        let mode = Arc::new(AtomicU8::new(0));
        let service = GatewayService {
            auth: format!("Bearer {}", STANDARD.encode(candidate.as_bytes())),
            connections: connection_tx,
            tunnels: tunnel_tx,
            mode: mode.clone(),
        };
        let task = tokio::spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))
                .unwrap()
                .add_service(RemoteControllerServiceServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            connections,
            tunnels,
            settings,
            mode,
            task,
        }
    }
}

impl GatewayService {
    fn check_auth(&self, request: &Request<impl Sized>) {
        let metadata = request.metadata();
        let keys = metadata.get_all("authorization");
        let values: Vec<_> = keys.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_str().unwrap(), self.auth);
        let registrations = metadata.get_all("x-agent-vmm-registration");
        let values: Vec<_> = registrations.iter().collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_str().unwrap(), REGISTRATION);
    }
}

#[tonic::async_trait]
impl RemoteControllerService for GatewayService {
    type ConnectStream = Stream<ConnectResponse>;
    type ServeSessionStream = Stream<ServeSessionResponse>;

    async fn connect(
        &self,
        request: Request<Streaming<ConnectRequest>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        self.check_auth(&request);
        let mut frames = request.into_inner();
        let Some(connect_request::Payload::Hello(hello)) =
            frames.message().await?.and_then(|frame| frame.payload)
        else {
            return Err(Status::invalid_argument("expected HostHello"));
        };
        match self.mode.load(Ordering::Relaxed) {
            1 => return Err(Status::unauthenticated("credential rejected by Salix")),
            2 => return Err(protocol_unsupported()),
            _ => {}
        }
        let (commands, rx) = mpsc::channel(8);
        let ready = ControllerReady {
            registration_id: hello.registration_id.clone(),
            connection_epoch: hello.connection_epoch,
            acknowledged_inventory_watermark: hello.inventory_watermark,
            enabled_features: vec!["remote-observation-v1".into()],
        };
        commands
            .send(Ok(ConnectResponse {
                payload: Some(connect_response::Payload::ControllerReady(ready)),
            }))
            .await
            .unwrap();
        self.connections
            .send(Control {
                hello,
                commands,
                frames,
            })
            .await
            .unwrap();
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn serve_session(
        &self,
        request: Request<Streaming<ServeSessionRequest>>,
    ) -> Result<Response<Self::ServeSessionStream>, Status> {
        self.check_auth(&request);
        let mut incoming = request.into_inner();
        let Some(serve_session_request::Payload::Header(header)) =
            incoming.message().await?.and_then(|frame| frame.payload)
        else {
            return Err(Status::invalid_argument("expected SessionHeader"));
        };
        let (transport, bridge) = duplex(64 << 10);
        self.tunnels
            .send(Session { header, transport })
            .await
            .unwrap();
        let (outbound, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let (mut reader, mut writer) = split(bridge);
            let read = async {
                let mut buffer = [0; 32 << 10];
                loop {
                    let count = reader.read(&mut buffer).await.unwrap();
                    let payload = if count == 0 {
                        serve_session_response::Payload::CloseWrite(true)
                    } else {
                        serve_session_response::Payload::Data(buffer[..count].into())
                    };
                    if outbound
                        .send(Ok(ServeSessionResponse {
                            payload: Some(payload),
                        }))
                        .await
                        .is_err()
                        || count == 0
                    {
                        break;
                    }
                }
            };
            let write = async {
                while let Ok(Some(frame)) = incoming.message().await {
                    match frame.payload {
                        Some(serve_session_request::Payload::Data(bytes)) => {
                            if writer.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        Some(serve_session_request::Payload::CloseWrite(true)) => break,
                        _ => panic!("invalid runtime tunnel frame"),
                    }
                }
                let _ = writer.shutdown().await;
            };
            tokio::join!(read, write);
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub async fn tunnel_channel(transport: DuplexStream) -> tonic::transport::Channel {
    let mut transport = Some(transport);
    Endpoint::from_static("http://runtime.invalid")
        .connect_with_connector(tower::service_fn(move |_| {
            let io = transport
                .take()
                .map(TokioIo::new)
                .ok_or_else(|| std::io::Error::other("no replacement tunnel"));
            std::future::ready(io)
        }))
        .await
        .unwrap()
}

#[derive(prost::Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: HashMap<String, String>,
}

fn protocol_unsupported() -> Status {
    let details = agent_vmm_proto::google::rpc::Status {
        code: 9,
        message: "unsupported".into(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.ErrorInfo".into(),
            value: ErrorInfo {
                reason: "REMOTE_PROTOCOL_UNSUPPORTED".into(),
                domain: "agent-vmm".into(),
                metadata: HashMap::from([
                    ("host_protocol".into(), "4".into()),
                    ("min_protocol".into(), "5".into()),
                    ("max_protocol".into(), "6".into()),
                    ("host_side".into(), "upgrade".into()),
                ]),
            }
            .encode_to_vec(),
        }],
    };
    Status::with_details(
        tonic::Code::FailedPrecondition,
        "unsupported",
        details.encode_to_vec().into(),
    )
}
