use super::registration::{REGISTRATION, configuration};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use vmm_remote_salix::{
    CandidateCredential, RemoteConnection,
    remote::{
        ConnectRequest, ConnectResponse, ControllerReady, HostHello, connect_request,
        connect_response,
        remote_controller_service_server::{
            RemoteControllerService, RemoteControllerServiceServer,
        },
    },
};
use vmm_rpc::{Client, Request, Response, Router, ServerTls, Status, Streaming};
type Stream<T> = ReceiverStream<Result<T, Status>>;

pub struct GatewayMock {
    pub connections: mpsc::Receiver<Control>,
    pub peers: mpsc::Receiver<Client>,
    pub settings: RemoteConnection,
    pub mode: Arc<AtomicU8>,
    task: JoinHandle<()>,
}
pub struct Control {
    pub hello: HostHello,
    pub commands: mpsc::Sender<Result<ConnectResponse, Status>>,
    pub frames: Streaming<ConnectRequest>,
}
#[derive(Clone)]
struct GatewayService {
    auth: String,
    connections: mpsc::Sender<Control>,
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
        let signing = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let settings = configuration(
            listener.local_addr().unwrap().to_string(),
            &signing.cert.pem(),
        );
        let tls = ServerTls::new(
            vec![signing.cert.der().clone()],
            signing.signing_key.serialize_der().try_into().unwrap(),
        )
        .unwrap();
        let (tx, connections) = mpsc::channel(8);
        let (out, peers) = mpsc::channel(16);
        let mode = Arc::new(AtomicU8::new(0));
        let router = Router::new(RemoteControllerServiceServer::new(GatewayService {
            auth: format!("Bearer {}", STANDARD.encode(candidate.as_bytes())),
            connections: tx,
            mode: mode.clone(),
        }));
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let session = tls.accept(socket, router.clone()).await.unwrap();
                if out.send(session.client()).await.is_err() {
                    break;
                }
            }
        });
        Self {
            settings,
            connections,
            peers,
            mode,
            task,
        }
    }
}
impl GatewayService {
    fn check_auth(&self, request: &Request<impl Sized>) {
        assert_eq!(
            request
                .metadata()
                .get_all("authorization")
                .collect::<Vec<_>>(),
            [self.auth.as_str()]
        );
        assert_eq!(
            request
                .metadata()
                .get_all("x-agentvmm-registration-id")
                .collect::<Vec<_>>(),
            [REGISTRATION]
        );
    }
}
#[vmm_rpc::async_trait]
impl RemoteControllerService for GatewayService {
    type ConnectStream = Stream<ConnectResponse>;
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
        vmm_rpc::Code::FailedPrecondition,
        "unsupported",
        details.encode_to_vec().into(),
    )
}
