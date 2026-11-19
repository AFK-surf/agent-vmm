use super::*;
use crate::{
    host::HostState,
    service_forward::{RouteTrust, ServiceForward, TrustedLease},
};
use agent_vmm_proto::{service::v1 as s, trust::v1 as t};
use std::{os::unix::fs::PermissionsExt, sync::RwLock, time::SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{Code, Status};
use vmm_core::{
    BootIdentity, Core, EgressMode, ImageSource, Limits, Origin, SandboxDefinition, SandboxKey,
    Scope, ScopeState, ServiceExport, fake::FakeDom0Backend,
};
use vmm_signing::{FileIdentityKey, IdentitySigner, SignedMessage};

#[derive(Default)]
struct Trust(RwLock<Option<TrustedLease>>);
impl RouteTrust for Trust {
    fn lookup(&self, _: &str, _: &str) -> Option<TrustedLease> {
        self.0.read().unwrap().clone()
    }
}
struct Guest(ServiceExport);
#[tonic::async_trait]
impl vmm_compat::guest::GuestDriver for Guest {
    async fn client(
        &self,
        _: &BootIdentity,
    ) -> std::result::Result<vmm_guest_proto::client::Client, Status> {
        unreachable!()
    }
    async fn connect_port(
        &self,
        boot: &BootIdentity,
        port: u16,
    ) -> std::result::Result<Box<dyn vmm_compat::guest::Duplex>, Status> {
        assert_eq!((boot, port), (&self.0.boot, self.0.port));
        let (socket, mut server) = tokio::io::duplex(128);
        tokio::spawn(async move {
            let mut body = Vec::new();
            server.read_to_end(&mut body).await.unwrap();
            server.write_all(&body).await.unwrap();
        });
        Ok(Box::new(socket))
    }
}
async fn export(core: &Core) -> ServiceExport {
    let scope = Scope {
        id: "tenant".into(),
        origin: Origin::Local,
        generation: 1,
        revision: 1,
        limits: Limits {
            pids_max: 64,
            disk_quota_bytes: 8192,
            vcpus: 1,
            boot_ram_mb: 128,
            max_ram_mb: 128,
        },
        egress_mode: EgressMode::DenyAll,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    };
    core.store.create_scope(&scope, None).unwrap();
    core.import_image(
        &scope.id,
        "app",
        &ImageSource {
            manifest_digest: [1; 32],
            staged_extent: "staged".into(),
            physical_bytes: 1024,
        },
    )
    .await
    .unwrap();
    let def = SandboxDefinition {
        key: SandboxKey {
            scope_id: scope.id,
            sandbox_id: "server".into(),
            generation_id: "g1".into(),
        },
        manifest_digest: [1; 32],
        read_only_root: true,
        root_overlay: None,
        pids_max: None,
        mounts: vec![],
        host_shares: vec![],
        image_reference: "app".into(),
        entrypoint: None,
        command: Some(vec!["server".into()]),
        workdir: None,
        user: None,
        log_limit_bytes: 1024,
        env: Default::default(),
    };
    core.define_sandbox(&def).await.unwrap();
    let target = ServiceExport {
        id: "export".into(),
        boot: core.start(&def.key).await.unwrap().boot,
        port: 8080,
    };
    core.publish_export(&target).await.unwrap();
    target
}
fn header(
    issuer: &FileIdentityKey,
    device: String,
    target: &ServiceExport,
) -> s::OpenServiceForwardHeader {
    let now = SystemTime::now();
    let mut cap = t::RouteCapability {
        capability_id: "cap".into(),
        source_device_id: "source".into(),
        authority: Some(t::AuthorityDescriptor {
            authority_id: "mesh".into(),
            authority_class: t::AuthorityClass::PersonalMesh as i32,
            trust_domain_ref: b"mesh".to_vec(),
            revision: 1,
            ..Default::default()
        }),
        nonce: vec![7; 32],
        route_class: "device".into(),
        issuer_device_id: device.clone(),
        destination_device_id: device,
        destination_export_id: target.id.clone(),
        route_generation: 1,
        policy_revision: 1,
        allowed_protocol: "tcp".into(),
        allowed_verbs: vec!["connect".into()],
        audience: "agent-vmm/service-tunnel/1".into(),
        budget: Some(t::RouteBudget {
            connection_limit: 2,
            concurrency_limit: 1,
            byte_limit: 8,
        }),
        not_before: Some(now.into()),
        expires_at: Some((now + Duration::from_secs(60)).into()),
        ..Default::default()
    };
    cap.signature = SignedMessage::try_from(&cap)
        .unwrap()
        .sign(issuer)
        .unwrap()
        .to_vec();
    s::OpenServiceForwardHeader {
        route_id: "route".into(),
        lease_id: "lease".into(),
        route_generation: 1,
        source_environment_id: "source-env".into(),
        source_container_instance_id: "source-boot".into(),
        capability: Some(cap),
    }
}
fn request(
    header: &s::OpenServiceForwardHeader,
) -> impl tokio_stream::Stream<Item = s::OpenServiceForwardRequest> + 'static {
    use s::open_service_forward_request::Payload;
    tokio_stream::iter(
        [
            Payload::Header(header.clone()),
            Payload::Data(b"ping".to_vec()),
            Payload::CloseWrite(true),
        ]
        .map(|payload| s::OpenServiceForwardRequest {
            payload: Some(payload),
        }),
    )
}
#[tokio::test]
async fn signed_endpoint_serves_only_live_trusted_service_routes_over_iroh() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let dir = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in("/tmp")
            .unwrap();
        let host = HostState::open(&dir.path().join("host.db")).unwrap();
        let signer = host
            .local_operator(dir.path(), None)
            .unwrap()
            .trust
            .unwrap();
        let identity = signer.identity().unwrap().identity.unwrap();
        let service = ServiceNode::bind(signer).await.unwrap();
        let endpoint = service.observations().borrow().clone();
        let status = service.node.status();
        assert_eq!(endpoint.endpoint_generation, status.endpoint_generation);
        assert_eq!(endpoint.endpoint_node_id, status.endpoint_id.as_bytes());
        assert_eq!(endpoint.device_id, identity.device_id);
        SignedMessage::try_from(&endpoint)
            .unwrap()
            .verify(&identity.root_public_key)
            .unwrap();
        let core = Core::new(host.store.clone(), Arc::new(FakeDom0Backend::default()));
        let target = export(&core).await;
        let trust = Arc::new(Trust::default());
        let routes = ServiceForward {
            device: identity.device_id.clone(),
            store: host.store.clone(),
            guest: Arc::new(Guest(target.clone())),
            trust: Some(trust.clone()),
            budgets: Default::default(),
        };
        let key = FileIdentityKey::generate();
        let header = header(&key, identity.device_id, &target);
        let (stop, signal) = watch::channel(false);
        let serving = tokio::spawn(async move { service.serve(routes, signal).await.unwrap() });
        let peer = DataNode::bind_service().await.unwrap();
        let stream = peer
            .open(Channel::Service, status.endpoint_addr)
            .await
            .unwrap();
        let path = dir.path().join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let bridge = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            stream.bridge(socket).await
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("unix:{}", path.display()))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = s::service_route_service_client::ServiceRouteServiceClient::new(channel);
        assert_eq!(
            client
                .open_service_forward(request(&header))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let mut lease = TrustedLease {
            header: header.clone(),
            target: target.clone(),
            issuer: FileIdentityKey::generate().public_key().unwrap().to_vec(),
        };
        *trust.0.write().unwrap() = Some(lease.clone());
        assert_eq!(
            client
                .open_service_forward(request(&header))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        lease.issuer = key.public_key().unwrap().to_vec();
        *trust.0.write().unwrap() = Some(lease);
        let mut output = client
            .open_service_forward(request(&header))
            .await
            .unwrap()
            .into_inner();
        use s::open_service_forward_response::Payload;
        assert_eq!(
            output.message().await.unwrap().unwrap().payload,
            Some(Payload::Data(b"ping".to_vec()))
        );
        assert_eq!(
            output.message().await.unwrap().unwrap().payload,
            Some(Payload::CloseWrite(true))
        );
        assert!(output.message().await.unwrap().is_none());
        assert_eq!(
            client
                .open_service_forward(request(&header))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        core.withdraw_export(&target).await.unwrap();
        assert_eq!(
            client
                .open_service_forward(request(&header))
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        drop(client);
        stop.send_replace(true);
        serving.await.unwrap();
        peer.shutdown().await;
        let _ = bridge.await.unwrap();
    })
    .await
    .unwrap();
}
