use agent_vmm_proto::{google::rpc, host::v1::ErrorReason};
use base64::{Engine, engine::general_purpose::STANDARD};
use prost::Message;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, Status, transport::Server};
use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
use tonic_types::pb::ErrorInfo;
use vmm_compat::{error, session::RuntimeAuth};
use vmm_core::{EgressMode, Limits, Origin, Scope, ScopeState, Store};
use zeroize::Zeroizing;

fn info(status: &Status) -> ErrorInfo {
    let rich = rpc::Status::decode(status.details()).unwrap();
    assert_eq!(rich.code, status.code() as i32);
    assert_eq!(rich.message, status.message());
    let detail = rich.details.first().unwrap();
    assert_eq!(detail.type_url, "type.googleapis.com/google.rpc.ErrorInfo");
    let info = ErrorInfo::decode(&*detail.value).unwrap();
    assert_eq!(info.domain, "agent-vmm");
    info
}

#[test]
fn gateway_error_detail_carries_fences_and_import_admission_counts() {
    let stale = error::host_status(
        Code::Aborted,
        ErrorReason::StaleContainerInstance,
        "stale instance",
    );
    assert_eq!(info(&stale).reason, "STALE_CONTAINER_INSTANCE");
    let capacity = error::import_headroom(0, u64::MAX);
    assert_eq!(capacity.code(), Code::ResourceExhausted);
    let info = info(&capacity);
    assert_eq!(info.reason, "RESOURCE_CAPACITY_EXHAUSTED");
    assert_eq!(info.metadata["resource_dimension"], "storage_headroom");
    assert_eq!(info.metadata["stage"], "import_admission");
    assert_eq!(info.metadata["available_bytes"], "0");
    assert_eq!(info.metadata["required_bytes"], u64::MAX.to_string());
}

fn scope(origin: Origin) -> Scope {
    Scope {
        id: "env".into(),
        origin,
        generation: 1,
        revision: 1,
        limits: Limits {
            pids_max: 64,
            disk_quota_bytes: 8192,
            vcpus: 1,
            boot_ram_mb: 64,
            max_ram_mb: 64,
        },
        egress_mode: EgressMode::DenyAll,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    }
}

#[tokio::test]
async fn replacement_fences_bound_and_bearer_rpcs_on_existing_connections() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("core.db")).unwrap());
    let scope = scope(Origin::Local);
    store.create_scope(&scope, None).unwrap();
    for bound in [true, false] {
        let (session, capability) = store.issue_session("env", None).unwrap();
        let auth = if bound {
            RuntimeAuth::bound(store.clone(), session.id, Zeroizing::new(capability))
        } else {
            RuntimeAuth::bearer(store.clone(), session.id)
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (_reporter, health) = tonic_health::server::health_reporter();
        let service = tonic::service::interceptor::InterceptedService::new(health, auth);
        let server = tokio::spawn(
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);
        let request = || {
            let mut req = Request::new(HealthCheckRequest {
                service: String::new(),
            });
            if !bound {
                req.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {}", STANDARD.encode(capability))
                        .parse()
                        .unwrap(),
                );
            }
            req
        };
        if !bound {
            let err = client
                .check(HealthCheckRequest::default())
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::Unauthenticated);
        }
        client.check(request()).await.unwrap();
        store.issue_session("env", None).unwrap();
        let err = client.check(request()).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert_eq!(info(&err).reason, "SESSION_TOKEN_INVALID");
        server.abort();
        let _ = server.await;
    }
}

#[test]
fn remote_context_requires_the_allocation_bound_to_the_authenticated_session() {
    use vmm_compat::{RuntimeContext, session::RemoteIdentity};
    use vmm_core::{Core, RemoteScopePolicy, fake::FakeDom0Backend};

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("core.db")).unwrap());
    store.create_registration("reg").unwrap();
    let channel = store.connect("reg", 1).unwrap();
    let desired = scope(Origin::Remote {
        registration_id: "reg".into(),
    });
    let policy = RemoteScopePolicy {
        revision: 1,
        max_pids: desired.limits.pids_max,
        max_disk_quota_bytes: desired.limits.disk_quota_bytes,
        max_egress_mode: desired.egress_mode.clone(),
    };
    for id in ["one", "two"] {
        store
            .ensure_remote_scope(&channel, id, id, desired.clone(), &policy)
            .unwrap();
    }
    let (session, _) = store.issue_remote_session(&channel, "one", 1).unwrap();
    let core = Core::new(store, Arc::new(FakeDom0Backend::default()));
    let mut req = Request::new(());
    req.extensions_mut().insert(session.clone());
    let resolve = |req: &Request<()>| RuntimeContext::from_request(&core, req);
    let missing = resolve(&req).err().unwrap();
    assert_eq!(info(&missing).reason, "ALLOCATION_REVOKED");
    let identity = RemoteIdentity {
        registration_id: "reg".into(),
        session_id: session.id,
        allocation_id: "one".into(),
        generation: 1,
        execution_owner_id: "owner".into(),
    };
    req.extensions_mut().insert(identity.clone());
    assert_eq!(resolve(&req).unwrap().execution_authority().unwrap(), "one");
    req.extensions_mut().insert(RemoteIdentity {
        allocation_id: "two".into(),
        ..identity
    });
    let error = resolve(&req).err().unwrap();
    assert_eq!(error.code(), Code::Unauthenticated);
    assert_eq!(info(&error).reason, "ALLOCATION_REVOKED");
}
