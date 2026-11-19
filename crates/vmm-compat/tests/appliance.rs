#[path = "image/fixture.rs"]
mod oci;
use agent_vmm_proto::appliance::v1::{
    self as a, appliance_admin_service_client::ApplianceAdminServiceClient as Client,
};
use prost::Message;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use vmm_compat::operator::{ApplianceOperations, McpManagement};
use vmm_mcp::{FileSigningKeys, TokenAuthority};

struct TestServer {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl TestServer {
    async fn close(self) {
        self.stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
async fn open(root: &Path) -> (Client<Channel>, TestServer) {
    let db = root.join("compat.sqlite");
    let keys = TokenAuthority::new(Arc::new(FileSigningKeys::open(root).unwrap()));
    let mcp = Arc::new(McpManagement::open(&db, keys).unwrap());
    let service = ApplianceOperations::open(
        mcp,
        &db,
        &root.join("content"),
        Arc::new(vmm_core::Store::open(root.join("core.sqlite")).unwrap()),
        Arc::new(vmm_signing::FileCredentialStore::new(root.join("secrets"))),
        51 << 30,
    )
    .unwrap();
    let socket = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = socket.local_addr().unwrap();
    let (stop, closed) = tokio::sync::oneshot::channel();
    let job = tokio::spawn(
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(socket), async {
                let _ = closed.await;
            }),
    );
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    (Client::new(channel), TestServer { stop, task: job })
}
fn frames(
    header: a::StageContentHeader,
    bytes: Option<Vec<u8>>,
) -> impl tokio_stream::Stream<Item = a::StageContentRequest> {
    use a::stage_content_request::Payload;
    tokio_stream::iter(
        std::iter::once(Payload::Header(header))
            .chain(bytes.map(Payload::ArchiveChunk))
            .map(|payload| a::StageContentRequest {
                payload: Some(payload),
            }),
    )
}
#[tokio::test]
async fn appliance_content_is_verified_and_replays_the_original_receipt_after_restart() {
    let state = tempfile::tempdir().unwrap();
    let (mut admin, job) = open(state.path()).await;
    let (bytes, r) = oci::fixture(b"expected", "arm64");
    let content = a::ApplianceContentReference {
        role: a::ApplianceContentRole::Controller as i32,
        image_reference: format!("example.com/test@{}", r.manifest_digest),
        image_manifest_digest: r.manifest_digest,
        archive_sha256: r.archive_sha256,
        archive_size: r.archive_size,
        platform: r.platform,
    };
    let header = a::StageContentHeader {
        request_id: "content-one".into(),
        deadline_unix_millis: (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
            + Duration::from_secs(30))
        .as_millis() as i64,
        content_ref: Some(content.clone()),
    };
    let request = a::GetContentStatusRequest {
        content_refs: vec![content.clone()],
    };
    assert!(
        !admin
            .get_content_status(request.clone())
            .await
            .unwrap()
            .into_inner()
            .contents[0]
            .available
    );
    let mut damaged = bytes.clone();
    damaged[0] ^= 1;
    assert_eq!(
        admin
            .stage_content(frames(
                a::StageContentHeader {
                    request_id: "bad".into(),
                    ..header.clone()
                },
                Some(damaged)
            ))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert!(
        !admin
            .get_content_status(request.clone())
            .await
            .unwrap()
            .into_inner()
            .contents[0]
            .available
    );
    let op = admin
        .stage_content(frames(header.clone(), Some(bytes)))
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(op.state(), a::ApplianceOperationState::Succeeded);
    drop(admin);
    job.close().await;
    // A crash after the atomic rename and before the receipt commit.
    let unfinished = a::ApplianceOperation {
        operation_id: "after-publish".into(),
        request_id: "crash".into(),
        receipt: header.encode_to_vec(),
        state: a::ApplianceOperationState::Running as i32,
        kind: a::ApplianceOperationKind::StageContent as i32,
        ..Default::default()
    };
    rusqlite::Connection::open(state.path().join("compat.sqlite"))
        .unwrap()
        .execute(
            "INSERT INTO appliance_receipts(id,request,operation) VALUES(?,?,?)",
            rusqlite::params![
                unfinished.operation_id,
                unfinished.request_id,
                unfinished.encode_to_vec()
            ],
        )
        .unwrap();
    let (mut admin, job) = open(state.path()).await;
    assert!(
        admin
            .get_content_status(request)
            .await
            .unwrap()
            .into_inner()
            .contents[0]
            .available
    );
    assert_eq!(
        admin
            .stage_content(frames(header.clone(), None))
            .await
            .unwrap()
            .into_inner()
            .operation,
        Some(op.clone())
    );
    let watched = admin
        .watch_appliance_operation(a::WatchApplianceOperationRequest {
            operation_id: op.operation_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watched.operation, Some(op));
    let mut recovery = admin
        .watch_appliance_operation(a::WatchApplianceOperationRequest {
            operation_id: unfinished.operation_id,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    for state in [
        a::ApplianceOperationState::Running,
        a::ApplianceOperationState::Succeeded,
    ] {
        assert_eq!(
            recovery
                .message()
                .await
                .unwrap()
                .unwrap()
                .operation
                .unwrap()
                .state(),
            state
        );
    }
    assert!(recovery.message().await.unwrap().is_none());
    let mut conflict = header.clone();
    conflict.content_ref.as_mut().unwrap().role = a::ApplianceContentRole::Harness as i32;
    assert_eq!(
        admin
            .stage_content(frames(conflict, None))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    let expired = a::StageContentHeader {
        request_id: "expired".into(),
        deadline_unix_millis: 1,
        ..header
    };
    assert_eq!(
        admin
            .stage_content(frames(expired, None))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    job.close().await;
}

fn artifact() -> (Vec<u8>, a::ControllerApplianceManifest) {
    let (bytes, image) = oci::fixture(b"expected", "arm64");
    let limits = agent_vmm_proto::host::v1::ResourceLimits {
        pids: Some(64),
        disk_bytes: Some(1 << 30),
    };
    let content = a::ApplianceContentReference {
        role: a::ApplianceContentRole::Controller as i32,
        image_reference: format!("example.com/product@{}", image.manifest_digest),
        archive_size: image.archive_size,
        archive_sha256: image.archive_sha256,
        image_manifest_digest: image.manifest_digest,
        platform: image.platform,
    };
    let manifest = a::ControllerApplianceManifest {
        image_reference: content.image_reference.clone(),
        image_manifest_digest: content.image_manifest_digest.clone(),
        platform: content.platform.clone(),
        content_refs: vec![content],
        product_id: "test-product".into(),
        contract_version: "1".into(),
        data_volume_bytes: 1 << 30,
        startup_deadline_seconds: 10,
        system_limits: Some(limits),
        requested_capacity: Some(a::CapacityGrant {
            per_environment_limits: Some(limits),
            ..Default::default()
        }),
        ..Default::default()
    };
    (bytes, manifest)
}
#[tokio::test]
async fn manifest_identity_and_stage_receipts_survive_restart_before_archive_upload() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, job) = open(root.path()).await;
    let (_, manifest) = artifact();
    let req = a::StageArtifactRequest {
        request_id: "stage".into(),
        deadline_unix_millis: (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
            + Duration::from_secs(60))
        .as_millis() as i64,
        manifest: Some(manifest.clone()),
    };
    let original = client
        .stage_artifact(req.clone())
        .await
        .unwrap()
        .into_inner();
    let distinct = client
        .stage_artifact(a::StageArtifactRequest {
            request_id: "also-stage".into(),
            ..req.clone()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(original.artifact_id, distinct.artifact_id);
    assert_ne!(original.operation, distinct.operation);
    drop(client);
    job.close().await;
    let (mut client, job) = open(root.path()).await;
    assert_eq!(
        client
            .stage_artifact(req.clone())
            .await
            .unwrap()
            .into_inner(),
        original
    );
    let watched = client
        .watch_appliance_operation(a::WatchApplianceOperationRequest {
            operation_id: original.operation.as_ref().unwrap().operation_id.clone(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watched.artifact_id, original.artifact_id);
    assert_eq!(watched.operation, original.operation);
    let conflict = a::ControllerApplianceManifest {
        release: "new".into(),
        ..manifest.clone()
    };
    assert_eq!(
        client
            .stage_artifact(a::StageArtifactRequest {
                manifest: Some(conflict),
                ..req.clone()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    for bad in [
        a::ControllerApplianceManifest {
            content_refs: vec![],
            ..manifest.clone()
        },
        a::ControllerApplianceManifest {
            platform: "linux/amd64".into(),
            ..manifest
        },
    ] {
        assert_eq!(
            client
                .stage_artifact(a::StageArtifactRequest {
                    request_id: "unused".into(),
                    manifest: Some(bad),
                    ..req.clone()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }
    job.close().await;
}

#[tokio::test]
async fn approved_install_owns_one_private_registration_and_replays_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let (mut client, job) = open(root.path()).await;
    let (bytes, m) = artifact();
    let end = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(60))
        .as_millis() as i64;
    let artifact = client
        .stage_artifact(a::StageArtifactRequest {
            request_id: "manifest".into(),
            deadline_unix_millis: end,
            manifest: Some(m.clone()),
        })
        .await
        .unwrap()
        .into_inner();
    let grant = a::ApprovedGrant {
        system_limits: m.system_limits,
        capacity: m.requested_capacity,
        ..Default::default()
    };
    let plan = a::PlanInstallRequest {
        installation_id: "controller".into(),
        request_id: "plan".into(),
        approved_grant: Some(grant.clone()),
        artifact_id: artifact.artifact_id.clone(),
        deadline_unix_millis: end,
        ..Default::default()
    };
    assert_eq!(
        client.plan_install(plan.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    client
        .stage_content(frames(
            a::StageContentHeader {
                request_id: "content".into(),
                deadline_unix_millis: end,
                content_ref: m.content_refs.into_iter().next(),
            },
            Some(bytes),
        ))
        .await
        .unwrap();
    let denied = a::PlanInstallRequest {
        approved_grant: Some(a::ApprovedGrant {
            lan_egress: true,
            ..grant.clone()
        }),
        ..plan.clone()
    };
    assert_eq!(
        client.plan_install(denied).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let confirmed = client
        .plan_install(plan)
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    assert_eq!(confirmed.kind(), a::AppliancePlanKind::Install);
    let req = a::InstallRequest {
        installation_id: confirmed.installation_id,
        artifact_id: confirmed.artifact_id,
        expected_revision: confirmed.revision,
        request_id: "install".into(),
        approved_grant: Some(grant),
        deadline_unix_millis: end,
        opaque_bootstrap_seed: vec![37; 8192],
    };
    let first = client
        .install(req.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(first.state(), a::ApplianceOperationState::Succeeded);
    assert_eq!(
        client
            .install(a::InstallRequest {
                opaque_bootstrap_seed: vec![38; 8192],
                ..req.clone()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Aborted
    );
    let mut status = client
        .get_appliance_status(a::GetApplianceStatusRequest {
            installation_id: "controller".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(status.appliance_state(), a::ApplianceState::Stopped);
    assert_eq!(status.revision, 1);
    assert_eq!(status.approved_grant.take().unwrap().revision, 1);
    assert!(!status.registration_id.is_empty());
    drop(client);
    job.close().await;
    let db = rusqlite::Connection::open(root.path().join("compat.sqlite")).unwrap();
    // Recover an interrupted projection after Core has committed the private registration.
    let pending = a::ApplianceOperation {
        state: a::ApplianceOperationState::Running as i32,
        ..first.clone()
    };
    db.execute(
        "UPDATE managed_installations SET published=0 WHERE id=?",
        ["controller"],
    )
    .unwrap();
    db.execute(
        "UPDATE appliance_receipts SET operation=? WHERE id=?",
        rusqlite::params![pending.encode_to_vec(), pending.operation_id],
    )
    .unwrap();
    drop(db);
    let (mut client, job) = open(root.path()).await;
    let replay = client
        .install(req)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(replay.operation_id, first.operation_id);
    assert_eq!(replay.state(), a::ApplianceOperationState::Succeeded);
    let all = client
        .list_appliances(a::ListAppliancesRequest {})
        .await
        .unwrap()
        .into_inner()
        .appliances;
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].registration_id, status.registration_id);
    let wrong = a::WatchApplianceOperationRequest {
        operation_id: first.operation_id,
        installation_id: String::new(),
    };
    assert_eq!(
        client
            .watch_appliance_operation(wrong.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    let watched = client
        .watch_appliance_operation(a::WatchApplianceOperationRequest {
            installation_id: "controller".into(),
            ..wrong
        })
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(watched.operation, Some(replay));
    job.close().await;
}
