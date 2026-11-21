#[path = "runtime/build.rs"]
mod build;
#[path = "runtime/guest.rs"]
mod guest;
#[path = "runtime/image_copy.rs"]
mod image_copy;
#[path = "runtime/mcp.rs"]
mod mcp;
#[path = "runtime/mesh.rs"]
mod mesh;
#[path = "image/fixture.rs"]
mod oci;
#[path = "runtime/unused.rs"]
mod unused;
#[path = "workspace/driver.rs"]
mod workspace;

use agent_vmm_proto::host::v1::{
    self as h, agent_runtime_service_client::AgentRuntimeServiceClient,
    agent_runtime_service_server::AgentRuntimeServiceServer,
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use vmm_rpc::Code;
use vmm_compat::{
    Containers, runtime::RuntimeService, session::RuntimeAuth, workspace::WorkspaceService,
};
use vmm_core::{
    Core, EgressMode, ImageSource, Limits, Origin, Scope, ScopeState, Store, fake::FakeDom0Backend,
};
use zeroize::Zeroizing;

#[tokio::test]
async fn session_drives_workload_and_workspace_over_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeDom0Backend::default());
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("db")).unwrap()),
        backend.clone(),
    );
    core.store
        .create_scope(
            &Scope {
                id: "env".into(),
                origin: Origin::Local,
                generation: 1,
                revision: 1,
                limits: Limits {
                    pids_max: 64,
                    disk_quota_bytes: 4096,
                    vcpus: 1,
                    boot_ram_mb: 64,
                    max_ram_mb: 64,
                },
                egress_mode: EgressMode::DenyAll,
                host_socket_rules: vec![],
                state: ScopeState::Active,
            },
            None,
        )
        .unwrap();
    core.import_image(
        "env",
        "base",
        &ImageSource {
            manifest_digest: [42; 32],
            staged_extent: "fixture".into(),
            physical_bytes: 1,
        },
    )
    .await
    .unwrap();
    let (session, secret) = core.store.issue_session("env", None).unwrap();
    let (driver, work) = guest::Guest::start().await;
    let driver = Arc::new(driver);
    let auth = RuntimeAuth::bound(core.store.clone(), session.id, Zeroizing::new(secret));
    let mut operator = vmm_compat::operator::LocalOperator::open(
        &dir.path().join("operator"),
        core.store.clone(),
        Arc::new(unused::Unused),
        Some(backend),
    )
    .unwrap();
    operator.guest = Some(driver.clone());
    let containers = Containers::open(dir.path().join("protocol"), driver.clone()).unwrap();
    operator.containers = Some(containers.clone());
    let imports = vmm_compat::image::ImageImports::new(dir.path().into(), Arc::new(ArchiveStage));
    let (builder, build_server) = build::builder().await;
    let images =
        vmm_compat::image::build::Builds::new(builder.clone(), containers.clone(), imports.clone())
            .unwrap();
    let runtime = Arc::new(RuntimeService {
        core,
        containers,
        workspace: WorkspaceService::new(Arc::new(workspace::TestDriver::default())),
        guest: driver,
        imports,
        images: Arc::new(images),
        mesh: Arc::new(vmm_compat::mesh::ServiceMesh {
            device_id: "host-device".into(),
        }),
    });
    let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
    let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
    let server = tokio::spawn(
        vmm_rpc::Router::new(vmm_compat::operator::LocalOperatorServer(Arc::new(
                operator,
            )))
            .add_service(vmm_rpc::Intercepted::new(
                AgentRuntimeServiceServer::from_arc(runtime.clone()),
                auth,
            ))
            .serve_unix(listener, 128, std::future::pending()),
    );
    let channel = vmm_rpc::Client::unix(&endpoint);
    let mut operator = h::operator_service_client::OperatorServiceClient::new(channel.clone());
    let mut c = AgentRuntimeServiceClient::new(channel);
    let (bytes, header) = oci::fixture(b"expected", "arm64");
    let mut responses = Vec::new();
    for request_id in ["first", "second", "first"] {
        let mut metadata = header.clone();
        metadata.request_id = request_id.into();
        let frames = [
            h::import_image_request::Payload::Header(metadata),
            h::import_image_request::Payload::ArchiveChunk(bytes.clone()),
        ]
        .map(|payload| h::ImportImageRequest {
            payload: Some(payload),
        });
        responses.push(
            c.import_image(tokio_stream::iter(frames))
                .await
                .unwrap()
                .into_inner()
                .image
                .unwrap(),
        );
    }
    assert_eq!(responses[0].digest, header.manifest_digest);
    assert_eq!(responses[0].reference, header.image_reference);
    assert!(responses[0].logical_bytes > 0);
    assert!(!responses[0].generation_id.is_empty());
    assert_eq!(responses[0], responses[1]);
    assert_eq!(responses[0], responses[2]);
    for _ in 0..2 {
        let mut pull = c
            .pull_image(h::PullImageRequest {
                request_id: "pull-existing".into(),
                reference: "example.com/test".into(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(pull.message().await.unwrap().unwrap().status, "resolving");
        let completed = pull.message().await.unwrap().unwrap();
        assert_eq!(completed.status, "complete");
        assert_eq!(
            completed.digest.as_deref(),
            Some(header.manifest_digest.as_str())
        );
        assert_eq!(completed.logical_bytes, responses[0].logical_bytes);
        assert_eq!(completed.canonical_reference, header.image_reference);
        assert!(pull.message().await.unwrap().is_none());
    }
    let listed = c
        .list_images(h::ListImagesRequest::default())
        .await
        .unwrap()
        .into_inner()
        .images;
    assert!(listed.contains(&responses[0]));
    let delete = h::DeleteImageRequest {
        request_id: "delete-grant".into(),
        reference: header.image_reference.clone(),
        expected_digest: header.manifest_digest.clone(),
        expected_generation_id: responses[0].generation_id.clone(),
        ..Default::default()
    };
    c.delete_image(delete.clone()).await.unwrap();
    c.delete_image(delete.clone()).await.unwrap();
    let mut recreated = header.clone();
    recreated.request_id = "recreate".into();
    let frames = [
        h::import_image_request::Payload::Header(recreated),
        h::import_image_request::Payload::ArchiveChunk(bytes),
    ]
    .map(|payload| h::ImportImageRequest {
        payload: Some(payload),
    });
    let generation = c
        .import_image(tokio_stream::iter(frames))
        .await
        .unwrap()
        .into_inner()
        .image
        .unwrap()
        .generation_id;
    assert_ne!(generation, delete.expected_generation_id);
    let err = c
        .delete_image(h::DeleteImageRequest {
            request_id: "stale-delete".into(),
            ..delete
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    let env = c
        .get_environment(h::GetEnvironmentRequest {})
        .await
        .unwrap()
        .into_inner()
        .environment
        .unwrap();
    assert_eq!(env.policy_revision, 1);
    c.create_volume(h::CreateVolumeRequest {
        request_id: "vol".into(),
        volume_id: "home".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    use h::write_workspace_request::Payload as W;
    let frames = [
        W::Header(h::WriteWorkspaceHeader {
            request_id: "write".into(),
            volume_id: "home".into(),
            path: "file".into(),
            mode: 0o600,
            ..Default::default()
        }),
        W::Data(b"payload".to_vec()),
        W::Finish(true),
    ];
    c.write_workspace(tokio_stream::iter(frames.map(|frame| {
        h::WriteWorkspaceRequest {
            payload: Some(frame),
        }
    })))
    .await
    .unwrap();
    let mut file = c
        .read_workspace(h::ReadWorkspaceRequest {
            volume_id: "home".into(),
            path: "file".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = file.message().await.unwrap() {
        bytes.extend(chunk.data);
    }
    assert_eq!(bytes, b"payload");
    let container = c
        .create_container(h::CreateContainerRequest {
            request_id: "create".into(),
            container_id: "worker".into(),
            image: "base".into(),
            log_limit_bytes: 1024,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .container
        .unwrap();
    let generation = container.generation_id;
    let live = c
        .start_container(h::StartContainerRequest {
            request_id: "start".into(),
            container_id: "worker".into(),
            expected_generation_id: generation.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .container
        .unwrap();
    let instance = live.instance_id;
    use h::{exec_request::Event as In, exec_response::Event as Out};
    let deadline = (SystemTime::now() + Duration::from_secs(60))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    mesh::exercise(&mut c, &instance).await;
    build::exercise(&mut c, &instance, deadline, &builder).await;
    build_server.await.unwrap();
    let frames = [
        In::Start(h::ExecStart {
            request_id: "exec".into(),
            container_id: "worker".into(),
            argv: vec!["cat".into()],
            expected_instance_id: instance.clone(),
            tty: true,
            execution_id: "exec-1".into(),
            execution_kind: h::ExecutionKind::Exec as i32,
            execution_deadline_unix_nano: deadline,
            ..Default::default()
        }),
        In::Stdin(b"hello".to_vec()),
        In::Resize(h::ExecResize {
            rows: 25,
            columns: 80,
        }),
        In::CloseStdin(true),
    ];
    let mut out = c
        .exec(tokio_stream::iter(
            frames.map(|event| h::ExecRequest { event: Some(event) }),
        ))
        .await
        .unwrap()
        .into_inner();
    for expected in [
        Out::Stdout(b"hello".to_vec()),
        Out::Stderr(b"diagnostic".to_vec()),
        Out::ExitCode(3),
    ] {
        assert_eq!(out.message().await.unwrap().unwrap().event, Some(expected));
    }
    assert!(out.message().await.unwrap().is_none());
    let mut logs = c
        .logs(h::LogsRequest {
            container_id: "worker".into(),
            tail_records: 2,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let record = logs.message().await.unwrap().unwrap();
    assert_eq!(record.cursor, format!("worker:{generation}:5"));
    assert_eq!(record.data, b"retained");
    assert_eq!(record.dropped_records, 4);
    assert!(logs.message().await.unwrap().is_none());
    let env = operator
        .get_environment(h::OperatorServiceGetEnvironmentRequest {
            environment_id: "env".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .environment
        .unwrap();
    assert_eq!(env.namespace_id, "env");
    assert_eq!(env.owner.unwrap().kind(), h::EnvironmentOwnerKind::Local);
    assert!(env.containers.iter().any(|c| c.instance_id == instance));
    let mut operator_logs = operator
        .logs(h::OperatorServiceLogsRequest {
            environment_id: "env".into(),
            query: Some(h::LogsRequest {
                container_id: "worker".into(),
                tail_records: 2,
                ..Default::default()
            }),
        })
        .await
        .unwrap()
        .into_inner();
    let host_record = operator_logs.message().await.unwrap().unwrap();
    assert_eq!(
        (
            host_record.cursor,
            host_record.data,
            host_record.dropped_records,
            host_record.truncated
        ),
        (record.cursor, record.data, 4, true)
    );
    assert!(operator_logs.message().await.unwrap().is_none());
    work.await.unwrap();
    c.quiesce_container(h::QuiesceContainerRequest {
        request_id: "quiet".into(),
        container_id: "worker".into(),
        expected_instance_id: instance.clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    c.stop_container(h::StopContainerRequest {
        request_id: "stop".into(),
        container_id: "worker".into(),
        expected_instance_id: instance,
        quiesce_request_id: "quiet".into(),
        timeout_seconds: Some(5),
    })
    .await
    .unwrap();
    c.delete_container(h::DeleteContainerRequest {
        request_id: "delete".into(),
        container_id: "worker".into(),
        expected_generation_id: generation,
    })
    .await
    .unwrap();
    c.release_session(h::ReleaseSessionRequest {
        request_id: "release".into(),
    })
    .await
    .unwrap();
    let stale = c
        .get_environment(h::GetEnvironmentRequest {})
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Unauthenticated);
    mcp::exercise(runtime, dir.path()).await;
    server.abort();
}

struct ArchiveStage;
impl vmm_compat::image::ImageStaging for ArchiveStage {
    fn stage_image(
        &self,
        _path: &std::path::Path,
        manifest: vmm_core::Digest,
    ) -> vmm_core::Result<ImageSource> {
        Ok(ImageSource {
            manifest_digest: manifest,
            staged_extent: "fixture".into(),
            physical_bytes: 4096,
        })
    }
    fn discard_staged_image(&self, token: &str) -> vmm_core::Result<()> {
        assert_eq!(token, "fixture");
        Ok(())
    }
}
