use super::*;
use h::image_source::Source;
use prost::Message;
use sha2::{Digest, Sha256};

#[tokio::test]
async fn remote_client_copies_retained_build_and_recovers_both_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("core")).unwrap());
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    store.create_registration("controller").unwrap();
    let channel = store.connect("controller", 1).unwrap();
    for name in ["builder", "dest"] {
        store
            .create_scope(
                &Scope {
                    id: name.into(),
                    origin: Origin::Remote {
                        registration_id: "controller".into(),
                    },
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
                },
                Some(&channel),
            )
            .unwrap();
    }
    let (builder, _) = store.issue_session("builder", Some(&channel)).unwrap();
    let (target, token) = store.issue_session("dest", Some(&channel)).unwrap();
    let reference = "docker.io/library/example:v1";
    let digest = [8; 32];
    let manifest = format!("sha256:{}", hex::encode(digest));
    core.import_image(
        "builder",
        reference,
        &ImageSource {
            manifest_digest: digest,
            staged_extent: "verified".into(),
            physical_bytes: 123,
        },
    )
    .await
    .unwrap();
    let build = h::ImageOperationReceipt {
        request_id: "build".into(),
        kind: h::ImageOperationKind::Build as i32,
        state: h::ImageOperationState::Succeeded as i32,
        writer_settled: true,
        image_reference: reference.into(),
        manifest_digest: manifest.clone(),
        ..Default::default()
    };
    store
        .retain_build_output(
            "controller",
            &builder,
            &vmm_core::RetainedImage {
                reference: reference.into(),
                digest,
                generation: "retained-g1".into(),
                build_request: build.request_id.clone(),
                build_receipt: build.encode_to_vec(),
                logical_bytes: 41,
            },
        )
        .unwrap();
    core.remove_image_reference("builder", reference, digest)
        .await
        .unwrap();
    let (guest, background) = guest::Guest::start().await;
    background.abort();
    let guest = Arc::new(guest);
    let runtime = RuntimeService {
        core,
        containers: Containers::open(dir.path().join("compat"), guest.clone()).unwrap(),
        guest,
        workspace: WorkspaceService::new(Arc::new(workspace::TestDriver::default())),
        imports: vmm_compat::image::ImageImports {
            directory: dir.path().into(),
            staging: Arc::new(ArchiveStage),
        },
        images: Arc::new(unused::Unused),
        mesh: Arc::new(unused::Unused),
    };
    let auth = RuntimeAuth::bound(store.clone(), target.id.clone(), Zeroizing::new(token))
        .for_remote(vmm_compat::session::RemoteIdentity {
            session_id: target.id.clone(),
            registration_id: "controller".into(),
            allocation_id: "dest".into(),
            generation: 1,
            execution_owner_id: "agent".into(),
        });
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(
        Server::builder()
            .add_service(AgentRuntimeServiceServer::with_interceptor(runtime, auth))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = AgentRuntimeServiceClient::new(channel);
    let retained = client
        .list_images(h::ListImagesRequest {
            origin: h::ImageOrigin::ControllerRetained as i32,
        })
        .await
        .unwrap()
        .into_inner()
        .images;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].build_receipt.as_ref(), Some(&build));
    let req = h::CopyImageRequest {
        request_id: "copy-a".into(),
        source: Some(h::ImageSource {
            source: Some(Source::Retained(h::RetainedImageSource {
                build_request_id: "build".into(),
            })),
        }),
        target_allocation_id: "dest".into(),
        target_allocation_generation: 1,
        image_reference: reference.into(),
        manifest_digest: manifest.clone(),
    };
    let copied = client
        .copy_image(req.clone())
        .await
        .unwrap()
        .into_inner()
        .image
        .unwrap();
    assert_eq!(copied.build_receipt.as_ref(), Some(&build));
    let copy = copied.copy_receipt.unwrap();
    assert_eq!(
        copy.input_sha256,
        hex::encode(Sha256::digest(req.encode_to_vec()))
    );
    assert!(copy.writer_settled);
    let list = client
        .list_images(h::ListImagesRequest::default())
        .await
        .unwrap()
        .into_inner()
        .images;
    assert_eq!(list[0].generation_id, copied.generation_id);
    assert_eq!(list[0].copy_receipt.as_ref(), Some(&copy));
    assert_eq!(list[0].build_receipt.as_ref(), Some(&build));
    client
        .delete_image(h::DeleteImageRequest {
            request_id: "delete".into(),
            reference: reference.into(),
            expected_generation_id: retained[0].generation_id.clone(),
            expected_digest: manifest,
            origin: h::ImageOrigin::ControllerRetained as i32,
        })
        .await
        .unwrap();
    let observed = client
        .cancel_image_operation(h::CancelImageOperationRequest {
            request_id: req.request_id.clone(),
            kind: copy.kind,
            input_sha256: copy.input_sha256.clone(),
            copy_source: req.source.clone(),
            target_allocation_id: "dest".into(),
            target_allocation_generation: 1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .receipt;
    assert_eq!(observed.as_ref(), Some(&copy));
    assert_eq!(
        client
            .copy_image(req.clone())
            .await
            .unwrap()
            .into_inner()
            .image
            .unwrap()
            .copy_receipt,
        observed
    );
    let err = client
        .copy_image(h::CopyImageRequest {
            request_id: "fresh".into(),
            ..req
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
    server.abort();
}
