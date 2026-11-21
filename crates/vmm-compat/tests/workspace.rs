#[path = "workspace/driver.rs"]
mod driver;
use agent_vmm_proto::host::v1 as h;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_stream::{StreamExt, iter};
use vmm_rpc::{Code, Request, Status};
use vmm_compat::{
    RuntimeContext,
    workspace::{DEFAULT_VOLUME, WorkspaceService},
};
use vmm_core::{
    Core, EgressMode, ImageSource, Limits, Origin, SandboxDefinition, SandboxKey, Scope,
    ScopeState, Store, fake::FakeDom0Backend,
};

fn session(core: &Core, id: &str) -> RuntimeContext {
    core.store
        .create_scope(
            &Scope {
                id: id.into(),
                origin: Origin::Local,
                generation: 1,
                revision: 1,
                limits: Limits {
                    pids_max: 64,
                    disk_quota_bytes: 120,
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
    let mut req = Request::new(());
    req.extensions_mut()
        .insert(core.store.issue_session(id, None).unwrap().0);
    RuntimeContext::from_request(core, &req).unwrap()
}
fn upload(
    volume: &str,
    id: &str,
    data: &[u8],
    prefix: &[u8],
    finish: bool,
) -> Vec<Result<h::WriteWorkspaceRequest, Status>> {
    use h::write_workspace_request::Payload;
    let mut frames = vec![
        Payload::Header(h::WriteWorkspaceHeader {
            request_id: id.into(),
            path: "doc".into(),
            offset: prefix.len() as u64,
            expected_prefix_sha256: if prefix.is_empty() {
                vec![]
            } else {
                Sha256::digest(prefix).to_vec()
            },
            mode: 0o600,
            volume_id: volume.into(),
        }),
        Payload::Data(data.to_vec()),
    ];
    if finish {
        frames.push(Payload::Finish(true))
    }
    frames
        .into_iter()
        .map(|p| Ok(h::WriteWorkspaceRequest { payload: Some(p) }))
        .collect()
}
async fn contents(
    ws: &WorkspaceService,
    ctx: &RuntimeContext,
    volume: &str,
    path: &str,
) -> Result<Vec<u8>, Status> {
    let mut rx = ws
        .read(
            ctx,
            h::ReadWorkspaceRequest {
                path: path.into(),
                volume_id: volume.into(),
                ..Default::default()
            },
        )
        .await?;
    let mut bytes = vec![];
    let mut final_seen = false;
    while let Some(frame) = rx.next().await.transpose()? {
        assert!(!final_seen);
        assert_eq!(frame.offset, bytes.len() as u64);
        if frame.r#final {
            assert_eq!(frame.sha256, Sha256::digest(&bytes).to_vec());
            final_seen = true
        }
        bytes.extend(frame.data);
    }
    assert!(final_seen);
    Ok(bytes)
}

#[tokio::test]
async fn workspace_roots_survive_workloads_and_share_quota() {
    let dir = tempfile::tempdir().unwrap();
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("db")).unwrap()),
        Arc::new(FakeDom0Backend::default()),
    );
    let ws = WorkspaceService::new(Arc::new(driver::TestDriver::default()));
    let a = session(&core, "a");
    let b = session(&core, "b");
    for ctx in [&a, &b] {
        ctx.create_volume(h::CreateVolumeRequest {
            volume_id: "vol".into(),
            request_id: "create".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    ws.write(&a, iter(upload("", "first", b"hello", &[], true)))
        .await
        .unwrap();
    let image = ImageSource {
        manifest_digest: [2; 32],
        staged_extent: "staged".into(),
        physical_bytes: 1,
    };
    core.import_image("a", "base", &image).await.unwrap();
    let mut boots = vec![];
    for id in ["one", "two"] {
        let key = SandboxKey {
            scope_id: "a".into(),
            sandbox_id: id.into(),
            generation_id: "g".into(),
        };
        core.define_sandbox(&SandboxDefinition {
            key: key.clone(),
            pids_max: None,
            image_reference: "base".into(),
            manifest_digest: image.manifest_digest,
            read_only_root: true,
            root_overlay: None,
            mounts: vec![],
            host_shares: vec![],
            entrypoint: None,
            command: None,
            workdir: None,
            user: None,
            log_limit_bytes: 1024,
            env: Default::default(),
        })
        .await
        .unwrap();
        boots.push(core.start(&key).await.unwrap().boot);
    }
    ws.write(&a, iter(upload("vol", "staged", b"abc", &[], false)))
        .await
        .unwrap_err();
    assert_eq!(
        contents(&ws, &a, "vol", "doc").await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        ws.write(&a, iter(upload("vol", "staged", b"x", b"bad", true)))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    ws.write(&a, iter(upload("vol", "staged", b"def", b"abc", true)))
        .await
        .unwrap();
    assert_eq!(contents(&ws, &a, "vol", "doc").await.unwrap(), b"abcdef");
    assert_eq!(contents(&ws, &a, "", "doc").await.unwrap(), b"hello");
    assert_eq!(
        contents(&ws, &b, "", "doc").await.unwrap_err().code(),
        Code::NotFound
    );
    assert_eq!(
        contents(&ws, &b, "vol", "doc").await.unwrap_err().code(),
        Code::NotFound
    );
    for boot in boots {
        core.stop(&boot, false).await.unwrap();
    }
    assert_eq!(contents(&ws, &a, "", "doc").await.unwrap(), b"hello");
    assert_eq!(
        ws.stat(&a, h::StatWorkspaceRequest { path: "doc".into() })
            .await
            .unwrap()
            .entry
            .unwrap()
            .size,
        5
    );
    assert_eq!(
        ws.list(
            &a,
            h::ListWorkspaceRequest {
                path: ".".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .entries
        .len(),
        1
    );
    assert_eq!(a.list_volumes().unwrap().volumes.len(), 1);
    assert_eq!(
        a.get_volume(h::GetVolumeRequest {
            volume_id: DEFAULT_VOLUME.into()
        })
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(core.store.scope_used_bytes("a").unwrap(), 11);
    let mut archive = ws
        .archive(&a, h::ArchiveWorkspaceRequest { path: ".".into() })
        .await
        .unwrap();
    let mut bytes = vec![];
    while let Some(frame) = archive.next().await {
        let frame = frame.unwrap();
        if frame.r#final {
            assert_eq!(frame.sha256, Sha256::digest(&bytes).to_vec())
        }
        bytes.extend(frame.data);
    }
    let extract = |dir: &str| {
        use h::extract_workspace_request::Payload;
        iter(
            [
                Payload::Header(h::ExtractWorkspaceHeader {
                    request_id: dir.into(),
                    path: dir.into(),
                }),
                Payload::Data(bytes.clone()),
                Payload::Finish(true),
            ]
            .into_iter()
            .map(|p| Ok(h::ExtractWorkspaceRequest { payload: Some(p) })),
        )
    };
    core.store.record_volume_usage("a", "vol", 116).unwrap();
    assert_eq!(
        ws.extract(&a, extract("blocked")).await.unwrap_err().code(),
        Code::ResourceExhausted
    );
    assert_eq!(
        ws.write(&a, iter(upload("", "no", b"x", &[], true)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    core.store.record_volume_usage("a", "vol", 6).unwrap();
    let restored = ws.extract(&a, extract("copy")).await.unwrap();
    assert_eq!(restored.sha256, Sha256::digest(&bytes).to_vec());
    assert_eq!(
        contents(&ws, &a, "", "copy/tree/doc").await.unwrap(),
        b"hello"
    );
    assert_eq!(core.store.scope_used_bytes("a").unwrap(), 16);
}

#[tokio::test]
async fn default_workspace_resolves_to_single_mounted_workspace_volume() {
    let dir = tempfile::tempdir().unwrap();
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("db")).unwrap()),
        Arc::new(FakeDom0Backend::default()),
    );
    let ws = WorkspaceService::new(Arc::new(driver::TestDriver::default()));
    let ctx = session(&core, "a");
    ctx.create_volume(h::CreateVolumeRequest {
        volume_id: "vol-a".into(),
        request_id: "create-a".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let image = ImageSource {
        manifest_digest: [3; 32],
        staged_extent: "staged".into(),
        physical_bytes: 1,
    };
    core.import_image("a", "base", &image).await.unwrap();
    let key = SandboxKey {
        scope_id: "a".into(),
        sandbox_id: "shell".into(),
        generation_id: "g1".into(),
    };
    core.define_sandbox(&SandboxDefinition {
        key: key.clone(),
        pids_max: None,
        image_reference: "base".into(),
        manifest_digest: image.manifest_digest,
        read_only_root: true,
        root_overlay: None,
        mounts: vec![vmm_core::Mount {
            volume_id: "vol-a".into(),
            guest_path: "/workspace".into(),
            access: vmm_core::AccessMode::ReadWrite,
        }],
        host_shares: vec![],
        entrypoint: None,
        command: None,
        workdir: None,
        user: None,
        log_limit_bytes: 1024,
        env: Default::default(),
    })
    .await
    .unwrap();

    // Empty volume_id writes, stats, lists, reads, archives, and extracts against vol-a.
    ws.write(&ctx, iter(upload("", "w1", b"hello", &[], true)))
        .await
        .unwrap();
    assert_eq!(contents(&ws, &ctx, "", "doc").await.unwrap(), b"hello");
    assert_eq!(contents(&ws, &ctx, "vol-a", "doc").await.unwrap(), b"hello");
    assert_eq!(
        ws.stat(&ctx, h::StatWorkspaceRequest { path: "doc".into() })
            .await
            .unwrap()
            .entry
            .unwrap()
            .size,
        5
    );
    assert_eq!(
        ws.list(
            &ctx,
            h::ListWorkspaceRequest {
                path: ".".into(),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .entries
        .len(),
        1
    );
    let mut archive = ws
        .archive(&ctx, h::ArchiveWorkspaceRequest { path: ".".into() })
        .await
        .unwrap();
    let mut tar_bytes = vec![];
    while let Some(frame) = archive.next().await {
        tar_bytes.extend(frame.unwrap().data);
    }
    let extract_frames = {
        use h::extract_workspace_request::Payload;
        iter(
            [
                Payload::Header(h::ExtractWorkspaceHeader {
                    request_id: "ext-1".into(),
                    path: "copy".into(),
                }),
                Payload::Data(tar_bytes),
                Payload::Finish(true),
            ]
            .into_iter()
            .map(|p| Ok(h::ExtractWorkspaceRequest { payload: Some(p) })),
        )
    };
    ws.extract(&ctx, extract_frames).await.unwrap();
    assert_eq!(
        contents(&ws, &ctx, "vol-a", "copy/tree/doc").await.unwrap(),
        b"hello"
    );
    assert!(matches!(
        core.store.volume("a", DEFAULT_VOLUME),
        Err(vmm_core::Error::Missing(_))
    ));
    assert_eq!(core.store.volume("a", "vol-a").unwrap().used_bytes, 10);

    // Leading slash rejection in target() remains intact.
    assert_eq!(
        ws.stat(
            &ctx,
            h::StatWorkspaceRequest {
                path: "/workspace/doc".into()
            }
        )
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
}
