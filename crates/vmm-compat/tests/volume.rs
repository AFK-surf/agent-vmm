use agent_vmm_proto::host::v1 as host;
use std::sync::Arc;
use vmm_rpc::{Code, Request};
use vmm_compat::RuntimeContext;
use vmm_core::{
    AccessMode, Core, EgressMode, ImageSource, Limits, Mount, Origin, RootOverlay,
    SandboxDefinition, SandboxKey, Scope, ScopeState, Store, fake::FakeDom0Backend,
};

#[tokio::test]
async fn sparse_volume_and_overlay_share_usage_and_preserve_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("core.db")).unwrap());
    let scope = Scope {
        id: "test".into(),
        origin: Origin::Local,
        generation: 1,
        revision: 1,
        limits: Limits {
            disk_quota_bytes: 100,
            pids_max: 64,
            vcpus: 1,
            boot_ram_mb: 64,
            max_ram_mb: 64,
        },
        egress_mode: EgressMode::DenyAll,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    };
    store.create_scope(&scope, None).unwrap();
    let (session, _) = store.issue_session(&scope.id, None).unwrap();
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    let mut transport = Request::new(());
    transport.extensions_mut().insert(session);
    let ctx = RuntimeContext::from_request(&core, &transport).unwrap();
    let create = |id: &str| host::CreateVolumeRequest {
        request_id: format!("create-{id}"),
        volume_id: id.into(),
        ..Default::default()
    };
    let first = ctx.create_volume(create("a")).await.unwrap();
    ctx.create_volume(create("b")).await.unwrap();
    let mut conflicting = create("a");
    conflicting.mode = Some(0o755);
    assert_eq!(
        ctx.create_volume(conflicting).await.unwrap_err().code(),
        Code::Aborted
    );

    let image = ImageSource {
        manifest_digest: [1; 32],
        staged_extent: "stage".into(),
        physical_bytes: 1,
    };
    core.import_image(&scope.id, "base", &image).await.unwrap();
    let key = SandboxKey {
        scope_id: scope.id.clone(),
        sandbox_id: "vm".into(),
        generation_id: "g".into(),
    };
    core.define_sandbox(&SandboxDefinition {
        key: key.clone(),
        pids_max: None,
        image_reference: "base".into(),
        manifest_digest: image.manifest_digest,
        read_only_root: false,
        root_overlay: Some(RootOverlay {
            extent_id: "rw".into(),
            quota_bytes: 100,
            used_bytes: 0,
        }),
        mounts: vec![Mount {
            volume_id: "a".into(),
            guest_path: "/work".into(),
            access: AccessMode::ReadWrite,
        }],
        host_shares: vec![],
        entrypoint: None,
        command: None,
        user: None,
        workdir: None,
        log_limit_bytes: 1024,
        env: Default::default(),
    })
    .await
    .unwrap();
    store.record_volume_usage(&scope.id, "a", 40).unwrap();
    store.record_volume_usage(&scope.id, "b", 20).unwrap();
    store.record_overlay_usage(&key, 40).unwrap();
    store.record_overlay_usage(&key, 41).unwrap();
    assert_eq!(store.scope_used_bytes(&scope.id).unwrap(), 101);
    let denied = ctx.create_volume(create("c")).await.unwrap_err();
    assert_eq!(denied.code(), Code::ResourceExhausted);
    store.record_overlay_usage(&key, 39).unwrap();
    assert_eq!(store.scope_used_bytes(&scope.id).unwrap(), 99);
    let mut retry = create("c");
    retry.request_id = "admitted-c".into();
    ctx.create_volume(retry).await.unwrap();
    assert_eq!(ctx.create_volume(create("a")).await.unwrap(), first);
    let volumes = ctx.list_volumes().unwrap().volumes;
    assert_eq!(volumes.len(), 3);
    let volume = ctx
        .get_volume(host::GetVolumeRequest {
            volume_id: "a".into(),
        })
        .unwrap()
        .volume
        .unwrap();
    assert_eq!(volumes[0], volume);
    assert_eq!(volume.limit_bytes, 100);
    assert_eq!(volume.usage.unwrap().disk_bytes, 40);
    assert_eq!(volume.mounted_by, ["vm"]);
    let delete = |id: &str| host::DeleteVolumeRequest {
        request_id: format!("remove-{id}"),
        volume_id: id.into(),
    };
    assert_eq!(
        ctx.delete_volume(delete("a")).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    ctx.delete_volume(delete("b")).await.unwrap();
    ctx.delete_volume(delete("b")).await.unwrap();
    assert_eq!(store.scope_used_bytes(&scope.id).unwrap(), 79);
}
