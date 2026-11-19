use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use vmm_rpc::{Code, Request, Status};
use vmm_compat::{Containers, RuntimeContext, RuntimeDriver};
use vmm_core::{
    BootIdentity, Core, EgressMode, HostShareMount, ImageSource, Limits, Origin, Scope, ScopeState,
    Store, fake::FakeDom0Backend,
};
use vmm_guest_proto::rpc::{HealthReply, MainState, ResourceUsage};

struct Driver {
    share_approved: AtomicBool,
}
#[async_trait]
impl RuntimeDriver for Driver {
    async fn authorize_shares(&self, scope: &str, shares: &[HostShareMount]) -> Result<(), Status> {
        assert_eq!(scope, "scope");
        assert_eq!(shares.len(), 1);
        assert_eq!(shares[0].share_id, "project");
        assert!(shares[0].read_only);
        if self.share_approved.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err(Status::permission_denied("share is revoked"))
        }
    }
    async fn health(&self, boot: &BootIdentity) -> Result<HealthReply, Status> {
        Ok(HealthReply {
            instance_id: boot.instance_id.clone(),
            boot_token_proof: [0; 32],
            main: MainState::Idle,
            quiesced: false,
            uptime_ms: 100,
            orphans_reaped: 0,
            usage: ResourceUsage {
                sampled_unix_nanos: 1_000_000_001,
                pids: Some(2),
                ..Default::default()
            },
        })
    }
}

#[tokio::test]
async fn preserves_process_overrides_pid_limits_and_instance_identity() {
    let dir = tempfile::tempdir().unwrap();
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("core")).unwrap()),
        Arc::new(FakeDom0Backend::default()),
    );
    let scope = Scope {
        id: "scope".into(),
        origin: Origin::Local,
        generation: 1,
        revision: 1,
        limits: Limits {
            pids_max: 16,
            disk_quota_bytes: 100,
            vcpus: 1,
            boot_ram_mb: 64,
            max_ram_mb: 64,
        },
        egress_mode: EgressMode::DenyAll,
        host_socket_rules: vec![],
        state: ScopeState::Active,
    };
    core.store.create_scope(&scope, None).unwrap();
    core.import_image(
        "scope",
        "base",
        &ImageSource {
            manifest_digest: [42; 32],
            staged_extent: "stage".into(),
            physical_bytes: 1,
        },
    )
    .await
    .unwrap();
    let mut req = Request::new(());
    req.extensions_mut()
        .insert(core.store.issue_session("scope", None).unwrap().0);
    let ctx = RuntimeContext::from_request(&core, &req).unwrap();
    let driver = Arc::new(Driver {
        share_approved: AtomicBool::new(true),
    });
    let host = Containers::open(dir.path().join("protocol"), driver.clone()).unwrap();
    let create = h::CreateContainerRequest {
        request_id: "create".into(),
        container_id: "worker".into(),
        image: "base".into(),
        entrypoint_override: Some(h::StringList::default()),
        command_override: None,
        workdir: Some(String::new()),
        user: None,
        limits: Some(h::ResourceLimits {
            pids: Some(8),
            disk_bytes: None,
        }),
        log_limit_bytes: 1024,
        volumes: vec![h::VolumeMount {
            source: Some(h::volume_mount::Source::HostShare("project".into())),
            destination: "/project".into(),
            read_only: true,
        }],
        ..Default::default()
    };
    for (pids, code) in [(0, Code::InvalidArgument), (17, Code::ResourceExhausted)] {
        let mut bad = create.clone();
        bad.request_id = format!("reject-{pids}");
        bad.limits.as_mut().unwrap().pids = Some(pids);
        assert_eq!(host.create(&ctx, bad).await.unwrap_err().code(), code);
    }
    let mut competing = create.clone();
    competing.request_id = "independent-create".into();
    let results = tokio::join!(
        host.create(&ctx, create.clone()),
        host.create(&ctx, competing)
    );
    let made = match results {
        (Ok(made), Err(other)) | (Err(other), Ok(made)) => {
            assert_eq!(other.code(), Code::AlreadyExists);
            made.container.unwrap()
        }
        _ => panic!("concurrent creation must produce one generation"),
    };
    assert_eq!(made.state, h::ContainerState::Created as i32);
    let defs = core.store.list_sandboxes("scope", None, 20).unwrap();
    assert_eq!(defs.len(), 1);
    let d = &defs[0].definition;
    assert_eq!(
        (
            d.pids_max,
            d.entrypoint.as_deref(),
            d.command.as_deref(),
            d.workdir.as_deref(),
            d.user.as_deref()
        ),
        (Some(8), Some([].as_slice()), None, Some(""), None)
    );
    for (workdir, matches) in [(Some(String::new()), true), (None, false)] {
        let mut expected = create.clone();
        expected.workdir = workdir;
        let got = host
            .get(
                &ctx,
                h::GetContainerRequest {
                    container_id: "worker".into(),
                    expected_configuration: Some(expected),
                },
            )
            .await
            .unwrap()
            .container
            .unwrap();
        assert_eq!(got.configuration_matches, Some(matches));
    }
    let start = h::StartContainerRequest {
        request_id: "start".into(),
        container_id: "worker".into(),
        expected_generation_id: made.generation_id,
    };
    driver.share_approved.store(false, Ordering::Relaxed);
    let mut denied = start.clone();
    denied.request_id = "revoked-share".into();
    assert_eq!(
        host.start(&ctx, denied).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert!(core.store.sandbox(&d.key).unwrap().boot.is_none());
    driver.share_approved.store(true, Ordering::Relaxed);
    let live = host
        .start(&ctx, start.clone())
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(live.state, h::ContainerState::Running as i32);
    assert_eq!(live.instance_id, live.start_request_instance_id);
    assert!(!live.instance_id.is_empty());
    assert_eq!(live.usage.as_ref().unwrap().pids, 2);
    assert_eq!(
        host.start(&ctx, start).await.unwrap().container.unwrap(),
        live
    );
    let id = &live.instance_id;
    let get_execs = || h::ListExecutionsRequest {
        container_id: "worker".into(),
        expected_instance_id: id.clone(),
    };
    let claim = h::AcquireExecutionRequest {
        execution_id: "main".into(),
        kind: h::ExecutionKind::MainExecution as i32,
        container_id: "worker".into(),
        expected_instance_id: id.clone(),
        ..Default::default()
    };
    assert!(host.acquire(&ctx, claim.clone()).unwrap().acquired);
    assert!(!host.acquire(&ctx, claim.clone()).unwrap().acquired);
    assert_eq!(
        host.executions(&ctx, get_execs()).unwrap().executions.len(),
        1
    );
    let q = h::QuiesceContainerRequest {
        request_id: "pause".into(),
        container_id: "worker".into(),
        expected_instance_id: id.clone(),
        ..Default::default()
    };
    assert_eq!(
        host.quiesce(&ctx, q.clone()).unwrap_err().code(),
        Code::ResourceExhausted
    );
    host.release(
        &ctx,
        h::ReleaseExecutionRequest {
            execution_id: "main".into(),
            container_id: "worker".into(),
            expected_instance_id: id.clone(),
            ..Default::default()
        },
    )
    .unwrap();
    host.quiesce(&ctx, q.clone()).unwrap();
    assert_eq!(
        host.acquire(&ctx, claim.clone()).unwrap_err().code(),
        Code::FailedPrecondition
    );
    let mut cancel = q.clone();
    cancel.cancel = true;
    host.quiesce(&ctx, cancel).unwrap();
    host.quiesce(&ctx, q).unwrap();
    let stop = h::StopContainerRequest {
        request_id: "stop".into(),
        container_id: "worker".into(),
        expected_instance_id: id.clone(),
        quiesce_request_id: "pause".into(),
        timeout_seconds: Some(5),
    };
    let mut wrong = stop.clone();
    wrong.request_id = "wrong-stop".into();
    wrong.quiesce_request_id = "old".into();
    assert_eq!(
        host.stop(&ctx, wrong).await.unwrap_err().code(),
        Code::Aborted
    );
    let stopped = host.stop(&ctx, stop).await.unwrap().container.unwrap();
    assert_eq!(stopped.state(), h::ContainerState::Stopped);
    assert_eq!(&stopped.start_request_instance_id, id);
    assert_eq!(
        host.executions(&ctx, get_execs()).unwrap().status(),
        h::ExecutionListStatus::Stopped
    );
    assert_eq!(host.acquire(&ctx, claim).unwrap_err().code(), Code::Aborted);
    host.delete(
        &ctx,
        h::DeleteContainerRequest {
            request_id: "delete".into(),
            container_id: "worker".into(),
            expected_generation_id: stopped.generation_id,
        },
    )
    .await
    .unwrap();
    assert!(host.list(&ctx).await.unwrap().containers.is_empty());
}
