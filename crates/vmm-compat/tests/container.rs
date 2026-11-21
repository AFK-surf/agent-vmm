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

struct StartFixture {
    _dir: tempfile::TempDir,
    backend: Arc<FakeDom0Backend>,
    core: Core,
    ctx: RuntimeContext,
    driver: Arc<Driver>,
    host: Containers,
}

async fn setup_start_fixture() -> StartFixture {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(FakeDom0Backend::default());
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("core")).unwrap()),
        backend.clone(),
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
    StartFixture {
        _dir: dir,
        backend,
        core,
        ctx,
        driver,
        host,
    }
}

async fn create_worker(fix: &StartFixture, id: &str, with_share: bool) -> h::Container {
    let volumes = if with_share {
        vec![h::VolumeMount {
            source: Some(h::volume_mount::Source::HostShare("project".into())),
            destination: "/project".into(),
            read_only: true,
        }]
    } else {
        vec![]
    };
    fix.host
        .create(
            &fix.ctx,
            h::CreateContainerRequest {
                request_id: format!("create-{id}"),
                container_id: id.into(),
                image: "base".into(),
                log_limit_bytes: 1024,
                volumes,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .container
        .unwrap()
}

#[tokio::test]
async fn legacy_failed_start_receipt_recovers_after_preserve_upgrade() {
    use prost::Message;
    use vmm_core::{ReceiptState, RequestKey};

    let fix = setup_start_fixture().await;
    let made = create_worker(&fix, "worker", false).await;
    let sb = fix
        .core
        .store
        .list_sandboxes("scope", None, 20)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    // Simulate a legacy failed start leaving phase='starting' with no live VM and a
    // Failed receipt carrying exact stored Internal("host store failure") bytes.
    fix.backend
        .inject(vmm_core::fake::Fault::InterruptedStart)
        .unwrap();
    assert!(fix.core.start(&sb.definition.key).await.is_err());

    let start = h::StartContainerRequest {
        request_id: "start:gen:initial".into(),
        container_id: "worker".into(),
        expected_generation_id: made.generation_id.clone(),
    };
    let req_bytes = start.encode_to_vec();
    let receipt_key = RequestKey {
        domain: "runtime-start-container",
        scope_id: "scope",
        request_id: "start:gen:initial",
    };
    fix.core
        .store
        .begin_operation(receipt_key, &req_bytes)
        .unwrap();
    let legacy_internal = hex::decode("080d1212686f73742073746f7265206661696c757265").unwrap();
    fix.core
        .store
        .finish_operation(
            receipt_key,
            &req_bytes,
            ReceiptState::Failed,
            &legacy_internal,
        )
        .unwrap();

    assert_eq!(
        Containers::migrate_legacy_failed_receipts(&fix.core.store).unwrap(),
        1
    );
    fix.core.reconcile_after_restart().await.unwrap();

    let receipt = fix.core.store.receipt(receipt_key).unwrap().unwrap();
    assert_eq!(receipt.state, ReceiptState::Retryable);

    let got = fix
        .host
        .get(
            &fix.ctx,
            h::GetContainerRequest {
                container_id: "worker".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(got.state(), h::ContainerState::Created);
    assert!(got.start_request_instance_id.is_empty());

    let listed = fix.host.list(&fix.ctx).await.unwrap().containers;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].state(), h::ContainerState::Created);
    assert!(listed[0].start_request_instance_id.is_empty());

    let env = fix.host.environment(&fix.ctx).await.unwrap();
    let env_containers = env.environment.unwrap().containers;
    assert_eq!(env_containers.len(), 1);
    assert_eq!(env_containers[0].state(), h::ContainerState::Created);
    assert!(env_containers[0].start_request_instance_id.is_empty());

    let live = fix
        .host
        .start(&fix.ctx, start)
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(live.state(), h::ContainerState::Running);
    assert!(!live.instance_id.is_empty());
    assert_eq!(live.instance_id, live.start_request_instance_id);
    assert_eq!(
        fix.core.store.receipt(receipt_key).unwrap().unwrap().state,
        ReceiptState::Succeeded
    );
}

#[tokio::test]
async fn transient_start_failures_retry_with_same_request_id() {
    use vmm_core::{ReceiptState, RequestKey, fake::Fault};

    let fix = setup_start_fixture().await;

    // Case 1: ResourceExhausted (ComputeSlots) -> Retryable -> direct retry succeeds.
    let w1 = create_worker(&fix, "worker-slots", false).await;
    let start1 = h::StartContainerRequest {
        request_id: "start-slots".into(),
        container_id: "worker-slots".into(),
        expected_generation_id: w1.generation_id,
    };
    fix.backend.inject(Fault::ComputeSlots).unwrap();
    let err = fix.host.start(&fix.ctx, start1.clone()).await.unwrap_err();
    assert_eq!(err.code(), Code::ResourceExhausted);
    let key1 = RequestKey {
        domain: "runtime-start-container",
        scope_id: "scope",
        request_id: "start-slots",
    };
    assert_eq!(
        fix.core.store.receipt(key1).unwrap().unwrap().state,
        ReceiptState::Retryable
    );
    let live1 = fix
        .host
        .start(&fix.ctx, start1)
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(live1.state(), h::ContainerState::Running);
    assert_eq!(
        fix.core.store.receipt(key1).unwrap().unwrap().state,
        ReceiptState::Succeeded
    );

    // Case 2: Unavailable (InterruptedStart) -> Retryable -> reconcile -> retry succeeds.
    let w2 = create_worker(&fix, "worker-interrupted", false).await;
    let start2 = h::StartContainerRequest {
        request_id: "start-interrupted".into(),
        container_id: "worker-interrupted".into(),
        expected_generation_id: w2.generation_id,
    };
    fix.backend.inject(Fault::InterruptedStart).unwrap();
    let err = fix.host.start(&fix.ctx, start2.clone()).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    let key2 = RequestKey {
        domain: "runtime-start-container",
        scope_id: "scope",
        request_id: "start-interrupted",
    };
    assert_eq!(
        fix.core.store.receipt(key2).unwrap().unwrap().state,
        ReceiptState::Retryable
    );
    fix.core.reconcile_after_restart().await.unwrap();
    let live2 = fix
        .host
        .start(&fix.ctx, start2)
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(live2.state(), h::ContainerState::Running);
    assert_eq!(
        fix.core.store.receipt(key2).unwrap().unwrap().state,
        ReceiptState::Succeeded
    );
}

#[tokio::test]
async fn deterministic_start_failures_stay_failed_and_do_not_retry() {
    use vmm_core::{ReceiptState, RequestKey};

    let fix = setup_start_fixture().await;
    let made = create_worker(&fix, "worker", true).await;
    let sb = fix
        .core
        .store
        .list_sandboxes("scope", None, 20)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    // 1. PermissionDenied (share revoked)
    fix.driver.share_approved.store(false, Ordering::Relaxed);
    let req_perm = h::StartContainerRequest {
        request_id: "start-perm".into(),
        container_id: "worker".into(),
        expected_generation_id: made.generation_id.clone(),
    };
    assert_eq!(
        fix.host
            .start(&fix.ctx, req_perm.clone())
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );

    // 2. InvalidArgument (missing expected_generation_id)
    let req_invalid = h::StartContainerRequest {
        request_id: "start-invalid".into(),
        container_id: "worker".into(),
        expected_generation_id: String::new(),
    };
    assert_eq!(
        fix.host
            .start(&fix.ctx, req_invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );

    // 3. Aborted (wrong expected_generation_id)
    let req_aborted = h::StartContainerRequest {
        request_id: "start-aborted".into(),
        container_id: "worker".into(),
        expected_generation_id: "wrong-gen".into(),
    };
    assert_eq!(
        fix.host
            .start(&fix.ctx, req_aborted)
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );

    assert_eq!(
        Containers::migrate_legacy_failed_receipts(&fix.core.store).unwrap(),
        0
    );
    for id in ["start-perm", "start-invalid", "start-aborted"] {
        let key = RequestKey {
            domain: "runtime-start-container",
            scope_id: "scope",
            request_id: id,
        };
        assert_eq!(
            fix.core.store.receipt(key).unwrap().unwrap().state,
            ReceiptState::Failed
        );
    }

    // Replaying the PermissionDenied request after share re-approval must still replay the error.
    fix.driver.share_approved.store(true, Ordering::Relaxed);
    assert_eq!(
        fix.host.start(&fix.ctx, req_perm).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert!(fix.core.store.sandbox(&sb.definition.key).unwrap().boot.is_none());
}

#[tokio::test]
async fn previously_started_container_reports_stopped_after_reconcile() {
    let fix = setup_start_fixture().await;
    let made = create_worker(&fix, "worker", false).await;
    let live = fix
        .host
        .start(
            &fix.ctx,
            h::StartContainerRequest {
                request_id: "start-once".into(),
                container_id: "worker".into(),
                expected_generation_id: made.generation_id,
            },
        )
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(live.state(), h::ContainerState::Running);

    let sb = fix
        .core
        .store
        .list_sandboxes("scope", None, 20)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let boot = sb.boot.as_ref().unwrap();
    fix.core.stop(boot, true).await.unwrap();
    fix.core.reconcile_after_restart().await.unwrap();

    let got = fix
        .host
        .get(
            &fix.ctx,
            h::GetContainerRequest {
                container_id: "worker".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(got.state(), h::ContainerState::Stopped);
    assert_eq!(got.start_request_instance_id, live.instance_id);
}

#[tokio::test]
async fn lost_start_reply_recovers_only_for_same_request_id() {
    use vmm_core::{ReceiptState, RequestKey, fake::Fault};

    let fix = setup_start_fixture().await;
    let made = create_worker(&fix, "worker", false).await;
    let start_lost = h::StartContainerRequest {
        request_id: "start-lost".into(),
        container_id: "worker".into(),
        expected_generation_id: made.generation_id.clone(),
    };

    fix.backend.inject(Fault::LostStartReply).unwrap();
    let err = fix
        .host
        .start(&fix.ctx, start_lost.clone())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);

    fix.core.reconcile_after_restart().await.unwrap();

    // A new request_id on an already-running container must still be rejected.
    let start_other = h::StartContainerRequest {
        request_id: "start-other".into(),
        container_id: "worker".into(),
        expected_generation_id: made.generation_id,
    };
    assert_eq!(
        fix.host
            .start(&fix.ctx, start_other)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );

    // Retrying with the original request_id adopts the live VM and backfills start_request_instance_id.
    let recovered = fix
        .host
        .start(&fix.ctx, start_lost)
        .await
        .unwrap()
        .container
        .unwrap();
    assert_eq!(recovered.state(), h::ContainerState::Running);
    assert!(!recovered.instance_id.is_empty());
    assert_eq!(recovered.start_request_instance_id, recovered.instance_id);
    let key = RequestKey {
        domain: "runtime-start-container",
        scope_id: "scope",
        request_id: "start-lost",
    };
    assert_eq!(
        fix.core.store.receipt(key).unwrap().unwrap().state,
        ReceiptState::Succeeded
    );
}
