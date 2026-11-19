use super::*;
use h::LocalDisposalTargetKind as Kind;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use vmm_rpc::Status;
use vmm_compat::operator::{LocalDisposals, RegistrationControl};
use vmm_core::{Core, fake::FakeDom0Backend};

#[derive(Default)]
pub(super) struct Connectors {
    stops: AtomicUsize,
    credential_busy: AtomicBool,
}

impl RegistrationControl for Connectors {
    fn metadata(&self, _: &str) -> Result<(u64, String), Status> {
        Ok((3, "External pool".into()))
    }
    fn isolate(
        &self,
        _: &str,
        revision: u64,
        fence: &mut dyn FnMut() -> Result<(), Status>,
    ) -> Result<(), Status> {
        if revision != 3 {
            return Err(Status::aborted("policy changed"));
        }
        self.stops.fetch_add(1, Ordering::SeqCst);
        fence()
    }
    fn delete_credential(&self, _: &str) -> Result<(), Status> {
        if self.credential_busy.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("keychain busy"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn confirmed_external_disposal_fences_once_and_recovers_the_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compat.sqlite");
    let store = Arc::new(Store::open(dir.path().join("host.sqlite")).unwrap());
    let control = Arc::new(Connectors::default());
    let open = || LocalDisposals::open(&path, store.clone(), control.clone()).unwrap();
    store.create_registration("pool").unwrap();
    store.install_appliance("service", "managed").unwrap();
    let channel = store.connect("pool", 5).unwrap();
    store
        .create_scope(
            &scope(
                "child",
                Origin::Remote {
                    registration_id: "pool".into(),
                },
            ),
            Some(&channel),
        )
        .unwrap();
    let api = open();
    let target = |kind: Kind, id: &str| h::PlanLocalDisposalRequest {
        target_kind: kind as i32,
        target_id: id.into(),
    };
    let confirm = |plan: h::LocalDisposalPlan, request: &str, yes| h::ExecuteLocalDisposalRequest {
        plan: Some(plan),
        request_id: request.into(),
        irreversible_confirmation: yes,
    };
    for (kind, id, code) in [
        (Kind::Environment, "child", Code::FailedPrecondition),
        (Kind::Registration, "managed", Code::FailedPrecondition),
        (Kind::Registration, "absent", Code::NotFound),
    ] {
        assert_eq!(api.plan(target(kind, id)).unwrap_err().code(), code);
    }
    let plan = api.plan(target(Kind::Registration, "pool")).unwrap();
    assert!(plan.activity_unknown);
    assert_eq!((plan.environment_count, plan.revision), (1, 3));
    assert_eq!(
        api.execute(confirm(plan.clone(), "accept", false))
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut wrong = plan.clone();
    wrong.revision += 1;
    assert_eq!(
        api.execute(confirm(wrong, "accept", true))
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    assert!(store.registration("pool").unwrap().connected);
    assert_eq!(control.stops.load(Ordering::SeqCst), 0);

    let op = api.execute(confirm(plan.clone(), "accept", true)).unwrap();
    assert_eq!(op.kind, "local-disposal");
    assert_eq!(op.stage, "isolated");
    let resource = op.resource.as_ref().unwrap();
    assert_eq!(resource.kind, "environment");
    assert_eq!(resource.environment_id, "pool");
    assert_eq!(control.stops.load(Ordering::SeqCst), 1);
    assert!(!store.registration("pool").unwrap().connected);
    assert_eq!(
        api.execute(confirm(plan.clone(), "accept", true)).unwrap(),
        op
    );
    assert_eq!(
        api.execute(confirm(plan.clone(), "another-confirmation", true))
            .unwrap(),
        op
    );
    let mut wrong = plan.clone();
    wrong.target_id = "managed".into();
    assert_eq!(
        api.execute(confirm(wrong, "accept", true))
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    drop(api);

    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    let api = open();
    assert_eq!(api.pending("").unwrap(), std::slice::from_ref(&op.id));
    control.credential_busy.store(true, Ordering::SeqCst);
    assert_eq!(
        api.reconcile(&core, &op.id).await.unwrap_err().code(),
        Code::Unavailable
    );
    assert_eq!(
        api.operation(&op.id).unwrap().error.unwrap().reason,
        "LOCAL_DISPOSAL_PENDING"
    );
    api.reconcile(&core, &op.id).await.unwrap();
    assert_eq!(store.scope("child").unwrap().state, ScopeState::Released);
    assert!(api.pending("").unwrap().is_empty());
    let done = api.operation(&op.id).unwrap();
    assert_eq!(done.state(), h::OperationState::Succeeded);
    assert!(done.error.is_none());
    assert_eq!(api.execute(confirm(plan, "accept", true)).unwrap(), done);
    assert_eq!(control.stops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn trusted_client_observes_private_pagination_and_resumes_local_disposal_over_rpc() {
    use h::operator_service_client::OperatorServiceClient;
    use vmm_compat::operator::{LocalOperator, LocalOperatorServer};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let path = dir.path().join("compat.sqlite");
    store
        .create_scope(&scope("owned", Origin::Local), None)
        .unwrap();
    for i in 0..31 {
        store.create_registration(&format!("r-{i:02}")).unwrap();
    }
    store.install_appliance("installer", "r-31").unwrap();
    store.create_registration("r-32").unwrap();
    let api = Arc::new(
        LocalOperator::open(&path, store.clone(), Arc::new(Connectors::default()), None).unwrap(),
    );
    let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
    let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
    let task = tokio::spawn(
        vmm_rpc::Router::new(LocalOperatorServer(api.clone()))
            .serve_unix(listener, 128, std::future::pending()),
    );
    let mut client = OperatorServiceClient::new(vmm_rpc::Client::unix(&endpoint));
    let disk = client
        .get_local_disk_summary(h::GetLocalDiskSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(disk.guest_state_fs.is_none());
    assert!(!disk.unavailable_reason.is_empty());
    let mut approval = h::SetHostSocketRequest {
        request_id: "socket".into(),
        environment_id: "owned".into(),
        port: 53,
        path: "/test.sock".into(),
    };
    assert_eq!(
        client
            .set_host_socket(approval.clone())
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    approval.port = 9393;
    let saved = client
        .set_host_socket(approval)
        .await
        .unwrap()
        .into_inner()
        .host_socket
        .unwrap();
    let listed = client
        .list_host_sockets(h::ListHostSocketsRequest::default())
        .await
        .unwrap()
        .into_inner()
        .host_sockets;
    assert_eq!(listed, [saved]);
    client
        .remove_host_socket(h::RemoveHostSocketRequest {
            environment_id: "owned".into(),
            port: 9393,
            request_id: "remove-socket".into(),
        })
        .await
        .unwrap();
    let page = client
        .list_local_environment_page(h::ListLocalEnvironmentPageRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.boot_id, disk.boot_id);
    assert_eq!(page.environments[0].state, "vm_stopped");
    let page = client
        .list_local_registration_page(h::ListLocalRegistrationPageRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(page.registrations.len(), 31);
    assert_eq!(page.next_cursor, "r-31");
    let last = client
        .list_local_registration_page(h::ListLocalRegistrationPageRequest {
            after_id: page.next_cursor,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(last.registrations[0].id, "r-32");
    assert!(last.next_cursor.is_empty());
    let impact = client
        .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(impact.complete && !impact.overflow);
    assert_eq!(impact.vm.unwrap().boot_id, disk.boot_id);
    assert_eq!(impact.known_workloads.len(), 34);
    assert!(
        impact
            .known_workloads
            .iter()
            .any(|row| row.environment_id == "owned" && row.generation == 1)
    );
    let watermark = |response: &h::GetVmImpactSummaryResponse| {
        response
            .known_workloads
            .iter()
            .find(|r| r.resource_id == "r-32")
            .unwrap()
            .generation
    };
    let before = watermark(
        &client
            .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
            .await
            .unwrap()
            .into_inner(),
    );
    let channel = store.connect("r-32", 1).unwrap();
    store
        .create_scope(
            &scope(
                "remote-child",
                Origin::Remote {
                    registration_id: "r-32".into(),
                },
            ),
            Some(&channel),
        )
        .unwrap();
    let updated = client
        .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(watermark(&updated) > before);
    let environment = client
        .get_environment(h::OperatorServiceGetEnvironmentRequest {
            environment_id: "remote-child".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .environment
        .unwrap();
    assert_eq!(environment.inventory_state(), h::InventoryState::VmStopped);
    assert!(environment.usage.unwrap().stale);
    assert_eq!(environment.owner.unwrap().registration_id, "r-32");
    for i in 0..70 {
        store
            .create_registration(&format!("overflow-{i:02}"))
            .unwrap();
    }
    let impact = client
        .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(!impact.complete && impact.overflow);
    assert_eq!(impact.known_workloads.len(), 100);
    assert!(!impact.issue.is_empty());
    let plan = client
        .plan_local_disposal(h::PlanLocalDisposalRequest {
            target_kind: Kind::Environment as i32,
            target_id: "owned".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .plan;
    let confirm = h::ExecuteLocalDisposalRequest {
        plan,
        request_id: "once".into(),
        irreversible_confirmation: true,
    };
    let receipt = client
        .execute_local_disposal(confirm.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    let replay = client
        .execute_local_disposal(confirm)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(receipt.id, replay.id);
    let get = h::GetOperationRequest {
        operation_id: receipt.id.clone(),
    };
    assert_eq!(
        client
            .get_operation(get.clone())
            .await
            .unwrap()
            .into_inner()
            .operation,
        Some(receipt)
    );
    let core = Core::new(store.clone(), Arc::new(FakeDom0Backend::default()));
    let worker = tokio::spawn(async move { api.drive_disposals(&core).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if client
                .get_operation(get.clone())
                .await
                .unwrap()
                .into_inner()
                .operation
                .unwrap()
                .state()
                == h::OperationState::Succeeded
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.scope("owned").unwrap().state, ScopeState::Released);
    worker.abort();
    task.abort();
}

#[tokio::test]
async fn vm_mutations_bind_boot_acknowledgement_and_replay_terminal_receipts() {
    use h::operator_service_client::OperatorServiceClient;
    use vmm_compat::operator::{LocalOperator, LocalOperatorServer};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host")).unwrap());
    let api = LocalOperator::open(
        &dir.path().join("compat"),
        store,
        Arc::new(Connectors::default()),
        Some(Arc::new(vmm_core::fake::FakeDom0Backend::default())),
    )
    .unwrap();
    let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
    let listener = tokio::net::UnixListener::bind(&endpoint).unwrap();
    let server = tokio::spawn(
        vmm_rpc::Router::new(LocalOperatorServer(Arc::new(api)))
            .serve_unix(listener, 128, std::future::pending()),
    );
    let mut client = OperatorServiceClient::new(vmm_rpc::Client::unix(&endpoint));
    let vm = client
        .get_vm_impact_summary(h::GetVmImpactSummaryRequest {})
        .await
        .unwrap()
        .into_inner()
        .vm
        .unwrap();
    let mut req = h::StopVmRequest {
        request_id: "stop".into(),
        force: true,
        fence: Some(h::VmMutationFence {
            state: vm.state,
            boot_id: vm.boot_id.clone(),
            ..Default::default()
        }),
    };
    assert_eq!(
        client.stop_vm(req.clone()).await.unwrap_err().code(),
        Code::Aborted
    );
    req.fence.as_mut().unwrap().whole_vm_impact_acknowledged = true;
    let accepted = client
        .stop_vm(req.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(accepted.state(), h::OperationState::Succeeded);
    assert_eq!(
        Some(&accepted),
        client
            .get_operation(h::GetOperationRequest {
                operation_id: accepted.id.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .operation
            .as_ref()
    );
    assert_eq!(
        Some(accepted),
        client
            .stop_vm(req.clone())
            .await
            .unwrap()
            .into_inner()
            .operation
    );
    let mut start = h::StartVmRequest {
        request_id: "start".into(),
        fence: req.fence,
    };
    assert_eq!(
        client.start_vm(start.clone()).await.unwrap_err().code(),
        Code::Aborted
    );
    let observed = client
        .get_vm(h::GetVmRequest {})
        .await
        .unwrap()
        .into_inner()
        .vm
        .unwrap();
    assert_eq!(observed.state(), h::VmState::Stopped);
    let fence = start.fence.as_mut().unwrap();
    fence.state = observed.state;
    fence.boot_id = observed.boot_id;
    let started = client
        .start_vm(start)
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(started.stage, "running");
    let observed = client
        .get_vm(h::GetVmRequest {})
        .await
        .unwrap()
        .into_inner()
        .vm
        .unwrap();
    assert_eq!(observed.state(), h::VmState::Running);
    assert_ne!(observed.boot_id, vm.boot_id);
    server.abort();
}

#[tokio::test]
async fn operator_client_plans_resets_and_confirms_named_volume_discard() {
    use h::operator_service_client::OperatorServiceClient;
    use vmm_compat::operator::{LocalOperator, LocalOperatorServer};
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("host.db")).unwrap());
    let backend = Arc::new(FakeDom0Backend::default());
    let core = Core::new(store.clone(), backend.clone());
    let id = "cleanup-local";
    store.create_scope(&scope(id, Origin::Local), None).unwrap();
    core.create_volume(&vmm_core::Volume {
        scope_id: id.into(),
        volume_id: "docs".into(),
        quota_bytes: 1,
        used_bytes: 0,
        owner_uid: 1000,
        owner_gid: 1000,
        mode: 0o700,
        key_id: None,
    })
    .await
    .unwrap();
    let api = LocalOperator::open(
        &dir.path().join("compat.db"),
        store.clone(),
        Arc::new(Connectors::default()),
        Some(backend),
    )
    .unwrap();
    let endpoint = dir.path().join(uuid::Uuid::new_v4().simple().to_string());
    let socket = tokio::net::UnixListener::bind(&endpoint).unwrap();
    let task = tokio::spawn(
        vmm_rpc::Router::new(LocalOperatorServer(Arc::new(api)))
            .serve_unix(socket, 128, std::future::pending()),
    );
    let mut client = OperatorServiceClient::new(
        vmm_rpc::Client::unix(&endpoint),
    );
    let reset = client
        .plan_reset_environment(h::PlanResetEnvironmentRequest {
            environment_id: id.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    assert!(!reset.impacts[0].irreversible);
    let op = client
        .execute_cleanup(h::ExecuteCleanupRequest {
            request_id: "reset".into(),
            plan: Some(reset),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(op.state(), h::OperationState::Succeeded);
    let discard = client
        .plan_discard_environment(h::PlanDiscardEnvironmentRequest {
            environment_id: id.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    let mut req = h::ExecuteCleanupRequest {
        plan: Some(discard),
        request_id: "discard".into(),
        ..Default::default()
    };
    let error = client.execute_cleanup(req.clone()).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(store.volume(id, "docs").is_ok());
    req.confirmed_volume_ids = vec!["docs".into()];
    let op = client
        .execute_cleanup(req.clone())
        .await
        .unwrap()
        .into_inner()
        .operation
        .unwrap();
    assert_eq!(op.state(), h::OperationState::Succeeded);
    assert_eq!(
        client
            .execute_cleanup(req)
            .await
            .unwrap()
            .into_inner()
            .operation
            .as_ref(),
        Some(&op)
    );
    assert_eq!(
        client
            .get_operation(h::GetOperationRequest {
                operation_id: op.id.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .operation,
        Some(op)
    );
    assert_eq!(store.scope(id).unwrap().state, ScopeState::Released);
    task.abort();
}
