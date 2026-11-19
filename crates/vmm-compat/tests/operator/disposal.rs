use super::*;
use h::LocalDisposalTargetKind as Kind;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tonic::Status;
use vmm_compat::operator::{LocalDisposals, RegistrationControl};
use vmm_core::{Core, fake::FakeDom0Backend};

#[derive(Default)]
struct Connectors {
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
async fn trusted_client_observes_private_pagination_and_resumes_local_disposal_over_grpc() {
    use h::operator_service_client::OperatorServiceClient;
    use tokio_stream::wrappers::TcpListenerStream;
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
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let endpoint = tonic::transport::Endpoint::from_shared(format!(
        "http://{}",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let task = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(LocalOperatorServer(api.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let mut client = OperatorServiceClient::new(endpoint.connect().await.unwrap());
    let disk = client
        .get_local_disk_summary(h::GetLocalDiskSummaryRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(disk.guest_state_fs.is_none());
    assert!(!disk.unavailable_reason.is_empty());
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
