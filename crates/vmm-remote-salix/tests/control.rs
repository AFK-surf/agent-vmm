#[path = "support/gateway.rs"]
mod gateway;
#[path = "support/registration.rs"]
mod registration;

use gateway::{Control, GatewayMock};
use registration::{REGISTRATION, Salix, configuration, identity};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{sync::mpsc, time::timeout};
use tokio_stream::wrappers::ReceiverStream;
use vmm_core::{
    Core, ImageSource, Limits, SandboxDefinition, SandboxKey, SandboxState, ScopeState, Store,
    Volume, fake::FakeDom0Backend,
};
use vmm_remote_salix::{
    CandidateCredential, Connector, ConnectorConfig, Error, Gateway, RegistrationClient,
    remote::{
        AllocationState, CapacityDimension, CommandAckDisposition as Ack,
        CommandOutcome as Outcome, ConnectResponse, ControllerCommand, DisposeAllocation,
        EnsureAllocation, ErrorReason, OpenSession, ReleaseAllocation, ResourceLimits,
        command_result, connect_request::Payload, connect_response, controller_command::Command,
    },
};
use vmm_signing::{CredentialStore, FileCredentialStore, FileIdentityKey, IdentitySigner};

const ALLOCATION: &str = "workload";
const GENERATION: u64 = 3;
const POLICY: u64 = 7;

#[tokio::test]
async fn claim_connect_replay_retire_and_run_rpc_over_tunnel() {
    timeout(Duration::from_secs(35), async {
        let candidate = CandidateCredential::generate().unwrap();
        let mut gateway = GatewayMock::start(&candidate).await;
        let key = FileIdentityKey::generate();
        let salix = Salix::start(
            gateway.settings.clone(),
            &candidate,
            key.public_key().unwrap(),
        )
        .await;
        let claim = salix
            .claim(&candidate, &key, "fixture-ticket")
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path().join("host.db")).unwrap());
        store.create_registration(REGISTRATION).unwrap();
        let credentials = Arc::new(FileCredentialStore::new(
            directory.path().join("credentials"),
        ));
        credentials
            .replace(REGISTRATION, candidate.as_bytes())
            .unwrap();
        let client = Gateway::new(REGISTRATION, &claim.remote_connection, credentials).unwrap();
        let (runtime, mut incoming_runtime) = mpsc::channel(8);
        let backend = Arc::new(FakeDom0Backend::default());
        let core = Core::new(store.clone(), backend.clone());
        let endpoint = vmm_remote_salix::trust::EndpointObservation {
            endpoint_generation: 17,
            ..Default::default()
        };
        let (publish, observed) = tokio::sync::watch::channel(endpoint.clone());
        let connector = Arc::new(
            Connector::new(
                core.clone(),
                client,
                ConnectorConfig {
                    connection: claim.remote_connection,
                    scope_defaults: defaults(),
                    runtime,
                    observe: None,
                },
            )
            .with_endpoint(observed),
        );

        let mut attempt = start_attempt(&connector);
        let mut first = gateway.connections.recv().await.unwrap();
        let epoch = first.hello.connection_epoch;
        assert_eq!(first.hello.endpoint_observation.as_ref(), Some(&endpoint));
        let renewed = vmm_remote_salix::trust::EndpointObservation {
            device_signature: vec![42],
            ..endpoint
        };
        publish.send_replace(renewed.clone());
        let frame = first.frames.message().await.unwrap().unwrap();
        let Some(Payload::EndpointObservationRenewal(renewal)) = frame.payload else {
            panic!("missing signed endpoint renewal")
        };
        assert_eq!(renewal.connection_epoch, epoch);
        assert_eq!(renewal.endpoint_observation, Some(renewed));
        assert_ne!(epoch, 0);
        assert_ne!(first.hello.inventory_watermark, 0);
        assert_eq!(first.hello.protocol_version, "4");
        assert_eq!(first.hello.registration_id, REGISTRATION);
        assert!(first.hello.inventory.is_empty());
        assert_eq!(first.hello.observation.as_ref().unwrap().sequence, 1);

        let too_large = ensure("too-large", 2049);
        send_command(&first, "too-large", 1, POLICY, too_large).await;
        assert_ack(&mut first, "too-large", 1, Ack::Admitted).await;
        let denied = result(&mut first, "too-large", 1).await;
        assert_eq!(denied.outcome(), Outcome::Rejected);
        assert_eq!(denied.reason(), ErrorReason::CapacityExhausted);
        assert_eq!(
            denied.capacity_dimension(),
            CapacityDimension::StorageHeadroom
        );

        let ensure = ensure(ALLOCATION, 1024);
        send_command(&first, "ensure", 2, POLICY, ensure.clone()).await;
        assert_ack(&mut first, "ensure", 2, Ack::Admitted).await;
        let key = vmm_core::RequestKey {
            domain: "salix-control",
            scope_id: REGISTRATION,
            request_id: "ensure",
        };
        // The controller loses the terminal result after the host commits it.
        while !store
            .receipt(key)
            .unwrap()
            .is_some_and(|receipt| receipt.state == vmm_core::ReceiptState::Succeeded)
        {
            tokio::task::yield_now().await;
        }
        drop(first);
        assert!(matches!(attempt.await.unwrap(), Err(Error::Disconnected)));

        attempt = start_attempt(&connector);
        let mut second = gateway.connections.recv().await.unwrap();
        let next_epoch = second.hello.connection_epoch;
        assert!(next_epoch > epoch);
        assert_eq!(second.hello.inventory.len(), 1);
        let created = second.hello.inventory[0].clone();
        assert_eq!(created.allocation_id, ALLOCATION);
        assert_eq!(created.display_label, "Name of workload");
        assert_eq!(created.state(), AllocationState::Ready);
        send_command(&second, "ensure", 1, POLICY, ensure).await;
        assert_ack(&mut second, "ensure", 1, Ack::Duplicate).await;
        let replay = result(&mut second, "ensure", 1).await;
        assert_eq!(
            replay.result,
            Some(command_result::Result::Allocation(created))
        );
        assert_eq!(replay.inventory_watermark, second.hello.inventory_watermark);
        assert_eq!(replay.connection_epoch, next_epoch);

        let open = Command::OpenSession(OpenSession {
            allocation_id: ALLOCATION.into(),
            allocation_generation: GENERATION,
            execution_owner_id: "instance:3".into(),
        });
        let stale_open = Command::OpenSession(OpenSession {
            allocation_id: ALLOCATION.into(),
            allocation_generation: GENERATION + 1,
            execution_owner_id: "instance:4".into(),
        });
        send_command(&second, "stale-open", 2, 0, stale_open).await;
        assert_ack(&mut second, "stale-open", 2, Ack::Admitted).await;
        assert_eq!(
            result(&mut second, "stale-open", 2).await.reason(),
            ErrorReason::StaleRevision
        );
        send_command(&second, "open", 3, 0, open.clone()).await;
        assert_ack(&mut second, "open", 3, Ack::Admitted).await;
        let ready = session_ready(result(&mut second, "open", 3).await);
        let provided = gateway.tunnels.recv().await.unwrap();
        let runtime = incoming_runtime.recv().await.unwrap();
        assert_eq!(provided.header.registration_id, REGISTRATION);
        assert_eq!(provided.header.allocation_id, ALLOCATION);
        assert_eq!(provided.header.allocation_generation, GENERATION);
        assert_eq!(provided.header.connection_epoch, next_epoch);
        assert_eq!(provided.header.tunnel_nonce, ready.tunnel_nonce);
        assert_eq!(
            provided.header.expires_unix_millis,
            ready.expires_unix_millis
        );
        assert!(!ready.tunnel_nonce.is_empty());
        assert_eq!(runtime.execution_owner_id, "instance:3");
        let scope = runtime.authorization.scope_id.clone();
        let sandbox = define_workload(&core, &scope).await;
        core.start(&sandbox).await.unwrap();
        let previous_id = runtime.authorization.id.clone();
        let previous_capability = runtime.capability.clone();
        assert!(
            store
                .authenticate_session(&previous_id, previous_capability.as_ref())
                .is_ok()
        );

        let (_reporter, service) = tonic_health::server::health_reporter();
        let (incoming, rx) = mpsc::channel(1);
        incoming
            .send(Ok::<_, std::io::Error>(runtime.transport))
            .await
            .unwrap();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(ReceiverStream::new(rx))
                .await
                .unwrap();
        });
        let mut health = tonic_health::pb::health_client::HealthClient::new(
            gateway::tunnel_channel(provided.transport).await,
        );
        let response = health
            .check(tonic_health::pb::HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap();
        assert_eq!(
            response.into_inner().status(),
            tonic_health::pb::health_check_response::ServingStatus::Serving
        );
        drop(second);
        assert!(matches!(attempt.await.unwrap(), Err(Error::Disconnected)));
        assert!(
            store
                .authenticate_session(&previous_id, previous_capability.as_ref())
                .is_err()
        );
        server.abort();

        let attempt = start_attempt(&connector);
        let mut third = gateway.connections.recv().await.unwrap();
        send_command(&third, "open", 1, 0, open).await;
        assert_ack(&mut third, "open", 1, Ack::Duplicate).await;
        let reopened = session_ready(result(&mut third, "open", 1).await);
        assert_ne!(reopened.tunnel_nonce, ready.tunnel_nonce);
        let reopened_header = gateway.tunnels.recv().await.unwrap().header;
        assert_eq!(
            reopened_header.connection_epoch,
            third.hello.connection_epoch
        );
        assert_eq!(reopened_header.tunnel_nonce, reopened.tunnel_nonce);
        let runtime = incoming_runtime.recv().await.unwrap();

        let stale = Command::DisposeAllocation(DisposeAllocation {
            allocation_id: ALLOCATION.into(),
            expected_revision: POLICY + 1,
            expected_generation: GENERATION,
            request_id: "stale".into(),
        });
        send_command(&third, "stale", 2, POLICY + 1, stale).await;
        assert_ack(&mut third, "stale", 2, Ack::Admitted).await;
        let stale = result(&mut third, "stale", 2).await;
        assert_eq!(stale.reason(), ErrorReason::DisposalRevisionNotAccepted);
        assert!(
            store
                .authenticate_session(&runtime.authorization.id, runtime.capability.as_ref())
                .is_ok()
        );

        let stale_release = Command::ReleaseAllocation(ReleaseAllocation {
            allocation_id: ALLOCATION.into(),
            expected_revision: POLICY,
            expected_generation: GENERATION + 1,
        });
        send_command(&third, "stale-release", 3, POLICY, stale_release).await;
        assert_ack(&mut third, "stale-release", 3, Ack::Admitted).await;
        assert_eq!(
            result(&mut third, "stale-release", 3).await.reason(),
            ErrorReason::StaleRevision
        );
        let release = Command::ReleaseAllocation(ReleaseAllocation {
            allocation_id: ALLOCATION.into(),
            expected_revision: POLICY,
            expected_generation: GENERATION,
        });
        send_command(&third, "release", 4, POLICY, release).await;
        assert_ack(&mut third, "release", 4, Ack::Admitted).await;
        assert_allocation_state(
            result(&mut third, "release", 4).await,
            AllocationState::Retained,
        );
        assert!(
            store
                .authenticate_session(&runtime.authorization.id, runtime.capability.as_ref())
                .is_err()
        );

        assert_eq!(backend.shutdown_calls().unwrap(), 1);
        assert_eq!(
            store.sandbox(&sandbox).unwrap().state,
            SandboxState::Stopped
        );
        assert!(store.volume(&scope, "data").is_ok());
        let dispose = Command::DisposeAllocation(DisposeAllocation {
            allocation_id: ALLOCATION.into(),
            expected_revision: POLICY,
            expected_generation: GENERATION,
            request_id: "delete-workload".into(),
        });
        send_command(&third, "dispose", 5, POLICY, dispose).await;
        assert_ack(&mut third, "dispose", 5, Ack::Admitted).await;
        assert_allocation_state(
            result(&mut third, "dispose", 5).await,
            AllocationState::Discarded,
        );
        assert_eq!(
            store.scope(&runtime.authorization.scope_id).unwrap().state,
            ScopeState::Released
        );

        assert_eq!(
            store.sandbox(&sandbox).unwrap().state,
            SandboxState::Deleted
        );
        assert!(store.volume(&scope, "data").is_err());
        // Gateway closes a stream after a durable inventory-changing result.
        drop(third);
        assert!(matches!(attempt.await.unwrap(), Err(Error::Disconnected)));
        let offline = store.operator_remote_allocation(&scope).unwrap();
        assert_eq!(offline.allocation_id, ALLOCATION);
        assert_eq!(offline.display_label, "Name of workload");
        assert_eq!(offline.scope.state, ScopeState::Released);
        let attempt = start_attempt(&connector);
        let mut fourth = gateway.connections.recv().await.unwrap();
        assert!(fourth.hello.inventory.is_empty());
        loop {
            if let Payload::ObservationRenewal(renewal) = next_frame(&mut fourth).await {
                assert_eq!(renewal.connection_epoch, fourth.hello.connection_epoch);
                let observation = renewal.observation.unwrap();
                assert_eq!(observation.sequence, 2);
                assert_eq!(
                    observation.inventory_watermark,
                    fourth.hello.inventory_watermark
                );
                break;
            }
        }
        drop(fourth);
        assert!(matches!(attempt.await.unwrap(), Err(Error::Disconnected)));
    })
    .await
    .expect("mock Salix and gateway contract deadline");
}

#[tokio::test]
async fn protocol_details_and_authentication_stop_reconnect() {
    timeout(Duration::from_secs(5), async {
        let candidate = CandidateCredential::generate().unwrap();
        let gateway = GatewayMock::start(&candidate).await;
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(directory.path().join("host.db")).unwrap());
        store.create_registration(REGISTRATION).unwrap();
        let credentials = Arc::new(FileCredentialStore::new(
            directory.path().join("credentials"),
        ));
        credentials
            .replace(REGISTRATION, candidate.as_bytes())
            .unwrap();
        let client = Gateway::new(REGISTRATION, &gateway.settings, credentials).unwrap();
        let connector = Connector::new(
            Core::new(store.clone(), Arc::new(FakeDom0Backend::default())),
            client,
            ConnectorConfig {
                connection: gateway.settings.clone(),
                scope_defaults: defaults(),
                runtime: mpsc::channel(1).0,
                observe: None,
            },
        );
        gateway.mode.store(2, Ordering::Relaxed);
        let Err(Error::ProtocolUnsupported(details)) = connector.run().await else {
            panic!("expected typed version refusal");
        };
        assert_eq!(details.host_protocol, "4");
        assert_eq!(details.min_protocol, 5);
        assert_eq!(details.max_protocol, 6);
        assert_eq!(details.host_side, "upgrade");
        gateway.mode.store(1, Ordering::Relaxed);
        let Err(Error::Authentication(status)) = connector.run().await else {
            panic!("expected terminal authentication failure");
        };
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        let registered = store.registration(REGISTRATION).unwrap();
        assert_eq!(registered.connection_epoch, 2);
        assert!(!registered.connected);
    })
    .await
    .expect("gateway terminal statuses");
}

fn start_attempt(
    connector: &Arc<Connector>,
) -> tokio::task::JoinHandle<vmm_remote_salix::Result<()>> {
    let connector = connector.clone();
    tokio::spawn(async move { connector.connect_once().await })
}

async fn define_workload(core: &Core, scope: &str) -> SandboxKey {
    let digest = [7; 32];
    core.import_image(
        scope,
        "test-image",
        &ImageSource {
            manifest_digest: digest,
            staged_extent: "test-extent".into(),
            physical_bytes: 16,
        },
    )
    .await
    .unwrap();
    core.create_volume(&Volume {
        scope_id: scope.into(),
        volume_id: "data".into(),
        quota_bytes: 128,
        used_bytes: 0,
        owner_uid: 1000,
        owner_gid: 1000,
        mode: 0o700,
        key_id: None,
    })
    .await
    .unwrap();
    let key = SandboxKey {
        scope_id: scope.into(),
        sandbox_id: "sandbox".into(),
        generation_id: "test-generation".into(),
    };
    core.define_sandbox(&SandboxDefinition {
        key: key.clone(),
        image_reference: "test-image".into(),
        manifest_digest: digest,
        read_only_root: true,
        root_overlay: None,
        pids_max: None,
        mounts: vec![],
        host_shares: vec![],
        entrypoint: None,
        command: None,
        workdir: None,
        user: None,
        log_limit_bytes: 4096,
        env: Default::default(),
    })
    .await
    .unwrap();
    key
}

fn defaults() -> Limits {
    Limits {
        pids_max: 16,
        disk_quota_bytes: 128,
        vcpus: 1,
        boot_ram_mb: 128,
        max_ram_mb: 128,
    }
}

fn ensure(id: &str, disk: u64) -> Command {
    Command::EnsureAllocation(EnsureAllocation {
        allocation_id: id.into(),
        generation: GENERATION,
        policy_revision: POLICY,
        display_label: "Name of workload".into(),
        egress_mode: 1,
        limits: Some(ResourceLimits {
            pids: Some(20),
            disk_bytes: Some(disk),
        }),
    })
}

async fn send_command(
    control: &Control,
    id: &str,
    sequence: u64,
    target_revision: u64,
    command: Command,
) {
    control
        .commands
        .send(Ok(ConnectResponse {
            payload: Some(connect_response::Payload::Command(ControllerCommand {
                command_id: id.into(),
                sequence,
                connection_epoch: control.hello.connection_epoch,
                timeout_millis: 5000,
                target_revision,
                command: Some(command),
            })),
        }))
        .await
        .unwrap();
}

async fn next_frame(control: &mut Control) -> Payload {
    control
        .frames
        .message()
        .await
        .unwrap()
        .unwrap()
        .payload
        .unwrap()
}

async fn assert_ack(control: &mut Control, id: &str, sequence: u64, disposition: Ack) {
    loop {
        match next_frame(control).await {
            Payload::ObservationRenewal(_) => {}
            Payload::Ack(ack) => {
                assert_eq!(ack.command_id, id);
                assert_eq!(ack.sequence, sequence);
                assert_eq!(ack.connection_epoch, control.hello.connection_epoch);
                assert_eq!(ack.disposition(), disposition);
                return;
            }
            payload => panic!("expected ack, got {payload:?}"),
        }
    }
}

async fn result(
    control: &mut Control,
    id: &str,
    sequence: u64,
) -> vmm_remote_salix::remote::CommandResult {
    loop {
        match next_frame(control).await {
            Payload::ObservationRenewal(_) => {}
            Payload::Result(result) => {
                assert_eq!(result.command_id, id);
                assert_eq!(result.sequence, sequence);
                assert_eq!(result.connection_epoch, control.hello.connection_epoch);
                return result;
            }
            payload => panic!("expected result, got {payload:?}"),
        }
    }
}

fn assert_allocation_state(
    result: vmm_remote_salix::remote::CommandResult,
    expected: AllocationState,
) {
    assert_eq!(result.outcome(), Outcome::Succeeded);
    let Some(command_result::Result::Allocation(allocation)) = result.result else {
        panic!("expected allocation");
    };
    assert_eq!(allocation.allocation_id, ALLOCATION);
    assert_eq!(allocation.state(), expected);
}

fn session_ready(
    result: vmm_remote_salix::remote::CommandResult,
) -> vmm_remote_salix::remote::SessionReady {
    assert_eq!(result.outcome(), Outcome::Succeeded);
    let Some(command_result::Result::SessionReady(ready)) = result.result else {
        panic!("expected session ready");
    };
    ready
}

#[tokio::test]
async fn claim_errors_and_recovery_proof_match_salix_wire() {
    let key = FileIdentityKey::generate();
    let candidate = CandidateCredential::generate().unwrap();
    let salix = Salix::start(
        configuration("localhost:443".into(), "test-ca"),
        &candidate,
        key.public_key().unwrap(),
    )
    .await;
    for (ticket, status, code) in [
        ("invalid", 401, "invalid_ticket"),
        ("expired", 410, "ticket_expired"),
    ] {
        let Err(Error::Registration(error)) = salix.claim(&candidate, &key, ticket).await else {
            panic!("Salix claim did not surface {code}");
        };
        assert_eq!(error.status, status);
        assert_eq!(error.code.as_deref(), Some(code));
    }
    let config = RegistrationClient::new()
        .unwrap()
        .recover_registration(
            &salix.recovery_url(),
            REGISTRATION,
            "fixture-recovery",
            &identity(&key),
            &candidate,
        )
        .await
        .unwrap();
    assert_eq!(config.registration_id, REGISTRATION);
    assert_eq!(config.remote_connection.policy_revision, 7);
}
