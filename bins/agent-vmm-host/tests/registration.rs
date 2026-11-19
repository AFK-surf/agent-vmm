use agent_vmm_host::{control::serve_local, host::HostState};
use agent_vmm_proto::host::v1::{self as h, operator_service_client::OperatorServiceClient};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};
use vmm_rpc::Code;

#[tokio::test]
async fn operator_enrolls_updates_and_revokes_across_host_restart() {
    let dir = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let socket = dir.path().join("operator.sock");
    let database = dir.path().join("host.sqlite");
    let certificate = rcgen::generate_simple_self_signed(vec!["gateway.example".into()]).unwrap();
    let public = h::RemoteRegistrationConnection {
        request_id: "source:registration:managed-enrollment".into(),
        endpoint: "gateway.example:443".into(),
        display_name: "work".into(),
        registration_id: "one".into(),
        source_policy_revision: 3,
        trust_bundle: certificate.cert.pem().into_bytes(),
        pool: Some(h::RemotePoolPolicy {
            per_environment_limits: Some(h::ResourceLimits {
                pids: Some(80),
                disk_bytes: Some(1 << 20),
            }),
            max_egress_mode: h::EgressMode::DenyAll.into(),
            stop_grace: Some(Duration::from_secs(15).try_into().unwrap()),
        }),
    };
    const TEST_CREDENTIAL: &[u8] = b"registration-test-only-sentinel";
    let apply = |config| h::ApplyRemoteRegistrationRequest {
        registration: Some(config),
        credential: TEST_CREDENTIAL.to_vec(),
    };
    for pass in 0..2 {
        let host = HostState::open(&database).unwrap();
        let api = host.local_operator(dir.path(), None).unwrap();
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = serve_local(&socket, api, None, async {
            let _ = stopped.await;
        });
        let verify = async {
            let rpc_socket = socket.clone();
        let channel = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if tokio::net::UnixStream::connect(&rpc_socket).await.is_ok() {
                    break vmm_rpc::Client::unix(&rpc_socket);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let mut client = OperatorServiceClient::new(channel);
            if pass == 0 {
                let created = client
                    .apply_remote_registration(apply(public.clone()))
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(created.enrollment_state(), h::RemoteEnrollmentState::Ready);
                assert_eq!(created.source_request_id, "source");
                let replay = client
                    .apply_remote_registration(apply(public.clone()))
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(created.revision, replay.revision);
                let mut changed = public.clone();
                changed
                    .pool
                    .as_mut()
                    .unwrap()
                    .per_environment_limits
                    .as_mut()
                    .unwrap()
                    .pids = Some(90);
                assert_eq!(
                    client
                        .apply_remote_registration(apply(changed.clone()))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::Aborted
                );
                changed.source_policy_revision += 1;
                let updated = client
                    .apply_remote_registration(apply(changed))
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(
                    updated.pool_policy_revision,
                    created.pool_policy_revision + 1
                );
                let drain = |rev| h::SetRemoteRegistrationStateRequest {
                    request_id: "drain".into(),
                    registration_id: "one".into(),
                    expected_revision: rev,
                    state: h::RemoteRegistrationState::Draining.into(),
                };
                assert_eq!(
                    client
                        .set_remote_registration_state(drain(created.revision))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::Aborted
                );
                let drained = client
                    .set_remote_registration_state(drain(updated.revision))
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                let begin = client
                    .begin_remote_revocation(h::BeginRemoteRevocationRequest {
                        request_id: "begin".into(),
                        registration_id: "one".into(),
                        expected_revision: drained.revision,
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(begin.enrollment_state(), h::RemoteEnrollmentState::Revoking);
            } else {
                let get = || h::GetRemoteRegistrationRequest {
                    registration_id: "one".into(),
                };
                let found = client
                    .get_remote_registration(get())
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(found.enrollment_state(), h::RemoteEnrollmentState::Revoking);
                let complete = client
                    .complete_remote_revocation(h::CompleteRemoteRevocationRequest {
                        request_id: "complete".into(),
                        registration_id: "one".into(),
                        expected_revision: found.revision,
                    })
                    .await
                    .unwrap()
                    .into_inner()
                    .registration
                    .unwrap();
                assert_eq!(complete.state(), h::RemoteRegistrationState::Revoked);
                assert_eq!(
                    client
                        .apply_remote_registration(apply(public.clone()))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::PermissionDenied
                );
            }
            drop(client);
            shutdown.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(server, verify);
        result.unwrap();
    }
    for name in ["host.sqlite", "compat.sqlite"] {
        let bytes = fs::read(dir.path().join(name)).unwrap();
        assert!(
            !bytes
                .windows(TEST_CREDENTIAL.len())
                .any(|chunk| chunk == TEST_CREDENTIAL)
        );
    }
}
