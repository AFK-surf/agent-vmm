use agent_vmm_host::{control::serve_local, host::HostState};
use agent_vmm_proto::host::v1::{self as h, operator_service_client::OperatorServiceClient};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tonic::Code;
use vmm_signing::{RECOVERY_PURPOSE, RecoveryChallenge};

fn challenge(registration: &str, now: i64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"purpose":RECOVERY_PURPOSE,"audience":"salix","subject":"owner","session_id":"live","controller_binding":"controller","environment_id":"env","registration_id":registration,"candidate_credential_hash":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","nonce":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","revision":1,"expires_at":now+90})).unwrap()
}
#[tokio::test]
async fn operator_signs_only_recoverable_registrations_with_original_device_key() {
    let dir = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/tmp")
        .unwrap();
    let path = dir.path().join("host.db");
    let socket = dir.path().join("operator.sock");
    let key = dir.path().join("device-root.key");
    let mut original = None;
    for pass in 0..3 {
        let host = HostState::open(&path).unwrap();
        if pass == 0 {
            host.store.install_appliance("managed", "private").unwrap();
            host.store.create_registration("retired").unwrap();
            host.store
                .begin_operator_registration_disposal("retired", "retire")
                .unwrap();
        }
        let api = host.local_operator(dir.path(), None).unwrap();
        let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
        let serving = serve_local(&socket, api, None, async {
            let _ = stopped.await;
        });
        let verify = async {
            let endpoint =
                tonic::transport::Endpoint::from_shared(format!("unix:{}", socket.display()))
                    .unwrap();
            let channel = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(channel) = endpoint.connect().await {
                        break channel;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let mut client = OperatorServiceClient::new(channel);
            let status = client
                .get_device_identity(h::GetDeviceIdentityRequest {})
                .await
                .unwrap()
                .into_inner();
            let expected = if pass < 2 {
                h::DeviceIdentityState::Ready
            } else {
                h::DeviceIdentityState::NeedsRepair
            };
            assert_eq!(status.state(), expected);
            let identity = status.identity.unwrap();
            assert_eq!(&identity, original.get_or_insert(identity.clone()));
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            let request = |id| h::SignRegistrationRecoveryRequest {
                challenge_json: challenge(id, now),
            };
            if pass < 2 {
                let signed = client
                    .sign_registration_recovery(request("missing"))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(Some(&identity), signed.identity.as_ref());
                let parsed = RecoveryChallenge::parse(&challenge("missing", now), now).unwrap();
                vmm_signing::verify(
                    &identity.root_public_key,
                    &parsed.canonical_bytes(now).unwrap(),
                    &signed.signature,
                )
                .unwrap();
                for id in ["private", "retired"] {
                    assert_eq!(
                        Code::FailedPrecondition,
                        client
                            .sign_registration_recovery(request(id))
                            .await
                            .unwrap_err()
                            .code()
                    );
                }
                assert_eq!(
                    Code::InvalidArgument,
                    client
                        .sign_registration_recovery(h::SignRegistrationRecoveryRequest {
                            challenge_json: b"{}".to_vec()
                        })
                        .await
                        .unwrap_err()
                        .code()
                );
            } else {
                assert_eq!(
                    Code::FailedPrecondition,
                    client
                        .sign_registration_recovery(request("missing"))
                        .await
                        .unwrap_err()
                        .code()
                );
                assert!(!key.exists());
            }
            drop(client);
            shutdown.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(serving, verify);
        result.unwrap();
        if pass == 1 {
            fs::remove_file(&key).unwrap();
        }
    }
}
