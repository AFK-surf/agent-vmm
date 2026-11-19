use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{net::TcpListener, task::JoinHandle};
use vmm_remote_salix::{
    CandidateCredential, EgressMode, Identity, PoolPolicy, RegistrationClient, RegistrationConfig,
    RemoteConnection, ResourceLimits,
};
use vmm_signing::{IdentitySigner, RecoveryChallenge};

pub const REGISTRATION: &str = "fixture-registration";
pub const CLAIM: &str = "/v1/compute/agent-vmm/registrations/claim";
pub const RECOVERY: &str = "/v1/comma/workspaces/fixture/compute-nodes/agent-vmm/registrations/fixture-registration/recovery";

#[derive(Clone)]
struct Fixture {
    connection: RemoteConnection,
    credential: Vec<u8>,
    public_key: [u8; 33],
    expires_at: Arc<AtomicI64>,
}

pub struct Salix {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Salix {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Salix {
    pub async fn start(
        connection: RemoteConnection,
        candidate: &CandidateCredential,
        public_key: [u8; 33],
    ) -> Self {
        let app = Router::new()
            .route(CLAIM, post(claim))
            .route(RECOVERY, post(replace))
            .route(&format!("{RECOVERY}/challenge"), post(challenge))
            .with_state(Fixture {
                connection,
                credential: candidate.as_bytes().into(),
                public_key,
                expires_at: Arc::new(AtomicI64::new(0)),
            });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self { url, task }
    }

    pub fn recovery_url(&self) -> String {
        format!("{}{RECOVERY}", self.url)
    }

    pub async fn claim(
        &self,
        candidate: &CandidateCredential,
        signer: &dyn IdentitySigner,
        ticket: &str,
    ) -> vmm_remote_salix::Result<RegistrationConfig> {
        RegistrationClient::new()?
            .claim_registration(
                &format!("{}{CLAIM}", self.url),
                REGISTRATION,
                ticket,
                &identity(signer),
                candidate,
            )
            .await
    }
}

pub fn identity(signer: &dyn IdentitySigner) -> Identity<'_> {
    Identity {
        device_id: "fixture-device",
        key_revision: 3,
        signer,
    }
}

pub fn configuration(gateway_endpoint: String, pem: &str) -> RemoteConnection {
    RemoteConnection {
        gateway_endpoint,
        trust_bundle: STANDARD.encode(pem),
        policy_revision: 7,
        pool_policy: PoolPolicy {
            per_environment_limits: ResourceLimits {
                pids: 128,
                disk_bytes: 2048,
            },
            max_egress_mode: EgressMode::DenyAll,
            stop_grace_seconds: 30,
        },
    }
}

async fn claim(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    match headers["authorization"].to_str().unwrap() {
        "RegistrationTicket fixture-ticket" => {}
        "RegistrationTicket expired" => {
            return (StatusCode::GONE, Json(json!({"error":"ticket_expired"}))).into_response();
        }
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error":"invalid_ticket"})),
            )
                .into_response();
        }
    }
    let keys: BTreeSet<_> = body
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            "version",
            "registration_id",
            "device_id",
            "root_public_key",
            "root_key_revision",
            "candidate_credential"
        ])
    );
    assert_eq!(body["version"], 1);
    assert_eq!(body["registration_id"], REGISTRATION);
    assert_eq!(body["device_id"], "fixture-device");
    assert_eq!(body["root_key_revision"], 3);
    assert_eq!(body["root_public_key"], STANDARD.encode(fixture.public_key));
    assert_eq!(body["candidate_credential"].as_str().unwrap().len(), 60);
    assert_eq!(
        body["candidate_credential"],
        STANDARD.encode(&fixture.credential)
    );
    response(fixture).into_response()
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .try_into()
        .unwrap()
}

fn recovery_challenge(fixture: &Fixture, expires_at: i64) -> Value {
    json!({
        "purpose": vmm_signing::RECOVERY_PURPOSE,
        "audience": "https://fixture.invalid",
        "subject": "original-owner",
        "session_id": "login-session",
        "controller_binding": "[\"group\",\"workspace\"]",
        "environment_id": "fixture-env",
        "registration_id": REGISTRATION,
        "candidate_credential_hash": STANDARD.encode(Sha256::digest(&fixture.credential)),
        "nonce": URL_SAFE_NO_PAD.encode([31u8; 32]),
        "revision": 3,
        "expires_at": expires_at
    })
}

async fn challenge(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    assert_eq!(headers["authorization"], "Bearer fixture-recovery");
    assert_eq!(
        body,
        json!({"candidate_credential_hash": STANDARD.encode(Sha256::digest(&fixture.credential))})
    );
    let expires_at = now() + 90;
    fixture.expires_at.store(expires_at, Ordering::Relaxed);
    Json(json!({"registration_id": REGISTRATION, "challenge": recovery_challenge(&fixture, expires_at)})).into_response()
}

async fn replace(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    assert_eq!(headers["authorization"], "Bearer fixture-recovery");
    assert_eq!(
        body["candidate_credential"],
        STANDARD.encode(&fixture.credential)
    );
    let proof = &body["proof"];
    assert_eq!(proof["device_id"], "fixture-device");
    assert_eq!(proof["root_key_revision"], 3);
    assert_eq!(
        proof["root_public_key"],
        STANDARD.encode(fixture.public_key)
    );
    assert_eq!(proof["nonce"], URL_SAFE_NO_PAD.encode([31u8; 32]));
    let signature = STANDARD
        .decode(proof["signature"].as_str().unwrap())
        .unwrap();
    let now = now();
    let challenge =
        recovery_challenge(&fixture, fixture.expires_at.load(Ordering::Relaxed)).to_string();
    let canonical = RecoveryChallenge::parse(challenge.as_bytes(), now)
        .unwrap()
        .canonical_bytes(now)
        .unwrap();
    vmm_signing::verify(&fixture.public_key, &canonical, &signature).unwrap();
    response(fixture).into_response()
}

fn response(fixture: Fixture) -> Json<Value> {
    Json(json!({"version":1,"registration_id":REGISTRATION,"remote_connection":fixture.connection}))
}
