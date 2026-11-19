use super::*;
use crate::journal::{self, LifecycleOperation};
use crate::testing::{Fake, installed};
use serde_json::json;
use std::sync::Arc;

const CLAIM_URL: &str = "https://salix.example/v1/compute/agent-vmm/registrations/claim";
const RECOVERY_URL: &str = "https://comma.example/v1/comma/workspaces/ws_1/compute-nodes/agent-vmm/registrations/registration-1/recovery";

struct Fixture {
    _root: tempfile::TempDir,
    paths: Paths,
    fake: Arc<Fake>,
    context: Context,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let paths = installed(&root.path().canonicalize().unwrap());
    let fake = Fake::new();
    Fixture { paths, context: fake.context(), fake, _root: root }
}

fn descriptor(ticket: &str) -> ClaimDescriptor {
    ClaimDescriptor { version: 1, registration_id: "registration-1".into(), request_id: String::new(), claim_url: CLAIM_URL.into(), ticket: ticket.into(), expires_at: "2026-11-19T12:30:00Z".into() }
}

fn configuration() -> serde_json::Value {
    json!({"version": 1, "registration_id": "registration-1", "remote_connection": {
        "gateway_endpoint": "gateway.example:443", "trust_bundle": "LS0t", "policy_revision": 4,
        "pool_policy": {"per_environment_limits": {"pids": 2048, "disk_bytes": 17179869184u64}, "max_egress_mode": "public_internet", "stop_grace_seconds": 10}}})
}

#[test]
fn a_claim_saves_its_candidate_before_it_reaches_the_server() {
    let fixture = fixture();
    fixture.fake.respond(503, json!({"error": "unavailable"}));
    let mut registrar = Registrar::new(&fixture.context, &fixture.paths, Authorization::Claim(descriptor("ticket-1")));
    let error = registrar.run("registration-claim", "request-1", false).unwrap_err();
    assert_eq!(error.kind, Kind::Http { status: 503, code: "unavailable".into() });
    assert_eq!(error.message, "lifecycle register: registration claim returned HTTP 503: unavailable");

    let plan = ClaimPlan::load(&fixture.context, &fixture.paths, "request-1").unwrap();
    assert!(plan.material.is_none());
    let (url, authorization, body) = fixture.fake.posts.lock().unwrap()[0].clone();
    assert_eq!((url.as_str(), authorization.as_str()), (CLAIM_URL, "RegistrationTicket ticket-1"));
    let mut keys: Vec<&str> = body.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, ["candidate_credential", "device_id", "registration_id", "root_key_revision", "root_public_key", "version"], "Salix refuses any other key");
    assert_eq!(body["candidate_credential"], json!(STANDARD.encode(&plan.candidate_credential)));
    assert_eq!(body["candidate_credential"].as_str().unwrap().len(), 60);

    // A fresh ticket continues with the same candidate, and the saved plan resumes without a descriptor.
    fixture.fake.respond(200, configuration());
    let mut retry = Registrar::new(&fixture.context, &fixture.paths, Authorization::Claim(descriptor("ticket-2")));
    retry.run("registration-claim", "request-1", false).unwrap();
    let (_, authorization, body) = fixture.fake.posts.lock().unwrap()[1].clone();
    assert_eq!(authorization, "RegistrationTicket ticket-2");
    assert_eq!(body["candidate_credential"], json!(STANDARD.encode(&plan.candidate_credential)));
    let applied = fixture.fake.host.lock().unwrap().applied[0].clone();
    let registration = applied.registration.unwrap();
    assert_eq!((registration.request_id.as_str(), registration.registration_id.as_str(), registration.endpoint.as_str()), ("request-1:registration", "registration-1", "gateway.example:443"));
    assert_eq!((registration.trust_bundle.as_slice(), registration.source_policy_revision), (&b"---"[..], 4));
    let pool = registration.pool.unwrap();
    assert_eq!((pool.max_egress_mode(), pool.stop_grace.unwrap().seconds, pool.per_environment_limits.unwrap().disk_bytes), (hostv1::EgressMode::PublicInternet, 10, Some(17179869184)));
    assert_eq!(applied.credential, plan.candidate_credential);

    let mut resumed = Registrar::new(&fixture.context, &fixture.paths, Authorization::saved(&fixture.context, &fixture.paths, "request-1").unwrap());
    resumed.run("registration-claim", "request-1", false).unwrap();
    assert_eq!(fixture.fake.posts.lock().unwrap().len(), 2, "a saved configuration is not claimed again");
    assert_eq!(Authorization::saved(&fixture.context, &fixture.paths, "request-2").err().unwrap().kind, Kind::PlanUnavailable);
}

#[test]
fn a_claim_refuses_a_changed_target_and_an_unknown_response_key() {
    let fixture = fixture();
    let mut extra = configuration();
    extra["credential"] = json!("secret");
    fixture.fake.respond(200, extra);
    let mut registrar = Registrar::new(&fixture.context, &fixture.paths, Authorization::Claim(descriptor("ticket-1")));
    assert_eq!(registrar.register("request-1", Deadline::after(Duration::from_secs(5))).unwrap_err().message, "decode registration configuration failed");

    let other = ClaimDescriptor { registration_id: "registration-2".into(), ..descriptor("ticket-1") };
    let mut changed = Registrar::new(&fixture.context, &fixture.paths, Authorization::Claim(other));
    assert_eq!(changed.register("request-1", Deadline::after(Duration::from_secs(5))).unwrap_err().message, "registration claim request target changed");

    fixture.fake.host.lock().unwrap().identity_ready = false;
    let mut unready = Registrar::new(&fixture.context, &fixture.paths, Authorization::Claim(descriptor("ticket-1")));
    assert_eq!(unready.register("request-2", Deadline::after(Duration::from_secs(5))).unwrap_err().message, "Host identity is not ready");
    assert!(ClaimPlan::load(&fixture.context, &fixture.paths, "request-2").unwrap_err().not_found());
}

#[test]
fn a_recovery_proves_the_original_host_identity() {
    let fixture = fixture();
    let recovery = RecoveryDescriptor { version: 1, registration_id: "registration-1".into(), recovery_url: RECOVERY_URL.into(), authorization: "session".into() };
    let mut registrar = Registrar::new(&fixture.context, &fixture.paths, Authorization::Recovery(recovery.clone()));
    // The first run stops at the challenge. Its candidate decides the hash that the challenge must bind.
    fixture.fake.respond(503, json!({}));
    assert!(registrar.run("registration-recover", "request-1", false).is_err());
    let plan = ClaimPlan::load(&fixture.context, &fixture.paths, "request-1").unwrap();
    let hash = STANDARD.encode(Sha256::digest(&plan.candidate_credential));
    assert_eq!(fixture.fake.posts.lock().unwrap()[0].2, json!({"candidate_credential_hash": hash}));

    let challenge = json!({"purpose": "agent-vmm/credential-replace/1", "registration_id": "registration-1", "candidate_credential_hash": hash, "nonce": "nonce-1", "revision": 3, "expires_at": 1795090554});
    fixture.fake.respond(200, json!({"registration_id": "registration-1", "challenge": challenge}));
    fixture.fake.respond(200, configuration());
    registrar.run("registration-recover", "request-1", false).unwrap();
    let posts = fixture.fake.posts.lock().unwrap().clone();
    assert_eq!((posts[1].0.as_str(), posts[1].1.as_str()), (format!("{RECOVERY_URL}/challenge").as_str(), "Bearer session"));
    assert_eq!(posts[2].0, RECOVERY_URL);
    let proof = &posts[2].2["proof"];
    assert_eq!((proof["device_id"].as_str(), proof["nonce"].as_str(), proof["root_key_revision"].as_u64()), (Some("device-a"), Some("nonce-1"), Some(1)));
    assert_eq!(proof["signature"], json!(STANDARD.encode([7u8; 64])));
    assert_eq!(posts[2].2["candidate_credential"], json!(STANDARD.encode(&plan.candidate_credential)));
    assert_eq!(fixture.fake.calls().iter().filter(|call| *call == "vm").count(), 0, "a recovery does not prepare the VM");

    let other = RecoveryDescriptor { recovery_url: RECOVERY_URL.replace("ws_1", "ws_2"), ..recovery };
    let mut changed = Registrar::new(&fixture.context, &fixture.paths, Authorization::Recovery(other));
    assert_eq!(changed.register("request-1", Deadline::after(Duration::from_secs(5))).unwrap_err().message, "credential replacement request target changed");
}

#[test]
fn only_a_recovery_adopts_the_request_that_owns_a_registration() {
    let fixture = fixture();
    let original = |requested: &str, kind: &str| original_request_id(&fixture.context, &fixture.paths, "registration-1", requested, kind).unwrap();
    assert_eq!(original("new-1", "registration-claim"), "new-1", "without a journal the request stands for itself");

    let legacy = LifecycleOperation { operation_id: "op-1".into(), outcome: "failed".into(), target_registration_id: "registration-1".into(), ..LifecycleOperation::unknown("legacy-1", "install-managed") };
    journal::persist(&fixture.context, &fixture.paths, &legacy).unwrap();
    assert_eq!(original("new-1", "registration-recover"), "legacy-1");
    assert_eq!(original("new-1", "registration-claim"), "new-1");

    let correlated = LifecycleOperation { kind: "registration-recover".into(), correlation_request_id: "new-1".into(), ..legacy.clone() };
    journal::persist(&fixture.context, &fixture.paths, &correlated).unwrap();
    assert_eq!(original("new-1", "registration-recover"), "legacy-1", "the committed correlation continues the same request");
    assert_eq!(original("new-2", "registration-recover"), "new-2", "another request does not take over the correlation");
}
