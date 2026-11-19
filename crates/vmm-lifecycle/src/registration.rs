//! Registration claim and credential replacement: the Host identity, one
//! durable candidate credential, the registration service, and the Host
//! operator that stores the result.

use crate::error::{Deadline, Error, Kind, Result, RpcCode, err};
use crate::journal::Journal;
use crate::registration_plan::{ClaimDescriptor, ClaimPlan, Configuration, RecoveryDescriptor, new_candidate_credential, unavailable};
use crate::{Context, INIT_TIMEOUT, Paths, legacy, operator, platform, status};
use agent_vmm_proto::host::v1 as hostv1;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Duration;

const RESPONSE_LIMIT: u64 = 8 << 20;
const OPERATION_TIMEOUT: Duration = Duration::from_secs(900);
const STEP_TIMEOUT: Duration = Duration::from_secs(120);

/// The error codes of the registration service that a receipt names.
const SERVICE_CODES: &[&str] = &[
    "invalid_ticket", "not_found", "ticket_expired", "registration_revoked", "authorization_expired", "authorization_changed",
    "host_identity_mismatch", "credential_reauthorization_required", "scope_host_bound", "unavailable", "connection_config_unavailable",
    "invalid_request", "invalid_candidate_credential", "invalid_host_identity", "invalid_recovery_challenge", "invalid_recovery_proof",
    "recovery_target_unavailable", "original_subject_unknown",
];

pub enum Authorization {
    Claim(ClaimDescriptor),
    Recovery(RecoveryDescriptor),
    /// The plan that an earlier run of the same request saved.
    Saved(Box<ClaimPlan>),
}

impl Authorization {
    /// The plan that an earlier run of `request_id` saved.
    pub fn saved(context: &Context, paths: &Paths, request_id: &str) -> Result<Self> {
        ClaimPlan::load(context, paths, request_id).map(|plan| Self::Saved(Box::new(plan))).map_err(|error| if error.not_found() { unavailable() } else { error })
    }

    pub fn registration_id(&self) -> &str {
        match self {
            Self::Claim(descriptor) => &descriptor.registration_id,
            Self::Recovery(descriptor) => &descriptor.registration_id,
            Self::Saved(plan) => &plan.descriptor.registration_id,
        }
    }
}

pub struct Registrar<'a> {
    context: &'a Context,
    paths: &'a Paths,
    authorization: Authorization,
}

fn http_error(response: &crate::HttpResponse, operation: &str) -> Error {
    let body: Value = serde_json::from_slice(&response.body[..response.body.len().min(4096)]).unwrap_or_default();
    let code = body["error"].as_str().or_else(|| body["error"]["code"].as_str()).filter(|code| SERVICE_CODES.contains(code)).unwrap_or_default();
    let mut message = format!("registration {operation} returned HTTP {}", response.status);
    if !code.is_empty() {
        message.push_str(&format!(": {code}"));
    }
    Error::new(Kind::Http { status: response.status, code: code.to_string() }, message)
}

impl<'a> Registrar<'a> {
    pub fn new(context: &'a Context, paths: &'a Paths, authorization: Authorization) -> Self {
        Self { context, paths, authorization }
    }

    /// Saves a new candidate for the current Host identity. `unready` names an identity that cannot be read.
    fn new_plan(&self, request_id: &str, descriptor: ClaimDescriptor, recovery_url: &str, unready: &str, deadline: Deadline) -> Result<ClaimPlan> {
        let response = self.context.operator(self.paths)?.device_identity(deadline).map_err(|_| err(unready))?;
        let identity = response.identity.clone().filter(|_| response.state() == hostv1::DeviceIdentityState::Ready).ok_or_else(|| err(unready))?;
        let plan = ClaimPlan {
            version: 2,
            request_id: request_id.to_string(),
            descriptor,
            device_id: identity.device_id,
            root_public_key: identity.root_public_key,
            root_key_revision: identity.root_key_revision,
            candidate_credential: new_candidate_credential()?,
            material: None,
            recovery_url: recovery_url.to_string(),
        };
        plan.persist(self.context, self.paths)?;
        Ok(plan)
    }

    fn claim_plan(&self, request_id: &str, descriptor: &ClaimDescriptor, deadline: Deadline) -> Result<ClaimPlan> {
        let mut plan = match ClaimPlan::load(self.context, self.paths, request_id) {
            Ok(mut saved) => {
                if saved.descriptor.registration_id != descriptor.registration_id || saved.descriptor.claim_url != descriptor.claim_url {
                    return Err(err("registration claim request target changed"));
                }
                if saved.descriptor.ticket != descriptor.ticket {
                    saved.descriptor = descriptor.clone();
                    saved.persist(self.context, self.paths)?;
                }
                saved
            }
            Err(error) if error.not_found() => self.new_plan(request_id, descriptor.clone(), "", "Host identity is not ready", deadline)?,
            Err(error) => return Err(error),
        };
        if plan.material.is_none() {
            plan.material = Some(self.claim(&plan)?);
            plan.persist(self.context, self.paths)?;
        }
        Ok(plan)
    }

    fn claim(&self, plan: &ClaimPlan) -> Result<Configuration> {
        let body = json!({
            "version": 1,
            "registration_id": plan.descriptor.registration_id,
            "device_id": plan.device_id,
            "root_public_key": STANDARD.encode(&plan.root_public_key),
            "root_key_revision": plan.root_key_revision,
            "candidate_credential": STANDARD.encode(&plan.candidate_credential),
        });
        let authorization = format!("RegistrationTicket {}", plan.descriptor.ticket);
        let response = self.context.system.post(&plan.descriptor.claim_url, &authorization, body.to_string().as_bytes(), RESPONSE_LIMIT).map_err(|_| err("registration claim request failed"))?;
        if response.status != 200 {
            return Err(http_error(&response, "claim"));
        }
        let decode = || -> Option<Configuration> {
            let raw: Value = serde_json::from_slice(&response.body).ok()?;
            let material: Configuration = serde_json::from_value(raw.clone()).ok()?;
            // The claim response carries no key that this release does not know.
            (serde_json::to_value(&material).ok()? == raw).then_some(material)
        };
        let material = decode().ok_or_else(|| err("decode registration configuration failed"))?;
        material.validate(&plan.descriptor.registration_id)?;
        Ok(material)
    }

    fn recovery_post(&self, descriptor: &RecoveryDescriptor, url: &str, body: &Value) -> Result<Value> {
        let authorization = format!("Bearer {}", descriptor.authorization);
        let response = self.context.system.post(url, &authorization, body.to_string().as_bytes(), RESPONSE_LIMIT).map_err(|_| err("credential replacement request failed"))?;
        if response.status != 200 {
            return Err(http_error(&response, "credential-replace"));
        }
        serde_json::from_slice(&response.body).map_err(|_| err("credential replacement response is invalid"))
    }

    fn recovery_plan(&self, request_id: &str, descriptor: &RecoveryDescriptor, deadline: Deadline) -> Result<ClaimPlan> {
        let saved = match ClaimPlan::load(self.context, self.paths, request_id) {
            Ok(plan) => Some(plan),
            Err(error) if error.not_found() || error.kind == Kind::PlanUnavailable => None,
            Err(error) => return Err(error),
        };
        if saved.as_ref().is_some_and(|plan| plan.descriptor.registration_id != descriptor.registration_id || !plan.recovery_url.is_empty() && plan.recovery_url != descriptor.recovery_url) {
            return Err(err("credential replacement request target changed"));
        }
        let mut plan = match saved {
            Some(plan) => plan,
            None => {
                let target = ClaimDescriptor { version: 1, registration_id: descriptor.registration_id.clone(), ..ClaimDescriptor::default() };
                self.new_plan(request_id, target, &descriptor.recovery_url, "original Host identity is unavailable", deadline)?
            }
        };
        if plan.material.is_some() {
            return Ok(plan);
        }
        let hash = STANDARD.encode(Sha256::digest(&plan.candidate_credential));
        let response = self.recovery_post(descriptor, &format!("{}/challenge", descriptor.recovery_url), &json!({"candidate_credential_hash": hash}))?;
        let challenge = &response["challenge"];
        let bound = |value: &Value| value["registration_id"] == descriptor.registration_id.as_str();
        if !bound(&response) || !bound(challenge) || challenge["candidate_credential_hash"] != hash.as_str() {
            return Err(err("credential replacement challenge target mismatch"));
        }
        // The Host validates the purpose, the expiry and every bound field before it signs.
        let proof = self.context.operator(self.paths)?.sign_recovery(challenge.to_string().as_bytes(), deadline)?;
        let identity = proof.identity.unwrap_or_default();
        let body = json!({
            "candidate_credential": STANDARD.encode(&plan.candidate_credential),
            "proof": {
                "device_id": identity.device_id,
                "root_public_key": STANDARD.encode(&identity.root_public_key),
                "root_key_revision": identity.root_key_revision,
                "nonce": challenge["nonce"],
                "signature": STANDARD.encode(&proof.signature),
            },
        });
        let material: Configuration = serde_json::from_value(self.recovery_post(descriptor, &descriptor.recovery_url, &body)?).map_err(|_| err("credential replacement response is invalid"))?;
        if material.registration_id != descriptor.registration_id || material.version != 1 {
            return Err(err("credential replacement result target mismatch"));
        }
        plan.material = Some(material);
        plan.persist(self.context, self.paths)?;
        Ok(plan)
    }

    /// Obtains the connection configuration and gives it to the Host with the candidate credential.
    pub fn register(&mut self, request_id: &str, deadline: Deadline) -> Result<()> {
        let plan = match &self.authorization {
            Authorization::Claim(descriptor) => self.claim_plan(request_id, descriptor, deadline)?,
            Authorization::Recovery(descriptor) => self.recovery_plan(request_id, descriptor, deadline)?,
            Authorization::Saved(plan) if plan.material.is_none() && plan.recovery_url.is_empty() => self.claim_plan(request_id, &plan.descriptor.clone(), deadline)?,
            Authorization::Saved(plan) => (**plan).clone(),
        };
        let Some(material) = &plan.material else { return Err(unavailable()) };
        let connection = &material.remote_connection;
        let egress = match connection.pool_policy.max_egress_mode.as_str() {
            "deny_all" => hostv1::EgressMode::DenyAll,
            "public_internet" => hostv1::EgressMode::PublicInternet,
            _ => return Err(err("remote pool egress mode is invalid")),
        };
        let limits = &connection.pool_policy.per_environment_limits;
        let pool = hostv1::RemotePoolPolicy {
            per_environment_limits: Some(hostv1::ResourceLimits { pids: Some(limits.pids), disk_bytes: Some(limits.disk_bytes) }),
            max_egress_mode: egress as i32,
            stop_grace: Some(prost_types::Duration { seconds: connection.pool_policy.stop_grace_seconds as i64, nanos: 0 }),
        };
        let registration = hostv1::RemoteRegistrationConnection {
            request_id: format!("{request_id}:registration"),
            endpoint: connection.gateway_endpoint.clone(),
            display_name: "Managed compute".into(),
            pool: Some(pool),
            registration_id: material.registration_id.clone(),
            trust_bundle: STANDARD.decode(&connection.trust_bundle).map_err(|_| err("decode public TLS configuration failed"))?,
            source_policy_revision: connection.policy_revision,
        };
        let request = hostv1::ApplyRemoteRegistrationRequest { registration: Some(registration), credential: plan.candidate_credential.clone() };
        let applied = self.context.operator(self.paths)?.apply_registration(request, deadline).map_err(|error| error.context("apply registration"))?;
        if applied.id != material.registration_id {
            return Err(err("registration identity mismatch"));
        }
        self.authorization = Authorization::Saved(Box::new(plan));
        Ok(())
    }

    /// A claim prepares the Host and the VM first. A recovery only replaces the credential.
    pub fn run(&mut self, kind: &str, request_id: &str, disable_pairing: bool) -> Result<()> {
        let (context, paths) = (self.context, self.paths);
        let overall = Deadline::after(OPERATION_TIMEOUT);
        let step = |name: &str, timeout: Duration, call: &mut dyn FnMut(Deadline) -> Result<()>| {
            let deadline = overall.within(timeout);
            call(deadline).and_then(|()| deadline.check()).map_err(|error| error.context(format!("lifecycle {name}")))
        };
        if kind != "registration-recover" {
            step("install", STEP_TIMEOUT, &mut |deadline| {
                let observed = status::read(context, paths, None, deadline);
                if observed.host_installed && observed.host_loaded && observed.host_readable && observed.host_healthy {
                    return Ok(());
                }
                platform::reconcile_service(context, paths, disable_pairing, deadline)
            })?;
            step("init", INIT_TIMEOUT, &mut |deadline| operator::ensure_vm_started(context, paths, request_id, deadline))?;
        }
        step("register", STEP_TIMEOUT, &mut |deadline| self.register(request_id, deadline))
    }
}

/// Returns the request that owns `registration_id` when `requested` continues it.
/// A recovery can adopt an unfinished historical request; nothing else is aliased.
pub fn original_request_id(context: &Context, paths: &Paths, registration_id: &str, requested: &str, kind: &str) -> Result<String> {
    let recover = kind == "registration-recover";
    let journal = match Journal::open(context, paths) {
        Ok(journal) => journal,
        Err(error) if error.not_found() => return Ok(requested.to_string()),
        Err(error) => return Err(error),
    };
    let state = journal.state()?;
    let open_legacy = |receipt: &crate::journal::LifecycleOperation, id: &str| {
        receipt.kind == "install-managed" && receipt.outcome != "succeeded" && receipt.outcome != "superseded" && state.superseded_by(id).is_empty() && !state.blocked(id)
    };
    let correlated = |receipt: &crate::journal::LifecycleOperation| !requested.is_empty() && receipt.correlation_request_id == requested;
    let original = state.registration_request(registration_id);
    if !original.is_empty() {
        let prior = journal.receipt(original)?;
        let foreign = !prior.correlation_request_id.is_empty() && prior.correlation_request_id != requested;
        let adopt = !foreign && recover && (correlated(&prior) || open_legacy(&prior, original));
        return Ok(if adopt { original } else { requested }.to_string());
    }
    for id in [&state.active_request, &state.latest_request] {
        if id.is_empty() || !recover {
            continue;
        }
        let prior = journal.receipt(id)?;
        if prior.kind == "registration-recover" && correlated(&prior) && prior.target_registration_id == registration_id {
            return Ok(id.clone());
        }
        if open_legacy(&prior, id) {
            match legacy::registration_target(context, paths, id) {
                Ok(target) if target == registration_id => return Ok(id.clone()),
                Err(error) if !error.not_found() => return Err(error),
                _ => {}
            }
        }
    }
    let strict = requested.is_empty() || recover;
    let registration = match context.operator(paths).and_then(|mut operator| operator.registration(registration_id, Deadline::after(Duration::from_secs(4)))) {
        Ok(registration) => registration,
        Err(error) if strict && error.rpc_code() != Some(RpcCode::NotFound) => return Err(error),
        Err(_) => return Ok(requested.to_string()),
    };
    let original = registration.source_request_id;
    if original.is_empty() || original == requested {
        return Ok(requested.to_string());
    }
    match journal.receipt(&original) {
        Ok(receipt) if recover && (correlated(&receipt) || open_legacy(&receipt, &original)) => Ok(original),
        Err(error) if !error.not_found() => Err(error),
        _ => Ok(requested.to_string()),
    }
}

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
