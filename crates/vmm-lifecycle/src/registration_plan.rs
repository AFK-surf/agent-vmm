//! Registration descriptors from standard input, and the durable claim plan.
//! Nothing reaches the server before the candidate credential and its exact
//! target are on disk.

use crate::error::{Error, Kind, Result, err};
use crate::journal::{Journal, PLAN_LIMIT};
use crate::{Context, Paths, timefmt, valid_id};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

pub const CLAIM_DESCRIPTOR_LIMIT: usize = 4096;
pub const RECOVERY_DESCRIPTOR_LIMIT: usize = 8192;
const CLAIM_PATH: &str = "/v1/compute/agent-vmm/registrations/claim";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimDescriptor {
    pub version: u32,
    pub registration_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(default)]
    pub claim_url: String,
    #[serde(default)]
    pub ticket: String,
    #[serde(default)]
    pub expires_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryDescriptor {
    pub version: u32,
    pub registration_id: String,
    pub recovery_url: String,
    pub authorization: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourcePolicy {
    pub pids: u32,
    pub disk_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolMaterial {
    pub per_environment_limits: ResourcePolicy,
    pub max_egress_mode: String,
    pub stop_grace_seconds: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionMaterial {
    pub gateway_endpoint: String,
    pub trust_bundle: String,
    pub policy_revision: u64,
    pub pool_policy: PoolMaterial,
}

/// The connection configuration that the registration service returns. It holds no credential.
/// A saved plan can hold keys of another release, so decoding ignores unknown keys.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Configuration {
    pub version: u32,
    pub registration_id: String,
    pub remote_connection: ConnectionMaterial,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<serde_json::Value>,
}

mod standard_base64 {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error> {
        let text = Option::<String>::deserialize(deserializer)?.unwrap_or_default();
        STANDARD.decode(text).map_err(serde::de::Error::custom)
    }
}

/// Version 2 of `install-operations/<request>/plan.json`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClaimPlan {
    pub version: u32,
    pub request_id: String,
    pub descriptor: ClaimDescriptor,
    pub device_id: String,
    #[serde(with = "standard_base64")]
    pub root_public_key: Vec<u8>,
    pub root_key_revision: u64,
    /// The 43 characters of an unpadded base64url secret, as bytes.
    #[serde(with = "standard_base64")]
    pub candidate_credential: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material: Option<Configuration>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub recovery_url: String,
}

fn loopback(host: &str) -> bool {
    matches!(host.to_ascii_lowercase().as_str(), "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

/// An absolute URL without credentials, query or fragment: HTTPS, or HTTP to this machine.
fn service_url(raw: &str) -> Option<url::Url> {
    let parsed = url::Url::parse(raw).ok()?;
    let host = parsed.host_str().filter(|host| !host.is_empty())?;
    let plain = parsed.username().is_empty() && parsed.password().is_none() && parsed.query().is_none() && parsed.fragment().is_none();
    let scheme = parsed.scheme() == "https" || parsed.scheme() == "http" && loopback(host);
    (plain && scheme).then_some(parsed)
}

impl ClaimDescriptor {
    pub fn decode(input: &[u8]) -> Result<Self> {
        if input.len() > CLAIM_DESCRIPTOR_LIMIT {
            return Err(err("registration descriptor is invalid"));
        }
        let descriptor: Self = serde_json::from_slice(input).map_err(|error| err(format!("decode registration descriptor: {error}")))?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn validate(&self) -> Result<()> {
        let url = service_url(&self.claim_url).filter(|url| url.path() == CLAIM_PATH);
        if self.version != 1 || !valid_id(&self.registration_id) || self.ticket.is_empty() || self.ticket.len() > 128 || url.is_none() {
            return Err(err("registration descriptor is invalid"));
        }
        // The server owns ticket expiry. A saved candidate can observe a committed claim after expiry.
        if timefmt::parse(&self.expires_at).is_none() {
            return Err(err("registration descriptor expiry is invalid"));
        }
        Ok(())
    }
}

/// `/v1/comma/workspaces/<w>/compute-nodes/agent-vmm/registrations/<id>/recovery`, or the
/// `/v1/orgs/<o>/projects/<p>/compute-nodes/...` form, for this registration.
fn recovery_path(path: &str, registration_id: &str) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    let name = |segment: &&str| !segment.is_empty() && segment.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    let tail = match segments.as_slice() {
        ["", "v1", "comma", "workspaces", workspace, tail @ ..] if name(workspace) => tail,
        ["", "v1", "orgs", organization, "projects", project, tail @ ..] if name(organization) && name(project) => tail,
        _ => return false,
    };
    matches!(tail, ["compute-nodes", "agent-vmm", "registrations", id, "recovery"] if name(id) && *id == registration_id)
}

impl RecoveryDescriptor {
    pub fn decode(input: &[u8]) -> Result<Self> {
        let invalid = || err("credential replacement descriptor is invalid");
        if input.len() > RECOVERY_DESCRIPTOR_LIMIT {
            return Err(invalid());
        }
        let descriptor: Self = serde_json::from_slice(input).map_err(|_| invalid())?;
        let url = service_url(&descriptor.recovery_url).filter(|url| recovery_path(url.path(), &descriptor.registration_id));
        let authorization = &descriptor.authorization;
        if descriptor.version != 1 || !valid_id(&descriptor.registration_id) || url.is_none() || authorization.is_empty() || authorization.len() > 4096 || authorization.contains(['\r', '\n']) {
            return Err(invalid());
        }
        Ok(descriptor)
    }
}

impl Configuration {
    pub fn validate(&self, registration_id: &str) -> Result<()> {
        if self.version != 1 || self.registration_id != registration_id {
            return Err(err("registration claim identity mismatch"));
        }
        let pool = &self.remote_connection.pool_policy;
        let egress = pool.max_egress_mode == "deny_all" || pool.max_egress_mode == "public_internet";
        if self.remote_connection.policy_revision == 0 || self.remote_connection.gateway_endpoint.is_empty() || pool.per_environment_limits.pids == 0 || pool.per_environment_limits.disk_bytes == 0 || !egress || pool.stop_grace_seconds == 0 {
            return Err(err("registration connection policy is incomplete"));
        }
        Ok(())
    }
}

pub fn new_candidate_credential() -> Result<Vec<u8>> {
    let mut raw = [0u8; 32];
    getrandom::fill(&mut raw).map_err(|error| err(error.to_string()))?;
    Ok(URL_SAFE_NO_PAD.encode(raw).into_bytes())
}

fn valid_candidate(candidate: &[u8]) -> bool {
    candidate.len() == 43 && URL_SAFE_NO_PAD.decode(candidate).is_ok_and(|raw| raw.len() == 32 && URL_SAFE_NO_PAD.encode(raw).as_bytes() == candidate)
}

pub fn unavailable() -> Error {
    Error::new(Kind::PlanUnavailable, "registration claim plan is unavailable; reauthorize the same registration with its original Host identity")
}

impl ClaimPlan {
    /// Reads the plan of `request_id`. A missing file is `Kind::NotFound`.
    pub fn load(context: &Context, paths: &Paths, request_id: &str) -> Result<Self> {
        let data = Journal::open(context, paths)?.plan_dir(request_id, false)?.read("plan.json", PLAN_LIMIT)?;
        let plan: Self = serde_json::from_slice(&data).map_err(|_| unavailable())?;
        let target = !plan.recovery_url.is_empty() || plan.descriptor.validate().is_ok();
        if plan.version != 2 || plan.request_id != request_id || !target || !valid_candidate(&plan.candidate_credential) || plan.device_id.is_empty() || plan.root_public_key.len() != 33 || plan.root_key_revision == 0 {
            return Err(unavailable());
        }
        if let Some(material) = &plan.material {
            material.validate(&plan.descriptor.registration_id)?;
        }
        Ok(plan)
    }

    pub fn persist(&self, context: &Context, paths: &Paths) -> Result<()> {
        let journal = Journal::open_or_create(context, paths)?;
        let directory = journal.plan_dir(&self.request_id, true).map_err(|error| error.context("create protected registration plan"))?;
        directory.write("plan.json", &serde_json::to_vec(self)?, 0o600, journal.owner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn golden() -> Value {
        serde_json::from_str(include_str!("../../vmm-comma-contract-tests/golden_vectors/salix_http/registration.json")).unwrap()
    }

    fn cases(vectors: &Value, section: &str) -> Vec<(String, bool, Vec<u8>)> {
        vectors[section]["cases"].as_array().unwrap().iter().map(|case| {
            let raw = case.get("raw").and_then(Value::as_str).map(|raw| raw.as_bytes().to_vec());
            (case["name"].as_str().unwrap().to_string(), case["accept"].as_bool().unwrap(), raw.unwrap_or_else(|| serde_json::to_vec(&case["value"]).unwrap()))
        }).collect()
    }

    #[test]
    fn descriptors_match_the_comma_vectors() {
        let vectors = golden();
        for (name, accept, input) in cases(&vectors, "claim_descriptor") {
            assert_eq!(ClaimDescriptor::decode(&input).is_ok(), accept, "claim descriptor: {name}");
        }
        for (name, accept, input) in cases(&vectors, "recovery_descriptor") {
            assert_eq!(RecoveryDescriptor::decode(&input).is_ok(), accept, "recovery descriptor: {name}");
        }
    }

    #[test]
    fn connection_configurations_match_the_comma_vectors() {
        for (name, accept, input) in cases(&golden(), "claim_response") {
            let accepted = serde_json::from_slice::<Configuration>(&input).is_ok_and(|material| material.validate(&material.registration_id).is_ok());
            assert_eq!(accepted, accept, "claim response: {name}");
        }
    }

    #[test]
    fn a_candidate_is_canonical_base64url() {
        let candidate = new_candidate_credential().unwrap();
        assert!(valid_candidate(&candidate));
        assert_eq!(STANDARD.encode(&candidate).len(), 60, "Salix requires exactly 60 characters");
        assert!(!valid_candidate(&candidate[..42]));
        assert!(!valid_candidate(&[b'='; 43]));
    }

    #[test]
    fn the_plan_file_keeps_the_go_field_encoding() {
        let plan = ClaimPlan { version: 2, request_id: "r1".into(), device_id: "d".into(), root_public_key: vec![2; 33], root_key_revision: 1, candidate_credential: new_candidate_credential().unwrap(), ..ClaimPlan::default() };
        let encoded = serde_json::to_value(&plan).unwrap();
        assert_eq!(encoded["root_public_key"], Value::String(STANDARD.encode([2u8; 33])));
        assert_eq!(serde_json::from_value::<ClaimPlan>(encoded).unwrap(), plan);
    }
}
