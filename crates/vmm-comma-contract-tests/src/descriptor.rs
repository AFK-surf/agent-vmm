//! Descriptors and HTTP bodies exchanged between the Host lifecycle owner and Salix.
//!
//! Descriptors come from Cue and must be accepted by agent-vmm with the rules
//! of the pinned Go helper. The claim request comes from agent-vmm and must
//! pass Salix's router. `golden_vectors/salix_http/registration.json` cites
//! the source of each rule.

use base64::Engine;
use serde_json::{Map, Value};

const CLAIM_PATH: &str = "/v1/compute/agent-vmm/registrations/claim";

/// `operationIDPattern`: `\A[A-Za-z0-9_-]{1,128}\z` (registration_claim.go:27).
pub fn operation_id(text: &str) -> bool {
    (1..=128).contains(&text.len()) && text.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn exact_keys(object: &Map<String, Value>, allowed: &[&str]) -> Result<(), String> {
    match object.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(format!("unknown key {key}")),
        None => Ok(()),
    }
}

fn text<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, String> {
    match object.get(key) {
        None => Ok(""),
        Some(Value::String(value)) => Ok(value),
        Some(_) => Err(format!("{key} must be a string")),
    }
}

struct Url<'a> {
    scheme: &'a str,
    hostname: String,
    path: &'a str,
}

/// The subset of URL syntax the Go helper accepts for registration endpoints:
/// a host, no user information, no query and no fragment.
fn parse_url(raw: &str) -> Option<Url<'_>> {
    let (scheme, rest) = raw.split_once("://")?;
    if raw.contains(['?', '#']) || raw.bytes().any(|byte| byte <= b' ') {
        return None;
    }
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, ""),
    };
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let hostname = if let Some(bracketed) = authority.strip_prefix('[') {
        bracketed.split_once(']')?.0.to_ascii_lowercase()
    } else {
        authority.split(':').next()?.to_ascii_lowercase()
    };
    (!hostname.is_empty()).then_some(Url { scheme, hostname, path })
}

fn https_or_loopback(url: &Url<'_>) -> bool {
    url.scheme == "https" || (url.scheme == "http" && matches!(url.hostname.as_str(), "localhost" | "127.0.0.1" | "::1"))
}

/// Go `time.Parse(time.RFC3339, value)` for the forms Cue produces.
pub fn rfc3339(value: &str) -> bool {
    crate::consumer::datetime_with_offset(value)
}

/// The Registration descriptor on stdin of `registration-claim --descriptor-stdin`
/// (registration_claim.go:83-109).
pub fn claim_descriptor(value: &Value) -> Result<(), String> {
    let descriptor = value.as_object().ok_or("descriptor must be an object")?;
    exact_keys(descriptor, &["version", "registration_id", "request_id", "claim_url", "ticket", "expires_at"])?;
    if serde_json::to_vec(value).map_err(|error| error.to_string())?.len() > 4096 {
        return Err("descriptor exceeds 4096 bytes".into());
    }
    text(descriptor, "request_id")?;
    let ticket = text(descriptor, "ticket")?;
    let url = parse_url(text(descriptor, "claim_url")?);
    let valid_url = url.is_some_and(|url| url.path == CLAIM_PATH && https_or_loopback(&url));
    if descriptor.get("version") != Some(&Value::from(1))
        || !operation_id(text(descriptor, "registration_id")?)
        || ticket.is_empty()
        || ticket.len() > 128
        || !valid_url
    {
        return Err("registration descriptor is invalid".into());
    }
    if !rfc3339(text(descriptor, "expires_at")?) {
        return Err("registration descriptor expiry is invalid".into());
    }
    Ok(())
}

/// The descriptor on stdin of `registration-recover --descriptor-stdin`
/// (registration_recovery.go:26-50).
pub fn recovery_descriptor(value: &Value) -> Result<(), String> {
    let invalid = || "credential replacement descriptor is invalid".to_string();
    let descriptor = value.as_object().ok_or_else(invalid)?;
    exact_keys(descriptor, &["version", "registration_id", "recovery_url", "authorization"]).map_err(|_| invalid())?;
    let registration = text(descriptor, "registration_id").map_err(|_| invalid())?;
    let authorization = text(descriptor, "authorization").map_err(|_| invalid())?;
    let url = parse_url(text(descriptor, "recovery_url").map_err(|_| invalid())?).ok_or_else(invalid)?;
    if descriptor.get("version") != Some(&Value::from(1))
        || !operation_id(registration)
        || !https_or_loopback(&url)
        || recovery_path_registration(url.path) != Some(registration)
        || authorization.is_empty()
        || authorization.len() > 4096
        || authorization.contains(['\r', '\n'])
    {
        return Err(invalid());
    }
    Ok(())
}

/// `recoveryPathPattern` (registration_recovery.go:33).
fn recovery_path_registration(path: &str) -> Option<&str> {
    let segment = |value: &str| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    let parts: Vec<&str> = path.strip_prefix("/v1/")?.split('/').collect();
    let tail = match parts.as_slice() {
        ["comma", "workspaces", workspace, tail @ ..] if segment(workspace) => tail,
        ["orgs", org, "projects", project, tail @ ..] if segment(org) && segment(project) => tail,
        _ => return None,
    };
    match tail {
        ["compute-nodes", "agent-vmm", "registrations", registration, "recovery"] if segment(registration) => Some(registration),
        _ => None,
    }
}

/// Salix's admission of `POST /v1/compute/agent-vmm/registrations/claim`
/// (salix_web/router.ex:231-264).
pub fn claim_request(value: &Value) -> Result<(), String> {
    let body = value.as_object().ok_or("body must be an object")?;
    let keys = ["version", "registration_id", "device_id", "root_public_key", "root_key_revision", "candidate_credential"];
    exact_keys(body, &keys)?;
    if let Some(missing) = keys.iter().find(|key| !body.contains_key(**key)) {
        return Err(format!("missing key {missing}"));
    }
    if body["version"] != 1 {
        return Err("version must be the integer 1".into());
    }
    let string = |key: &str| body[key].as_str().ok_or_else(|| format!("{key} must be a string"));
    let registration = string("registration_id")?;
    if !(1..=200).contains(&registration.len()) {
        return Err("registration_id must have 1..200 bytes".into());
    }
    let key = string("root_public_key")?;
    let credential = string("candidate_credential")?;
    if key.len() > 684 || credential.len() != 60 {
        return Err("root_public_key or candidate_credential has the wrong size".into());
    }
    let standard = base64::engine::general_purpose::STANDARD;
    standard.decode(key).map_err(|_| "root_public_key is not standard base64")?;
    standard.decode(credential).map_err(|_| "candidate_credential is not standard base64")?;
    Ok(())
}

/// The Host's acceptance of the claim response
/// (registration_claim.go:39-60,191-211).
pub fn claim_response(value: &Value, registration_id: &str) -> Result<(), String> {
    let material = value.as_object().ok_or("response must be an object")?;
    exact_keys(material, &["version", "registration_id", "remote_connection", "registration"])?;
    if material.get("version") != Some(&Value::from(1)) || material.get("registration_id").and_then(Value::as_str) != Some(registration_id) {
        return Err("registration claim identity mismatch".into());
    }
    let remote = material.get("remote_connection").and_then(Value::as_object).ok_or("remote_connection is required")?;
    exact_keys(remote, &["gateway_endpoint", "trust_bundle", "policy_revision", "pool_policy"])?;
    let pool = remote.get("pool_policy").and_then(Value::as_object).ok_or("pool_policy is required")?;
    exact_keys(pool, &["per_environment_limits", "max_egress_mode", "stop_grace_seconds"])?;
    let limits = pool.get("per_environment_limits").and_then(Value::as_object).ok_or("per_environment_limits is required")?;
    exact_keys(limits, &["pids", "disk_bytes"])?;
    let positive = |object: &Map<String, Value>, key: &str| object.get(key).and_then(Value::as_u64).is_some_and(|number| number > 0);
    let egress = pool.get("max_egress_mode").and_then(Value::as_str);
    if !positive(remote, "policy_revision")
        || remote.get("gateway_endpoint").and_then(Value::as_str).is_none_or(str::is_empty)
        || !positive(limits, "pids")
        || limits.get("pids").and_then(Value::as_u64).is_some_and(|pids| pids > u64::from(u32::MAX))
        || !positive(limits, "disk_bytes")
        || !matches!(egress, Some("deny_all") | Some("public_internet"))
        || !positive(pool, "stop_grace_seconds")
    {
        return Err("registration connection policy is incomplete".into());
    }
    Ok(())
}

/// The bytes a Host signs for credential replacement
/// (salix_store/agent_vmm_recovery.ex:11-31).
pub fn recovery_wire(challenge: &Value, fields: &[&str]) -> Result<String, String> {
    let purpose = "agent-vmm/credential-replace/1";
    if challenge.get("purpose").and_then(Value::as_str) != Some(purpose) {
        return Err("invalid_recovery_challenge".into());
    }
    let encode = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut lines = vec![purpose.to_string()];
    for field in fields {
        let value = challenge.get(*field).and_then(Value::as_str).ok_or("invalid_recovery_challenge")?;
        let limit = if *field == "controller_binding" { 1500 } else { 500 };
        if value.is_empty() || value.len() > limit {
            return Err("invalid_recovery_challenge".into());
        }
        lines.push(encode.encode(value));
    }
    let revision = challenge.get("revision").and_then(Value::as_u64).filter(|revision| *revision > 0).ok_or("invalid_recovery_challenge")?;
    let expires = challenge.get("expires_at").and_then(Value::as_i64).ok_or("invalid_recovery_challenge")?;
    lines.extend([revision.to_string(), expires.to_string(), String::new()]);
    Ok(lines.join("\n"))
}
