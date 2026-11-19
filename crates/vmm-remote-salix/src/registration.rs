use crate::{Error, Result, unix_millis};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use reqwest::{Client, Url, header};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, time::Duration};
use vmm_signing::{IdentitySigner, RecoveryChallenge};
use zeroize::Zeroizing;

const CLAIM_PATH: &str = "/v1/compute/agent-vmm/registrations/claim";
const MAX_BODY: usize = 8 << 20;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    pub version: u32,
    pub registration_id: String,
    pub remote_connection: RemoteConnection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<Box<RawValue>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteConnection {
    pub gateway_endpoint: String,
    /// Padded standard base64 of the gateway CA PEM bundle.
    pub trust_bundle: String,
    pub policy_revision: u64,
    pub pool_policy: PoolPolicy,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolPolicy {
    pub per_environment_limits: ResourceLimits,
    pub max_egress_mode: EgressMode,
    pub stop_grace_seconds: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceLimits {
    pub pids: u32,
    pub disk_bytes: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EgressMode {
    DenyAll,
    PublicInternet,
}

/// Generate once and retain across retries of the same claim or replacement operation.
pub struct CandidateCredential(Zeroizing<Vec<u8>>);

impl CandidateCredential {
    pub fn generate() -> Result<Self> {
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| Error::Random)?;
        Ok(Self(Zeroizing::new(
            URL_SAFE_NO_PAD.encode(bytes.as_slice()).into_bytes(),
        )))
    }

    pub fn from_saved(bytes: Vec<u8>) -> Result<Self> {
        let bytes = Zeroizing::new(bytes);
        let mut raw = Zeroizing::new([0; 33]);
        if bytes.len() != 43 || URL_SAFE_NO_PAD.decode_slice(&*bytes, raw.as_mut()) != Ok(32) {
            return Err(Error::Invalid("candidate credential"));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone)]
pub struct RegistrationClient {
    external: Client,
    loopback: Client,
}

pub struct Identity<'a> {
    pub device_id: &'a str,
    pub key_revision: u64,
    pub signer: &'a dyn IdentitySigner,
}

#[derive(Debug, thiserror::Error)]
#[error("Salix registration returned HTTP {status}{}", code.as_deref().map_or(String::new(), |code| format!(": {code}")))]
pub struct RegistrationError {
    pub status: u16,
    pub code: Option<String>,
}

impl RegistrationClient {
    pub fn new() -> Result<Self> {
        let builder = || {
            Client::builder()
                .timeout(Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
        };
        Ok(Self {
            external: builder().build().map_err(|_| Error::HttpTransport)?,
            loopback: builder()
                .no_proxy()
                .resolve("localhost", SocketAddr::from(([127, 0, 0, 1], 0)))
                .build()
                .map_err(|_| Error::HttpTransport)?,
        })
    }

    pub async fn claim_registration(
        &self,
        url: &str,
        registration_id: &str,
        ticket: &str,
        identity: &Identity<'_>,
        candidate: &CandidateCredential,
    ) -> Result<RegistrationConfig> {
        let url = registration_url(url)?;
        if url.path() != CLAIM_PATH || ticket.len() > 128 {
            return Err(Error::Invalid("claim descriptor"));
        }
        let authorization = authorization("RegistrationTicket", ticket)?;
        validate_identity(registration_id, identity)?;
        let request = ClaimRequest {
            version: 1,
            registration_id,
            device_id: identity.device_id,
            root_public_key: STANDARD.encode(identity.signer.public_key()?),
            root_key_revision: identity.key_revision,
            candidate_credential: Zeroizing::new(STANDARD.encode(candidate.as_bytes())),
        };
        let config = self.post(url, &authorization, &request).await?;
        validate_config(config, registration_id)
    }

    pub async fn recover_registration(
        &self,
        url: &str,
        registration_id: &str,
        bearer: &str,
        identity: &Identity<'_>,
        candidate: &CandidateCredential,
    ) -> Result<RegistrationConfig> {
        let url = registration_url(url)?;
        if !valid_recovery_path(url.path(), registration_id) || bearer.len() > 4096 {
            return Err(Error::Invalid("recovery descriptor"));
        }
        validate_identity(registration_id, identity)?;
        let authorization = authorization("Bearer", bearer)?;
        let mut challenge_url = url.clone();
        challenge_url
            .path_segments_mut()
            .map_err(|_| Error::Invalid("recovery URL"))?
            .push("challenge");
        let hash = STANDARD.encode(Sha256::digest(candidate.as_bytes()));
        let reply: ChallengeReply = self
            .post(
                challenge_url,
                &authorization,
                &ChallengeRequest {
                    candidate_credential_hash: &hash,
                },
            )
            .await?;
        let now = unix_millis() / 1000;
        let challenge = RecoveryChallenge::parse(reply.challenge.get().as_bytes(), now)?;
        if reply.registration_id != registration_id
            || challenge.registration_id != registration_id
            || challenge.candidate_credential_hash != hash
        {
            return Err(Error::Invalid("recovery challenge target"));
        }
        let request = ReplaceRequest {
            candidate_credential: Zeroizing::new(STANDARD.encode(candidate.as_bytes())),
            proof: Proof {
                device_id: identity.device_id,
                root_public_key: STANDARD.encode(identity.signer.public_key()?),
                root_key_revision: identity.key_revision,
                nonce: challenge.nonce.clone(),
                signature: STANDARD.encode(challenge.sign(identity.signer, now)?),
            },
        };
        let config = self.post(url, &authorization, &request).await?;
        validate_config(config, registration_id)
    }

    async fn post<T: DeserializeOwned>(
        &self,
        url: Url,
        authorization: &header::HeaderValue,
        body: &impl Serialize,
    ) -> Result<T> {
        let client = if url.scheme() == "http" {
            &self.loopback
        } else {
            &self.external
        };
        let mut response = client
            .post(url)
            .header(header::AUTHORIZATION, authorization)
            .json(body)
            .send()
            .await
            .map_err(|_| Error::HttpTransport)?;
        let status = response.status();
        if status != reqwest::StatusCode::OK {
            let bytes = read_limited(&mut response, 4096).await.unwrap_or_default();
            let code = serde_json::from_slice::<ErrorBody>(&bytes)
                .ok()
                .map(|body| match body.error {
                    ErrorCode::Code(code) | ErrorCode::Detail { code } => code,
                })
                .filter(|code| known_error(code));
            return Err(RegistrationError {
                status: status.as_u16(),
                code,
            }
            .into());
        }
        let bytes = read_limited(&mut response, MAX_BODY).await?;
        serde_json::from_slice(&bytes).map_err(|_| Error::Invalid("registration response"))
    }
}

#[derive(Serialize)]
struct ClaimRequest<'a> {
    version: u32,
    registration_id: &'a str,
    device_id: &'a str,
    root_public_key: String,
    root_key_revision: u64,
    #[serde(serialize_with = "serialize_secret")]
    candidate_credential: Zeroizing<String>,
}

#[derive(Serialize)]
struct ChallengeRequest<'a> {
    candidate_credential_hash: &'a str,
}

#[derive(Deserialize)]
struct ChallengeReply {
    registration_id: String,
    challenge: Box<RawValue>,
}

#[derive(Serialize)]
struct ReplaceRequest<'a> {
    #[serde(serialize_with = "serialize_secret")]
    candidate_credential: Zeroizing<String>,
    proof: Proof<'a>,
}

#[derive(Serialize)]
struct Proof<'a> {
    device_id: &'a str,
    root_public_key: String,
    root_key_revision: u64,
    nonce: String,
    signature: String,
}

fn serialize_secret<S: serde::Serializer>(
    value: &str,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_str(value)
}

#[derive(Deserialize)]
struct ErrorBody {
    error: ErrorCode,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ErrorCode {
    Code(String),
    Detail { code: String },
}

fn known_error(code: &str) -> bool {
    matches!(
        code,
        "invalid_ticket"
            | "not_found"
            | "ticket_expired"
            | "registration_revoked"
            | "authorization_expired"
            | "authorization_changed"
            | "host_identity_mismatch"
            | "credential_reauthorization_required"
            | "scope_host_bound"
            | "unavailable"
            | "connection_config_unavailable"
            | "invalid_request"
            | "invalid_candidate_credential"
            | "invalid_host_identity"
            | "invalid_recovery_challenge"
            | "invalid_recovery_proof"
            | "recovery_target_unavailable"
            | "original_subject_unknown"
    )
}

async fn read_limited(response: &mut reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| Error::HttpTransport)? {
        if chunk.len() > limit - body.len() {
            return Err(Error::Invalid("registration response size"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn authorization(scheme: &str, value: &str) -> Result<header::HeaderValue> {
    if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_graphic()) {
        return Err(Error::Invalid("registration authorization"));
    }
    let mut header = header::HeaderValue::from_str(&format!("{scheme} {value}"))
        .map_err(|_| Error::Invalid("registration authorization"))?;
    header.set_sensitive(true);
    Ok(header)
}

fn registration_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).map_err(|_| Error::Invalid("registration URL"))?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !url.has_host()
        || (!url.username().is_empty() || url.password().is_some())
        || url.query().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
    {
        return Err(Error::Invalid("registration URL"));
    }
    Ok(url)
}

fn valid_recovery_path(path: &str, registration_id: &str) -> bool {
    let pieces: Vec<_> = path.split('/').collect();
    let safe = |value: &str| {
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    };
    let id = match pieces.as_slice() {
        [
            "",
            "v1",
            "comma",
            "workspaces",
            workspace,
            "compute-nodes",
            "agent-vmm",
            "registrations",
            id,
            "recovery",
        ] if safe(workspace) => id,
        [
            "",
            "v1",
            "orgs",
            org,
            "projects",
            project,
            "compute-nodes",
            "agent-vmm",
            "registrations",
            id,
            "recovery",
        ] if safe(org) && safe(project) => id,
        _ => return false,
    };
    *id == registration_id && safe(id)
}

fn validate_identity(registration_id: &str, identity: &Identity<'_>) -> Result<()> {
    if registration_id.is_empty()
        || registration_id.len() > 200
        || identity.device_id.is_empty()
        || identity.key_revision == 0
    {
        return Err(Error::Invalid("registration identity"));
    }
    Ok(())
}

fn validate_config(
    config: RegistrationConfig,
    registration_id: &str,
) -> Result<RegistrationConfig> {
    let connection = &config.remote_connection;
    let pool = &connection.pool_policy;
    if config.version != 1
        || config.registration_id != registration_id
        || connection.policy_revision == 0
        || connection.gateway_endpoint.is_empty()
        || pool.per_environment_limits.pids == 0
        || pool.per_environment_limits.disk_bytes == 0
        || !(1..=300).contains(&pool.stop_grace_seconds)
    {
        return Err(Error::Invalid("registration connection policy"));
    }
    Ok(config)
}
