use super::{failure, interrupted, invalid, timeout};
use crate::image::download::public;
use agent_vmm_proto::host::v1::ErrorReason as Reason;
use oci_spec::distribution::Reference;
use reqwest::{Client, Response, StatusCode, header, redirect::Policy};
use std::{net::IpAddr, time::Duration};
use tokio::net::lookup_host;
use tonic::Status;
use url::Url;
use zeroize::Zeroizing;

const IDLE: Duration = Duration::from_secs(30);
const ACCEPT: &str = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json";

pub(super) struct Registry {
    base: Url,
    repository: String,
    token: Option<Zeroizing<String>>,
}

impl Registry {
    pub fn new(reference: &Reference) -> Result<Self, Status> {
        let registry = match reference.registry() {
            "docker.io" => "registry-1.docker.io",
            name => name,
        };
        let repository = reference.repository();
        let base =
            Url::parse(&format!("https://{registry}/v2/{repository}/")).map_err(|_| invalid())?;
        Ok(Self {
            base,
            repository: repository.into(),
            token: None,
        })
    }
}

impl super::RegistrySource for Registry {
    async fn get(&mut self, id: &str, blob: bool) -> Result<Response, Status> {
        let path = if blob { "blobs" } else { "manifests" };
        let url = self
            .base
            .join(&format!("{path}/{id}"))
            .map_err(|_| invalid())?;
        let mut response = fetch(url.clone(), self.token.as_deref().map(String::as_str)).await?;
        if response.status() == StatusCode::UNAUTHORIZED
            && response.url().origin() == self.base.origin()
        {
            let realm = challenge(&response, &self.repository)?;
            let token = fetch(realm, None).await?;
            if !token.status().is_success() {
                return Err(auth());
            }
            #[derive(serde::Deserialize)]
            struct Token {
                token: Option<String>,
                access_token: Option<String>,
            }
            let body: Token =
                serde_json::from_slice(&small(token, 1 << 20).await?).map_err(|_| auth())?;
            let value = body
                .token
                .filter(|s| !s.is_empty())
                .or(body.access_token)
                .filter(|s| !s.is_empty())
                .ok_or_else(auth)?;
            self.token = Some(Zeroizing::new(value));
            response = fetch(url, self.token.as_deref().map(String::as_str)).await?;
        }
        match response.status() {
            StatusCode::OK => Ok(response),
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(auth()),
            StatusCode::TOO_MANY_REQUESTS => Err(failure(Reason::ImageRegistryRateLimited)),
            StatusCode::NOT_FOUND if !blob => Err(failure(Reason::ImageNotFound)),
            _ if blob => Err(interrupted()),
            _ => Err(failure(Reason::ImageManifestFetchFailed)),
        }
    }
}

fn auth() -> Status {
    failure(Reason::ImageAuthUnavailable)
}

fn challenge(response: &Response, repository: &str) -> Result<Url, Status> {
    for header in response.headers().get_all(header::WWW_AUTHENTICATE) {
        let Ok(raw) = header.to_str() else { continue };
        for item in http_auth::parse_challenges(raw).unwrap_or_default() {
            if !item.scheme.eq_ignore_ascii_case("bearer") {
                continue;
            }
            let param = |name: &str| {
                item.params
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v.to_unescaped())
            };
            let mut url = Url::parse(&param("realm").ok_or_else(auth)?).map_err(|_| auth())?;
            if let Some(service) = param("service") {
                url.query_pairs_mut().append_pair("service", &service);
            }
            url.query_pairs_mut()
                .append_pair("scope", &format!("repository:{repository}:pull"));
            return Ok(url);
        }
    }
    Err(auth())
}

async fn fetch(mut url: Url, bearer: Option<&str>) -> Result<Response, Status> {
    let origin = url.origin();
    for _ in 0..=8 {
        let unavailable = || failure(Reason::ImageRegistryUnavailable);
        if url.scheme() != "https"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(unavailable());
        }
        let host = url.host_str().ok_or_else(unavailable)?;
        let port = url.port_or_known_default().ok_or_else(unavailable)?;
        let addresses = tokio::time::timeout(IDLE, lookup_host((host, port)))
            .await
            .map_err(|_| timeout())?
            .map_err(|_| unavailable())?;
        let mut selected = None;
        for addr in addresses {
            if let IpAddr::V4(ip) = addr.ip() {
                if !public(ip) {
                    return Err(unavailable());
                }
                selected.get_or_insert(addr);
            }
        }
        let client = Client::builder()
            .https_only(true)
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(IDLE)
            .read_timeout(IDLE)
            .resolve(host, selected.ok_or_else(unavailable)?)
            .build()
            .map_err(|_| unavailable())?;
        let mut request = client.get(url.clone()).header(header::ACCEPT, ACCEPT);
        if let Some(value) = bearer.filter(|_| origin == url.origin()) {
            request = request.bearer_auth(value);
        }
        let response = request.send().await.map_err(network)?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        url = response
            .headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| url.join(v).ok())
            .ok_or_else(unavailable)?;
    }
    Err(failure(Reason::ImageRegistryUnavailable))
}

pub(super) fn network(error: reqwest::Error) -> Status {
    if error.is_timeout() {
        timeout()
    } else {
        interrupted()
    }
}

pub(super) async fn small(mut response: Response, limit: usize) -> Result<Vec<u8>, Status> {
    let mut out = Vec::new();
    while let Some(part) = response.chunk().await.map_err(network)? {
        if part.len() > limit - out.len() {
            return Err(invalid());
        }
        out.extend_from_slice(&part);
    }
    Ok(out)
}
