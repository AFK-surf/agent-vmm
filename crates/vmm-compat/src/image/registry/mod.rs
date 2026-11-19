//! Fetch a public linux/arm64 registry image into the same verified layout used by ImportImage.
mod archive;
mod http;
#[cfg(test)]
mod tests;

trait RegistrySource {
    fn get(
        &mut self,
        id: &str,
        blob: bool,
    ) -> impl std::future::Future<Output = Result<reqwest::Response, tonic::Status>> + Send;
}

use super::{MAX_ARCHIVE_BYTES, VerifiedArchive, oci};
use crate::error::{host_status, invalid_field};
use agent_vmm_proto::host::v1::{ErrorReason as Reason, ImportImageHeader};
use oci_spec::distribution::Reference;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path, time::Duration};
use tonic::{Code, Status};

const META: usize = 16 << 20;
const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const INDEX: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_INDEX: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";

fn failure(reason: Reason) -> Status {
    let (code, msg) = match reason {
        Reason::ImageNotFound => (Code::NotFound, "image reference does not exist"),
        Reason::ImagePlatformUnavailable => {
            (Code::FailedPrecondition, "image has no linux/arm64 variant")
        }
        Reason::ImageRegistryRateLimited => {
            (Code::Unavailable, "image registry rate limit reached")
        }
        Reason::ImageAuthUnavailable => {
            (Code::Unavailable, "image registry credentials unavailable")
        }
        Reason::ImageDownloadTimeout => (Code::Unavailable, "image download timed out"),
        Reason::ImageDownloadInterrupted => (Code::Unavailable, "image download interrupted"),
        Reason::ImageManifestFetchFailed => {
            (Code::Unavailable, "invalid or unavailable image manifest")
        }
        _ => (Code::Unavailable, "image registry unavailable"),
    };
    host_status(code, reason, msg)
}
fn invalid() -> Status {
    failure(Reason::ImageManifestFetchFailed)
}
fn interrupted() -> Status {
    failure(Reason::ImageDownloadInterrupted)
}
fn timeout() -> Status {
    failure(Reason::ImageDownloadTimeout)
}
fn platform() -> Status {
    failure(Reason::ImagePlatformUnavailable)
}
fn too_large() -> Status {
    invalid_field("reference", "image exceeds the 8 GiB import limit")
}

fn supported(p: &Value) -> bool {
    p["os"] == "linux"
        && p["architecture"] == "arm64"
        && (p.get("variant").is_none() || p["variant"] == "" || p["variant"] == "v8")
}

fn hash(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}

fn descriptor(v: &Value) -> Result<(&str, u64), Status> {
    let key = v["digest"].as_str().ok_or_else(invalid)?;
    oci::digest(key).map_err(|_| invalid())?;
    Ok((key, v["size"].as_u64().ok_or_else(invalid)?))
}

async fn manifest(
    registry: &mut impl RegistrySource,
    locator: &str,
    pinned: Option<(&str, Option<u64>)>,
) -> Result<(Vec<u8>, Value), Status> {
    let response = registry.get(locator, false).await?;
    let expected = response
        .headers()
        .get("docker-content-digest")
        .map(|h| h.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| invalid())?;
    let data = http::small(response, META).await?;
    let digest = hash(&data);
    if expected.is_some_and(|h| h != digest)
        || pinned.is_some_and(|(h, len)| h != digest || len.is_some_and(|n| n != data.len() as u64))
    {
        return Err(invalid());
    }
    let value: Value = serde_json::from_slice(&data).map_err(|_| invalid())?;
    if value["schemaVersion"] != 2 {
        return Err(invalid());
    }
    Ok((data, value))
}

pub(super) async fn export(dir: &Path, reference: &Reference) -> Result<VerifiedArchive, Status> {
    tokio::time::timeout(
        Duration::from_secs(600),
        export_inner(dir, reference, &mut http::Registry::new(reference)?),
    )
    .await
    .map_err(|_| timeout())?
}

async fn export_inner(
    dir: &Path,
    reference: &Reference,
    registry: &mut impl RegistrySource,
) -> Result<VerifiedArchive, Status> {
    let locator = reference.digest().or(reference.tag()).ok_or_else(invalid)?;
    let (mut raw, mut doc) =
        manifest(registry, locator, reference.digest().map(|v| (v, None))).await?;
    if matches!(doc["mediaType"].as_str(), Some(INDEX | DOCKER_INDEX)) {
        let entry = doc["manifests"]
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .find(|e| supported(&e["platform"]))
            .ok_or_else(platform)?;
        let (id, n) = descriptor(entry)?;
        (raw, doc) = manifest(registry, id, Some((id, Some(n)))).await?;
    }
    if !matches!(doc["mediaType"].as_str(), Some(MANIFEST | DOCKER_MANIFEST)) {
        return Err(invalid());
    }
    let (config_key, size) = descriptor(&doc["config"])?;
    if size > META as u64 {
        return Err(invalid());
    }
    let mut config = http::small(registry.get(config_key, true).await?, size as usize).await?;
    if config.len() as u64 != size || hash(&config) != config_key {
        return Err(interrupted());
    }
    let mut cfg: Value = serde_json::from_slice(&config).map_err(|_| invalid())?;
    if !supported(&cfg) {
        return Err(platform());
    }
    let mut changed = doc["mediaType"] != MANIFEST;
    doc["mediaType"] = MANIFEST.into();
    if cfg["variant"] == "v8" {
        cfg.as_object_mut().ok_or_else(invalid)?.remove("variant");
        config = serde_json::to_vec(&cfg).map_err(|_| invalid())?;
        doc["config"]["size"] = config.len().into();
        doc["config"]["digest"] = hash(&config).into();
        changed = true;
    }
    if doc["config"]["mediaType"] == "application/vnd.docker.container.image.v1+json" {
        doc["config"]["mediaType"] = "application/vnd.oci.image.config.v1+json".into();
        changed = true;
    }
    let mut layers = BTreeMap::new();
    let mut total = config.len() as u64;
    for layer in doc["layers"].as_array_mut().ok_or_else(invalid)? {
        let (key, size) = descriptor(layer)?;
        total = total
            .checked_add(size)
            .filter(|v| *v <= MAX_ARCHIVE_BYTES)
            .ok_or_else(too_large)?;
        if layers
            .insert(key.to_owned(), size)
            .is_some_and(|old| old != size)
        {
            return Err(invalid());
        }
        let ty = match layer["mediaType"].as_str() {
            Some("application/vnd.docker.image.rootfs.diff.tar.gzip") => {
                "application/vnd.oci.image.layer.v1.tar+gzip"
            }
            Some("application/vnd.docker.image.rootfs.diff.tar") => {
                "application/vnd.oci.image.layer.v1.tar"
            }
            Some("application/vnd.docker.image.rootfs.foreign.diff.tar.gzip") => {
                "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip"
            }
            _ => continue,
        };
        layer["mediaType"] = ty.into();
        changed = true;
    }
    if changed {
        raw = serde_json::to_vec(&doc).map_err(|_| invalid())?
    }
    let identity = hash(&raw);
    let index = json!({"schemaVersion":2,"manifests":[{"mediaType":MANIFEST,"digest":identity,"size":raw.len(),"platform":{"os":"linux","architecture":"arm64"}}]});
    let mut archive = archive::Archive::new(dir)?;
    archive
        .bytes("oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#)
        .await?;
    archive
        .bytes(
            "index.json",
            &serde_json::to_vec(&index).map_err(|_| invalid())?,
        )
        .await?;
    let name = |key: &str| format!("blobs/sha256/{}", &key[7..]);
    archive.bytes(&name(&identity), &raw).await?;
    archive.bytes(&name(&hash(&config)), &config).await?;
    for (key, size) in layers {
        let response = registry.get(&key, true).await?;
        archive.layer(&name(&key), size, response).await?;
    }
    let file = archive.finish().await?;
    let digest = oci::digest(&identity)?;
    let header = ImportImageHeader {
        image_reference: reference.whole(),
        platform: "linux/arm64".into(),
        manifest_digest: identity,
        ..Default::default()
    };
    tokio::task::spawn_blocking(move || {
        let logical_bytes = oci::validate(file.path(), digest).map_err(|s| {
            if s.code() == Code::InvalidArgument {
                interrupted()
            } else {
                s
            }
        })?;
        Ok(VerifiedArchive {
            file,
            logical_bytes,
            header,
            digest,
        })
    })
    .await
    .map_err(|_| interrupted())?
}

pub(super) fn with_image(status: Status, reference: &str) -> Status {
    crate::error::with_resource(status, "image", reference)
}
