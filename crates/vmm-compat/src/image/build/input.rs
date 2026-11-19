use super::super::oci;
use agent_vmm_proto::host::v1 as h;
use oci_spec::distribution::Reference;
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::File,
    io::{Seek, SeekFrom},
    path::{Component, Path},
};
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;
use zeroize::Zeroizing;

pub(super) fn fingerprint(header: &h::BuildImageHeader) -> String {
    let input = h::BuildInputFingerprint {
        dockerfile_path: header.dockerfile_path.clone(),
        image: header.image.clone(),
        target: header.target.clone(),
        build_args: header.build_args.clone(),
        network_mode: header.network_mode,
        secrets: header.secrets.clone(),
        base_image_reference: header.base_image_reference.clone(),
        base_manifest_digest: header.base_manifest_digest.clone(),
        context_sha256: header.context_sha256.clone(),
    };
    hex::encode(Sha256::digest(input.encode_to_vec()))
}

fn valid_name(name: &str) -> bool {
    name.as_bytes()
        .first()
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
}

pub(super) fn validate(header: &h::BuildImageHeader) -> Result<String, Status> {
    let bad = || Status::invalid_argument("invalid build header");
    let path = Path::new(&header.dockerfile_path);
    if header.request_id.is_empty()
        || header.request_id.len() > 128
        || path.file_name().is_none()
        || path
            .components()
            .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        || header.dockerfile_path.contains(['\0', '\\'])
        || header.build_args.len() > 128
        || header.secrets.len() > 32
        || header
            .target
            .as_deref()
            .is_some_and(|target| !valid_name(target))
        || header.network_mode() == h::BuildNetworkMode::Unspecified
    {
        return Err(bad());
    }
    if !header.context_sha256.is_empty() {
        oci::digest(&format!("sha256:{}", header.context_sha256))?;
    }
    if !header.base_image_reference.is_empty() || !header.base_manifest_digest.is_empty() {
        header
            .base_image_reference
            .parse::<Reference>()
            .map_err(|_| bad())?;
        oci::digest(&header.base_manifest_digest)?;
    }
    let mut names = HashSet::new();
    for arg in &header.build_args {
        if !valid_name(&arg.name)
            || !names.insert(&arg.name)
            || arg.value.contains('\0')
            || arg.value.len() > 1 << 20
        {
            return Err(bad());
        }
    }
    names.clear();
    for secret in &header.secrets {
        if !valid_name(&secret.id) || !names.insert(&secret.id) {
            return Err(bad());
        }
    }
    Ok(header
        .image
        .parse::<Reference>()
        .map_err(|_| bad())?
        .whole())
}

pub(super) struct Upload {
    pub context: File,
    pub secrets: Vec<Zeroizing<Vec<u8>>>,
}
pub(super) async fn receive<S>(
    dir: &Path,
    header: &h::BuildImageHeader,
    mut stream: S,
) -> Result<Upload, Status>
where
    S: Stream<Item = Result<h::BuildImageRequest, Status>> + Unpin,
{
    use h::build_image_request::Event;
    let io = |_| Status::unavailable("build context staging unavailable");
    let mut file = tokio::fs::File::from_std(tempfile::tempfile_in(dir).map_err(io)?);
    let mut bytes = 0usize;
    let mut hash = Sha256::new();
    let mut context_done = false;
    let mut secrets = Vec::new();
    let mut current = Zeroizing::new(Vec::new());
    let mut total = 0usize;
    while let Some(frame) = stream.next().await.transpose()? {
        match frame.event {
            Some(Event::Context(part)) if !context_done => {
                bytes = bytes
                    .checked_add(part.data.len())
                    .filter(|b| *b <= 256 << 20)
                    .ok_or_else(|| Status::resource_exhausted("build context exceeds 256 MiB"))?;
                file.write_all(&part.data).await.map_err(io)?;
                hash.update(&part.data);
                context_done = part.r#final;
            }
            Some(Event::Secret(part))
                if context_done
                    && header
                        .secrets
                        .get(secrets.len())
                        .is_some_and(|s| s.id == part.id) =>
            {
                let data = Zeroizing::new(part.data);
                total = total.saturating_add(data.len());
                if total > 4 << 20 || current.len().saturating_add(data.len()) > 1 << 20 {
                    return Err(Status::resource_exhausted("build secret exceeds limit"));
                }
                current.extend_from_slice(&data);
                if part.r#final {
                    secrets.push(std::mem::take(&mut current));
                }
            }
            _ => return Err(Status::invalid_argument("build frames are out of order")),
        }
    }
    if !context_done || secrets.len() != header.secrets.len() {
        return Err(Status::invalid_argument("incomplete build upload"));
    }
    if !header.context_sha256.is_empty() && hex::encode(hash.finalize()) != header.context_sha256 {
        return Err(Status::invalid_argument("build context digest mismatch"));
    }
    file.flush().await.map_err(io)?;
    let mut context = file.into_std().await;
    context.seek(SeekFrom::Start(0)).map_err(io)?;
    Ok(Upload { context, secrets })
}
