use anyhow::{Result, ensure};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    path::Path,
};
use tar::{Builder, EntryType, Header};

const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
const CONFIG: &str = "application/vnd.oci.image.config.v1+json";
const LAYER: &str = "application/vnd.oci.image.layer.v1.tar";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Descriptor {
    media_type: &'static str,
    digest: String,
    size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<Value>,
}

impl Descriptor {
    fn new(media_type: &'static str, hash: &[u8], size: u64) -> Self {
        Self {
            media_type,
            digest: format!("sha256:{}", hex::encode(hash)),
            size,
            platform: None,
        }
    }

    fn blob(media_type: &'static str, data: &[u8]) -> Self {
        Self::new(media_type, &Sha256::digest(data), data.len() as u64)
    }
}

// Map keys were sorted by Go's encoding/json, while descriptor fields retained declaration order.
// Both orders are part of the published content identities.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Manifest<'a> {
    config: &'a Descriptor,
    layers: [&'a Descriptor; 1],
    media_type: &'static str,
    schema_version: u8,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Index {
    manifests: [Descriptor; 1],
    schema_version: u8,
}

fn entry<W: Write>(
    tar: &mut Builder<W>,
    path: &str,
    kind: EntryType,
    mode: u32,
    size: u64,
    data: impl Read,
) -> Result<()> {
    let mut h = Header::new_ustar();
    h.set_path(path)?;
    h.set_entry_type(kind);
    h.set_mode(mode);
    h.set_uid(0);
    h.set_gid(0);
    if size >= 1 << 33 {
        // USTAR's eleven octal digits cannot encode this size. Go writes a
        // separate PAX record and zeroes the size field in the following header.
        let field = format!("size={size}\n");
        // For these u64 sizes the total record length always has two digits.
        let record = format!("{} {field}", field.len() + 3);
        let (dir, blob) = path
            .rsplit_once('/')
            .expect("only layer blobs exceed 8 GiB");
        let mut pax = Header::new_ustar();
        pax.set_path(format!("{dir}/PaxHeaders.0/{blob}"))?;
        pax.set_entry_type(EntryType::XHeader);
        pax.set_mode(0);
        pax.set_uid(0);
        pax.set_gid(0);
        pax.set_size(record.len() as u64);
        checksum(&mut pax)?;
        tar.append(&pax, record.as_bytes())?;
        h.set_size(0);
    } else {
        h.set_size(size);
    }
    h.set_username("root")?;
    h.set_groupname("root")?;
    h.set_mtime(0);
    h.set_device_major(0)?;
    h.set_device_minor(0)?;
    checksum(&mut h)?;
    tar.append(&h, data)?;
    Ok(())
}

fn checksum(h: &mut Header) -> Result<()> {
    h.set_cksum();
    // POSIX checksum spelling used by Go's archive/tar.
    let sum = format!("{:06o}\0 ", h.cksum()?);
    h.as_old_mut().cksum.copy_from_slice(sum.as_bytes());
    Ok(())
}

pub fn run(args: &[String]) -> Result<()> {
    ensure!(
        (2..=3).contains(&args.len()),
        "usage: oci ROOTFS_TAR OUTPUT_OCI_TAR [ENTRYPOINT]"
    );
    if let Some(path) = args.get(2) {
        ensure!(
            Path::new(path).is_absolute(),
            "OCI entrypoint must be absolute"
        );
    }
    let mut input = BufReader::new(File::open(&args[0])?);
    let mut snapshot = tempfile::tempfile()?;
    let mut hash = Sha256::new();
    loop {
        let chunk = input.fill_buf()?;
        if chunk.is_empty() {
            break;
        }
        hash.update(chunk);
        snapshot.write_all(chunk)?;
        let n = chunk.len();
        input.consume(n);
    }
    let layer = Descriptor::new(LAYER, &hash.finalize(), snapshot.stream_position()?);
    snapshot.rewind()?;
    let platform = json!({"architecture": "arm64", "os": "linux"});
    let mut config = platform.clone();
    config["rootfs"] = json!({"diff_ids": [&layer.digest], "type": "layers"});
    if let Some(path) = args.get(2) {
        config["config"] = json!({"Entrypoint": [path]});
    }
    config.sort_all_objects();
    let config = serde_json::to_string(&config)?
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029");
    let config_desc = Descriptor::blob(CONFIG, config.as_bytes());
    let manifest = serde_json::to_vec(&Manifest {
        config: &config_desc,
        layers: [&layer],
        media_type: MANIFEST,
        schema_version: 2,
    })?;
    let mut manifest_desc = Descriptor::blob(MANIFEST, &manifest);
    manifest_desc.platform = Some(platform);

    let mut tar = Builder::new(BufWriter::new(File::create(&args[1])?));
    for dir in ["blobs", "blobs/sha256"] {
        entry(&mut tar, dir, EntryType::Directory, 0o755, 0, &[][..])?;
    }
    let mut blobs: [(&Descriptor, u32, &mut dyn Read); 3] = [
        (&layer, 0o600, &mut snapshot),
        (&config_desc, 0o644, &mut config.as_bytes()),
        (&manifest_desc, 0o644, &mut manifest.as_slice()),
    ];
    blobs.sort_by_key(|(d, _, _)| &d.digest);
    for (d, mode, data) in blobs {
        entry(
            &mut tar,
            &format!("blobs/sha256/{}", &d.digest[7..]),
            EntryType::Regular,
            mode,
            d.size,
            data,
        )?;
    }
    let index = serde_json::to_vec(&Index {
        manifests: [manifest_desc],
        schema_version: 2,
    })?;
    for (path, data) in [
        ("index.json", index.as_slice()),
        ("oci-layout", b"{\"imageLayoutVersion\":\"1.0.0\"}\n"),
    ] {
        entry(
            &mut tar,
            path,
            EntryType::Regular,
            0o644,
            data.len() as u64,
            data,
        )?;
    }
    tar.into_inner()?.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_layer_emits_the_go_pax_headers() {
        let mut tar = Builder::new(Vec::new());
        let path = "blobs/sha256/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        // Inspect only the headers, without allocating or writing the 8 GiB payload.
        entry(
            &mut tar,
            path,
            EntryType::Regular,
            0o600,
            1 << 33,
            std::io::empty(),
        )
        .unwrap();
        assert_eq!(tar.get_ref().len(), 1536);
        assert_eq!(
            format!("{:x}", Sha256::digest(tar.get_ref())),
            "fcadb5544d4719da0cd8e6314b2804b6b7718c7d22cf6c4369f0059a94d9c53a"
        );
    }
}
