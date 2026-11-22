use crate::{Digest, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom},
    path::Path,
};

pub const MAGIC: &[u8; 16] = b"COMMA_EROFS_V1\n\0";
pub const MAX_HEADER_BYTES: usize = 64 * 1024;
pub const MAX_EROFS_BYTES: u64 = 16 << 30; // 16 GiB
pub const EROFS_BLOCK_BYTES: u64 = 4096;
pub const EROFS_SUPER_OFFSET: usize = 1024;
pub const EROFS_SUPER_MAGIC_LE: [u8; 4] = [0xe2, 0xe1, 0xf5, 0xe0]; // 0xe0f5e1e2 LE

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ContainerDefaults {
    pub entrypoint: Option<Vec<String>>,
    pub cmd: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub working_dir: Option<String>,
    pub user: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErofsBundleHeader {
    pub schema_version: u32,
    pub manifest_digest: String,
    pub platform: String,
    pub logical_bytes: u64,
    pub erofs_bytes: u64,
    pub erofs_sha256: String,
    pub compression: ErofsBundleCompression,
    #[serde(default)]
    pub defaults: ContainerDefaults,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErofsBundleCompression {
    None,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedErofsBundle {
    pub header: ErofsBundleHeader,
    pub manifest_digest: Digest,
    pub erofs_sha256: Digest,
    pub payload_offset: u64,
    pub payload_len: u64,
}

pub fn has_magic(prefix: &[u8]) -> bool {
    prefix.len() >= MAGIC.len() && &prefix[..MAGIC.len()] == MAGIC
}

pub fn encode_frame(header: &ErofsBundleHeader, payload: &[u8]) -> Result<Vec<u8>> {
    let header_bytes = serde_json::to_vec(header)?;
    let header_len = u32::try_from(header_bytes.len())
        .ok()
        .filter(|&len| (1..=MAX_HEADER_BYTES).contains(&(len as usize)))
        .ok_or(Error::Invalid("EROFS bundle header length"))?;
    let mut out = Vec::with_capacity(MAGIC.len() + 4 + header_bytes.len() + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&header_len.to_be_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_lower_hex_32(hex: &str, label: &'static str) -> Result<Digest> {
    if hex.len() != 64 {
        return Err(Error::Invalid(label));
    }
    let mut out = [0u8; 32];
    for (slot, &[hi, lo]) in out.iter_mut().zip(hex.as_bytes().as_chunks::<2>().0) {
        let hi = lower_hex_nibble(hi).ok_or(Error::Invalid(label))?;
        let lo = lower_hex_nibble(lo).ok_or(Error::Invalid(label))?;
        *slot = (hi << 4) | lo;
    }
    Ok(out)
}

fn lower_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        _ => None,
    }
}

fn read_prefix_up_to(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut read_total = 0;
    while read_total < buf.len() {
        match file.read(&mut buf[read_total..]) {
            Ok(0) => break,
            Ok(n) => read_total += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(read_total)
}

pub fn parse_and_validate_file(
    path: &Path,
    expected_manifest: Option<Digest>,
) -> Result<Option<ParsedErofsBundle>> {
    let mut file = File::open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() {
        return Ok(None);
    }
    let mut magic = [0u8; MAGIC.len()];
    let n = read_prefix_up_to(&mut file, &mut magic)?;
    if !has_magic(&magic[..n]) {
        return Ok(None);
    }

    let mut len_buf = [0u8; 4];
    file.read_exact(&mut len_buf).map_err(|e| {
        if e.kind() == ErrorKind::UnexpectedEof {
            Error::Invalid("truncated EROFS bundle header length")
        } else {
            Error::Io(e)
        }
    })?;
    let header_len = u32::from_be_bytes(len_buf) as usize;
    if !(1..=MAX_HEADER_BYTES).contains(&header_len) {
        return Err(Error::Invalid("EROFS bundle header length"));
    }

    let file_len = meta.len();
    let payload_offset = (MAGIC.len() + 4) as u64 + header_len as u64;
    if file_len <= payload_offset {
        return Err(Error::Invalid("EROFS bundle missing payload"));
    }
    let payload_len = file_len - payload_offset;

    let mut header_bytes = vec![0u8; header_len];
    file.read_exact(&mut header_bytes).map_err(|e| {
        if e.kind() == ErrorKind::UnexpectedEof {
            Error::Invalid("truncated EROFS bundle header")
        } else {
            Error::Io(e)
        }
    })?;
    let header: ErofsBundleHeader = serde_json::from_slice(&header_bytes)
        .map_err(|_| Error::Invalid("EROFS bundle header JSON"))?;

    if header.schema_version != 1 {
        return Err(Error::Invalid("EROFS bundle schema version"));
    }
    let manifest_hex = header
        .manifest_digest
        .strip_prefix("sha256:")
        .ok_or(Error::Invalid("EROFS bundle manifest digest"))?;
    let manifest_digest = decode_lower_hex_32(manifest_hex, "EROFS bundle manifest digest")?;
    if let Some(expected) = expected_manifest
        && manifest_digest != expected
    {
        return Err(Error::Invalid("EROFS bundle manifest digest mismatch"));
    }
    if header.platform != "linux/arm64" {
        return Err(Error::Invalid("EROFS bundle platform"));
    }
    if header.logical_bytes == 0 {
        return Err(Error::Invalid("EROFS bundle logical_bytes"));
    }
    if !(EROFS_BLOCK_BYTES..=MAX_EROFS_BYTES).contains(&header.erofs_bytes)
        || !header.erofs_bytes.is_multiple_of(EROFS_BLOCK_BYTES)
    {
        return Err(Error::Invalid("EROFS bundle erofs_bytes"));
    }
    let erofs_sha256 = decode_lower_hex_32(&header.erofs_sha256, "EROFS bundle erofs_sha256")?;

    if header.compression == ErofsBundleCompression::None {
        if payload_len != header.erofs_bytes {
            return Err(Error::Invalid("EROFS bundle uncompressed payload length"));
        }
        file.seek(SeekFrom::Start(payload_offset + EROFS_SUPER_OFFSET as u64))?;
        let mut super_magic = [0u8; 4];
        file.read_exact(&mut super_magic).map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                Error::Invalid("EROFS bundle superblock magic")
            } else {
                Error::Io(e)
            }
        })?;
        if super_magic != EROFS_SUPER_MAGIC_LE {
            return Err(Error::Invalid("EROFS bundle superblock magic"));
        }
    }

    Ok(Some(ParsedErofsBundle {
        header,
        manifest_digest,
        erofs_sha256,
        payload_offset,
        payload_len,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest as _, Sha256};

    fn fake_erofs_block() -> Vec<u8> {
        let mut data = vec![0u8; EROFS_BLOCK_BYTES as usize];
        data[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4].copy_from_slice(&EROFS_SUPER_MAGIC_LE);
        data
    }

    fn hex_lower(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            const LUT: &[u8; 16] = b"0123456789abcdef";
            out.push(LUT[(b >> 4) as usize] as char);
            out.push(LUT[(b & 0x0f) as usize] as char);
        }
        out
    }

    fn sample_header(
        manifest: Digest,
        erofs_sha: Digest,
        compression: ErofsBundleCompression,
    ) -> ErofsBundleHeader {
        ErofsBundleHeader {
            schema_version: 1,
            manifest_digest: format!("sha256:{}", hex_lower(&manifest)),
            platform: "linux/arm64".into(),
            logical_bytes: 123_456,
            erofs_bytes: EROFS_BLOCK_BYTES,
            erofs_sha256: hex_lower(&erofs_sha),
            compression,
            defaults: ContainerDefaults {
                entrypoint: Some(vec!["/bin/sh".into()]),
                cmd: Some(vec!["-c".into(), "true".into()]),
                env: Some(vec!["PATH=/usr/bin:/bin".into()]),
                working_dir: Some("/work".into()),
                user: Some("1000:1000".into()),
            },
        }
    }

    #[test]
    fn parses_valid_none_and_zstd_bundles() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let manifest = [0xab; 32];
        let erofs = fake_erofs_block();
        let erofs_sha: Digest = Sha256::digest(&erofs).into();

        // Compression = None
        let header_none = sample_header(manifest, erofs_sha, ErofsBundleCompression::None);
        let path_none = dir.path().join("none.erofs-bundle");
        let frame_none = encode_frame(&header_none, &erofs)?;
        std::fs::write(&path_none, &frame_none)?;
        let parsed = parse_and_validate_file(&path_none, Some(manifest))?
            .expect("should parse uncompressed bundle");
        assert_eq!(parsed.header, header_none);
        assert_eq!(parsed.manifest_digest, manifest);
        assert_eq!(parsed.erofs_sha256, erofs_sha);
        assert_eq!(parsed.payload_len, EROFS_BLOCK_BYTES);

        // Compression = Zstd (payload is opaque to header validator)
        let header_zstd = sample_header(manifest, erofs_sha, ErofsBundleCompression::Zstd);
        let path_zstd = dir.path().join("zstd.erofs-bundle");
        let fake_zstd = b"\x28\xb5\x2f\xfdcompressed-bytes";
        let frame_zstd = encode_frame(&header_zstd, fake_zstd)?;
        std::fs::write(&path_zstd, &frame_zstd)?;
        let parsed = parse_and_validate_file(&path_zstd, Some(manifest))?
            .expect("should parse zstd bundle");
        assert_eq!(parsed.header, header_zstd);
        assert_eq!(parsed.payload_len, fake_zstd.len() as u64);
        assert_eq!(
            parsed.payload_offset + parsed.payload_len,
            frame_zstd.len() as u64
        );
        Ok(())
    }

    #[test]
    fn non_magic_and_short_files_return_ok_none() -> Result<()> {
        let dir = tempfile::tempdir()?;
        for (name, bytes) in [
            ("empty", &b""[..]),
            ("short", b"COMMA_EROFS"),
            ("oci-tar", b"ordinary tar archive content that does not match magic"),
        ] {
            let path = dir.path().join(name);
            std::fs::write(&path, bytes)?;
            assert!(parse_and_validate_file(&path, None)?.is_none());
        }
        assert!(!has_magic(b"COMMA_EROFS"));
        assert!(has_magic(MAGIC));
        Ok(())
    }

    #[test]
    fn matched_magic_rejects_malformed_bundles_without_fallback() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let manifest = [0x12; 32];
        let erofs = fake_erofs_block();
        let erofs_sha: Digest = Sha256::digest(&erofs).into();

        // 1. Bad JSON
        let bad_json = dir.path().join("bad-json");
        let mut raw = MAGIC.to_vec();
        raw.extend_from_slice(&5u32.to_be_bytes());
        raw.extend_from_slice(b"{bad}");
        raw.extend_from_slice(b"payload");
        std::fs::write(&bad_json, raw)?;
        assert!(matches!(
            parse_and_validate_file(&bad_json, None),
            Err(Error::Invalid(_))
        ));

        // 2. Non-4096-aligned erofs_bytes
        let mut bad_align_hdr = sample_header(manifest, erofs_sha, ErofsBundleCompression::Zstd);
        bad_align_hdr.erofs_bytes = 4097;
        let bad_align = dir.path().join("bad-align");
        std::fs::write(&bad_align, encode_frame(&bad_align_hdr, b"x")?)?;
        assert!(matches!(
            parse_and_validate_file(&bad_align, Some(manifest)),
            Err(Error::Invalid(_))
        ));

        // 3. Manifest digest mismatch
        let ok_hdr = sample_header(manifest, erofs_sha, ErofsBundleCompression::Zstd);
        let mismatch = dir.path().join("mismatch");
        std::fs::write(&mismatch, encode_frame(&ok_hdr, b"x")?)?;
        let mut wrong = manifest;
        wrong[0] ^= 1;
        assert!(matches!(
            parse_and_validate_file(&mismatch, Some(wrong)),
            Err(Error::Invalid(_))
        ));

        // 4. Unsupported platform
        let mut bad_plat_hdr = sample_header(manifest, erofs_sha, ErofsBundleCompression::Zstd);
        bad_plat_hdr.platform = "linux/amd64".into();
        let bad_plat = dir.path().join("bad-plat");
        std::fs::write(&bad_plat, encode_frame(&bad_plat_hdr, b"x")?)?;
        assert!(matches!(
            parse_and_validate_file(&bad_plat, Some(manifest)),
            Err(Error::Invalid(_))
        ));

        // 5. Uppercase erofs_sha256 rejected
        let mut upper_sha_hdr = sample_header(manifest, erofs_sha, ErofsBundleCompression::Zstd);
        upper_sha_hdr.erofs_sha256 = upper_sha_hdr.erofs_sha256.to_uppercase();
        let upper_sha = dir.path().join("upper-sha");
        std::fs::write(&upper_sha, encode_frame(&upper_sha_hdr, b"x")?)?;
        assert!(matches!(
            parse_and_validate_file(&upper_sha, Some(manifest)),
            Err(Error::Invalid(_))
        ));

        // 6. Uncompressed bundle with invalid EROFS superblock magic
        let mut bad_erofs = vec![0u8; EROFS_BLOCK_BYTES as usize];
        bad_erofs[EROFS_SUPER_OFFSET..EROFS_SUPER_OFFSET + 4].copy_from_slice(b"NOPE");
        let bad_magic_hdr = sample_header(
            manifest,
            Sha256::digest(&bad_erofs).into(),
            ErofsBundleCompression::None,
        );
        let bad_magic = dir.path().join("bad-magic");
        std::fs::write(&bad_magic, encode_frame(&bad_magic_hdr, &bad_erofs)?)?;
        assert!(matches!(
            parse_and_validate_file(&bad_magic, Some(manifest)),
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}
