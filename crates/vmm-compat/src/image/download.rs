use crate::{
    error::{check_import_headroom, import_headroom, invalid_field, is_enospc},
    mutation::ReentrantFailure,
};
use sha2::{Digest as _, Sha256};
use std::{
    fs::File,
    io,
    net::Ipv4Addr,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{net::lookup_host, process::Command};
use url::Url;
use vmm_rpc::Status;

const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(720);

pub(super) fn public(ip: Ipv4Addr) -> bool {
    !matches!(
        ip.octets(),
        [0 | 10 | 127 | 224..=255, ..]
            | [100, 64..=127, ..]
            | [169, 254, ..]
            | [172, 16..=31, ..]
            | [192, 0, 0 | 2, _]
            | [192, 31, 196, _]
            | [192, 52, 193, _]
            | [192, 88, 99, _]
            | [192, 168, ..]
            | [192, 175, 48, _]
            | [198, 51, 100, _]
            | [203, 0, 113, _]
    )
}

pub(super) fn validate_public_url(raw: &str) -> Result<Url, Status> {
    let url = Url::parse(raw).map_err(|_| invalid_field("archive_url", "invalid archive URL"))?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.cannot_be_a_base()
    {
        return Err(invalid_field(
            "archive_url",
            "public HTTPS archive URL required",
        ));
    }
    Ok(url)
}

pub(super) fn part_path(staging: &Path, archive_sha256: [u8; 32]) -> PathBuf {
    staging
        .join("parts")
        .join(format!("{}.part", hex::encode(archive_sha256)))
}

async fn hash_file(path: PathBuf) -> Result<(u64, [u8; 32]), io::Error> {
    tokio::task::spawn_blocking(move || {
        let mut file = File::open(&path)?;
        let mut sha = Sha256::new();
        let len = io::copy(&mut file, &mut sha)?;
        Ok((len, sha.finalize().into()))
    })
    .await
    .map_err(|_| io::Error::other("hash task interrupted"))?
}

/// Download a public HTTPS OCI archive into `staging/parts/<archive_sha256>.part`
/// with HTTP Range resume (`curl -C`) and stall detection (`--speed-limit 1024 --speed-time 60`).
/// If the `.part` file is already complete and matches `(expected_size, expected_sha256)`,
/// returns immediately with zero network traffic.
pub(super) async fn download_resumable(
    staging: &Path,
    raw: &str,
    expected_size: u64,
    expected_sha256: [u8; 32],
    min_free_bytes: u64,
) -> Result<PathBuf, ReentrantFailure> {
    let url = validate_public_url(raw).map_err(ReentrantFailure::terminal)?;
    let parts_dir = staging.join("parts");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&parts_dir)
        .map_err(|e| map_fs_error(staging, expected_size, e))?;
    let dest = part_path(staging, expected_sha256);

    // Fast path: already downloaded and verified in a prior attempt.
    if let Ok(meta) = tokio::fs::metadata(&dest).await
        && meta.is_file()
        && meta.len() == expected_size
    {
        if let Ok((len, sha)) = hash_file(dest.clone()).await
            && len == expected_size
            && sha == expected_sha256
        {
            return Ok(dest);
        }
        let _ = tokio::fs::remove_file(&dest).await;
    }

    let host = url
        .host_str()
        .ok_or_else(|| ReentrantFailure::terminal(invalid_field("archive_url", "missing archive host")))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| ReentrantFailure::terminal(invalid_field("archive_url", "invalid HTTPS port")))?;
    let mut selected = None;
    for address in tokio::time::timeout(Duration::from_secs(30), lookup_host((host, port)))
        .await
        .map_err(|_| {
            ReentrantFailure::retryable(Status::deadline_exceeded(
                "archive name resolution timed out",
            ))
        })?
        .map_err(|_| {
            ReentrantFailure::retryable(Status::unavailable("archive name resolution failed"))
        })?
    {
        if let std::net::IpAddr::V4(ip) = address.ip() {
            if !public(ip) {
                return Err(ReentrantFailure::terminal(Status::invalid_argument(
                    "archive host resolves to a non-public address",
                )));
            }
            selected.get_or_insert(ip);
        }
    }
    let ip = selected.ok_or_else(|| {
        ReentrantFailure::terminal(Status::invalid_argument(
            "archive host needs a public IPv4 address",
        ))
    })?;

    for attempt in 0..2 {
        let mut existing_len = match tokio::fs::metadata(&dest).await {
            Ok(meta) if meta.is_file() && meta.len() < expected_size => meta.len(),
            Ok(_) => {
                let _ = tokio::fs::remove_file(&dest).await;
                0
            }
            Err(_) => 0,
        };
        if attempt == 1 {
            let _ = tokio::fs::remove_file(&dest).await;
            existing_len = 0;
        }
        check_import_headroom(
            staging,
            expected_size.saturating_sub(existing_len),
            min_free_bytes,
        )
        .map_err(ReentrantFailure::retryable)?;

        let mut cmd = Command::new("/usr/bin/curl");
        cmd.kill_on_drop(true)
            .env_clear()
            .args([
                "-q",
                "--globoff",
                "--silent",
                "--fail",
                "--proto",
                "=https",
                "--tlsv1.2",
                "--max-redirs",
                "0",
                "--proxy",
                "",
                "--noproxy",
                "*",
                "--ipv4",
                "--resolve",
                &format!("{host}:{port}:{ip}"),
                "--max-time",
                "720",
                "--speed-limit",
                "1024",
                "--speed-time",
                "60",
                "--max-filesize",
                &expected_size.to_string(),
                "--write-out",
                "%{http_code}",
                "--output",
            ])
            .arg(&dest);
        if existing_len > 0 {
            cmd.args(["-C", &existing_len.to_string()]);
        }
        cmd.args(["--url", url.as_str()])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());

        let output = tokio::time::timeout(DOWNLOAD_TIMEOUT, cmd.output())
            .await
            .map_err(|_| {
                ReentrantFailure::retryable(Status::deadline_exceeded("archive download timed out"))
            })?
            .map_err(|_| {
                ReentrantFailure::retryable(Status::unavailable("archive downloader unavailable"))
            })?;

        if !output.status.success() {
            let code = output.status.code();
            // Exit 33 = HTTP range error (server does not support Range); retry from 0 once.
            if existing_len > 0 && code == Some(33) && attempt == 0 {
                continue;
            }
            // Exit 23 = write error: check whether the filesystem ran out of headroom.
            if code == Some(23) {
                check_import_headroom(
                    staging,
                    expected_size.saturating_sub(existing_len),
                    min_free_bytes,
                )
                .map_err(ReentrantFailure::retryable)?;
            }
            // Exit 28 = timeout or speed-limit stall.
            if code == Some(28) {
                return Err(ReentrantFailure::retryable(Status::deadline_exceeded(
                    "archive download timed out",
                )));
            }
            return Err(ReentrantFailure::retryable(Status::unavailable(
                "archive download failed",
            )));
        }

        let http_code = output.stdout.as_slice();
        let expected_http = if existing_len > 0 {
            &b"206"[..]
        } else {
            &b"200"[..]
        };
        if http_code != expected_http {
            if existing_len > 0 && attempt == 0 {
                continue;
            }
            return Err(ReentrantFailure::retryable(Status::unavailable(
                "archive HTTP status mismatch",
            )));
        }

        let (actual_len, actual_sha) = hash_file(dest.clone())
            .await
            .map_err(|e| map_fs_error(staging, expected_size, e))?;
        if actual_len < expected_size {
            // Truncated transfer: keep `.part` and retry.
            return Err(ReentrantFailure::retryable(Status::unavailable(
                "archive download incomplete",
            )));
        }
        if actual_len != expected_size || actual_sha != expected_sha256 {
            let _ = tokio::fs::remove_file(&dest).await;
            if existing_len > 0 && attempt == 0 {
                // Resumed `.part` was corrupt or stale; retry cleanly from byte 0 once.
                continue;
            }
            return Err(ReentrantFailure::terminal(invalid_field(
                "archive",
                "OCI archive size or sha256 does not match the header",
            )));
        }
        return Ok(dest);
    }

    Err(ReentrantFailure::retryable(Status::unavailable(
        "archive download failed",
    )))
}

fn map_fs_error(staging: &Path, required: u64, err: io::Error) -> ReentrantFailure {
    if is_enospc(&err) {
        ReentrantFailure::retryable(import_headroom(
            crate::error::available_bytes(staging),
            required,
        ))
    } else {
        ReentrantFailure::retryable(Status::unavailable("image staging write failed"))
    }
}
