//! Downloads the selected Host artifact and unpacks it next to the installed Host.
//! The lifecycle owner replaces the installation; this module changes nothing that is in use.

use crate::envelope::bounded;
use crate::facts::{self, Facts};
use crate::options::Options;
use crate::runtime::PreparationError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_ARTIFACT_BYTES: u64 = 4 << 30;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const PART_PREFIX: &str = ".agent-vmm-update-";
const PART_SUFFIX: &str = ".archive.part";
const META_SUFFIX: &str = ".archive.part.json";

/// The name of the installed Host below its parent directory.
#[cfg(target_os = "macos")]
const SERVICE_APP_NAME: &str = vmm_lifecycle::HOST_APP_NAME;
#[cfg(target_os = "linux")]
const SERVICE_APP_NAME: &str = "current";

/// The hidden sibling of the installed Host that holds the prepared release of one request.
fn prepared_path(target: &Path, request_id: &str) -> PathBuf {
    let name = if cfg!(target_os = "macos") { format!(".Agent VMM Host.{request_id}.app") } else { format!(".{request_id}") };
    target.with_file_name(name)
}

fn preparation(error: impl ToString) -> PreparationError {
    PreparationError::at("preparation", error.to_string())
}

fn download(error: impl ToString) -> PreparationError {
    PreparationError::at("download", error.to_string())
}

/// Accepts an exact HTTPS URL without credentials, query or fragment.
fn artifact_url(text: &str) -> Option<url::Url> {
    let url = url::Url::parse(text).ok()?;
    let plain = url.username().is_empty() && url.password().is_none() && url.query().is_none_or(str::is_empty) && url.fragment().is_none_or(str::is_empty);
    (url.scheme() == "https" && url.host_str().is_some_and(|host| !host.is_empty()) && plain).then_some(url)
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PartMetadata {
    url: String,
    artifact_sha256: String,
    artifact_size: u64,
    validator: Option<String>,
}

impl PartMetadata {
    fn matches(&self, url: &url::Url, options: &Options) -> bool {
        self.url == url.as_str() && self.artifact_sha256.eq_ignore_ascii_case(&options.artifact_sha256) && self.artifact_size == options.artifact_size
    }
}

fn part_path(parent: &Path, request_id: &str) -> PathBuf {
    parent.join(format!("{PART_PREFIX}{request_id}{PART_SUFFIX}"))
}

fn meta_path(parent: &Path, request_id: &str) -> PathBuf {
    parent.join(format!("{PART_PREFIX}{request_id}{META_SUFFIX}"))
}

fn remove_part_files(part: &Path, meta: &Path) {
    let _ = std::fs::remove_file(part);
    let _ = std::fs::remove_file(meta);
}

fn cleanup_stale_parts(parent: &Path, request_id: &str) {
    let keep_part = format!("{PART_PREFIX}{request_id}{PART_SUFFIX}");
    let keep_meta = format!("{PART_PREFIX}{request_id}{META_SUFFIX}");
    let Ok(entries) = std::fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let is_part = name.starts_with(PART_PREFIX) && (name.ends_with(PART_SUFFIX) || name.ends_with(META_SUFFIX));
        if is_part && name != keep_part && name != keep_meta {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

fn read_part_metadata(path: &Path) -> Option<PartMetadata> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_part_metadata(path: &Path, metadata: &PartMetadata) -> Result<(), PreparationError> {
    let bytes = serde_json::to_vec(metadata).map_err(download)?;
    std::fs::write(path, bytes).map_err(download)
}

fn response_validator(headers: &ureq::http::HeaderMap) -> Option<String> {
    headers
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|etag| !etag.is_empty() && !etag.starts_with("W/") && !etag.starts_with("w/"))
        .or_else(|| headers.get("last-modified").and_then(|value| value.to_str().ok()).map(str::trim).filter(|value| !value.is_empty()))
        .map(str::to_string)
}

fn parse_content_range(value: &str) -> Option<(u64, u64, Option<u64>)> {
    let rest = value.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    if end < start {
        return None;
    }
    let total = if total == "*" { None } else { Some(total.parse::<u64>().ok()?) };
    Some((start, end, total))
}

fn verify_part_sha256(path: &Path, expected_size: u64, expected_sha256: &str) -> Result<(), PreparationError> {
    let mut file = std::fs::File::open(path).map_err(download)?;
    let (mut hash, mut total, mut chunk) = (Sha256::new(), 0u64, vec![0u8; 1 << 16]);
    loop {
        let count = file.read(&mut chunk).map_err(download)?;
        if count == 0 {
            break;
        }
        hash.update(&chunk[..count]);
        total += count as u64;
    }
    let digest: String = hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    if total != expected_size || !digest.eq_ignore_ascii_case(expected_sha256) {
        return Err(download("Host artifact bytes do not match the selected size and SHA-256"));
    }
    Ok(())
}

pub fn prepare(options: &Options, status: &Facts) -> Result<String, PreparationError> {
    if options.artifact_size == 0 || options.artifact_size > MAX_ARTIFACT_BYTES {
        return Err(PreparationError::at("target_selection", "Host artifact size is outside the managed update bound"));
    }
    let url = artifact_url(&options.artifact_url).ok_or_else(|| PreparationError::at("target_selection", "Host artifact URL must be exact HTTPS without credentials, query, or fragment"))?;
    let target: PathBuf = Path::new(facts::string(status, "serviceApp")).components().collect();
    let Some(parent) = target.parent().filter(|_| target.is_absolute() && target.file_name().is_some_and(|name| name == SERVICE_APP_NAME)) else {
        return Err(PreparationError::at("target_selection", "installed Host target is invalid"));
    };
    let staged = prepared_path(&target, &options.request_id);
    let staged_text = staged.display().to_string();
    let part = part_path(parent, &options.request_id);
    let meta = meta_path(parent, &options.request_id);
    let current = vmm_lifecycle::release_identity(&target).is_ok_and(|current| current == options.target_release);
    match std::fs::symlink_metadata(&staged) {
        Ok(_) => {
            remove_part_files(&part, &meta);
            return Ok(staged_text);
        }
        // The installed Host is the target: there is nothing to prepare.
        Err(_) if current => {
            remove_part_files(&part, &meta);
            return Ok(String::new());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(preparation("prepared update path is not readable")),
    }
    let tls = ureq::tls::TlsConfig::builder().root_certs(ureq::tls::RootCerts::PlatformVerifier).build();
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(DOWNLOAD_TIMEOUT))
        .timeout_recv_response(Some(IDLE_TIMEOUT))
        .timeout_per_read(Some(IDLE_TIMEOUT))
        .http_status_as_error(false)
        .proxy(ureq::Proxy::try_from_env())
        .tls_config(tls)
        .build()
        .new_agent();
    fetch_with_agent(&url, options, parent, &agent)?;
    extract(&part, &staged, &options.target_release)?;
    remove_part_files(&part, &meta);
    Ok(staged_text)
}

/// Writes the artifact to a deterministic `.part` file in `parent`, resuming with HTTP Range when possible, and verifies its size and SHA-256.
fn fetch_with_agent(url: &url::Url, options: &Options, parent: &Path, agent: &ureq::Agent) -> Result<PathBuf, PreparationError> {
    cleanup_stale_parts(parent, &options.request_id);
    let part = part_path(parent, &options.request_id);
    let meta = meta_path(parent, &options.request_id);
    let (offset, saved_validator) = match (std::fs::metadata(&part), read_part_metadata(&meta)) {
        (Ok(file_meta), Some(saved)) if file_meta.is_file() && saved.matches(url, options) && file_meta.len() > 0 => (file_meta.len(), saved.validator),
        _ => {
            remove_part_files(&part, &meta);
            (0, None)
        }
    };
    let mut request = agent.get(url.as_str()).header("User-Agent", "agent-vmm-managed-updater/1");
    if offset > 0 {
        request = request.header("Range", format!("bytes={offset}-"));
        if let Some(validator) = saved_validator.as_deref() {
            request = request.header("If-Range", validator);
        }
    }
    let mut response = request.call().map_err(|error| {
        let cancelled = matches!(error, ureq::Error::Timeout(_));
        PreparationError { cancelled, ..PreparationError::download_unavailable(error.to_string()) }
    })?;
    let status = response.status().as_u16();
    let (mut file, start_offset) = match status {
        206 => {
            let valid_range = offset > 0
                && offset < options.artifact_size
                && response
                    .headers()
                    .get("content-range")
                    .and_then(|value| value.to_str().ok())
                    .and_then(parse_content_range)
                    .is_some_and(|(start, end, total)| start == offset && total.is_none_or(|size| size == options.artifact_size) && end < options.artifact_size);
            if !valid_range {
                remove_part_files(&part, &meta);
                return Err(PreparationError::download_unavailable("Host artifact Content-Range does not match the resumed offset"));
            }
            let metadata = PartMetadata {
                url: url.as_str().to_string(),
                artifact_sha256: options.artifact_sha256.to_lowercase(),
                artifact_size: options.artifact_size,
                validator: response_validator(response.headers()).or(saved_validator),
            };
            write_part_metadata(&meta, &metadata)?;
            let mut file = std::fs::OpenOptions::new().write(true).open(&part).map_err(download)?;
            file.seek(SeekFrom::Start(offset)).map_err(download)?;
            (file, offset)
        }
        200 => {
            let declared = response.headers().get("content-length").and_then(|value| value.to_str().ok()).and_then(|value| value.parse::<u64>().ok());
            if declared.is_some_and(|length| length != options.artifact_size) {
                remove_part_files(&part, &meta);
                return Err(download("Host artifact Content-Length does not match the selected target"));
            }
            let file = std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(&part).map_err(download)?;
            let metadata = PartMetadata {
                url: url.as_str().to_string(),
                artifact_sha256: options.artifact_sha256.to_lowercase(),
                artifact_size: options.artifact_size,
                validator: response_validator(response.headers()),
            };
            write_part_metadata(&meta, &metadata)?;
            (file, 0)
        }
        416 => {
            if offset == options.artifact_size {
                if let Err(error) = verify_part_sha256(&part, options.artifact_size, &options.artifact_sha256) {
                    remove_part_files(&part, &meta);
                    return Err(error);
                }
                return Ok(part);
            }
            remove_part_files(&part, &meta);
            return Err(PreparationError::download_unavailable("Host artifact download returned HTTP 416"));
        }
        500..=599 => {
            return Err(PreparationError::download_unavailable(format!("Host artifact download returned HTTP {status}")));
        }
        _ => {
            return Err(download(format!("Host artifact download returned HTTP {status}")));
        }
    };
    let remaining = options.artifact_size - start_offset;
    let incomplete = |error: std::io::Error| PreparationError {
        cancelled: error.kind() == std::io::ErrorKind::TimedOut,
        ..PreparationError::download_unavailable("Host artifact download did not complete")
    };
    let mut body = response.body_mut().with_config().limit(remaining + 2).reader().take(remaining + 1);
    let (mut received, mut chunk) = (0u64, vec![0u8; 1 << 16]);
    loop {
        let count = match body.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) => {
                let _ = file.flush();
                return Err(incomplete(error));
            }
        };
        file.write_all(&chunk[..count]).map_err(|error| {
            let _ = file.flush();
            incomplete(error)
        })?;
        received += count as u64;
    }
    file.flush().map_err(incomplete)?;
    drop(file);
    let total = start_offset + received;
    if total < options.artifact_size {
        return Err(PreparationError::download_unavailable("Host artifact download did not complete"));
    }
    if total > options.artifact_size {
        remove_part_files(&part, &meta);
        return Err(download("Host artifact bytes do not match the selected size and SHA-256"));
    }
    if let Err(error) = verify_part_sha256(&part, options.artifact_size, &options.artifact_sha256) {
        remove_part_files(&part, &meta);
        return Err(error);
    }
    Ok(part)
}

#[cfg(target_os = "macos")]
fn extract(archive: &Path, staged: &Path, target_release: &str) -> Result<(), PreparationError> {
    let extracted = tempfile::Builder::new().prefix(".agent-vmm-update-extract-").tempdir_in(staged.parent().unwrap_or(Path::new("/"))).map_err(preparation)?;
    let output = std::process::Command::new("/usr/bin/ditto").arg("-x").arg("-k").arg(archive).arg(extracted.path()).output().map_err(preparation)?;
    if !output.status.success() {
        let text = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        return Err(preparation(format!("Host artifact extraction failed: {}", bounded(&text, 256))));
    }
    let prepared = extracted.path().join(vmm_lifecycle::HOST_APP_NAME);
    if !vmm_lifecycle::release_identity(&prepared).is_ok_and(|release| release == target_release) {
        return Err(preparation("prepared Host app does not match the selected target"));
    }
    std::fs::rename(&prepared, staged).map_err(preparation)
}

#[cfg(target_os = "linux")]
fn extract(archive: &Path, staged: &Path, target_release: &str) -> Result<(), PreparationError> {
    let extracted = tempfile::Builder::new().prefix(".agent-vmm-update-extract-").tempdir_in(staged.parent().unwrap_or(Path::new("/"))).map_err(preparation)?;
    extract_release_archive(archive, extracted.path()).map_err(|error| preparation(format!("Host artifact extraction failed: {}", bounded(&error, 256))))?;
    if !vmm_lifecycle::release_identity(extracted.path()).is_ok_and(|release| release == target_release) {
        return Err(preparation("Host artifact release inventory does not match the selected target"));
    }
    std::fs::rename(extracted.path(), staged).map_err(preparation)
}

/// Shortens a slash-separated path as Go `path.Clean` does.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn clean(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." if parts.last().is_some_and(|last| *last != "..") => {
                parts.pop();
            }
            ".." if rooted => {}
            _ => parts.push(part),
        }
    }
    let joined = parts.join("/");
    if rooted {
        format!("/{joined}")
    } else if joined.is_empty() {
        ".".into()
    } else {
        joined
    }
}

/// Unpacks a gzip tar archive whose entries are below one top-level directory.
/// Only directories and regular files are accepted; a file is never replaced.
#[cfg(target_os = "linux")]
fn extract_release_archive(archive: &Path, target: &Path) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let text = |error: std::io::Error| error.to_string();
    let directory = |path: &Path| std::fs::DirBuilder::new().recursive(true).mode(0o755).create(path).map_err(text);
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(std::fs::File::open(archive).map_err(text)?));
    let mut top: Option<String> = None;
    for entry in archive.entries().map_err(text)? {
        let mut entry = entry.map_err(text)?;
        let raw = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        let name = clean(&raw);
        let (first, relative) = name.split_once('/').unwrap_or((&name, ""));
        let top = top.get_or_insert_with(|| first.to_string());
        if first != top.as_str() || name.starts_with('/') || first == ".." || relative.starts_with("..") {
            return Err(format!("archive entry {raw:?} is outside the release directory"));
        }
        let destination = target.join(relative);
        match entry.header().entry_type() {
            tar::EntryType::Directory => directory(&destination)?,
            tar::EntryType::Regular if relative.is_empty() => return Err("archive top-level entry must be a directory".into()),
            tar::EntryType::Regular => {
                directory(destination.parent().unwrap_or(target))?;
                let mut output = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o644).open(&destination).map_err(text)?;
                std::io::copy(&mut entry, &mut output).map_err(text)?;
            }
            _ => return Err(format!("archive entry {raw:?} is not a regular file or directory")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn options(url: &str, size: u64) -> Options {
        Options { request_id: "update-1".into(), target_release: "agent-vmm-2".into(), artifact_url: url.into(), artifact_sha256: "a".repeat(64), artifact_size: size, ..Options::default() }
    }

    fn status(app: &Path) -> Facts {
        json!({"serviceApp": app.display().to_string()}).as_object().unwrap().clone()
    }

    #[test]
    fn the_target_is_checked_before_anything_is_downloaded() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().canonicalize().unwrap().join(SERVICE_APP_NAME);
        let refused = |options: &Options, status: &Facts| prepare(options, status).unwrap_err();
        assert_eq!(refused(&options("https://releases.example/host.zip", 0), &status(&app)).code, "update_target_selection_failed");
        assert_eq!(refused(&options("https://releases.example/host.zip", MAX_ARTIFACT_BYTES + 1), &status(&app)).stage, "target_selection");
        for url in ["http://releases.example/host.zip", "https://user@releases.example/host.zip", "https://releases.example/host.zip?token=1", "https://releases.example/host.zip#part", "host.zip"] {
            assert_eq!(refused(&options(url, 42), &status(&app)).stage, "target_selection", "{url}");
        }
        assert_eq!(refused(&options("https://releases.example/host.zip", 42), &status(&directory.path().join("Other.app"))).message, "installed Host target is invalid");
        assert_eq!(refused(&options("https://releases.example/host.zip", 42), &status(Path::new("relative").join(SERVICE_APP_NAME).as_path())).stage, "target_selection");
    }

    #[test]
    fn a_release_that_one_request_prepared_is_used_again() {
        let directory = tempfile::tempdir().unwrap();
        let app = directory.path().canonicalize().unwrap().join(SERVICE_APP_NAME);
        let parent = app.parent().unwrap();
        let staged = prepared_path(&app, "update-1");
        assert_eq!(staged.parent(), app.parent());
        assert!(staged.file_name().unwrap().to_string_lossy().starts_with('.'), "the prepared release is hidden");
        std::fs::create_dir(&staged).unwrap();
        let part = part_path(parent, "update-1");
        let meta = meta_path(parent, "update-1");
        std::fs::write(&part, b"partial").unwrap();
        std::fs::write(&meta, b"{}").unwrap();
        // The unroutable URL shows that nothing is downloaded.
        assert_eq!(prepare(&options("https://releases.invalid/host.zip", 42), &status(&app)), Ok(staged.display().to_string()));
        assert!(!part.exists() && !meta.exists(), "stale part files are removed when staged already exists");
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn test_agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .timeout_recv_response(Some(Duration::from_secs(5)))
            .timeout_per_read(Some(Duration::from_secs(5)))
            .http_status_as_error(false)
            .proxy(None)
            .build()
            .new_agent()
    }

    fn spawn_http_server<F>(responses: usize, handler: F) -> (url::Url, std::thread::JoinHandle<Vec<String>>)
    where
        F: Fn(usize, &str, &mut std::net::TcpStream) + Send + 'static,
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for index in 0..responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                let mut buf = Vec::new();
                let mut byte = [0u8; 1];
                while !buf.ends_with(b"\r\n\r\n") {
                    if stream.read(&mut byte).unwrap_or(0) == 0 {
                        break;
                    }
                    buf.push(byte[0]);
                }
                let req = String::from_utf8_lossy(&buf).into_owned();
                handler(index, &req, &mut stream);
                requests.push(req);
            }
            requests
        });
        (url::Url::parse(&format!("http://{addr}/host.zip")).unwrap(), handle)
    }

    #[test]
    fn interrupted_download_resumes_with_range_and_if_range() {
        let payload = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".to_vec();
        let split = 20usize;
        let payload_copy = payload.clone();
        let (url, server) = spawn_http_server(2, move |index, _req, stream| {
            if index == 0 {
                let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n", payload_copy.len());
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(&payload_copy[..split]).unwrap();
                let _ = stream.shutdown(std::net::Shutdown::Both);
            } else {
                let rest = &payload_copy[split..];
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nETag: \"v1\"\r\nConnection: close\r\n\r\n",
                    split,
                    payload_copy.len() - 1,
                    payload_copy.len(),
                    rest.len()
                );
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(rest).unwrap();
            }
        });
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options(url.as_str(), payload.len() as u64);
        opts.artifact_sha256 = sha256_hex(&payload);
        let agent = test_agent();

        let err = fetch_with_agent(&url, &opts, dir.path(), &agent).unwrap_err();
        assert!(err.retryable && err.code == "update_download_unavailable");
        let part = part_path(dir.path(), &opts.request_id);
        let meta = meta_path(dir.path(), &opts.request_id);
        assert_eq!(std::fs::read(&part).unwrap(), &payload[..split]);
        assert_eq!(read_part_metadata(&meta).unwrap().validator.as_deref(), Some("\"v1\""));

        let fetched = fetch_with_agent(&url, &opts, dir.path(), &agent).unwrap();
        assert_eq!(fetched, part);
        assert_eq!(std::fs::read(&fetched).unwrap(), payload);
        remove_part_files(&part, &meta);
        assert!(!part.exists() && !meta.exists());

        let requests = server.join().unwrap();
        assert!(!requests[0].to_ascii_lowercase().contains("range:"));
        assert!(requests[1].to_ascii_lowercase().contains(&format!("range: bytes={split}-")));
        assert!(requests[1].to_ascii_lowercase().contains("if-range: \"v1\""));
    }

    #[test]
    fn server_returning_200_to_range_rewrites_from_zero() {
        let payload = b"fresh-full-artifact-payload".to_vec();
        let payload_copy = payload.clone();
        let (url, server) = spawn_http_server(1, move |_index, _req, stream| {
            let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"v2\"\r\nConnection: close\r\n\r\n", payload_copy.len());
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(&payload_copy).unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options(url.as_str(), payload.len() as u64);
        opts.artifact_sha256 = sha256_hex(&payload);
        let part = part_path(dir.path(), &opts.request_id);
        let meta = meta_path(dir.path(), &opts.request_id);
        std::fs::write(&part, b"stale-prefix").unwrap();
        write_part_metadata(&meta, &PartMetadata { url: url.as_str().into(), artifact_sha256: opts.artifact_sha256.clone(), artifact_size: opts.artifact_size, validator: Some("\"v1\"".into()) }).unwrap();

        fetch_with_agent(&url, &opts, dir.path(), &test_agent()).unwrap();
        assert_eq!(std::fs::read(&part).unwrap(), payload);
        assert_eq!(read_part_metadata(&meta).unwrap().validator.as_deref(), Some("\"v2\""));
        let requests = server.join().unwrap();
        assert!(requests[0].to_ascii_lowercase().contains("range: bytes=12-"));
    }

    #[test]
    fn http_416_verifies_complete_part_and_cleans_oversized_part() {
        let payload = b"already-complete-artifact".to_vec();
        let (url, server) = spawn_http_server(2, |_index, _req, stream| {
            stream.write_all(b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let mut opts = options(url.as_str(), payload.len() as u64);
        opts.artifact_sha256 = sha256_hex(&payload);
        let part = part_path(dir.path(), &opts.request_id);
        let meta = meta_path(dir.path(), &opts.request_id);
        let metadata = PartMetadata { url: url.as_str().into(), artifact_sha256: opts.artifact_sha256.clone(), artifact_size: opts.artifact_size, validator: None };

        // Complete .part passes verification on 416.
        std::fs::write(&part, &payload).unwrap();
        write_part_metadata(&meta, &metadata).unwrap();
        assert_eq!(fetch_with_agent(&url, &opts, dir.path(), &test_agent()).unwrap(), part);

        // Oversized .part is removed and returns retryable download_unavailable on 416.
        let mut oversized = payload.clone();
        oversized.extend_from_slice(b"extra");
        std::fs::write(&part, &oversized).unwrap();
        write_part_metadata(&meta, &metadata).unwrap();
        let err = fetch_with_agent(&url, &opts, dir.path(), &test_agent()).unwrap_err();
        assert!(err.retryable && err.code == "update_download_unavailable");
        assert!(!part.exists() && !meta.exists());
        server.join().unwrap();
    }

    #[test]
    fn sha256_mismatch_deletes_part_and_sidecar() {
        let payload = b"corrupted-bytes".to_vec();
        let payload_copy = payload.clone();
        let (url, server) = spawn_http_server(1, move |_index, _req, stream| {
            let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload_copy.len());
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(&payload_copy).unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let opts = options(url.as_str(), payload.len() as u64);
        let part = part_path(dir.path(), &opts.request_id);
        let meta = meta_path(dir.path(), &opts.request_id);

        let err = fetch_with_agent(&url, &opts, dir.path(), &test_agent()).unwrap_err();
        assert!(!err.retryable && err.code == "update_download_failed");
        assert!(!part.exists() && !meta.exists());
        server.join().unwrap();
    }

    #[test]
    fn stale_part_files_of_other_requests_are_cleaned_before_download() {
        let payload = b"ok-payload".to_vec();
        let payload_copy = payload.clone();
        let (url, server) = spawn_http_server(1, move |_index, _req, stream| {
            let header = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload_copy.len());
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(&payload_copy).unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let old_part = part_path(dir.path(), "old-request");
        let old_meta = meta_path(dir.path(), "old-request");
        let unrelated = dir.path().join("keep-me.txt");
        std::fs::write(&old_part, b"old").unwrap();
        std::fs::write(&old_meta, b"{}").unwrap();
        std::fs::write(&unrelated, b"keep").unwrap();

        let mut opts = options(url.as_str(), payload.len() as u64);
        opts.artifact_sha256 = sha256_hex(&payload);
        fetch_with_agent(&url, &opts, dir.path(), &test_agent()).unwrap();

        assert!(!old_part.exists() && !old_meta.exists());
        assert!(unrelated.exists());
        server.join().unwrap();
    }

    #[test]
    fn paths_are_shortened_as_the_archive_writer_names_them() {
        for (path, cleaned) in [("release/bin/agent-vmm", "release/bin/agent-vmm"), ("release//bin/./agent-vmm", "release/bin/agent-vmm"), ("release/../../etc", "../etc"), ("/etc/passwd", "/etc/passwd"), ("/../etc", "/etc"), ("./", "."), ("", ".")] {
            assert_eq!(clean(path), cleaned, "{path}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_archive_is_unpacked_below_one_directory_only() {
        fn archive(entries: &[(&str, tar::EntryType, &[u8])]) -> tempfile::NamedTempFile {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(file.reopen().unwrap(), flate2::Compression::fast()));
            for (path, kind, data) in entries {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(*kind);
                header.set_size(data.len() as u64);
                header.set_mode(0o755);
                // The name is written without the path checks of the builder.
                header.as_old_mut().name[..path.len()].copy_from_slice(path.as_bytes());
                header.set_cksum();
                builder.append(&header, *data).unwrap();
            }
            builder.into_inner().unwrap().finish().unwrap();
            file
        }
        use tar::EntryType::{Directory, Regular, Symlink};
        let target = tempfile::tempdir().unwrap();
        let good = archive(&[("agent-vmm-2/", Directory, b"".as_slice()), ("agent-vmm-2/release.json", Regular, b"{}".as_slice()), ("agent-vmm-2/bin/agent-vmm", Regular, b"binary".as_slice())]);
        extract_release_archive(good.path(), target.path()).unwrap();
        assert_eq!(std::fs::read(target.path().join("bin/agent-vmm")).unwrap(), b"binary");
        assert_eq!(std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(target.path().join("bin/agent-vmm")).unwrap().permissions()) & 0o777, 0o644);
        assert!(extract_release_archive(good.path(), target.path()).is_err(), "a file is never replaced");

        for entries in [
            vec![("agent-vmm-2/a", Regular, b"1".as_slice()), ("other/b", Regular, b"2".as_slice())],
            vec![("agent-vmm-2/../../escape", Regular, b"1".as_slice())],
            vec![("/absolute/a", Regular, b"1".as_slice())],
            vec![("agent-vmm-2/link", Symlink, b"".as_slice())],
            vec![("agent-vmm-2", Regular, b"1".as_slice())],
        ] {
            let target = tempfile::tempdir().unwrap();
            assert!(extract_release_archive(archive(&entries).path(), target.path()).is_err(), "{:?}", entries[0].0);
            assert!(!target.path().parent().unwrap().join("escape").exists());
        }
    }
}
