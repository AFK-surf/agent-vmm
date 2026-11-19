//! Downloads the selected Host artifact and unpacks it next to the installed Host.
//! The lifecycle owner replaces the installation; this module changes nothing that is in use.

use crate::envelope::bounded;
use crate::facts::{self, Facts};
use crate::options::Options;
use crate::runtime::PreparationError;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_ARTIFACT_BYTES: u64 = 4 << 30;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);

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
    let current = vmm_lifecycle::release_identity(&target).is_ok_and(|current| current == options.target_release);
    match std::fs::symlink_metadata(&staged) {
        Ok(_) => return Ok(staged_text),
        // The installed Host is the target: there is nothing to prepare.
        Err(_) if current => return Ok(String::new()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err(preparation("prepared update path is not readable")),
    }
    let mut archive = tempfile::Builder::new().prefix(".agent-vmm-update-").suffix(".archive").tempfile_in(parent).map_err(download)?;
    fetch(&url, options, archive.as_file_mut())?;
    extract(archive.path(), &staged, &options.target_release)?;
    Ok(staged_text)
}

/// Writes the artifact to `archive` and checks its size and digest.
fn fetch(url: &url::Url, options: &Options, archive: &mut std::fs::File) -> Result<(), PreparationError> {
    let tls = ureq::tls::TlsConfig::builder().root_certs(ureq::tls::RootCerts::PlatformVerifier).build();
    let agent = ureq::Agent::config_builder().timeout_global(Some(DOWNLOAD_TIMEOUT)).http_status_as_error(false).proxy(ureq::Proxy::try_from_env()).tls_config(tls).build().new_agent();
    let mut response = agent.get(url.as_str()).header("User-Agent", "agent-vmm-managed-updater/1").call().map_err(|error| {
        let cancelled = matches!(error, ureq::Error::Timeout(_));
        PreparationError { cancelled, ..PreparationError::download_unavailable(error.to_string()) }
    })?;
    let status = response.status().as_u16();
    if status != 200 {
        let message = format!("Host artifact download returned HTTP {status}");
        return Err(if (500..=599).contains(&status) { PreparationError::download_unavailable(message) } else { download(message) });
    }
    let declared = response.headers().get("content-length").and_then(|value| value.to_str().ok()).and_then(|value| value.parse::<u64>().ok());
    if declared.is_some_and(|length| length != options.artifact_size) {
        return Err(download("Host artifact Content-Length does not match the selected target"));
    }
    let incomplete = |_| PreparationError::download_unavailable("Host artifact download did not complete");
    let mut body = response.body_mut().with_config().limit(options.artifact_size + 2).reader().take(options.artifact_size + 1);
    let (mut hash, mut written, mut chunk) = (Sha256::new(), 0u64, vec![0u8; 1 << 16]);
    loop {
        let count = body.read(&mut chunk).map_err(incomplete)?;
        if count == 0 {
            break;
        }
        archive.write_all(&chunk[..count]).map_err(incomplete)?;
        hash.update(&chunk[..count]);
        written += count as u64;
    }
    archive.flush().map_err(incomplete)?;
    let digest: String = hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    if written != options.artifact_size || !digest.eq_ignore_ascii_case(&options.artifact_sha256) {
        return Err(download("Host artifact bytes do not match the selected size and SHA-256"));
    }
    Ok(())
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
        let staged = prepared_path(&app, "update-1");
        assert_eq!(staged.parent(), app.parent());
        assert!(staged.file_name().unwrap().to_string_lossy().starts_with('.'), "the prepared release is hidden");
        std::fs::create_dir(&staged).unwrap();
        // The unroutable URL shows that nothing is downloaded.
        assert_eq!(prepare(&options("https://releases.invalid/host.zip", 42), &status(&app)), Ok(staged.display().to_string()));
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
