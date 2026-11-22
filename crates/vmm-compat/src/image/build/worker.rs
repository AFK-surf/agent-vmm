use super::{
    super::{MAX_ARCHIVE_BYTES, VerifiedArchive, base, oci},
    input::Upload,
};
use crate::error::client_status;
use agent_vmm_proto::host::v1 as h;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{self, Read, Seek, Write},
    path::{Component, Path},
};
use tokio::sync::mpsc;
use vmm_guest_proto::{
    client::{Client, ExecEvent},
    rpc as g,
};
use vmm_rpc::Status;

pub(super) fn progress(text: String) -> h::BuildImageResponse {
    h::BuildImageResponse {
        event: Some(h::build_image_response::Event::Progress(text)),
    }
}
fn target(path: &str) -> g::WorkspaceTarget {
    g::WorkspaceTarget {
        volume: Some("workspace".into()),
        path: path.into(),
    }
}
fn write(client: &Client, path: &str, mut data: impl Read) -> Result<(), Status> {
    client
        .workspace_write(
            g::WorkspaceWriteRequest {
                request_id: uuid::Uuid::new_v4().to_string(),
                target: target(path),
                offset: 0,
                expected_prefix_sha256: None,
                mode: 0o600,
            },
            &mut data,
        )
        .map_err(client_status)?;
    Ok(())
}
fn guest_command(argv: Vec<String>) -> g::ExecRequest {
    let env = [
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/workspace/home",
        "USER=user",
        "XDG_RUNTIME_DIR=/workspace/runtime",
        "TMPDIR=/workspace/tmp",
        "BUILDKIT_HOST=unix:///workspace/runtime/buildkit/buildkitd.sock",
        "BUILDCTL_CONNECT_RETRIES_MAX=50",
        "ROOTLESSKIT=/usr/bin/rootlesskit --net=host",
        "BUILDKITD_FLAGS=--root=/workspace/state --oci-worker-snapshotter=overlayfs --oci-worker-binary=/usr/bin/buildkit-runc --oci-worker-no-process-sandbox",
    ];
    g::ExecRequest {
        command: g::CommandSpec {
            argv,
            user: Some("1000:1000".into()),
            workdir: Some("/workspace".into()),
            env: env.into_iter().map(String::from).collect(),
        },
        stdin: false,
        tty: None,
        timeout_ms: 0,
        max_output_bytes: 0,
    }
}

struct OutputArchive<'a> {
    file: &'a mut File,
    written: u64,
    digest: Sha256,
}

impl Write for OutputArchive<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > MAX_ARCHIVE_BYTES - self.written {
            return Err(io::Error::other("build output exceeds archive limit"));
        }
        let n = self.file.write(bytes)?;
        self.written += n as u64;
        self.digest.update(&bytes[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

type Sender = mpsc::Sender<Result<h::BuildImageResponse, Status>>;

fn prepare_context(mut file: File, max_bytes: u64) -> Result<File, Status> {
    let bad = || Status::invalid_argument("invalid build context archive");
    let staging_err = |_| Status::unavailable("build context staging unavailable");
    let mut magic = [0u8; 2];
    let mut read = 0;
    while read < magic.len() {
        match file.read(&mut magic[read..]).map_err(|_| bad())? {
            0 => break,
            n => read += n,
        }
    }
    file.rewind().map_err(|_| bad())?;
    if read == 2 && magic == [0x1f, 0x8b] {
        let mut out = tempfile::tempfile().map_err(staging_err)?;
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut buf = [0u8; 64 * 1024];
        let mut total = 0u64;
        loop {
            let n = decoder.read(&mut buf).map_err(|_| bad())?;
            if n == 0 {
                break;
            }
            total = total.checked_add(n as u64).ok_or_else(bad)?;
            if total > max_bytes {
                return Err(bad());
            }
            out.write_all(&buf[..n]).map_err(staging_err)?;
        }
        out.flush().map_err(staging_err)?;
        out.rewind().map_err(|_| bad())?;
        return Ok(out);
    }
    Ok(file)
}

fn validate_context(file: &mut File) -> Result<(), Status> {
    let bad = || Status::invalid_argument("invalid build context archive");
    let mut entries = 0;
    let mut hardlinks = 0;
    for entry in tar::Archive::new(&mut *file).entries().map_err(|_| bad())? {
        let entry = entry.map_err(|_| bad())?;
        let path = entry.path().map_err(|_| bad())?;
        let kind = entry.header().entry_type();
        entries += 1;
        if entries > 10000
            || path.is_absolute()
            || path.components().any(|c| matches!(c, Component::ParentDir))
            || path.as_os_str().is_empty()
            || (kind.is_file() && entry.size() > 64 << 20)
        {
            return Err(bad());
        }
        if kind.is_symlink() || kind.is_hard_link() {
            let link = entry.link_name().map_err(|_| bad())?.ok_or_else(bad)?;
            if link.is_absolute() || link.as_os_str().is_empty() {
                return Err(bad());
            }
            let mut depth = if kind.is_symlink() {
                path.parent().map_or(0, |p| {
                    p.components()
                        .filter(|c| matches!(c, Component::Normal(_)))
                        .count()
                })
            } else {
                hardlinks += 1;
                0
            };
            for part in link.components() {
                match part {
                    Component::Normal(_) => depth += 1,
                    Component::CurDir => {}
                    Component::ParentDir if depth > 0 => depth -= 1,
                    _ => return Err(bad()),
                }
            }
            if hardlinks > 1024 {
                return Err(bad());
            }
        } else if !kind.is_dir() && !kind.is_file() {
            return Err(bad());
        }
    }
    file.rewind().map_err(|_| bad())
}

pub(super) fn run(
    client: Client,
    header: h::BuildImageHeader,
    mut upload: Upload,
    base_file: Option<File>,
    dir: &Path,
    tx: Sender,
) -> Result<VerifiedArchive, Status> {
    upload.context = prepare_context(upload.context, MAX_ARCHIVE_BYTES)?;
    validate_context(&mut upload.context)?;
    write(&client, "context.tar", upload.context)?;
    if let Some(file) = base_file {
        write(&client, "base.tar", file)?;
    }
    for (secret, data) in header.secrets.iter().zip(upload.secrets) {
        write(&client, &format!("secret-{}", secret.id), data.as_slice())?;
    }
    let setup = "set -eu\numask 077\nmkdir -p context home runtime tmp state\ntar -xf context.tar -C context\nif test -f base.tar; then mkdir base && tar -xf base.tar -C base; fi";
    let prepared = client
        .exec(
            guest_command(vec!["/bin/sh".into(), "-c".into(), setup.into()]),
            &[],
        )
        .map_err(client_status)?;
    if prepared.result.exit.code != 0 {
        return Err(Status::invalid_argument(
            "could not prepare isolated build context",
        ));
    }
    let mut args = [
        "/usr/bin/buildctl-daemonless.sh",
        "build",
        "--progress=plain",
        "--frontend",
        "dockerfile.v0",
        "--local",
        "context=/workspace/context",
        "--local",
        "dockerfile=/workspace/context",
        "--opt",
        "platform=linux/arm64",
        "--opt",
    ]
    .map(String::from)
    .to_vec();
    args.extend([
        format!("filename={}", header.dockerfile_path),
        "--output".into(),
        format!("type=oci,dest=/workspace/image.tar,name={}", header.image),
    ]);
    if !header.base_manifest_digest.is_empty() {
        args.extend([
            "--oci-layout".into(),
            "base=/workspace/base".into(),
            "--opt".into(),
            format!(
                "context:base=oci-layout://base@{}",
                header.base_manifest_digest
            ),
        ]);
    }
    if let Some(target) = header.target {
        args.extend(["--opt".into(), format!("target={target}")]);
    }
    for arg in header.build_args {
        args.extend([
            "--opt".into(),
            format!("build-arg:{}={}", arg.name, arg.value),
        ]);
    }
    for secret in header.secrets {
        args.extend([
            "--secret".into(),
            format!("id={0},src=/workspace/secret-{0}", secret.id),
        ]);
    }
    let mut session = client
        .exec_stream(guest_command(args))
        .map_err(client_status)?;
    loop {
        match session.next_event().map_err(client_status)? {
            ExecEvent::Stdout(data) | ExecEvent::Stderr(data) => {
                let _ = tx.try_send(Ok(progress(String::from_utf8_lossy(&data).into_owned())));
            }
            ExecEvent::Exit(result) if result.exit.code == 0 => break,
            ExecEvent::Exit(result) => {
                return Err(Status::failed_precondition(format!(
                    "isolated BuildKit exited with code {}",
                    result.exit.code
                )));
            }
        }
    }
    let mut file = tempfile::NamedTempFile::new_in(dir)
        .map_err(|_| Status::unavailable("cannot stage build output"))?;
    let mut sink = OutputArchive {
        file: file.as_file_mut(),
        written: 0,
        digest: Sha256::new(),
    };
    let read = client
        .workspace_read(
            g::WorkspaceReadRequest {
                target: target("image.tar"),
                offset: 0,
                max_bytes: MAX_ARCHIVE_BYTES,
            },
            &mut sink,
        )
        .map_err(client_status)?;
    if read.end_offset != read.file_size || read.file_size == 0 {
        return Err(Status::resource_exhausted(
            "build output exceeds archive limit",
        ));
    }
    if sink.written != read.end_offset || <[u8; 32]>::from(sink.digest.finalize()) != read.sha256 {
        return Err(Status::data_loss("inconsistent build output stream"));
    }
    let digest = base::output_digest(file.path())?;
    let logical_bytes = oci::validate_built(file.path(), digest)?;
    Ok(VerifiedArchive::from_temp(
        file,
        logical_bytes,
        h::ImportImageHeader {
            image_reference: header.image,
            manifest_digest: format!("sha256:{}", hex::encode(digest)),
            platform: "linux/arm64".into(),
            ..Default::default()
        },
        digest,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::GzEncoder};
    use vmm_rpc::Code;

    fn temp_file_with(bytes: &[u8]) -> File {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.rewind().unwrap();
        file
    }

    fn tar_bytes(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, path, *data).unwrap();
        }
        builder.into_inner().unwrap()
    }

    fn gzip_bytes(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn raw_tar_with_path(path: &str, data: &[u8]) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        let raw = header.as_mut_bytes();
        raw[..path.len()].copy_from_slice(path.as_bytes());
        raw[path.len()] = 0;
        header.set_cksum();
        let mut out = Vec::new();
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(data);
        let rem = data.len() % 512;
        if rem != 0 {
            out.extend_from_slice(&vec![0u8; 512 - rem]);
        }
        out.extend_from_slice(&[0u8; 1024]);
        out
    }

    #[test]
    fn plain_tar_context_passes() {
        let tar = tar_bytes(&[("Dockerfile", b"FROM scratch\n")]);
        let mut prepared = prepare_context(temp_file_with(&tar), MAX_ARCHIVE_BYTES).unwrap();
        validate_context(&mut prepared).unwrap();
    }

    #[test]
    fn gzipped_tar_context_decompresses_and_passes() {
        let tar = tar_bytes(&[("Dockerfile", b"FROM scratch\n")]);
        let gz = gzip_bytes(&tar);
        let mut prepared = prepare_context(temp_file_with(&gz), MAX_ARCHIVE_BYTES).unwrap();
        validate_context(&mut prepared).unwrap();
        let mut archive = tar::Archive::new(&mut prepared);
        let mut entries = archive.entries().unwrap();
        let mut first = entries.next().unwrap().unwrap();
        assert_eq!(first.path().unwrap(), Path::new("Dockerfile"));
        let mut body = Vec::new();
        first.read_to_end(&mut body).unwrap();
        assert_eq!(body, b"FROM scratch\n");
    }

    #[test]
    fn corrupt_gzip_context_rejected_as_invalid_argument() {
        let corrupt = [0x1f, 0x8b, 0x08, 0x00, 0xde, 0xad, 0xbe, 0xef];
        let err = prepare_context(temp_file_with(&corrupt), MAX_ARCHIVE_BYTES).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "invalid build context archive");
    }

    #[test]
    fn gzip_context_exceeding_limit_and_parent_traversal_rejected() {
        let oversized = gzip_bytes(&vec![0u8; 128 * 1024]);
        let err = prepare_context(temp_file_with(&oversized), 64 * 1024).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "invalid build context archive");

        let traversal = gzip_bytes(&raw_tar_with_path("../Dockerfile", b"FROM scratch\n"));
        let mut prepared = prepare_context(temp_file_with(&traversal), MAX_ARCHIVE_BYTES).unwrap();
        let err = validate_context(&mut prepared).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert_eq!(err.message(), "invalid build context archive");
    }
}
