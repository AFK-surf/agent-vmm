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
use vmm_rpc::Status;
use vmm_guest_proto::{
    client::{Client, ExecEvent},
    rpc as g,
};

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
