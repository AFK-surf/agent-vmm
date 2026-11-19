use super::ArchiveWriter;
use crate::error::invalid_field;
use std::{net::Ipv4Addr, process::Stdio, time::Duration};
use tokio::{io::AsyncReadExt, net::lookup_host, process::Command};
use tonic::Status;
use url::Url;

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

pub(super) async fn download(raw: &str, to: &mut ArchiveWriter) -> Result<(), Status> {
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
    let host = url
        .host_str()
        .ok_or_else(|| invalid_field("archive_url", "missing archive host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| invalid_field("archive_url", "invalid HTTPS port"))?;
    let mut selected = None;
    for address in tokio::time::timeout(Duration::from_secs(30), lookup_host((host, port)))
        .await
        .map_err(|_| Status::deadline_exceeded("archive name resolution timed out"))?
        .map_err(|_| Status::unavailable("archive name resolution failed"))?
    {
        if let std::net::IpAddr::V4(ip) = address.ip() {
            if !public(ip) {
                return Err(Status::invalid_argument(
                    "archive host resolves to a non-public address",
                ));
            }
            selected.get_or_insert(ip);
        }
    }
    let ip = selected
        .ok_or_else(|| Status::invalid_argument("archive host needs a public IPv4 address"))?;
    // Use the system curl and trust store. Pin the validated address through TLS and
    // exclude user configuration, proxies and redirects.
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
            "600",
            "--max-filesize",
            &to.remaining.to_string(),
            "--write-out",
            "%{http_code}",
            "--url",
            url.as_str(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd
        .spawn()
        .map_err(|_| Status::unavailable("archive downloader unavailable"))?;
    let mut output = child
        .stdout
        .take()
        .ok_or_else(|| Status::internal("archive stream missing"))?;
    let transfer = async {
        let mut buffer = vec![0; 64 << 10];
        while to.remaining > 0 {
            let max = to.remaining.min(buffer.len() as u64) as usize;
            let count = output
                .read(&mut buffer[..max])
                .await
                .map_err(|_| Status::unavailable("archive download interrupted"))?;
            if count == 0 {
                return Err(Status::invalid_argument("archive download too short"));
            }
            to.append(&buffer[..count]).await?;
        }
        let mut status = Vec::new();
        output
            .take(4)
            .read_to_end(&mut status)
            .await
            .map_err(|_| Status::unavailable("archive status unavailable"))?;
        if status != b"200" {
            return Err(Status::invalid_argument(
                "archive length or HTTP status mismatch",
            ));
        }
        if !child
            .wait()
            .await
            .map_err(|_| Status::unavailable("archive download interrupted"))?
            .success()
        {
            return Err(Status::unavailable("archive download failed"));
        }
        Ok(())
    };
    tokio::time::timeout(Duration::from_secs(600), transfer)
        .await
        .map_err(|_| Status::deadline_exceeded("archive download timed out"))?
}
