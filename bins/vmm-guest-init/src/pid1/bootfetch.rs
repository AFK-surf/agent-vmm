//! Fetches the boot configuration from dom0 with one plain HTTP request.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use vmm_guest_proto::boot::{BOOT_PATH, BootConfig, MAX_BOOT_CONFIG_BYTES};
use zeroize::Zeroizing;

const MAX_HEADER_BYTES: usize = 8192;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
/// dom0 registers the configuration before it starts the VM. The time covers a slow link.
const FETCH_BUDGET: Duration = Duration::from_secs(30);

/// Returns the body of a complete `200` response.
fn response_body(response: &[u8]) -> Result<&[u8], String> {
    let split = response.windows(4).position(|window| window == b"\r\n\r\n");
    let (head, body) = match split {
        Some(at) => (&response[..at], &response[at + 4..]),
        None => return Err("the boot response has no header end".into()),
    };
    let head = std::str::from_utf8(head).map_err(|_| "the boot response header is not text")?;
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap_or_default();
    if status.split(' ').nth(1) != Some("200") {
        return Err(format!("dom0 answered {status:?}"));
    }
    for line in lines {
        let (name, value) = line.split_once(':').unwrap_or((line, ""));
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("the boot response uses a transfer encoding".into());
        }
        if name.eq_ignore_ascii_case("content-length") && value.trim().parse() != Ok(body.len()) {
            return Err("the boot response is incomplete".into());
        }
    }
    Ok(body)
}

fn attempt(server: SocketAddr) -> Result<BootConfig, String> {
    let io = |error: std::io::Error| error.to_string();
    let mut stream = TcpStream::connect_timeout(&server, ATTEMPT_TIMEOUT).map_err(io)?;
    stream.set_read_timeout(Some(ATTEMPT_TIMEOUT)).map_err(io)?;
    stream
        .set_write_timeout(Some(ATTEMPT_TIMEOUT))
        .map_err(io)?;
    let request =
        format!("GET {BOOT_PATH} HTTP/1.1\r\nHost: {server}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).map_err(io)?;

    // One fixed buffer holds the secrets, so no reallocation leaves a copy behind.
    let mut response = Zeroizing::new(vec![0u8; MAX_HEADER_BYTES + MAX_BOOT_CONFIG_BYTES]);
    let mut length = 0;
    loop {
        match stream.read(&mut response[length..]).map_err(io)? {
            0 => break,
            count => length += count,
        }
        if length == response.len() {
            return Err("the boot response is too large".into());
        }
    }
    BootConfig::from_json(response_body(&response[..length])?).map_err(|error| error.to_string())
}

/// Fetches and validates the boot configuration. Retries until dom0 answers `200`.
pub fn fetch(gateway: Ipv4Addr, port: u16) -> Result<BootConfig, String> {
    let server = SocketAddr::from((gateway, port));
    let deadline = Instant::now() + FETCH_BUDGET;
    loop {
        match attempt(server) {
            Ok(config) => return Ok(config),
            Err(error) if Instant::now() >= deadline => {
                return Err(format!(
                    "fetch the boot configuration from {server}: {error}"
                ));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(250)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_a_complete_200_response() {
        let ok =
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(response_body(ok).unwrap(), b"{}");
        assert_eq!(
            response_body(b"HTTP/1.1 200 OK\r\n\r\nbody").unwrap(),
            b"body"
        );

        let cases: [&[u8]; 4] = [
            b"HTTP/1.1 403 Forbidden\r\ncontent-length: 2\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n{}",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n\r\n",
            b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n",
        ];
        for response in cases {
            assert!(
                response_body(response).is_err(),
                "{:?}",
                String::from_utf8_lossy(response)
            );
        }
    }

    #[test]
    fn fetches_a_configuration_from_a_local_server() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 512];
            let count = stream.read(&mut request).unwrap();
            assert!(request[..count].starts_with(b"GET /v1/boot HTTP/1.1\r\n"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 12\r\n\r\n{\"bad\":true}")
                .unwrap();
        });
        // The body reaches the validator, which rejects this placeholder.
        let error = attempt(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).unwrap_err();
        assert!(error.contains("invalid boot configuration"), "{error}");
        server.join().unwrap();
    }
}
