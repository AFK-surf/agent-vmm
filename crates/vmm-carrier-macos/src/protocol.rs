#![forbid(unsafe_code)]
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, io, path::PathBuf};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub type Shares = BTreeMap<String, PathBuf>;
pub const CONTROL_PORT: u32 = 8180;
pub const GUEST_PORT: u32 = 8183;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum State {
    Stopped,
    Running,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CarrierStatus {
    pub state: State,
    pub boot_id: Option<String>,
    pub pid: u32,
    pub cpu_count: usize,
    pub memory_bytes: u64,
    pub balloon_target_bytes: u64,
    pub pool_logical_bytes: u64,
    pub pool_allocated_bytes: u64,
    pub pool_hole_bytes: u64,
    pub host_free_bytes: u64,
    pub last_error: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    Status,
    EnsureStarted { operation_id: String },
    Stop { force: bool },
    SetBalloonTarget { target_memory_bytes: u64 },
    SetHostShares { shares: Shares },
    PunchPoolRange { offset: u64, len: u64 },
    OpenVsock { port: u32 },
}

#[derive(Serialize, Deserialize)]
pub(crate) enum Reply {
    Status(CarrierStatus),
    Stream,
    Error(String),
}

const MAX_FRAME: usize = 1 << 20;

pub(crate) async fn read<T: DeserializeOwned>(
    stream: &mut (impl AsyncRead + Unpin),
) -> io::Result<T> {
    let n = stream.read_u32().await? as usize;
    if n == 0 || n > MAX_FRAME {
        return Err(io::Error::other("invalid carrier frame size"));
    }
    let mut buf = vec![0; n];
    stream.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

pub(crate) async fn write<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    msg: &T,
) -> io::Result<()> {
    let buf = serde_json::to_vec(msg)?;
    if buf.len() > MAX_FRAME {
        return Err(io::Error::other("carrier frame too large"));
    }
    stream.write_u32(buf.len() as u32).await?;
    stream.write_all(&buf).await
}
