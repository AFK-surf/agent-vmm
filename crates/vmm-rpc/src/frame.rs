use crate::{Bytes, Code, Metadata, Status};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_BYTES: usize = 16 << 20;
pub const MAX_HEADER_BYTES: usize = 64 << 10;

#[derive(Serialize, Deserialize)]
pub struct RequestHeader {
    pub v: u8,
    pub method: String,
    pub metadata: Metadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct WireStatus {
    code: u8,
    message: String,
    #[serde(default)]
    details_b64: String,
}

pub enum Frame {
    Header(RequestHeader),
    Message(Bytes),
    End,
    Status(Status),
}

impl Frame {
    fn length(prefix: &[u8; 5]) -> Result<usize, Status> {
        let length = u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize;
        let limit = match prefix[0] {
            0 => MAX_HEADER_BYTES,
            1 | 3 => MAX_FRAME_BYTES,
            2 => 0,
            _ => return Err(Status::invalid_argument("unknown frame tag")),
        };
        if length > limit {
            return Err(Status::resource_exhausted("frame exceeds size limit"));
        }
        Ok(length)
    }

    fn decode(tag: u8, payload: Bytes) -> Result<Self, Status> {
        match tag {
            0 => {
                let header: RequestHeader = serde_json::from_slice(&payload)
                    .map_err(|_| Status::invalid_argument("invalid RPC header"))?;
                if header.v != 1
                    || !header.method.starts_with('/')
                    || !header.metadata.0.iter().all(|(key, value)| {
                        (!key.ends_with("-bin") || STANDARD.decode(value).is_ok())
                            && !key.is_empty()
                            && key.bytes().all(|b| {
                                b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_.".contains(&b)
                            })
                    })
                {
                    return Err(Status::invalid_argument("unsupported RPC header"));
                }
                Ok(Self::Header(header))
            }
            1 => Ok(Self::Message(payload)),
            2 => Ok(Self::End),
            3 => {
                let s: WireStatus = serde_json::from_slice(&payload)
                    .map_err(|_| Status::data_loss("invalid terminal status"))?;
                let details = STANDARD
                    .decode(s.details_b64)
                    .map_err(|_| Status::data_loss("invalid terminal details"))?;
                Ok(Self::Status(Status::with_details(
                    Code::try_from(s.code)?,
                    s.message,
                    details.into(),
                )))
            }
            _ => Err(Status::invalid_argument("unknown frame tag")),
        }
    }

    fn parts(&self) -> Result<([u8; 5], Bytes), Status> {
        let (tag, payload) = match self {
            Self::Header(h) => (
                0,
                serde_json::to_vec(h)
                    .map_err(|e| Status::internal(e.to_string()))?
                    .into(),
            ),
            Self::Message(data) => (1, data.clone()),
            Self::End => (2, Bytes::new()),
            Self::Status(s) => (
                3,
                serde_json::to_vec(&WireStatus {
                    code: s.code as u8,
                    message: s.message.clone(),
                    details_b64: STANDARD.encode(&s.details),
                })
                .map_err(|e| Status::internal(e.to_string()))?
                .into(),
            ),
        };
        let size = u32::try_from(payload.len())
            .map_err(|_| Status::resource_exhausted("frame exceeds size limit"))?;
        let mut prefix = [tag, 0, 0, 0, 0];
        prefix[1..].copy_from_slice(&size.to_be_bytes());
        Self::length(&prefix)?;
        Ok((prefix, payload))
    }

    pub(crate) async fn read_header(
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<RequestHeader, Status> {
        let mut prefix = [0; 5];
        reader.read_exact(&mut prefix).await?;
        if prefix[0] != 0 {
            return Err(Status::invalid_argument("expected RPC header"));
        }
        let mut bytes = vec![0; Self::length(&prefix)?];
        reader.read_exact(&mut bytes).await?;
        match Self::decode(0, bytes.into())? {
            Self::Header(header) => Ok(header),
            _ => unreachable!(),
        }
    }

    pub(crate) async fn read_body(
        reader: &mut (impl AsyncRead + Unpin),
        response: bool,
        max_message: usize,
    ) -> Result<Self, Status> {
        let mut prefix = [0; 5];
        reader.read_exact(&mut prefix).await?;
        let tag = prefix[0];
        if tag != 1 && tag != if response { 3 } else { 2 } {
            return Err(Status::invalid_argument("unexpected RPC frame"));
        }
        let len = Self::length(&prefix)?;
        if tag == 1 && len > max_message {
            return Err(Status::resource_exhausted("RPC message too large"));
        }
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes).await?;
        Self::decode(tag, bytes.into())
    }

    pub async fn write(&self, writer: &mut (impl AsyncWrite + Unpin)) -> Result<(), Status> {
        let (prefix, payload) = self.parts()?;
        writer.write_all(&prefix).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;
        Ok(())
    }

    pub fn read_sync(reader: &mut impl Read) -> Result<Self, Status> {
        let mut prefix = [0u8; 5];
        reader.read_exact(&mut prefix)?;
        let mut payload = vec![0; Self::length(&prefix)?];
        reader.read_exact(&mut payload)?;
        Self::decode(prefix[0], payload.into())
    }

    pub fn write_sync(&self, writer: &mut impl Write) -> Result<(), Status> {
        let (prefix, payload) = self.parts()?;
        writer.write_all(&prefix)?;
        writer.write_all(&payload)?;
        Ok(())
    }
}
