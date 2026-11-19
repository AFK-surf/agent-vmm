//! Frames of one RPC connection: a big-endian `u32` length, then a postcard body.
//!
//! Each connection carries one call. The client sends [`ClientFrame::Hello`]
//! first, and the server ends the call with [`ServerFrame::Done`].

use std::io::{self, Read, Write};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::rpc::{Hello, LogRecord, Reply, RpcError, Signal, TtySize};

pub const MAX_FRAME_BYTES: usize = 1 << 20;

#[derive(Debug, Serialize, Deserialize)]
pub enum ClientFrame {
    Hello(Box<Hello>),
    Data(#[serde(with = "serde_bytes")] Vec<u8>),
    /// Ends the client's data: closed stdin, or a complete upload.
    End,
    Signal(Signal),
    Resize(TtySize),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ServerFrame {
    Stdout(#[serde(with = "serde_bytes")] Vec<u8>),
    Stderr(#[serde(with = "serde_bytes")] Vec<u8>),
    Data(#[serde(with = "serde_bytes")] Vec<u8>),
    Log(LogRecord),
    /// Sent during a long wait so that a closed peer is detected.
    Keepalive,
    Done(Result<Reply, RpcError>),
}

fn invalid(error: postcard::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

pub fn write_frame<W: Write, T: Serialize>(writer: &mut W, frame: &T) -> io::Result<()> {
    let body = Zeroizing::new(postcard::to_allocvec(frame).map_err(invalid)?);
    if body.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame exceeds the size limit",
        ));
    }
    let mut out = Zeroizing::new(Vec::with_capacity(4 + body.len()));
    out.extend_from_slice(&(body.len() as u32).to_be_bytes());
    out.extend_from_slice(&body);
    writer.write_all(&out)
}

/// Reads one frame. Returns `None` when the peer closed the stream between frames.
pub fn read_frame<R: Read, T: DeserializeOwned>(reader: &mut R) -> io::Result<Option<T>> {
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => filled += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    let length = u32::from_be_bytes(header) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds the size limit",
        ));
    }
    let mut body = Zeroizing::new(vec![0u8; length]);
    reader.read_exact(&mut body)?;
    postcard::from_bytes(&body).map(Some).map_err(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{Call, ErrorCode, HealthRequest};
    use crate::{PROTOCOL_VERSION, Secret};

    #[test]
    fn frames_round_trip_and_stop_at_a_clean_end() {
        let mut wire = Vec::new();
        let hello = Hello {
            version: PROTOCOL_VERSION,
            token: Secret::new("token"),
            call: Call::Health(HealthRequest { nonce: [3; 16] }),
        };
        write_frame(&mut wire, &ClientFrame::Hello(Box::new(hello))).unwrap();
        write_frame(&mut wire, &ClientFrame::Data(vec![0, 255, 7])).unwrap();
        write_frame(&mut wire, &ClientFrame::End).unwrap();

        let mut reader = wire.as_slice();
        let Some(ClientFrame::Hello(hello)) = read_frame(&mut reader).unwrap() else {
            panic!("expected Hello");
        };
        assert_eq!(hello.token, Secret::new("token"));
        assert!(matches!(hello.call, Call::Health(HealthRequest { nonce }) if nonce == [3; 16]));
        assert!(matches!(
            read_frame(&mut reader).unwrap(),
            Some(ClientFrame::Data(data)) if data == [0, 255, 7]
        ));
        assert!(matches!(
            read_frame(&mut reader).unwrap(),
            Some(ClientFrame::End)
        ));
        assert!(read_frame::<_, ClientFrame>(&mut reader).unwrap().is_none());
    }

    #[test]
    fn rejects_oversized_and_truncated_frames() {
        let oversized = ((MAX_FRAME_BYTES + 1) as u32).to_be_bytes();
        let error = read_frame::<_, ServerFrame>(&mut oversized.as_slice()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let mut wire = Vec::new();
        let done = ServerFrame::Done(Err(RpcError::new(ErrorCode::NotFound, "missing")));
        write_frame(&mut wire, &done).unwrap();
        wire.pop();
        let error = read_frame::<_, ServerFrame>(&mut wire.as_slice()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

        let large = ClientFrame::Data(vec![0; MAX_FRAME_BYTES]);
        assert!(write_frame(&mut Vec::new(), &large).is_err());
    }
}
