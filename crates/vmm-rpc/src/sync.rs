use crate::{Frame, Metadata, RequestHeader, Status};
use prost::Message;
use rustix::event::{PollFd, PollFlags, Timespec, poll};
use socket2::{Domain, SockAddr, Socket, Type};
use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    time::{Duration, Instant},
};

struct TimedStream {
    socket: UnixStream,
    started: Instant,
    timeout: Duration,
}

impl TimedStream {
    fn remaining(&self) -> io::Result<Duration> {
        self.timeout
            .checked_sub(self.started.elapsed())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "RPC deadline exceeded"))
    }

    fn transfer(
        &mut self,
        event: PollFlags,
        mut op: impl FnMut(&mut UnixStream) -> io::Result<usize>,
    ) -> io::Result<usize> {
        loop {
            let timeout = Timespec::try_from(self.remaining()?).map_err(io::Error::other)?;
            let mut fds = [PollFd::new(&self.socket, event)];
            match poll(&mut fds, Some(&timeout)) {
                Err(rustix::io::Errno::INTR) | Ok(0) => continue,
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
            match op(&mut self.socket) {
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) =>
                {
                    continue;
                }
                result => return result,
            }
        }
    }
}
impl Read for TimedStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.transfer(PollFlags::IN, |socket| socket.read(buf))
    }
}
impl Write for TimedStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.transfer(PollFlags::OUT, |socket| socket.write(buf))
    }
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

pub fn sync_unary<T: Message, U: Message + Default>(
    socket: &Path,
    method: &str,
    message: &T,
    timeout: Duration,
) -> Result<U, Status> {
    let started = Instant::now();
    if timeout.is_zero() {
        return Err(Status::deadline_exceeded("RPC deadline exceeded"));
    }
    let client = Socket::new(Domain::UNIX, Type::STREAM, None)?;
    client.connect_timeout(&SockAddr::unix(socket)?, timeout)?;
    client.set_nonblocking(true)?;
    let mut io = TimedStream {
        socket: client.into(),
        started,
        timeout,
    };
    Frame::Header(RequestHeader {
        v: 1,
        method: method.into(),
        metadata: Metadata::default(),
        timeout_ms: Some(io.remaining()?.as_millis().try_into().unwrap_or(u64::MAX)),
    })
    .write_sync(&mut io)?;
    Frame::Message(message.encode_to_vec().into()).write_sync(&mut io)?;
    Frame::End.write_sync(&mut io)?;
    io.socket.shutdown(std::net::Shutdown::Write)?;
    let mut message = None;
    loop {
        match Frame::read_sync(&mut io)? {
            Frame::Message(bytes) if message.is_none() => {
                message = Some(
                    U::decode(bytes).map_err(|_| Status::data_loss("invalid protobuf response"))?,
                )
            }
            Frame::Status(s) if s.code == crate::Code::Ok => {
                return message.ok_or_else(|| Status::data_loss("missing unary response"));
            }
            Frame::Status(s) => return Err(s),
            _ => return Err(Status::data_loss("unexpected unary response frame")),
        }
    }
}
