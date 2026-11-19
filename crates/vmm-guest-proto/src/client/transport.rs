use super::{ClientError, Result};
use crate::Secret;
use std::{
    fmt,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    sync::Arc,
    time::Duration,
};

/// One established byte stream to a Guest. The carrier supplies an upgraded Unix socket.
pub enum GuestStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
}

macro_rules! delegate {
    ($this:expr, $method:ident($($arg:expr),*)) => {
        match $this {
            Self::Tcp(s) => s.$method($($arg),*),
            #[cfg(unix)]
            Self::Unix(s) => s.$method($($arg),*),
        }
    };
}
impl Read for GuestStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        delegate!(self, read(buf))
    }
}
impl Write for GuestStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        delegate!(self, write(buf))
    }
    fn flush(&mut self) -> io::Result<()> {
        delegate!(self, flush())
    }
}
impl GuestStream {
    pub(super) fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        delegate!(self, shutdown(how))
    }
    pub(super) fn try_clone(&self) -> io::Result<Self> {
        match self {
            Self::Tcp(s) => s.try_clone().map(Self::Tcp),
            #[cfg(unix)]
            Self::Unix(s) => s.try_clone().map(Self::Unix),
        }
    }
}

type Dial = dyn Fn(Duration) -> Result<(GuestStream, Secret)> + Send + Sync;
#[derive(Clone)]
pub(super) enum Transport {
    Direct(SocketAddr, Secret),
    Bound { instance: String, dial: Arc<Dial> },
}
impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(address, _) => f.debug_tuple("Direct").field(address).finish(),
            Self::Bound { instance, .. } => f.debug_tuple("Bound").field(instance).finish(),
        }
    }
}
impl Transport {
    pub(super) fn connect(&self, timeout: Duration) -> Result<(GuestStream, Secret)> {
        match self {
            Self::Direct(address, token) => {
                let stream =
                    TcpStream::connect_timeout(address, timeout).map_err(ClientError::Io)?;
                stream.set_nodelay(true)?;
                Ok((GuestStream::Tcp(stream), token.clone()))
            }
            Self::Bound { dial, .. } => dial(timeout),
        }
    }
}
