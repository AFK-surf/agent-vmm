#![forbid(unsafe_code)]
use crate::protocol::*;
use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, copy_bidirectional},
    net::{TcpListener, UnixStream},
    task::{JoinHandle, JoinSet},
};

pub use crate::backend::CarrierDom0Backend;
pub type CarrierDom0Client = CarrierDom0Backend;
#[derive(Clone)]
pub struct CarrierClient {
    socket: PathBuf,
}

impl CarrierClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }
    pub fn dom0(&self) -> CarrierDom0Client {
        CarrierDom0Backend::new(self.clone())
    }
    async fn exchange(&self, req: Request) -> io::Result<(UnixStream, Reply)> {
        let mut socket = UnixStream::connect(&self.socket).await?;
        write(&mut socket, &(1u8, req)).await?;
        match read(&mut socket).await? {
            Reply::Error(e) => Err(io::Error::other(e)),
            reply => Ok((socket, reply)),
        }
    }
    pub async fn control(&self, req: Request) -> io::Result<CarrierStatus> {
        match self.exchange(req).await?.1 {
            Reply::Status(status) => Ok(status),
            _ => Err(io::Error::other("expected carrier status")),
        }
    }
    pub async fn open_vsock(&self, port: u32) -> io::Result<UnixStream> {
        match self.exchange(Request::OpenVsock { port }).await? {
            (stream, Reply::Stream) => Ok(stream),
            _ => Err(io::Error::other("expected stream upgrade")),
        }
    }
    /// The relay acknowledges the six-byte IPv4/port tuple with a zero byte
    /// after connecting. Bytes in either direction thereafter are Guest RPC.
    pub async fn connect_guest_rpc(&self, slot_ip: Ipv4Addr, port: u16) -> io::Result<UnixStream> {
        let mut stream = self.open_vsock(GUEST_PORT).await?;
        stream.write_all(&slot_ip.octets()).await?;
        stream.write_u16(port).await?;
        if stream.read_u8().await? != 0 {
            return Err(io::Error::other("guest connection rejected"));
        }
        Ok(stream)
    }
    pub async fn bridge_guest_tcp(&self, slot_ip: Ipv4Addr, port: u16) -> io::Result<GuestBridge> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let local_addr = listener.local_addr()?;
        let client = self.clone();
        let task = tokio::spawn(async move {
            let mut streams = JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = streams.join_next(), if !streams.is_empty() => {}
                    pair = listener.accept() => {
                        let Ok((mut tcp,_)) = pair else { break };
                        let client = client.clone();
                        streams.spawn(async move {
                            let mut guest = client.connect_guest_rpc(slot_ip,port).await?;
                            copy_bidirectional(&mut tcp, &mut guest).await
                        });
                    }
                }
            }
        });
        Ok(GuestBridge { local_addr, task })
    }
}

#[async_trait::async_trait]
impl vmm_dom0::remote::Connect for CarrierClient {
    async fn connect(&self) -> io::Result<UnixStream> {
        self.open_vsock(CONTROL_PORT).await
    }
}

pub struct GuestBridge {
    pub local_addr: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for GuestBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}
