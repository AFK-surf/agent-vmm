use crate::{
    Connector, Error, Result,
    gateway::{ControlClient, status_error},
    remote::{
        OpenSession, ServeSessionRequest, SessionHeader, SessionReady, serve_session_request,
        serve_session_response,
    },
    unix_millis,
};
use std::{
    collections::HashMap,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{
        AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex, split,
    },
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::server::Connected;
use vmm_core::{Channel, Session, Store};
use zeroize::Zeroizing;

const FRAME_BYTES: usize = 32 << 10;
const MAX_TUNNELS: usize = 32;
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);

/// An authenticated runtime service's local end of a session. The capability stays on the host.
pub struct RuntimeSession {
    pub registration_id: String,
    pub allocation_id: String,
    pub allocation_generation: u64,
    pub execution_owner_id: String,
    pub authorization: Session,
    pub capability: Zeroizing<[u8; 16]>,
    pub transport: TunnelIo,
}

/// Receives and sends raw HTTP/2 bytes. It can be passed to tonic's serve_with_incoming.
pub struct TunnelIo(DuplexStream);

impl Connected for TunnelIo {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

impl AsyncRead for TunnelIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl AsyncWrite for TunnelIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub(crate) struct Tunnels<'a> {
    connector: &'a Connector,
    channel: &'a Channel,
    active: HashMap<String, ActiveTunnel>,
}

struct ActiveTunnel {
    store: Arc<Store>,
    session_id: String,
    stream: JoinHandle<()>,
    command_id: String,
    ready: SessionReady,
}

impl Drop for ActiveTunnel {
    fn drop(&mut self) {
        self.stream.abort();
        let _ = self.store.revoke_session(&self.session_id);
    }
}

struct SessionLease {
    store: Arc<Store>,
    id: String,
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let _ = self.store.revoke_session(&self.id);
    }
}

impl<'a> Tunnels<'a> {
    pub(crate) fn new(connector: &'a Connector, channel: &'a Channel) -> Self {
        Self {
            connector,
            channel,
            active: HashMap::new(),
        }
    }

    pub(crate) fn close(&mut self, allocation: &str) -> Result<()> {
        if let Some(active) = self.active.remove(allocation) {
            active.store.revoke_session(&active.session_id)?;
        }
        Ok(())
    }

    pub(crate) fn replay(&self, command_id: &str, allocation: &str) -> Option<SessionReady> {
        let active = self.active.get(allocation)?;
        (active.command_id == command_id && !active.stream.is_finished())
            .then(|| active.ready.clone())
    }

    pub(crate) async fn open(
        &mut self,
        command_id: &str,
        request: &OpenSession,
    ) -> Result<SessionReady> {
        self.active.retain(|_, active| !active.stream.is_finished());
        if self.active.len() == MAX_TUNNELS && !self.active.contains_key(&request.allocation_id) {
            return Err(
                vmm_core::Error::Capacity(vmm_core::CapacityDimension::ComputeSlots).into(),
            );
        }
        let sink = self.connector.config.runtime.clone();
        let sink = sink.reserve_owned().await.map_err(|_| Error::Runtime)?;
        // Store issues and replaces one capability in the same transaction, with allocation fences.
        let store = &self.connector.core.store;
        let (session, capability) = store.issue_remote_session(
            self.channel,
            &request.allocation_id,
            request.allocation_generation,
        )?;
        self.active.remove(&request.allocation_id);

        let nonce: String = session
            .id
            .chars()
            .filter(|character| *character != '-')
            .collect();
        let expires = unix_millis().saturating_add(SESSION_TTL.as_secs() as i64 * 1000);
        let header = SessionHeader {
            registration_id: self.channel.registration_id.clone(),
            allocation_id: request.allocation_id.clone(),
            allocation_generation: request.allocation_generation,
            tunnel_nonce: nonce.clone(),
            connection_epoch: self.channel.connection_epoch,
            expires_unix_millis: expires,
        };
        let (runtime, transport) = duplex(2 * FRAME_BYTES);
        let id = session.id.clone();
        let lease = SessionLease {
            store: store.clone(),
            id: id.clone(),
        };
        let client = self.connector.gateway.controller();
        let (sent, sent_header) = oneshot::channel();
        let stream = tokio::spawn(async move {
            let _lease = lease;
            let _ = timeout(SESSION_TTL, bridge(client, header, transport, sent)).await;
        });
        let ready = SessionReady {
            allocation_id: request.allocation_id.clone(),
            allocation_generation: request.allocation_generation,
            tunnel_nonce: nonce,
            expires_unix_millis: expires,
        };
        let active = ActiveTunnel {
            store: store.clone(),
            session_id: id,
            stream,
            command_id: command_id.into(),
            ready: ready.clone(),
        };
        sent_header.await.map_err(|_| Error::Runtime)?;
        sink.send(RuntimeSession {
            registration_id: self.channel.registration_id.clone(),
            allocation_id: request.allocation_id.clone(),
            allocation_generation: request.allocation_generation,
            execution_owner_id: request.execution_owner_id.clone(),
            authorization: session,
            capability: Zeroizing::new(capability),
            transport: TunnelIo(runtime),
        });
        self.active.insert(request.allocation_id.clone(), active);
        Ok(ready)
    }
}

async fn bridge(
    mut client: ControlClient,
    header: SessionHeader,
    transport: DuplexStream,
    sent: oneshot::Sender<()>,
) -> Result<()> {
    let (frames, request_stream) = mpsc::channel(8);
    let mut sent = Some(sent);
    let requests = tokio_stream::once(serve_session_request::Payload::Header(header))
        .chain(ReceiverStream::new(request_stream))
        .map(move |payload| {
            if let Some(sent) = sent.take() {
                let _ = sent.send(());
            }
            ServeSessionRequest {
                payload: Some(payload),
            }
        });
    let mut response = client
        .serve_session(requests)
        .await
        .map_err(status_error)?
        .into_inner();
    let (mut read, mut write) = split(transport);
    let send_frames = async {
        let mut buffer = vec![0; FRAME_BYTES];
        loop {
            let count = read.read(&mut buffer).await.map_err(|_| Error::Runtime)?;
            if count == 0 {
                frames
                    .send(serve_session_request::Payload::CloseWrite(true))
                    .await
                    .map_err(|_| Error::Runtime)?;
                return Ok(());
            }
            frames
                .send(serve_session_request::Payload::Data(
                    buffer[..count].to_vec(),
                ))
                .await
                .map_err(|_| Error::Runtime)?;
        }
    };
    let receive_frames = async {
        while let Some(frame) = response.message().await.map_err(status_error)? {
            match frame.payload {
                Some(serve_session_response::Payload::Data(value))
                    if value.len() <= FRAME_BYTES =>
                {
                    write.write_all(&value).await.map_err(|_| Error::Runtime)?;
                }
                Some(serve_session_response::Payload::CloseWrite(true)) => {
                    break;
                }
                _ => return Err(Error::Protocol("invalid session frame")),
            }
        }
        write.shutdown().await.map_err(|_| Error::Runtime)
    };
    tokio::try_join!(send_frames, receive_frames).map(|((), ())| ())
}
