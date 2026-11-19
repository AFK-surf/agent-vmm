//! Process-local entry point for the iroh service and mesh-control transports.
use crate::{
    ALPNS, Accounting, Command, MAX_STREAMS, MESH_CONTROL_ALPN, PAIRING_ALPN, SERVICE_TUNNEL_ALPN,
    Status, pairing_runtime, probe,
};
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Semaphore, mpsc},
    task::JoinSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    Service,
    MeshControl,
}

impl Channel {
    fn alpn(self) -> &'static [u8] {
        match self {
            Self::Service => SERVICE_TUNNEL_ALPN,
            Self::MeshControl => MESH_CONTROL_ALPN,
        }
    }
}

/// A stream retains the authenticated iroh connection and a share of the node's stream budget.
pub struct Tunnel {
    pub channel: Channel,
    connection: Connection,
    send: SendStream,
    receive: RecvStream,
    permit: StreamPermit,
}

struct StreamPermit(Arc<Accounting>);

impl StreamPermit {
    fn acquire(accounting: &Arc<Accounting>) -> Result<Self> {
        ensure!(accounting.stream_opened(), "data node is draining or full");
        Ok(Self(accounting.clone()))
    }
}
impl Drop for StreamPermit {
    fn drop(&mut self) {
        self.0.stream_closed();
    }
}

impl Tunnel {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    pub async fn bridge(self, io: impl AsyncRead + AsyncWrite) -> Result<()> {
        let Self {
            mut send,
            mut receive,
            permit,
            connection: _connection,
            ..
        } = self;
        let (mut read, mut write) = tokio::io::split(io);
        let (outgoing, incoming) = tokio::try_join!(
            async {
                let bytes = tokio::io::copy(&mut read, &mut send).await?;
                send.finish()?;
                Ok::<_, anyhow::Error>(bytes)
            },
            async {
                let bytes = tokio::io::copy(&mut receive, &mut write).await?;
                write.shutdown().await?;
                Ok::<_, anyhow::Error>(bytes)
            }
        )?;
        permit.0.add_out(outgoing);
        permit.0.add_in(incoming);
        Ok(())
    }
}

#[derive(Clone)]
pub struct DataNode {
    endpoint: Endpoint,
    generation: u64,
    accounting: Arc<Accounting>,
    sessions: pairing_runtime::PairingSessions,
}

impl DataNode {
    pub async fn bind() -> Result<Self> {
        let endpoint = Endpoint::builder(presets::N0)
            .alpns(ALPNS.iter().map(|alpn| alpn.to_vec()).collect())
            .bind()
            .await
            .context("bind iroh endpoint")?;
        Ok(Self {
            endpoint,
            generation: SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as u64,
            accounting: Default::default(),
            sessions: Default::default(),
        })
    }

    pub fn status(&self) -> Status {
        self.accounting
            .status(self.endpoint.addr(), self.generation)
    }

    pub fn drain(&self) {
        self.accounting.drain();
    }

    pub async fn shutdown(&self) {
        self.drain();
        self.endpoint.close().await;
    }

    /// Pairing and status requests use the same registry as incoming connections.
    pub async fn command(&self, command: Command) -> Result<Vec<u8>> {
        let status = match command {
            Command::Status => self.status(),
            Command::Drain => {
                self.drain();
                self.status()
            }
            Command::Probe {
                endpoint_addr,
                payload,
            } => {
                let _permit = StreamPermit::acquire(&self.accounting)?;
                let reply = probe(&self.endpoint, endpoint_addr, payload.as_bytes()).await?;
                self.accounting.add_out(payload.len() as u64);
                self.accounting.add_in(reply.len() as u64);
                let mut status = self.status();
                status.probe_payload = Some(String::from_utf8(reply)?);
                status
            }
            Command::OpenMeshControlTunnel { .. } | Command::OpenServiceTunnel { .. } => {
                anyhow::bail!("stream commands require DataNode::open")
            }
            command => {
                return pairing_runtime::handle_command(command, &self.endpoint, &self.sessions)
                    .await;
            }
        };
        Ok(serde_json::to_vec(&status)?)
    }

    pub async fn open(&self, channel: Channel, addr: EndpointAddr) -> Result<Tunnel> {
        let permit = StreamPermit::acquire(&self.accounting)?;
        tokio::time::timeout(Duration::from_secs(10), async {
            let connection = self.endpoint.connect(addr, channel.alpn()).await?;
            let (send, receive) = connection.open_bi().await?;
            Ok(Tunnel {
                channel,
                connection,
                send,
                receive,
                permit,
            })
        })
        .await
        .context("iroh tunnel connect timed out")?
    }

    /// Delivers application streams to the host's service handlers. Dropping the
    /// receiver or shutting down the endpoint also ends the accept loop.
    pub async fn serve(&self, tx: mpsc::Sender<Tunnel>) {
        let pairing = Arc::new(Semaphore::new(pairing_runtime::PAIRING_INCOMING_LIMIT));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                _ = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = self.endpoint.accept() => {
                    let Some(incoming) = incoming else {break};
                    if !self.accounting.accepting() { continue; }
                    let tx = tx.clone();
                    let node = self.clone();
                    let pairing = pairing.clone();
                    tasks.spawn(async move {
                        if let Ok(connection) = incoming.await {
                            node.accept(connection, tx, pairing).await;
                        }
                    });
                }
            }
        }
        tasks.shutdown().await;
    }

    async fn accept(
        &self,
        connection: Connection,
        tx: mpsc::Sender<Tunnel>,
        pairing: Arc<Semaphore>,
    ) {
        let channel = match connection.alpn() {
            PAIRING_ALPN => {
                let _ =
                    pairing_runtime::handle_incoming(connection, self.sessions.clone(), pairing)
                        .await;
                return;
            }
            SERVICE_TUNNEL_ALPN => Channel::Service,
            MESH_CONTROL_ALPN => Channel::MeshControl,
            _ => return,
        };
        while let Ok((send, receive)) = connection.accept_bi().await {
            let Ok(permit) = StreamPermit::acquire(&self.accounting) else {
                break;
            };
            let tunnel = Tunnel {
                channel,
                connection: connection.clone(),
                send,
                receive,
                permit,
            };
            if tx.try_send(tunnel).is_err() {
                break;
            }
        }
    }

    pub fn incoming() -> (mpsc::Sender<Tunnel>, mpsc::Receiver<Tunnel>) {
        mpsc::channel(MAX_STREAMS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, duplex};

    async fn local_node() -> DataNode {
        DataNode {
            endpoint: Endpoint::builder(presets::Minimal)
                .alpns(ALPNS.iter().map(|alpn| alpn.to_vec()).collect())
                .bind()
                .await
                .unwrap(),
            generation: 1,
            accounting: Default::default(),
            sessions: Default::default(),
        }
    }

    #[tokio::test]
    async fn embedded_nodes_route_both_protocols_and_release_streams() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let host = local_node().await;
            let peer = local_node().await;
            let (tx, mut rx) = DataNode::incoming();
            let server = host.clone();
            let task = tokio::spawn(async move { server.serve(tx).await });
            for channel in [Channel::Service, Channel::MeshControl] {
                let outbound = peer.open(channel, host.endpoint.addr()).await.unwrap();
                let (mut client, socket) = duplex(128);
                let send = tokio::spawn(outbound.bridge(socket));
                client.write_all(b"request").await.unwrap();
                client.shutdown().await.unwrap();
                let incoming = rx.recv().await.unwrap();
                assert_eq!(incoming.channel, channel);
                let (mut service, socket) = duplex(128);
                let receive = tokio::spawn(incoming.bridge(socket));
                let mut request = Vec::new();
                service.read_to_end(&mut request).await.unwrap();
                assert_eq!(request, b"request");
                service.write_all(b"ok").await.unwrap();
                service.shutdown().await.unwrap();
                let mut response = Vec::new();
                client.read_to_end(&mut response).await.unwrap();
                assert_eq!(response, b"ok");
                send.await.unwrap().unwrap();
                receive.await.unwrap().unwrap();
            }
            assert_eq!(host.status().active_streams, 0);
            assert_eq!(peer.status().bytes_out, 14);
            peer.drain();
            assert!(
                peer.open(Channel::Service, host.endpoint.addr())
                    .await
                    .is_err()
            );
            drop(rx);
            task.await.unwrap();
            host.shutdown().await;
            peer.shutdown().await;
        })
        .await
        .unwrap();
    }
}
