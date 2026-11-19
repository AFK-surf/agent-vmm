//! Terminate the service ALPN inside the host. Each iroh stream is one HTTP/2 connection.
use agent_vmm_proto::{
    service::v1::service_route_service_server::{ServiceRouteService, ServiceRouteServiceServer},
    trust::v1::EndpointObservation,
};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    net::UnixStream,
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;
use vmm_compat::{
    node::{Channel, DataNode},
    operator::DeviceTrust,
};

pub(crate) struct ServiceNode {
    node: DataNode,
    signer: Arc<dyn DeviceTrust>,
    observations: watch::Sender<EndpointObservation>,
}

impl ServiceNode {
    pub async fn bind(signer: Arc<dyn DeviceTrust>) -> io::Result<Self> {
        let node = DataNode::bind_service().await.map_err(io::Error::other)?;
        let initial = observation(&node, signer.clone()).await?;
        let (observations, _) = watch::channel(initial);
        Ok(Self {
            node,
            signer,
            observations,
        })
    }

    pub fn observations(&self) -> watch::Receiver<EndpointObservation> {
        self.observations.subscribe()
    }

    pub async fn serve(
        &self,
        routes: impl ServiceRouteService,
        mut stop: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let (tx, mut rx) = DataNode::incoming();
        let (sockets, incoming) = mpsc::channel(32);
        let mut bridges = JoinSet::new();
        let dispatch = async {
            loop {
                tokio::select! {
                    _ = bridges.join_next(), if !bridges.is_empty() => {},
                    tunnel = rx.recv() => {
                        let tunnel = tunnel.ok_or_else(|| io::Error::other("iroh listener closed"))?;
                        if tunnel.channel != Channel::Service { continue; }
                        let (application, transport) = UnixStream::pair()?;
                        sockets.send(Ok::<_,io::Error>(application)).await.map_err(io::Error::other)?;
                        bridges.spawn(async move { let _ = tunnel.bridge(transport).await; });
                    }
                }
            }
        };
        let renew = async {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                self.observations
                    .send_replace(observation(&self.node, self.signer.clone()).await?);
            }
        };
        let server = tonic::transport::Server::builder()
            .max_concurrent_streams(32)
            .add_service(
                ServiceRouteServiceServer::new(routes).max_decoding_message_size(128 << 10),
            )
            .serve_with_incoming(ReceiverStream::new(incoming));
        let result = tokio::select! {
            _ = stop.changed() => Ok(()),
            _ = self.node.serve(tx) => Err(io::Error::other("iroh listener stopped")),
            r = renew => r,
            r = dispatch => r,
            r = server => r.map_err(io::Error::other),
        };
        self.node.shutdown().await;
        result
    }
}

async fn observation(
    node: &DataNode,
    signer: Arc<dyn DeviceTrust>,
) -> io::Result<EndpointObservation> {
    let status = node.status();
    tokio::task::spawn_blocking(move || {
        signer.sign_service_endpoint(&status.endpoint_id, status.endpoint_generation)
    })
    .await
    .map_err(io::Error::other)?
    .map_err(io::Error::other)
}

#[cfg(test)]
#[path = "service_tunnel_tests.rs"]
mod tests;
