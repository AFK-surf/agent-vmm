use crate::{ALPN, Mode, MuxSession, Router};
use std::{io, sync::Arc};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self, ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
};

#[derive(Clone)]
pub struct ServerTls(TlsAcceptor);

impl ServerTls {
    pub fn new(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> io::Result<Self> {
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(io::Error::other)?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(io::Error::other)?;
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(Self(TlsAcceptor::from(Arc::new(config))))
    }

    pub async fn accept(&self, socket: TcpStream, router: Router) -> io::Result<MuxSession> {
        let stream = self.0.accept(socket).await?;
        if stream.get_ref().1.alpn_protocol() != Some(ALPN) {
            return Err(io::Error::other("missing RPC ALPN"));
        }
        Ok(MuxSession::start(stream, Mode::Server, router))
    }
}
