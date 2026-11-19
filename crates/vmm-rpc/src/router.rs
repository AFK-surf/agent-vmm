use crate::{Body, Frame, Request, Status, async_trait};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::UnixListener,
    task::JoinSet,
};

#[async_trait]
pub trait Service: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    async fn call(&self, method: &str, request: Request<Body>) -> Result<Body, Status>;
}

#[derive(Clone)]
pub struct Router {
    max_request: usize,
    services: HashMap<&'static str, Arc<dyn Service>>,
}

impl Default for Router {
    fn default() -> Self {
        Self {
            services: HashMap::new(),
            max_request: crate::MAX_FRAME_BYTES,
        }
    }
}

impl Router {
    pub fn max_request_bytes(mut self, bytes: usize) -> Self {
        self.max_request = bytes.min(crate::MAX_FRAME_BYTES);
        self
    }
    pub fn new(service: impl Service) -> Self {
        Self::default().add_service(service)
    }
    pub fn add_service(mut self, service: impl Service) -> Self {
        self.services.insert(service.name(), Arc::new(service));
        self
    }
    pub async fn serve_stream(&self, io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) {
        let (reader, writer) = tokio::io::split(io);
        self.serve_parts(reader, writer).await;
    }
    pub async fn serve_parts(
        &self,
        mut reader: impl AsyncRead + Unpin + Send + 'static,
        mut writer: impl AsyncWrite + Unpin,
    ) {
        let mut between_frames = true;
        let result = async {
            let header =
                tokio::time::timeout(Duration::from_secs(10), Frame::read_header(&mut reader))
                    .await
                    .map_err(|_| Status::deadline_exceeded("RPC header deadline exceeded"))??;
            let (name, method) = header
                .method
                .strip_prefix('/')
                .and_then(|p| p.split_once('/'))
                .ok_or_else(|| Status::unimplemented("unknown method"))?;
            let service = self
                .services
                .get(name)
                .ok_or_else(|| Status::unimplemented("unknown service"))?;
            let request = Request {
                message: Body::frames(reader, false, self.max_request),
                metadata: header.metadata,
                extensions: Default::default(),
            };
            let dispatch = async {
                let mut response = service.call(method, request).await?;
                while let Some(bytes) = response.message().await? {
                    if bytes.len() > crate::MAX_FRAME_BYTES {
                        return Err(Status::resource_exhausted(
                            "response exceeds frame size limit",
                        ));
                    }
                    between_frames = false;
                    let frame = Frame::Message(bytes);
                    if let Some(signal) = response.1.clone() {
                        tokio::select! {
                            biased;
                            status = signal => return Err(status),
                            result = frame.write(&mut writer) => result?,
                        }
                    } else {
                        frame.write(&mut writer).await?;
                    }
                    between_frames = true;
                }
                Ok(())
            };
            if let Some(ms) = header.timeout_ms {
                tokio::time::timeout(Duration::from_millis(ms), dispatch)
                    .await
                    .map_err(|_| Status::deadline_exceeded("RPC deadline exceeded"))?
            } else {
                dispatch.await
            }
        }
        .await;
        // A cancelled partial write cannot be followed by another frame.
        if between_frames {
            let _ = Frame::Status(result.err().unwrap_or_else(|| Status::ok("")))
                .write(&mut writer)
                .await;
        }
        let _ = writer.shutdown().await;
    }

    pub async fn serve_unix(
        self,
        listener: UnixListener,
        limit: usize,
        shutdown: impl Future<Output = ()>,
    ) -> std::io::Result<()> {
        let mut calls = JoinSet::new();
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                _ = calls.join_next(), if !calls.is_empty() => {},
                incoming = listener.accept(), if calls.len() < limit => {
                    let (io, _) = incoming?;
                    let router = self.clone();
                    let monitor = std::os::fd::AsFd::as_fd(&io).try_clone_to_owned()?;
                    calls.spawn(async move {
                        tokio::select! {
                            _ = router.serve_stream(io) => {},
                            _ = unix_peer_gone(monitor) => {},
                        }
                    });
                }
            }
        }
        calls.shutdown().await;
        Ok(())
    }
}

// A zero-byte send distinguishes a closed peer from shutdown(Write) without
// consuming a frame or adding bytes to an idle subscription.
async fn unix_peer_gone(socket: std::os::fd::OwnedFd) {
    use std::io::{ErrorKind, Write};
    let mut socket = std::os::unix::net::UnixStream::from(socket);
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        match socket.write(&[]) {
            Ok(_) => {}
            Err(err) if matches!(err.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) => {}
            Err(_) => return,
        }
    }
}

pub struct Intercepted<S, I> {
    service: S,
    interceptor: I,
}
impl<S, I> Intercepted<S, I> {
    pub fn new(service: S, interceptor: I) -> Self {
        Self {
            service,
            interceptor,
        }
    }
}
#[async_trait]
impl<S: Service, I: crate::Interceptor + Clone + 'static> Service for Intercepted<S, I> {
    fn name(&self) -> &'static str {
        self.service.name()
    }
    async fn call(&self, method: &str, request: Request<Body>) -> Result<Body, Status> {
        let Request {
            message,
            metadata,
            extensions,
        } = request;
        let context = self.interceptor.clone().call(Request {
            message: (),
            metadata,
            extensions,
        })?;
        self.service.call(method, context.map(|()| message)).await
    }
}
