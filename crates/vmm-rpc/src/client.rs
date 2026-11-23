use crate::{Body, Frame, Metadata, Request, RequestHeader, Response, Status, Streaming};
use futures::{Stream, StreamExt, future::BoxFuture, stream};
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

pub trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}
pub type Transport = Box<dyn Duplex>;
type Authorize = dyn Fn(&mut Metadata) -> Result<(), Status> + Send + Sync;
type Dial = dyn Fn() -> BoxFuture<'static, Result<Transport, Status>> + Send + Sync;

#[derive(Clone)]
pub struct Client {
    dial: Arc<Dial>,
    authorize: Option<Arc<Authorize>>,
}

impl Client {
    pub fn new(
        dial: impl Fn() -> BoxFuture<'static, Result<Transport, Status>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            dial: Arc::new(dial),
            authorize: None,
        }
    }
    pub fn authorized(
        mut self,
        authorize: impl Fn(&mut Metadata) -> Result<(), Status> + Send + Sync + 'static,
    ) -> Self {
        self.authorize = Some(Arc::new(authorize));
        self
    }
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self::new(move || {
            let path = path.clone();
            Box::pin(async move {
                Ok(Box::new(tokio::net::UnixStream::connect(path).await?) as Transport)
            })
        })
    }
    pub async fn call<T, U, S>(
        &self,
        method: &str,
        mut request: Request<S>,
    ) -> Result<Response<Streaming<U>>, Status>
    where
        T: Message + Send + 'static,
        U: Message + Default + Send + 'static,
        S: Stream<Item = T> + Send + 'static,
    {
        if let Some(authorize) = &self.authorize {
            authorize(request.metadata_mut())?;
        }
        let io = (self.dial)().await?;
        let (reader, mut writer) = tokio::io::split(io);
        Frame::Header(RequestHeader {
            v: 1,
            method: method.into(),
            metadata: request.metadata,
            timeout_ms: None,
        })
        .write(&mut writer)
        .await?;
        let mut messages = Box::pin(request.message);
        let sending = async move {
            while let Some(message) = messages.next().await {
                Frame::Message(message.encode_to_vec().into())
                    .write(&mut writer)
                    .await?;
            }
            Frame::End.write(&mut writer).await?;
            match writer.shutdown().await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotConnected => {}
                Err(err) => return Err(err.into()),
            }
            Ok::<_, Status>(())
        };
        let (sent, done) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            let _ = sent.send(sending.await);
        });
        let guard = tokio_util::task::AbortOnDropHandle::new(writer);
        let response = Body::frames(reader, true, crate::MAX_FRAME_BYTES).decode::<U>();
        let output = stream::try_unfold(
            (response, Some(done), guard),
            |(mut response, mut done, guard)| async move {
                let message = if let Some(mut sending) = done.take() {
                    tokio::select! {
                        biased;
                        msg = response.message() => { done = Some(sending); msg? },
                        result = &mut sending => {
                            let send_result =
                                result.map_err(|_| Status::unavailable("request writer stopped"));
                            match response.message().await {
                                Ok(Some(msg)) => Some(msg),
                                Ok(None) => {
                                    send_result??;
                                    None
                                }
                                Err(err)
                                    if err.code() == crate::Code::Unavailable
                                        || (err.code() == crate::Code::DataLoss
                                            && err.message() == "truncated RPC stream") =>
                                {
                                    send_result??;
                                    return Err(err);
                                }
                                Err(err) => return Err(err),
                            }
                        }
                    }
                } else {
                    response.message().await?
                };
                Ok(message.map(|m| (m, (response, done, guard))))
            },
        );
        Ok(Response::new(Streaming::new(output)))
    }
}
