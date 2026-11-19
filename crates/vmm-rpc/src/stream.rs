use crate::{Bytes, Code, Frame, Request, Response, Status};
use futures::{
    FutureExt, Stream, StreamExt, TryStreamExt,
    future::{BoxFuture, Shared},
    stream::{self, BoxStream},
};
use prost::Message;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncRead;

type Cancellation = Shared<BoxFuture<'static, Status>>;
pub struct Streaming<T>(
    BoxStream<'static, Result<T, Status>>,
    pub(crate) Option<Cancellation>,
);
pub type Body = Streaming<Bytes>;

impl<T: Send + 'static> Streaming<T> {
    pub fn new(s: impl Stream<Item = Result<T, Status>> + Send + 'static) -> Self {
        Self(s.boxed(), None)
    }
    pub async fn message(&mut self) -> Result<Option<T>, Status> {
        self.try_next().await
    }
    pub fn until_error(
        self,
        cancelled: impl std::future::Future<Output = Status> + Send + 'static,
    ) -> Self {
        let signal = cancelled.boxed().shared();
        let mut result = Self::new(stream::try_unfold(
            (self, signal.clone()),
            |(mut stream, mut cancelled)| async move {
                tokio::select! {
                    biased;
                    status = &mut cancelled => Err(status),
                    item = stream.message() => Ok(item?.map(|value| (value, (stream, cancelled)))),
                }
            },
        ));
        result.1 = Some(signal);
        result
    }
    pub async fn single(mut self) -> Result<T, Status> {
        let message = self
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing unary message"))?;
        if self.message().await?.is_some() {
            return Err(Status::invalid_argument("multiple unary messages"));
        }
        Ok(message)
    }
}

impl<T> Stream for Streaming<T> {
    type Item = Result<T, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.as_mut().poll_next(cx)
    }
}

impl Body {
    pub(crate) fn frames(
        reader: impl AsyncRead + Unpin + Send + 'static,
        response: bool,
        max_message: usize,
    ) -> Self {
        Self::new(stream::try_unfold(reader, move |mut r| async move {
            match Frame::read_body(&mut r, response, max_message).await? {
                Frame::Message(bytes) => Ok(Some((bytes, r))),
                Frame::End if !response => Ok(None),
                Frame::Status(s) if response && s.code == Code::Ok => Ok(None),
                Frame::Status(s) if response => Err(s),
                _ => Err(Status::invalid_argument(
                    "unexpected frame in message stream",
                )),
            }
        }))
    }
    pub fn decode<T: Message + Default + Send + 'static>(self) -> Streaming<T> {
        Streaming::new(self.and_then(|b| async move {
            T::decode(b).map_err(|_| Status::invalid_argument("invalid protobuf message"))
        }))
    }
    pub fn encode<T: Message + Send + 'static>(
        s: impl Stream<Item = Result<T, Status>> + Send + 'static,
    ) -> Self {
        Self::new(s.map_ok(|m| m.encode_to_vec().into()))
    }
}

impl Request<Body> {
    pub async fn unary<T: Message + Default + Send + 'static>(self) -> Result<Request<T>, Status> {
        let Request {
            message,
            metadata,
            extensions,
        } = self;
        Ok(Request {
            message: message.decode().single().await?,
            metadata,
            extensions,
        })
    }
    pub fn streaming<T: Message + Default + Send + 'static>(self) -> Request<Streaming<T>> {
        self.map(Body::decode)
    }
}

impl<T: Message + Send + 'static> Response<T> {
    pub fn unary_body(self) -> Body {
        Body::encode(stream::once(async move { Ok(self.into_inner()) }))
    }
}

impl<T> std::fmt::Debug for Streaming<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Streaming")
    }
}
