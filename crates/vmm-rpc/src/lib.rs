#![forbid(unsafe_code)]

mod frame;
mod status;

pub use async_trait::async_trait;
pub use bytes::Bytes;
pub use frame::{Frame, MAX_FRAME_BYTES, MAX_HEADER_BYTES, RequestHeader};
pub use futures::{Stream, StreamExt, stream::BoxStream};
pub use http::Extensions;
pub use status::{Code, Status};
pub const ALPN: &[u8] = b"agent-vmm/rpc/1";

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Metadata(Vec<(String, String)>);

impl Metadata {
    pub fn append(&mut self, key: &str, value: impl Into<String>) {
        self.0.push((key.to_ascii_lowercase(), value.into()));
    }

    pub fn insert(&mut self, key: &str, value: impl Into<String>) {
        self.0.retain(|(name, _)| !name.eq_ignore_ascii_case(key));
        self.append(key, value);
    }

    pub fn get_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> {
        self.0
            .iter()
            .filter(move |(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    }
}

pub struct Request<T> {
    message: T,
    metadata: Metadata,
    extensions: Extensions,
}

impl<T> Request<T> {
    pub fn new(message: T) -> Self {
        Self {
            message,
            metadata: Metadata::default(),
            extensions: Extensions::new(),
        }
    }
    pub fn get_ref(&self) -> &T {
        &self.message
    }
    pub fn into_inner(self) -> T {
        self.message
    }
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Request<U> {
        Request {
            message: f(self.message),
            metadata: self.metadata,
            extensions: self.extensions,
        }
    }
}

#[derive(Debug)]
pub struct Response<T>(T);
impl<T> Response<T> {
    pub fn new(message: T) -> Self {
        Self(message)
    }
    pub fn into_inner(self) -> T {
        self.0
    }
    pub fn get_ref(&self) -> &T {
        &self.0
    }
}

pub trait IntoRequest<T> {
    fn into_request(self) -> Request<T>;
}
impl<T> IntoRequest<T> for Request<T> {
    fn into_request(self) -> Request<T> {
        self
    }
}
impl<T> IntoRequest<T> for T {
    fn into_request(self) -> Request<T> {
        Request::new(self)
    }
}

pub trait Interceptor: Send + Sync {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status>;
}

mod client;
mod router;
mod stream;
mod sync;
pub use client::{Client, Duplex, Transport};
pub use router::{Intercepted, Router, Service};
pub use stream::{Body, Streaming};
pub use sync::sync_unary;
mod mux;
pub use mux::{Mode, MuxSession};

#[cfg(test)]
mod tests;

pub use futures::stream::once;
pub trait IntoStreamingRequest<T> {
    type Stream: Stream<Item = T> + Send + 'static;
    fn into_streaming_request(self) -> Request<Self::Stream>;
}
impl<T, S: Stream<Item = T> + Send + 'static> IntoStreamingRequest<T> for S {
    type Stream = S;
    fn into_streaming_request(self) -> Request<S> {
        Request::new(self)
    }
}
impl<T, S: Stream<Item = T> + Send + 'static> IntoStreamingRequest<T> for Request<S> {
    type Stream = S;
    fn into_streaming_request(self) -> Request<S> {
        self
    }
}
mod tls;
pub use tls::ServerTls;
