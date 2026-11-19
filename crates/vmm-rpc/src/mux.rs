use crate::{Client, Router, Status, Transport};
use std::{future::poll_fn, task::Poll};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

pub use yamux::Mode;
type Open = oneshot::Sender<Result<yamux::Stream, Status>>;

#[derive(Clone)]
pub struct MuxSession {
    open: mpsc::Sender<Open>,
    driver: tokio::task::AbortHandle,
}

impl MuxSession {
    pub fn start(
        io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        mode: Mode,
        router: Router,
    ) -> Self {
        let mut config = yamux::Config::default();
        config
            .set_split_send_size(8 * 1024)
            .set_max_num_streams(2048);
        let mut connection = yamux::Connection::new(io.compat(), config, mode);
        let (open, mut requests) = mpsc::channel::<Open>(128);
        let driver = tokio::spawn(async move {
            let mut pending: Option<Open> = None;
            let mut inbound = JoinSet::new();
            let mut requests_closed = false;
            loop {
                let next = poll_fn(|cx| {
                    if pending.is_none() && !requests_closed {
                        match requests.poll_recv(cx) {
                            Poll::Ready(Some(reply)) => pending = Some(reply),
                            Poll::Ready(None) => requests_closed = true,
                            Poll::Pending => {}
                        }
                    }
                    if pending.is_some()
                        && let Poll::Ready(result) = connection.poll_new_outbound(cx)
                    {
                        let _ = pending
                            .take()
                            .unwrap()
                            .send(result.map_err(|e| Status::unavailable(e.to_string())));
                        cx.waker().wake_by_ref();
                    }
                    connection.poll_next_inbound(cx)
                });
                tokio::select! {
                    stream = next => match stream {
                        Some(Ok(stream)) => {
                            let router = router.clone();
                            inbound.spawn(async move { router.serve_stream(stream.compat()).await; });
                        }
                        _ => break,
                    },
                    _ = inbound.join_next(), if !inbound.is_empty() => {}
                }
            }
            inbound.shutdown().await;
        });
        Self {
            open,
            driver: driver.abort_handle(),
        }
    }

    pub fn client(&self) -> Client {
        let open = self.open.clone();
        Client::new(move || {
            let open = open.clone();
            Box::pin(async move {
                let (reply, response) = oneshot::channel();
                open.send(reply)
                    .await
                    .map_err(|_| Status::unavailable("multiplexed connection closed"))?;
                let stream = response
                    .await
                    .map_err(|_| Status::unavailable("multiplexed connection closed"))??;
                Ok(Box::new(stream.compat()) as Transport)
            })
        })
    }
    pub fn close(&self) {
        self.driver.abort();
    }

    pub async fn closed(&self) {
        self.open.closed().await;
    }
}
