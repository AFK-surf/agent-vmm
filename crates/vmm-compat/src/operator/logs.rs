//! Operator log queries select an environment on the private host socket.
use super::{LocalOperator, h};
use std::{pin::Pin, sync::Arc};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, codegen::BoxFuture, server::ServerStreamingService};

pub(super) struct OperatorLogs(pub Arc<LocalOperator>);

impl ServerStreamingService<h::OperatorServiceLogsRequest> for OperatorLogs {
    type Response = h::OperatorServiceLogsResponse;
    type ResponseStream = Pin<Box<dyn Stream<Item = Result<Self::Response, Status>> + Send>>;
    type Future = BoxFuture<Response<Self::ResponseStream>, Status>;

    fn call(&mut self, request: Request<h::OperatorServiceLogsRequest>) -> Self::Future {
        let operator = self.0.clone();
        Box::pin(async move {
            let request = request.into_inner();
            let store = operator.inventory.store.clone();
            store
                .scope(&request.environment_id)
                .map_err(crate::error::core_status)?;
            let query = request
                .query
                .ok_or_else(|| Status::invalid_argument("log query is required"))?;
            let guest = operator
                .guest
                .as_ref()
                .ok_or_else(|| Status::unavailable("guest runtime is unavailable"))?;
            let logs =
                crate::guest::logs_in_scope(store, &request.environment_id, guest.as_ref(), query)
                    .await?;
            let output = logs.map(|record| {
                record.map(|r| h::OperatorServiceLogsResponse {
                    cursor: r.cursor,
                    timestamp: r.timestamp,
                    stream: r.stream,
                    data: r.data,
                    truncated: r.truncated,
                    dropped_records: r.dropped_records,
                })
            });
            Ok(Response::new(Box::pin(output) as Self::ResponseStream))
        })
    }
}
