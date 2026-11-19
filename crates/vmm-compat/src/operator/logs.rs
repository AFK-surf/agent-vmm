//! Operator log queries select an environment on the private host socket.
use super::{LocalOperator, h};
use tokio_stream::StreamExt;
use vmm_rpc::{Body, Status};

pub(super) async fn logs(
    operator: &LocalOperator,
    request: h::OperatorServiceLogsRequest,
) -> Result<Body, Status> {
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
        crate::guest::logs_in_scope(store, &request.environment_id, guest.as_ref(), query).await?;
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
    Ok(Body::encode(output))
}
