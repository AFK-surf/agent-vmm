use crate::context::core_status;
use agent_vmm_proto::google::rpc;
use prost::Message;
use std::{future::Future, sync::Arc};
use tonic::{Code, Status};
use vmm_core::{BeginOperation, ReceiptState, RequestKey, Store};

/// Commit the attempt before dispatch. Receiver cancellation cannot interrupt settlement.
pub(crate) async fn mutate<R>(
    store: Arc<Store>,
    scope: String,
    domain: &'static str,
    id: String,
    input: Vec<u8>,
    action: impl Future<Output = Result<R, Status>> + Send + 'static,
) -> Result<R, Status>
where
    R: Message + Default + Send + 'static,
{
    if id.is_empty() {
        return Err(Status::invalid_argument("request_id is required"));
    }
    tokio::spawn(async move {
        let key = RequestKey {
            scope_id: &scope,
            domain,
            request_id: &id,
        };
        if let BeginOperation::Replay(old) =
            store.begin_operation(key, &input).map_err(core_status)?
        {
            return match old.state {
                ReceiptState::Succeeded => R::decode(old.response.as_slice())
                    .map_err(|_| Status::internal("invalid stored response")),
                ReceiptState::Failed | ReceiptState::Retryable => {
                    let wire = rpc::Status::decode(old.response.as_slice())
                        .map_err(|_| Status::internal("invalid stored error"))?;
                    Err(Status::with_details(
                        Code::from_i32(wire.code),
                        wire.message,
                        old.response.into(),
                    ))
                }
                ReceiptState::Running | ReceiptState::Interrupted => Err(Status::unavailable(
                    "accepted operation has no terminal receipt",
                )),
                ReceiptState::Fenced => Err(Status::aborted("operation superseded")),
            };
        }
        let response = action.await;
        let (state, bytes) = match &response {
            Ok(result) => (ReceiptState::Succeeded, result.encode_to_vec()),
            Err(status) => {
                let bytes = if status.details().is_empty() {
                    rpc::Status {
                        code: status.code() as i32,
                        message: status.message().into(),
                        details: vec![],
                    }
                    .encode_to_vec()
                } else {
                    status.details().to_vec()
                };
                (ReceiptState::Failed, bytes)
            }
        };
        store
            .finish_operation(key, &input, state, &bytes)
            .map_err(core_status)?;
        response
    })
    .await
    .map_err(|_| Status::internal("operation task interrupted"))?
}
