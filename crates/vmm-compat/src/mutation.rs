use crate::{context::core_status, error::operation_in_progress};
use agent_vmm_proto::google::rpc;
use prost::Message;
use std::{future::Future, sync::Arc};
use vmm_core::{BeginOperation, ReceiptState, RequestKey, Store};
use vmm_rpc::{Code, Status};

pub(crate) struct ReentrantFailure {
    pub status: Status,
    pub terminal: bool,
}

impl ReentrantFailure {
    pub(crate) fn terminal(status: Status) -> Self {
        Self {
            status,
            terminal: true,
        }
    }

    pub(crate) fn retryable(status: Status) -> Self {
        Self {
            status,
            terminal: false,
        }
    }
}

pub(crate) fn is_terminal_core_error(error: &vmm_core::Error) -> bool {
    matches!(
        error,
        vmm_core::Error::Invalid(_) | vmm_core::Error::Conflict("request fingerprint")
    )
}

fn encode_status(status: &Status) -> Vec<u8> {
    if status.details().is_empty() {
        rpc::Status {
            code: status.code() as i32,
            message: status.message().into(),
            details: vec![],
        }
        .encode_to_vec()
    } else {
        status.details().to_vec()
    }
}

fn decode_status(bytes: &[u8]) -> Result<Status, Status> {
    let wire = rpc::Status::decode(bytes).map_err(|_| Status::internal("invalid stored error"))?;
    Ok(Status::with_details(
        Code::from_i32(wire.code),
        wire.message,
        bytes.to_vec().into(),
    ))
}

struct PanicRetryableGuard {
    store: Arc<Store>,
    scope: String,
    domain: &'static str,
    id: String,
    input: Vec<u8>,
    armed: bool,
}

impl Drop for PanicRetryableGuard {
    fn drop(&mut self) {
        if self.armed {
            let key = RequestKey {
                scope_id: &self.scope,
                domain: self.domain,
                request_id: &self.id,
            };
            let bytes = encode_status(&Status::unavailable("operation interrupted"));
            let _ = self
                .store
                .finish_operation(key, &self.input, ReceiptState::Retryable, &bytes);
        }
    }
}

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
                    Err(decode_status(old.response.as_slice())?)
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
            Err(status) => (ReceiptState::Failed, encode_status(status)),
        };
        store
            .finish_operation(key, &input, state, &bytes)
            .map_err(core_status)?;
        response
    })
    .await
    .map_err(|_| Status::internal("operation task interrupted"))?
}

/// Re-entrant idempotent mutation for resumable image domains (`runtime-import-image`,
/// `runtime-pull-image`). Preserves `Failed` only for deterministic invalid input/artifacts;
/// records all transient/storage failures as `Retryable` so retries resume via `resume_operation`.
pub(crate) async fn mutate_reentrant<R>(
    store: Arc<Store>,
    scope: String,
    domain: &'static str,
    stage: &'static str,
    id: String,
    input: Vec<u8>,
    action: impl Future<Output = Result<R, ReentrantFailure>> + Send + 'static,
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
        match store.begin_operation(key, &input).map_err(core_status)? {
            BeginOperation::Accepted => {}
            BeginOperation::Replay(old) => match old.state {
                ReceiptState::Succeeded => {
                    return R::decode(old.response.as_slice())
                        .map_err(|_| Status::internal("invalid stored response"));
                }
                ReceiptState::Failed => {
                    return Err(decode_status(old.response.as_slice())?);
                }
                ReceiptState::Running => {
                    return Err(operation_in_progress(stage));
                }
                ReceiptState::Retryable | ReceiptState::Interrupted => {
                    match store.resume_operation(key, &input) {
                        Ok(()) => {}
                        Err(vmm_core::Error::Conflict("operation cannot be resumed")) => {
                            return Err(operation_in_progress(stage));
                        }
                        Err(err) => return Err(core_status(err)),
                    }
                }
                ReceiptState::Fenced => {
                    return Err(Status::aborted("operation superseded"));
                }
            },
        }
        let mut guard = PanicRetryableGuard {
            store: store.clone(),
            scope: scope.clone(),
            domain,
            id: id.clone(),
            input: input.clone(),
            armed: true,
        };
        let response = action.await;
        guard.armed = false;
        let (state, bytes, result) = match response {
            Ok(val) => {
                let encoded = val.encode_to_vec();
                (ReceiptState::Succeeded, encoded, Ok(val))
            }
            Err(failure) => {
                let state = if failure.terminal {
                    ReceiptState::Failed
                } else {
                    ReceiptState::Retryable
                };
                let encoded = encode_status(&failure.status);
                (state, encoded, Err(failure.status))
            }
        };
        store
            .finish_operation(key, &input, state, &bytes)
            .map_err(core_status)?;
        result
    })
    .await
    .map_err(|_| Status::unavailable("operation task interrupted"))?
}
