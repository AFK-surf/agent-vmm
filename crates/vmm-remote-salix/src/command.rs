use crate::{
    Connector, Result,
    allocation::{execute, failure, success},
    control::send,
    remote::{
        self, CommandAck, CommandAckDisposition, CommandOutcome, CommandResult, ControllerCommand,
        ErrorReason, command_result, connect_request, controller_command::Command,
    },
    sessions::Sessions,
};
use prost::Message;
use std::{collections::VecDeque, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, timeout_at},
};
use vmm_core::{BeginOperation, Channel, ReceiptState, RequestKey, Store};

const RECEIPT_DOMAIN: &str = "salix-control";
const REPLAY_WINDOW: usize = 128;

#[derive(Default)]
pub(crate) struct Commands {
    accepted: VecDeque<(u64, String)>,
    latest: u64,
}

impl Commands {
    pub async fn process(
        &mut self,
        connector: &Connector,
        channel: &Channel,
        command: ControllerCommand,
        outbound: &mpsc::Sender<remote::ConnectRequest>,
        sessions: &mut Sessions<'_>,
    ) -> Result<()> {
        let received = Instant::now();
        let mut ack = CommandAck {
            command_id: command.command_id.clone(),
            sequence: command.sequence,
            connection_epoch: channel.connection_epoch,
            disposition: CommandAckDisposition::Rejected.into(),
            ..Default::default()
        };
        if let Err(reason) = self.validate(channel, &command) {
            return reject(outbound, ack, reason).await;
        }

        let store = &connector.core.store;
        let key = RequestKey {
            domain: RECEIPT_DOMAIN,
            scope_id: &channel.registration_id,
            request_id: &command.command_id,
        };
        // Delivery identity and retry timeout are allowed to change when Salix reclaims a command.
        let request = ControllerCommand {
            target_revision: command.target_revision,
            command: command.command.clone(),
            ..Default::default()
        }
        .encode_to_vec();
        // A replacement controller must never begin effects through an older local fence.
        store.remote_inventory(channel, 0)?;
        let (cached, record) = match store.begin_operation(key, &request) {
            Ok(BeginOperation::Accepted) => {
                ack.disposition = CommandAckDisposition::Admitted.into();
                (None, true)
            }
            Ok(BeginOperation::Replay(receipt)) => {
                ack.disposition = CommandAckDisposition::Duplicate.into();
                match receipt.state {
                    ReceiptState::Succeeded | ReceiptState::Failed => {
                        let result = CommandResult::decode(&*receipt.response)?;
                        if let Some(Command::OpenSession(open)) = &command.command
                            && receipt.state == ReceiptState::Succeeded
                        {
                            let replay = sessions.replay(&command.command_id, &open.allocation_id);
                            let result = if let Some(ready) = replay {
                                Some(success(command_result::Result::SessionReady(ready)))
                            } else if result.connection_epoch == channel.connection_epoch {
                                Some(failure(ErrorReason::Unavailable, CommandOutcome::Unknown))
                            } else {
                                None
                            };
                            (result, false)
                        } else {
                            (Some(result), false)
                        }
                    }
                    ReceiptState::Interrupted | ReceiptState::Retryable => {
                        store.resume_operation(key, &request)?;
                        (None, true)
                    }
                    _ => (
                        Some(failure(ErrorReason::Unavailable, CommandOutcome::Unknown)),
                        false,
                    ),
                }
            }
            Err(vmm_core::Error::Conflict(_)) => {
                return reject(outbound, ack, ErrorReason::SequenceConflict).await;
            }
            Err(vmm_core::Error::Fenced) => {
                return reject(outbound, ack, ErrorReason::StaleRevision).await;
            }
            Err(error) => return Err(error.into()),
        };
        let mut pending = PendingReceipt {
            store,
            key,
            request: &request,
            running: record,
        };
        send(outbound, connect_request::Payload::Ack(ack)).await?;

        self.remember(&command);
        let mut result = if let Some(result) = cached {
            result
        } else {
            let timeout = Duration::from_millis(command.timeout_millis);
            match timeout_at(
                received + timeout,
                execute(connector, channel, &command, sessions),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => failure(ErrorReason::DeadlineExceeded, CommandOutcome::Unknown),
            }
        };
        result.command_id.clone_from(&command.command_id);
        result.sequence = command.sequence;
        result.connection_epoch = channel.connection_epoch;
        if record {
            let state = match result.outcome() {
                CommandOutcome::Succeeded => ReceiptState::Succeeded,
                CommandOutcome::Rejected => ReceiptState::Failed,
                _ => ReceiptState::Retryable,
            };
            // A replay obtains its bearer from the live connection's session table.
            let mut receipt = result.clone();
            if let Some(command_result::Result::SessionReady(session)) = &mut receipt.result {
                session.tunnel_nonce.clear();
            }
            pending.finish(state, &receipt.encode_to_vec())?;
        }
        result.inventory_watermark = store.remote_inventory(channel, 0)?.watermark;
        send(outbound, connect_request::Payload::Result(result)).await
    }

    fn validate(
        &mut self,
        channel: &Channel,
        command: &ControllerCommand,
    ) -> std::result::Result<(), ErrorReason> {
        use ErrorReason::*;
        if command.command_id.is_empty() || command.command_id.len() > 128 {
            return Err(InvalidArgument);
        }
        if command.connection_epoch != channel.connection_epoch {
            return Err(StaleConnectionEpoch);
        }
        if command.sequence == 0 {
            return Err(SequenceConflict);
        }
        if command.sequence <= self.latest {
            if self
                .accepted
                .iter()
                .any(|(seq, id)| *seq == command.sequence && id == &command.command_id)
            {
                return Ok(());
            }
            return Err(SequenceConflict);
        }
        if self.latest.checked_add(1) != Some(command.sequence) {
            return Err(SequenceConflict);
        }
        self.latest = command.sequence;
        if command.timeout_millis == 0 || command.timeout_millis > (i64::MAX as u64 / 1_000_000) {
            return Err(InvalidArgument);
        }
        match &command.command {
            Some(Command::EnsureAllocation(value)) => {
                if !id_valid(&value.allocation_id)
                    || value.generation == 0
                    || value.limits.is_none()
                {
                    return Err(InvalidArgument);
                }
                if value.policy_revision == 0 || command.target_revision != value.policy_revision {
                    return Err(StaleRevision);
                }
            }
            Some(Command::OpenSession(value)) => {
                if !id_valid(&value.allocation_id)
                    || value.allocation_generation == 0
                    || value.execution_owner_id.is_empty()
                    || value.execution_owner_id.len() > 256
                {
                    return Err(InvalidArgument);
                }
            }
            Some(Command::ReleaseAllocation(value)) => {
                validate_retirement(
                    command,
                    &value.allocation_id,
                    value.expected_generation,
                    value.expected_revision,
                )?;
            }
            Some(Command::DisposeAllocation(value)) => {
                validate_retirement(
                    command,
                    &value.allocation_id,
                    value.expected_generation,
                    value.expected_revision,
                )?;
                if value.request_id.is_empty() || value.request_id.len() > 128 {
                    return Err(InvalidArgument);
                }
            }
            _ => return Err(InvalidArgument),
        }
        Ok(())
    }

    fn remember(&mut self, command: &ControllerCommand) {
        if self
            .accepted
            .back()
            .is_some_and(|(seq, _)| *seq >= command.sequence)
        {
            return;
        }
        if self.accepted.len() == REPLAY_WINDOW {
            self.accepted.pop_front();
        }
        self.accepted
            .push_back((command.sequence, command.command_id.clone()));
    }
}

struct PendingReceipt<'a> {
    store: &'a Store,
    key: RequestKey<'a>,
    request: &'a [u8],
    running: bool,
}

impl PendingReceipt<'_> {
    fn finish(&mut self, state: ReceiptState, response: &[u8]) -> Result<()> {
        self.store
            .finish_operation(self.key, self.request, state, response)?;
        self.running = false;
        Ok(())
    }
}

impl Drop for PendingReceipt<'_> {
    fn drop(&mut self) {
        if self.running {
            let _ = self.finish(ReceiptState::Retryable, &[]);
        }
    }
}

fn id_valid(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}

fn validate_retirement(
    command: &ControllerCommand,
    id: &str,
    generation: u64,
    revision: u64,
) -> std::result::Result<(), ErrorReason> {
    if !id_valid(id) || generation == 0 || revision == 0 || command.target_revision != revision {
        return Err(ErrorReason::StaleRevision);
    }
    Ok(())
}

async fn reject(
    outbound: &mpsc::Sender<remote::ConnectRequest>,
    mut ack: CommandAck,
    reason: ErrorReason,
) -> Result<()> {
    ack.reason = reason.into();
    send(outbound, connect_request::Payload::Ack(ack)).await
}
