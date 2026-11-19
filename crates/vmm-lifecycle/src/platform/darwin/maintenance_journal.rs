//! Journal reads and admission checks of Host maintenance.

use super::maintenance::{MaintenanceReceipt, MaintenanceRequest, Outcome, mode_name};
use crate::cutover::TARGET_OLDER_THAN_DATA;
use crate::error::{Result, err};
use crate::journal::{self, Journal, MaintenanceState, receipt_name};
use crate::{Context, Paths, valid_id};

const UNAVAILABLE: &str = "accepted maintenance receipt is unavailable; keep the owner journal for recovery";

/// A data reset fences every old install request before it removes their plans,
/// so a retry with an old identity cannot become a new enrollment.
pub fn collect_install_ids(context: &Context, paths: &Paths, journal: &Journal, state: &mut MaintenanceState) -> Result<()> {
    if let Some(old) = journal::read_latest(context, paths).filter(|old| old.kind == "install-managed") {
        state.blocked_installs.insert(old.request_id, true);
    }
    for directory in ["install-operations", "install-completions"] {
        let names = match journal.dir().child(directory) {
            Ok(directory) => directory.names()?,
            Err(error) if error.not_found() => continue,
            Err(error) => return Err(error),
        };
        for name in names {
            let id = if directory == "install-completions" { name.strip_suffix(".json").unwrap_or(&name) } else { &name };
            if valid_id(id) {
                state.blocked_installs.insert(id.to_string(), true);
            }
        }
    }
    Ok(())
}

/// Records a terminal refusal that made no effect. The owner state stays as it
/// was, so the current Host and an active request are not disturbed.
pub fn refuse(journal: &Journal, paths: &Paths, request: &MaintenanceRequest, message: &str) -> Outcome {
    let receipt = MaintenanceReceipt { failure_code: TARGET_OLDER_THAN_DATA.into(), failure_message: message.into(), ..MaintenanceReceipt::new(paths, request, "failed") };
    match journal.write(&receipt_name(&request.request_id), &receipt) {
        Ok(()) => (Some(receipt), Err(err(message))),
        Err(error) => (Some(receipt), Err(error)),
    }
}

/// The caller holds the owner lock, so the executor of the replaced request is gone.
pub fn validate_supersession(journal: &Journal, paths: &Paths, request: &MaintenanceRequest, state: &MaintenanceState) -> Result<()> {
    let previous_id = &request.supersedes_request_id;
    if state.active_request != *previous_id || state.latest_request != *previous_id || state.completed(previous_id) || !state.superseded_by(previous_id).is_empty() {
        return Err(err("only the exact active maintenance request can be replaced"));
    }
    let previous: MaintenanceReceipt = journal.read(&receipt_name(previous_id))?;
    let retryable_failure = previous.outcome == "failed" && previous.retryable;
    let same_host = previous.request.target_app == request.target_app && previous.service_mode == mode_name(paths.service.mode) && previous.service_user == paths.service.user;
    if previous.request.version != 1 || previous.request.request_id != *previous_id || !retryable_failure || !same_host {
        return Err(err("replacement requires a retryable failed receipt for the same Host and service"));
    }
    Ok(())
}

/// Reads the receipt of `id`, or of the latest request. The owner state decides
/// what a receipt means: a success counts only after its completion commit.
pub fn status(context: &Context, paths: &Paths, id: &str) -> Result<Option<MaintenanceReceipt>> {
    let journal = match Journal::open(context, paths) {
        Ok(journal) => journal,
        Err(error) if error.not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    let state = journal.state()?;
    let id = if id.is_empty() { state.latest_request.as_str() } else { id };
    if id.is_empty() {
        return Ok(None);
    }
    let mut receipt: MaintenanceReceipt = match journal.read(&receipt_name(id)) {
        Ok(receipt) => receipt,
        Err(error) if error.not_found() && state.accepted(id) => return Err(err(UNAVAILABLE)),
        Err(error) if error.not_found() => return Ok(None),
        Err(error) => return Err(error),
    };
    let next = state.superseded_by(id);
    if receipt.request.version == 0 && receipt.request.action.is_empty() {
        // The receipt of a registration or service request, in the common fields.
        let operation = journal.receipt(id)?;
        receipt.request.version = 1;
        receipt.request.action = operation.kind.clone();
        receipt.request.request_id = operation.request_id;
        receipt.request.target_release_id = operation.target_release_id;
        receipt.kind = operation.kind;
        receipt.operation_id = operation.operation_id;
        receipt.target_registration_id = operation.target_registration_id;
        receipt.stage = operation.stage;
        receipt.outcome = operation.outcome;
        receipt.attempt = operation.attempt;
        receipt.retryable = operation.retryable;
        receipt.failure_code = operation.failure_code;
        receipt.failure_message = operation.failure_message;
        receipt.started_at = operation.started_at;
        receipt.updated_at = operation.updated_at;
        return Ok(Some(if next.is_empty() { receipt } else { receipt.superseded(next) }));
    }
    if receipt.request.version != 1 || receipt.request.request_id != id {
        return Err(err("Host maintenance receipt has an unsupported identity or version"));
    }
    if !next.is_empty() {
        return Ok(Some(receipt.superseded(next)));
    }
    if state.completed(id) {
        return Ok(Some(receipt.completed()));
    }
    if receipt.outcome == "succeeded" {
        return Err(err("maintenance completion is not committed"));
    }
    // A receipt that was written before a failed admission commit is recovery material, not an accepted request.
    let terminal = receipt.outcome == "failed" && !receipt.retryable && state.latest_request == id;
    let accepted = receipt.failure_code == TARGET_OLDER_THAN_DATA || state.active_request == id || terminal;
    Ok(accepted.then_some(receipt))
}

/// A failed reset can already have removed `service.json`. A continued or
/// replacing request keeps the service identity of the operation it continues.
pub fn service_receipt(context: &Context, paths: &Paths, request: &MaintenanceRequest) -> Result<Option<MaintenanceReceipt>> {
    match status(context, paths, &request.request_id)? {
        None if !request.supersedes_request_id.is_empty() => status(context, paths, &request.supersedes_request_id),
        saved => Ok(saved),
    }
}
