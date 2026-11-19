//! Host maintenance: update, reinstall and uninstall of the shared Host, as one
//! request that the owner journal makes recoverable at every step.

use super::maintenance_journal::{collect_install_ids, refuse, validate_supersession};
use super::paths::{shared_host_app, validate_maintenance_path};
use crate::cutover::{self, CutoverProgress, HOST_START_FAILED, Steps, TARGET_OLDER_THAN_DATA};
use crate::error::{Error, Kind, Result, err};
use crate::journal::{self, Journal, MaintenanceState, receipt_name};
use crate::schema::{self, DataSchema};
use crate::{CONTRACT_VERSION, Context, HOST_APP_NAME, Paths, ServiceMode, now_millis, valid_id};
use serde::{Deserialize, Serialize};
use std::path::Path;

const RESTORE_WINDOW_ATTEMPTS: u32 = 3;
pub const REQUEST_KEYS: [&str; 11] =
    ["version", "continue", "requestId", "action", "dataPolicy", "targetApp", "sourceApp", "targetReleaseId", "supersedesRequestId", "confirmSharedHost", "confirmDataDeletion"];

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MaintenanceRequest {
    pub version: u32,
    #[serde(rename = "continue", skip_serializing_if = "is_false")]
    pub resume: bool,
    pub request_id: String,
    pub action: String,
    pub data_policy: String,
    pub target_app: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub source_app: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_release_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub supersedes_request_id: String,
    pub confirm_shared_host: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub confirm_data_deletion: bool,
}

impl MaintenanceRequest {
    /// Decodes the request of `host-maintain`. A key outside the contract is refused.
    pub fn decode(input: &[u8]) -> Result<Self> {
        let invalid = || err("invalid maintenance request");
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(input).map_err(|_| invalid())?;
        if raw.keys().any(|key| !REQUEST_KEYS.contains(&key.as_str())) {
            return Err(invalid());
        }
        serde_json::from_value(raw.into()).map_err(|_| invalid())
    }

    pub fn uninstall(&self) -> bool {
        self.action == "uninstall"
    }

    pub fn reset(&self) -> bool {
        self.data_policy == "reset"
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MaintenanceReceipt {
    pub operation_id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_registration_id: String,
    pub attempt: u32,
    #[serde(skip_serializing_if = "is_zero")]
    pub restore_window_attempts: u32,
    #[serde(flatten)]
    pub request: MaintenanceRequest,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub admission_previous_request_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub superseded_by_request_id: String,
    pub service_mode: String,
    pub service_user: String,
    pub disable_pairing: bool,
    pub stage: String,
    pub outcome: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_message: String,
    #[serde(flatten)]
    pub progress: CutoverProgress,
    pub started_at: i64,
    pub updated_at: i64,
}

impl MaintenanceReceipt {
    pub fn new(paths: &Paths, request: &MaintenanceRequest, outcome: &str) -> Self {
        let now = now_millis();
        Self {
            request: request.clone(),
            service_mode: mode_name(paths.service.mode).into(),
            service_user: paths.service.user.clone(),
            disable_pairing: paths.service.disable_pairing,
            stage: "accepted".into(),
            outcome: outcome.into(),
            started_at: now,
            updated_at: now,
            ..Self::default()
        }
    }

    pub fn superseded(mut self, next: &str) -> Self {
        self.outcome = "superseded".into();
        self.retryable = false;
        self.superseded_by_request_id = next.into();
        self
    }

    pub fn completed(mut self) -> Self {
        self.stage = "settled".into();
        self.outcome = "succeeded".into();
        self.retryable = false;
        self.failure_code.clear();
        self.failure_message.clear();
        self
    }

    fn fail(&mut self, code: &str, message: &str, retryable: bool) {
        self.outcome = "failed".into();
        self.retryable = retryable;
        self.failure_code = code.into();
        self.failure_message = message.into();
        self.updated_at = now_millis();
    }
}

pub fn mode_name(mode: ServiceMode) -> &'static str {
    match mode {
        ServiceMode::Agent => "agent",
        ServiceMode::Daemon => "daemon",
        ServiceMode::Auto => "auto",
    }
}

/// The value that `host-maintain` and `maintenance-status` print.
pub fn report(receipt: Option<&MaintenanceReceipt>) -> serde_json::Value {
    let Some(receipt) = receipt else { return serde_json::Value::Null };
    let mut copy = receipt.clone();
    if copy.operation_id.is_empty() {
        copy.operation_id = copy.request.request_id.clone();
    }
    if copy.kind.is_empty() {
        copy.kind = copy.request.action.clone();
    }
    let mut value = serde_json::to_value(&copy).unwrap_or_default();
    value["contractVersion"] = CONTRACT_VERSION.into();
    value
}

fn validate(paths: &Paths, request: &MaintenanceRequest) -> Result<()> {
    if request.version != 1 || !valid_id(&request.request_id) || !request.confirm_shared_host {
        return Err(err("maintenance requires version 1, a stable requestId and explicit shared Host confirmation"));
    }
    if !matches!(request.action.as_str(), "update" | "reinstall" | "uninstall") {
        return Err(err("unsupported maintenance action"));
    }
    if !matches!(request.data_policy.as_str(), "preserve" | "reset") {
        return Err(err("maintenance dataPolicy must be preserve or reset"));
    }
    if request.action == "update" && request.reset() {
        return Err(err("update must preserve data"));
    }
    if request.reset() != request.confirm_data_deletion {
        return Err(err(if request.reset() { "reset requires explicit deletion of all shared Host data" } else { "data deletion confirmation conflicts with preserve" }));
    }
    let supersedes = &request.supersedes_request_id;
    if !supersedes.is_empty() && (!valid_id(supersedes) || *supersedes == request.request_id) {
        return Err(err("maintenance replacement requires an exact prior request"));
    }
    let standard = shared_host_app(&paths.home);
    if Path::new(&request.target_app) != standard || paths.app != standard {
        return Err(err("maintenance target must be this service user's standard shared Host app"));
    }
    for target in [&paths.app, &paths.application_data, &paths.logs, &paths.agent_plist] {
        validate_maintenance_path(&paths.home, target)?;
    }
    if request.uninstall() {
        if !request.source_app.is_empty() || !request.target_release_id.is_empty() {
            return Err(err("uninstall does not accept an installation artifact"));
        }
        return Ok(());
    }
    if request.target_release_id.is_empty() {
        return Err(err("maintenance requires the selected release"));
    }
    let source = Path::new(&request.source_app);
    if !source.is_absolute() || source.file_name().is_none_or(|name| name != HOST_APP_NAME) || request.source_app == request.target_app {
        return Err(err("maintenance requires a separate verified source Host app"));
    }
    if [&paths.app, &paths.application_data, &paths.logs].iter().any(|removed| source.starts_with(removed)) {
        return Err(err("maintenance source must be outside the target and reset data"));
    }
    Ok(())
}

/// The effects of maintenance outside the journal.
pub trait Actions {
    fn validate_source(&self, request: &MaintenanceRequest) -> Result<()>;
    /// The `(minimum, current)` schema range of the target release.
    fn target_schema(&self, request: &MaintenanceRequest) -> Result<(u32, u32)>;
    fn data_schema(&self, paths: &Paths) -> Result<DataSchema>;
    /// The commit of admission and of completion. A test makes them fail.
    fn commit(&self, journal: &Journal, state: &MaintenanceState) -> Result<()> {
        journal.write_state(state)
    }
    fn stop(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()>;
    fn remove_software(&mut self, paths: &Paths) -> Result<()>;
    fn reset_data(&mut self, paths: &Paths) -> Result<()>;
    fn replace(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()>;
    fn restore(&mut self, paths: &Paths, request_id: &str) -> Result<()>;
    fn verify(&mut self, paths: &Paths, request: &MaintenanceRequest) -> Result<()>;
}

pub type Outcome = (Option<MaintenanceReceipt>, Result<()>);

struct Execution<'a> {
    paths: &'a Paths,
    journal: &'a Journal,
    receipt: MaintenanceReceipt,
    actions: &'a mut dyn Actions,
}

impl Steps for Execution<'_> {
    fn save(&mut self, stage: &str, progress: &CutoverProgress) -> Result<()> {
        if stage == "restore" {
            if self.receipt.restore_window_attempts >= RESTORE_WINDOW_ATTEMPTS {
                return Err(Error::new(Kind::RestoreWindowExhausted, "Host restore window is paused. Continue this request to try again."));
            }
            self.receipt.restore_window_attempts += 1;
            self.receipt.attempt += 1;
        }
        self.receipt.progress = *progress;
        self.receipt.stage = stage.into();
        self.receipt.outcome = "pending".into();
        self.receipt.retryable = true;
        self.receipt.failure_code.clear();
        self.receipt.failure_message.clear();
        self.receipt.updated_at = now_millis();
        self.journal.write(&receipt_name(&self.receipt.request.request_id), &self.receipt)
    }

    fn stop(&mut self) -> Result<()> {
        self.actions.stop(self.paths, &self.receipt.request)
    }

    fn reset(&mut self) -> Result<()> {
        self.actions.reset_data(self.paths)
    }

    fn replace(&mut self) -> Result<()> {
        self.actions.replace(self.paths, &self.receipt.request)
    }

    fn remove(&mut self) -> Result<()> {
        self.actions.remove_software(self.paths)
    }

    fn restore(&mut self) -> Result<()> {
        let id = cutover::restore_request_id(&self.receipt.request.request_id, self.receipt.attempt);
        self.actions.restore(self.paths, &id)
    }

    fn verify(&mut self) -> Result<()> {
        self.actions.verify(self.paths, &self.receipt.request)
    }
}

/// The state write is the completion commit. A status read projects it after
/// a crash and never trusts a success receipt that the state does not confirm.
fn settle(journal: &Journal, mut state: MaintenanceState, mut receipt: MaintenanceReceipt, actions: &dyn Actions) -> Outcome {
    let name = receipt_name(&receipt.request.request_id);
    state.active_request.clear();
    state.uninstalled = receipt.request.uninstall();
    state.completed_requests.insert(receipt.request.request_id.clone(), true);
    if let Err(error) = actions.commit(journal, &state) {
        receipt.fail("maintenance_incomplete", "Maintenance effects are verified, but completion could not be committed. Retry this request.", true);
        let _ = journal.write(&name, &receipt);
        return (Some(receipt), Err(error));
    }
    let mut receipt = receipt.completed();
    receipt.updated_at = now_millis();
    let written = journal.write(&name, &receipt);
    (Some(receipt), written)
}

fn execute(paths: &Paths, journal: &Journal, mut state: MaintenanceState, receipt: MaintenanceReceipt, actions: &mut dyn Actions) -> Outcome {
    let mut progress = receipt.progress;
    let (reset, remove) = (receipt.request.reset(), receipt.request.uninstall());
    let mut execution = Execution { paths, journal, receipt, actions };
    let result = cutover::cutover(&mut progress, reset, remove, &mut execution);
    let Execution { mut receipt, actions, .. } = execution;
    let Err(error) = result else { return settle(journal, state, receipt, actions) };
    let (code, retryable) = match &error.kind {
        Kind::Maintenance(code) => (code.as_str(), false),
        Kind::RestoreWindowExhausted => (HOST_START_FAILED, true),
        _ => ("maintenance_incomplete", true),
    };
    receipt.fail(code, &error.message, retryable);
    let mut saved = journal.write(&receipt_name(&receipt.request.request_id), &receipt);
    if !retryable {
        state.active_request.clear();
        saved = saved.and(actions.commit(journal, &state));
    }
    let error = saved.err().map_or(error.clone(), |save| Error::new(error.kind.clone(), format!("{}\n{}", error.message, save.message)));
    (Some(receipt), Err(error))
}

pub fn run(context: &Context, paths: &Paths, request: &MaintenanceRequest, actions: &mut dyn Actions) -> Outcome {
    match admit(context, paths, request, actions) {
        Ok(outcome) => outcome,
        Err(error) => (None, Err(error)),
    }
}

fn admit(context: &Context, paths: &Paths, request: &MaintenanceRequest, actions: &mut dyn Actions) -> Result<Outcome> {
    validate(paths, request)?;
    let _lock = journal::lock(context, paths)?;
    let journal = Journal::open(context, paths)?;
    let mut state = journal.state()?;
    let id = &request.request_id;
    if !state.active_request.is_empty() && state.active_request != *id && !journal.receipt(&state.active_request)?.exclusive() {
        state.active_request.clear();
        journal.write_state(&state)?;
    }
    let name = receipt_name(id);
    let refuse_with = |receipt: MaintenanceReceipt, message: &str| -> Result<Outcome> { Ok((Some(receipt), Err(err(message)))) };
    let saved = match journal.read::<MaintenanceReceipt>(&name) {
        Ok(receipt) => {
            let same = receipt.request.action == request.action
                && receipt.request.data_policy == request.data_policy
                && receipt.request.target_app == request.target_app
                && receipt.request.target_release_id == request.target_release_id
                && receipt.service_mode == mode_name(paths.service.mode)
                && receipt.service_user == paths.service.user;
            if !same {
                return refuse_with(receipt, "maintenance requestId belongs to a different action, data policy, service or artifact");
            }
            let next = state.superseded_by(id);
            if !next.is_empty() {
                return Ok((Some(receipt.superseded(next)), Ok(())));
            }
            if state.completed(id) {
                return Ok((Some(receipt.completed()), Ok(())));
            }
            if receipt.failure_code == TARGET_OLDER_THAN_DATA || receipt.outcome == "failed" && !receipt.retryable {
                let message = receipt.failure_message.clone();
                return refuse_with(receipt, &message);
            }
            let stale = state.latest_request != *id
                && request.supersedes_request_id.is_empty()
                && (!state.active_request.is_empty() || receipt.stage != "accepted" || receipt.outcome != "pending" || receipt.admission_previous_request_id != state.latest_request);
            if stale {
                return refuse_with(receipt, "this unfinished request is no longer current; inspect the latest maintenance request");
            }
            if receipt.progress.effects_verified && state.active_request == *id {
                return Ok(settle(&journal, state, receipt, actions));
            }
            Some(receipt)
        }
        Err(error) if error.not_found() && state.accepted(id) => return Err(err("accepted maintenance receipt is unavailable; keep the owner journal for recovery")),
        Err(error) if error.not_found() => None,
        Err(error) => return Err(error),
    };
    let previous = &request.supersedes_request_id;
    let superseding = !previous.is_empty() && state.active_request != *id;
    if superseding {
        validate_supersession(&journal, paths, request, &state)?;
    } else if !previous.is_empty() && state.superseded_by(previous) != id {
        return Err(err("maintenance supersession is not owned by this request"));
    } else if !state.active_request.is_empty() && state.active_request != *id {
        return Err(err("continue the existing maintenance request before starting another"));
    }
    if !request.uninstall() {
        actions.validate_source(request)?;
    }
    let new = saved.is_none();
    // Before any effect, and before a supersession commit, the target must open the existing data.
    if new && !request.uninstall() && !request.reset() {
        let data = actions.data_schema(paths).map_err(|error| error.context("local Host database schema is unreadable"))?;
        if data != DataSchema::Fresh {
            let (minimum, current) = actions.target_schema(request)?;
            if let Some(message) = schema::refusal(data, minimum, current) {
                return Ok(refuse(&journal, paths, request, &message));
            }
        }
    }
    let mut receipt = saved.unwrap_or_else(|| MaintenanceReceipt { admission_previous_request_id: state.latest_request.clone(), retryable: true, ..MaintenanceReceipt::new(paths, request, "pending") });
    state.active_request = id.clone();
    state.latest_request = id.clone();
    if request.reset() {
        collect_install_ids(context, paths, &journal, &mut state)?;
    }
    // A new receipt is staged before the owner pointer. Until the state commit it authorizes no effect.
    if new || superseding {
        journal.write(&name, &receipt)?;
    }
    if superseding {
        state.superseded_requests.insert(previous.clone(), id.clone());
    }
    actions.commit(&journal, &state)?;
    if request.resume {
        receipt.restore_window_attempts = 0;
        receipt.request.resume = false;
        journal.write(&name, &receipt)?;
    }
    Ok(execute(paths, &journal, state, receipt, actions))
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;
