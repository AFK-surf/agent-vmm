//! The owner journal: `state.json`, one receipt per request, and `owner.lock`.
//! It lives outside the application bundle and survives a Host data reset.

use crate::cutover::CutoverProgress;
use crate::error::{Error, Kind, Result, err};
use crate::statefs::{FileOwner, StateDir, hex};
use crate::{Context, Paths};
use rustix::fs::FlockOperation;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Seek, Write};
use std::time::{Duration, Instant};

const RECORD_LIMIT: u64 = 4 << 20;
pub const PLAN_LIMIT: u64 = 8 << 20;
const STATE_FILE: &str = "state.json";
const LOCK_FILE: &str = "owner.lock";

type Extra = serde_json::Map<String, serde_json::Value>;

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// The receipt of one request. `kind` is the command name that Comma compares.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LifecycleOperation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cutover: Option<CutoverProgress>,
    pub operation_id: String,
    pub request_id: String,
    pub kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_registration_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_release_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_artifact_sha256: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub target_artifact_size: u64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub stage: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "is_zero")]
    pub attempt: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_stage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_code: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_message: String,
    #[serde(skip_serializing_if = "is_false")]
    pub retryable: bool,
    pub started_at: i64,
    pub updated_at: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub correlation_request_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub superseded_by_request_id: String,
    /// Fields of another receipt form in the same file. A rewrite keeps them.
    #[serde(flatten)]
    pub extra: Extra,
}

impl LifecycleOperation {
    pub fn unknown(request_id: &str, kind: &str) -> Self {
        Self { request_id: request_id.into(), kind: kind.into(), outcome: "unknown".into(), ..Self::default() }
    }

    /// An exclusive transition owns the Host until it settles.
    pub fn exclusive(&self) -> bool {
        matches!(self.kind.as_str(), "update" | "repair" | "remove" | "drain" | "reinstall" | "uninstall")
    }

    pub fn same_target(&self, other: &Self) -> bool {
        self.request_id == other.request_id
            && self.kind == other.kind
            && self.target_registration_id == other.target_registration_id
            && self.target_release_id == other.target_release_id
            && self.target_artifact_sha256 == other.target_artifact_sha256
            && self.target_artifact_size == other.target_artifact_size
    }
}

pub fn unfinished(outcome: &str, retryable: bool) -> bool {
    outcome == "pending" || outcome == "unknown" || outcome == "failed" && retryable
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct MaintenanceState {
    pub version: u32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub active_request: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub latest_request: String,
    pub uninstalled: bool,
    /// The count of accepted data resets. A change means that the Host data of an earlier epoch is gone.
    #[serde(skip_serializing_if = "is_zero")]
    pub data_epoch: u64,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub completed_requests: BTreeMap<String, bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub superseded_requests: BTreeMap<String, String>,
    /// Requests that a shared data reset fenced before it removed their plans.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub blocked_installs: BTreeMap<String, bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub operation_requests: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub registration_requests: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub removed_registrations: BTreeMap<String, bool>,
    #[serde(flatten)]
    pub extra: Extra,
}

impl MaintenanceState {
    pub fn completed(&self, id: &str) -> bool {
        self.completed_requests.get(id).copied().unwrap_or(false)
    }

    pub fn blocked(&self, id: &str) -> bool {
        self.blocked_installs.get(id).copied().unwrap_or(false)
    }

    pub fn removed(&self, registration_id: &str) -> bool {
        self.removed_registrations.get(registration_id).copied().unwrap_or(false)
    }

    pub fn superseded_by(&self, id: &str) -> &str {
        self.superseded_requests.get(id).map_or("", String::as_str)
    }

    pub fn registration_request(&self, registration_id: &str) -> &str {
        self.registration_requests.get(registration_id).map_or("", String::as_str)
    }

    /// True when the journal accepted `id`, so its receipt must exist.
    pub fn accepted(&self, id: &str) -> bool {
        self.active_request == id || self.latest_request == id || self.completed(id) || !self.superseded_by(id).is_empty()
    }
}

pub fn receipt_name(id: &str) -> String {
    format!("request-{}.json", hex(&Sha256::digest(id.as_bytes())))
}

/// The fields that a version 1 maintenance receipt shares with a lifecycle receipt.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct MaintenanceRecord {
    version: u32,
    action: String,
    stage: String,
    outcome: String,
    target_release_id: String,
    failure_code: String,
    failure_message: String,
    retryable: bool,
}

pub struct Journal {
    dir: StateDir,
    owner: Option<FileOwner>,
}

fn open_data(paths: &Paths, owner: Option<FileOwner>, create: bool) -> Result<StateDir> {
    // Linux prepares its fixed state directory with the service account.
    if paths.home.as_os_str().is_empty() {
        return StateDir::open(&paths.application_data, owner);
    }
    let relative = paths.application_data.strip_prefix(&paths.home).map_err(|_| err("state directory is outside the service home"))?;
    let names: Vec<&str> = relative.iter().map(|name| name.to_str().ok_or_else(|| err("state path is invalid"))).collect::<Result<_>>()?;
    let mut directory = StateDir::open(&paths.home, None).map_err(|error| error.context("open service home"))?;
    for (index, name) in names.iter().enumerate() {
        directory = match (create, index + 1 == names.len()) {
            (false, _) => directory.child(name)?,
            (true, false) => directory.child_or_create(name, None)?,
            (true, true) => directory.child_or_create(name, owner)?,
        };
    }
    Ok(directory)
}

impl Journal {
    pub fn open(context: &Context, paths: &Paths) -> Result<Self> {
        Ok(Self { dir: open_data(paths, None, false)?, owner: paths.file_owner(context.system.euid()) })
    }

    pub fn open_or_create(context: &Context, paths: &Paths) -> Result<Self> {
        let owner = paths.file_owner(context.system.euid());
        Ok(Self { dir: open_data(paths, owner, true)?, owner })
    }

    pub fn dir(&self) -> &StateDir {
        &self.dir
    }

    pub fn owner(&self) -> Option<FileOwner> {
        self.owner
    }

    pub fn read<T: DeserializeOwned>(&self, name: &str) -> Result<T> {
        let data = self.dir.read(name, RECORD_LIMIT)?;
        serde_json::from_slice(&data).map_err(|_| err("Host maintenance journal is unreadable"))
    }

    pub fn write<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        let data = serde_json::to_vec(value)?;
        if data.len() as u64 > RECORD_LIMIT {
            return Err(err("Host maintenance journal exceeds its bounded record size"));
        }
        self.dir.write(name, &data, 0o600, self.owner)
    }

    pub fn state(&self) -> Result<MaintenanceState> {
        match self.read::<MaintenanceState>(STATE_FILE) {
            Ok(state) if state.version == 1 => Ok(state),
            Ok(_) => Err(err("unsupported Host maintenance state")),
            Err(error) if error.not_found() => Ok(MaintenanceState { version: 1, ..MaintenanceState::default() }),
            Err(error) => Err(error),
        }
    }

    pub fn write_state(&self, state: &MaintenanceState) -> Result<()> {
        self.write(STATE_FILE, state)
    }

    /// Reads the receipt of `id`. A maintenance receipt is projected to the common fields.
    pub fn receipt(&self, id: &str) -> Result<LifecycleOperation> {
        let name = receipt_name(id);
        let mut operation: LifecycleOperation = self.read(&name)?;
        let record: MaintenanceRecord = self.read(&name)?;
        if record.version > 1 {
            return Err(err("Host receipt has an unsupported version"));
        }
        if operation.operation_id.is_empty() {
            operation.operation_id = id.to_string();
            operation.request_id = id.to_string();
            operation.kind = record.action;
            operation.stage = record.stage;
            operation.outcome = record.outcome;
            operation.target_release_id = record.target_release_id;
            operation.failure_code = record.failure_code;
            operation.failure_message = record.failure_message;
            operation.retryable = record.retryable;
        }
        if operation.request_id != id || operation.kind.is_empty() || operation.outcome.is_empty() {
            return Err(err("Host receipt identity or kind is unreadable"));
        }
        Ok(operation)
    }

    /// Writes the receipt, then the indexes that find it.
    pub fn persist(&self, operation: &LifecycleOperation) -> Result<()> {
        let mut state = self.state()?;
        self.write(&receipt_name(&operation.request_id), operation)?;
        state.latest_request = operation.request_id.clone();
        state.operation_requests.insert(operation.operation_id.clone(), operation.request_id.clone());
        state.operation_requests.insert(operation.request_id.clone(), operation.request_id.clone());
        if !operation.target_registration_id.is_empty() {
            state.registration_requests.insert(operation.target_registration_id.clone(), operation.request_id.clone());
        }
        if operation.exclusive() && unfinished(&operation.outcome, operation.retryable) {
            state.active_request = operation.request_id.clone();
        } else if state.active_request == operation.request_id {
            state.active_request.clear();
        }
        if operation.outcome == "succeeded" {
            state.completed_requests.insert(operation.request_id.clone(), true);
        }
        self.write_state(&state)
    }

    /// The directory of one request's registration plan, below `install-operations`.
    pub fn plan_dir(&self, id: &str, create: bool) -> Result<StateDir> {
        if !crate::valid_id(id) {
            return Err(err("install operation id is invalid"));
        }
        if create {
            return self.dir.child_or_create("install-operations", self.owner)?.child_or_create(id, self.owner);
        }
        self.dir.child("install-operations")?.child(id)
    }

    /// Removes the Host data and keeps the owner journal.
    pub fn remove_host_data(&self) -> Result<()> {
        for name in self.dir.names()? {
            let journal = name == LOCK_FILE || name == STATE_FILE || name.starts_with("request-") && name.ends_with(".json");
            if !journal {
                self.dir.remove_tree(&name)?;
            }
        }
        Ok(())
    }
}

pub fn persist(context: &Context, paths: &Paths, operation: &LifecycleOperation) -> Result<()> {
    Journal::open_or_create(context, paths)?.persist(operation)
}

pub fn read_request(context: &Context, paths: &Paths, id: &str) -> Option<LifecycleOperation> {
    Journal::open(context, paths).ok()?.receipt(id).ok()
}

/// The receipt of the latest request, when the journal has one.
pub fn read_latest(context: &Context, paths: &Paths) -> Option<LifecycleOperation> {
    let journal = Journal::open(context, paths).ok()?;
    let state = journal.state().ok()?;
    (!state.latest_request.is_empty()).then(|| journal.receipt(&state.latest_request).ok()).flatten()
}

/// The data epoch of the owner state. A journal that does not exist has epoch 0.
pub fn read_data_epoch(context: &Context, paths: &Paths) -> u64 {
    Journal::open(context, paths).and_then(|journal| journal.state()).map_or(0, |state| state.data_epoch)
}

pub fn read_plan_state(context: &Context, paths: &Paths, id: &str) -> Result<Vec<u8>> {
    Journal::open(context, paths)?.plan_dir(id, false)?.read("plan.json", PLAN_LIMIT)
}

/// Holds `owner.lock`. The lock proves that no other lifecycle executor runs.
pub struct OwnerLock(#[allow(dead_code)] File);

pub fn lock(context: &Context, paths: &Paths) -> Result<OwnerLock> {
    let journal = Journal::open_or_create(context, paths).map_err(|error| error.context("assign lifecycle state directory ownership"))?;
    let mut file = journal.dir.lock_file(LOCK_FILE, journal.owner).map_err(|error| error.context("open lifecycle operation lock"))?;
    let started = Instant::now();
    loop {
        match rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => break,
            Err(rustix::io::Errno::WOULDBLOCK) if started.elapsed() < Duration::from_secs(2) => std::thread::sleep(Duration::from_millis(10)),
            Err(rustix::io::Errno::WOULDBLOCK) => {
                return Err(Error::new(Kind::OwnerBusy, "owner_busy: another Host lifecycle operation holds the owner lock"));
            }
            Err(error) => return Err(error.into()),
        }
    }
    file.set_len(0)?;
    file.rewind()?;
    writeln!(file, "{}", std::process::id())?;
    Ok(OwnerLock(file))
}

// Only the macOS maintenance uses this item.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub const UNINSTALLED_MESSAGE: &str = "the local owner removed this Host installation; start a new authorized setup";

// Only the macOS maintenance uses this item.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
/// Takes the lock for a mutation outside a request, such as publication.
pub fn lock_mutation(context: &Context, paths: &Paths) -> Result<OwnerLock> {
    let lock = lock(context, paths)?;
    let journal = Journal::open(context, paths)?;
    let mut state = journal.state()?;
    if !state.active_request.is_empty() {
        if journal.receipt(&state.active_request)?.exclusive() {
            return Err(Error::new(Kind::ExclusiveBusy(state.active_request.clone()), format!("exclusive_transition_in_progress: continue request {}", state.active_request)));
        }
        state.active_request.clear();
        journal.write_state(&state)?;
    }
    if state.uninstalled {
        return Err(Error::new(Kind::HostUninstalled, UNINSTALLED_MESSAGE));
    }
    Ok(lock)
}
