//! The `host.v1.OperatorService` calls that lifecycle makes on the local Host socket.

use crate::error::{Deadline, Error, Kind, Result, RpcCode, err};
use crate::{Context, Paths};
use agent_vmm_proto::host::v1 as hostv1;
use hostv1::{DeviceIdentityState, HealthState, RemoteConnectionState, RemoteRegistrationState, VmState};
use std::time::{Duration, Instant};

pub trait Operator {
    fn health(&mut self, deadline: Deadline) -> Result<hostv1::GetSystemHealthSummaryResponse>;
    fn registration(&mut self, id: &str, deadline: Deadline) -> Result<hostv1::RemoteRegistration>;
    fn vm(&mut self, deadline: Deadline) -> Result<hostv1::Vm>;
    /// The VM of the whole-VM impact summary that a mutation must acknowledge.
    fn vm_impact(&mut self, deadline: Deadline) -> Result<hostv1::Vm>;
    fn start_vm(&mut self, request_id: &str, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()>;
    fn stop_vm(&mut self, request_id: &str, force: bool, fence: hostv1::VmMutationFence, deadline: Deadline) -> Result<()>;
    fn device_identity(&mut self, deadline: Deadline) -> Result<hostv1::GetDeviceIdentityResponse>;
    fn sign_recovery(&mut self, challenge: &[u8], deadline: Deadline) -> Result<hostv1::SignRegistrationRecoveryResponse>;
    fn apply_registration(&mut self, request: hostv1::ApplyRemoteRegistrationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration>;
    fn set_registration_state(&mut self, request: hostv1::SetRemoteRegistrationStateRequest, deadline: Deadline) -> Result<()>;
    fn begin_revocation(&mut self, request: hostv1::BeginRemoteRevocationRequest, deadline: Deadline) -> Result<hostv1::RemoteRegistration>;
    fn complete_revocation(&mut self, request: hostv1::CompleteRemoteRevocationRequest, deadline: Deadline) -> Result<()>;
    /// Calls the RPC of one `local-*` command with an encoded request and returns the encoded response.
    fn local(&mut self, command: &str, request: &[u8], deadline: Deadline) -> Result<Vec<u8>>;
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteProtocol {
    pub host_protocol: String,
    pub min_protocol: u32,
    pub max_protocol: u32,
    pub host_side: String,
}

#[derive(Default)]
pub struct OperatorFacts {
    pub host_readable: bool,
    pub host_healthy: bool,
    pub connector_connected: bool,
    pub registration_read: String,
    pub registration_state: String,
    pub registration_reason: String,
    pub remote_protocol: Option<RemoteProtocol>,
    pub snapshot: Option<hostv1::GetSystemHealthSummaryResponse>,
    pub device_id: String,
    pub device_identity_state: String,
}

fn healthy(snapshot: &hostv1::GetSystemHealthSummaryResponse) -> bool {
    snapshot.vm.as_ref().is_some_and(|vm| vm.state() == VmState::Running)
        && snapshot.health.as_ref().is_some_and(|health| health.state() == HealthState::Healthy)
}

fn fence(vm: &hostv1::Vm) -> hostv1::VmMutationFence {
    hostv1::VmMutationFence { state: vm.state, boot_id: vm.boot_id.clone(), observed_workloads: Vec::new(), whole_vm_impact_acknowledged: true }
}

/// Reads the Host and, for a registration, the connector. A failure leaves the facts it could not read empty.
pub fn read_facts(context: &Context, paths: &Paths, registration_id: Option<&str>, deadline: Deadline) -> OperatorFacts {
    let unreadable = || OperatorFacts { registration_read: "unreadable".into(), registration_reason: "operator_unavailable".into(), ..OperatorFacts::default() };
    let Ok(mut operator) = context.operator(paths) else { return unreadable() };
    let Ok(snapshot) = operator.health(deadline) else { return unreadable() };
    let mut facts = OperatorFacts { host_readable: true, host_healthy: healthy(&snapshot), snapshot: Some(snapshot), ..OperatorFacts::default() };
    if let Some(id) = registration_id {
        match operator.registration(id, deadline) {
            Ok(remote) => {
                facts.registration_read = "present".into();
                facts.registration_state = match remote.state() {
                    RemoteRegistrationState::Enabled => "enabled",
                    RemoteRegistrationState::Draining => "draining",
                    RemoteRegistrationState::Revoked => "revoked",
                    RemoteRegistrationState::Unspecified => "",
                }
                .into();
                facts.remote_protocol = remote.connector_status.as_ref().and_then(|status| status.remote_protocol.as_ref()).map(|protocol| RemoteProtocol {
                    host_protocol: protocol.host_protocol.clone(),
                    min_protocol: protocol.min_protocol,
                    max_protocol: protocol.max_protocol,
                    host_side: protocol.host_side.clone(),
                });
                facts.connector_connected = remote.connection_state() == RemoteConnectionState::Connected;
            }
            Err(error) if error.rpc_code() == Some(RpcCode::NotFound) => {
                facts.registration_read = "absent".into();
                facts.registration_reason = "registration_not_found".into();
            }
            Err(_) => {
                facts.registration_read = "unreadable".into();
                facts.registration_reason = "registration_unreadable".into();
            }
        }
    }
    // The identity is read last, so a slow read cannot hide the registration.
    if let Ok(response) = operator.device_identity(deadline) {
        facts.device_identity_state = match response.state() {
            DeviceIdentityState::Ready => "ready",
            DeviceIdentityState::NeedsRepair => "needs_repair",
            DeviceIdentityState::Unspecified => "",
        }
        .into();
        facts.device_id = response.identity.map(|identity| identity.device_id).unwrap_or_default();
    }
    facts
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Progress<'a> {
    event: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    request_id: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    elapsed_ms: u128,
    #[serde(skip_serializing_if = "str::is_empty")]
    vm_state: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    health_state: &'a str,
}

fn is_zero(value: &u128) -> bool {
    *value == 0
}

fn progress(event: &str, request_id: &str, started: Option<Instant>, vm_state: &str, health_state: &str) {
    let elapsed_ms = started.map_or(0, |started| started.elapsed().as_millis());
    if let Ok(line) = serde_json::to_string(&Progress { event, request_id, elapsed_ms, vm_state, health_state }) {
        eprintln!("{line}");
    }
}

const POLL: Duration = Duration::from_millis(250);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(15);

/// Starts the VM when it is stopped and waits until the Host reports it healthy.
pub fn ensure_vm_started(context: &Context, paths: &Paths, request_id: &str, deadline: Deadline) -> Result<()> {
    ensure_vm_started_until(context, paths, request_id, deadline, &mut || Ok(()))
}

/// `check` runs between observations; its error ends the wait.
pub fn ensure_vm_started_until(context: &Context, paths: &Paths, request_id: &str, deadline: Deadline, check: &mut dyn FnMut() -> Result<()>) -> Result<()> {
    let mut check = || {
        if context.system.interrupted() {
            return Err(Error::new(Kind::Interrupted, "the lifecycle helper was asked to stop"));
        }
        check()
    };
    let started = Instant::now();
    progress("operator_connect_started", request_id, None, "", "");
    let mut operator = context.operator(paths).map_err(|_| err("Agent VMM operator is not readable"))?;
    let vm = loop {
        match operator.vm(deadline) {
            Ok(vm) => break vm,
            Err(_) if deadline.expired() => return Err(Error::new(Kind::Timeout, "Agent VMM VM status could not be read")),
            Err(_) => {
                check()?;
                std::thread::sleep(POLL.min(deadline.remaining()));
            }
        }
    };
    progress("operator_connected", request_id, Some(started), "", "");
    // A Host that resumes its VM at startup owns that transition; wait for it instead of racing it.
    if matches!(vm.state(), VmState::Stopped | VmState::Failed) {
        progress("vm_start_requested", request_id, Some(started), vm.state().as_str_name(), "");
        let impact = operator.vm_impact(deadline).map_err(|_| err("Agent VMM whole-VM impact could not be acknowledged before VM start"))?;
        operator.start_vm(request_id, fence(&impact), deadline).map_err(|error| error.context("Agent VMM VM could not be started"))?;
        progress("vm_start_accepted", request_id, Some(started), "", "");
    }
    let mut reported: Option<Instant> = None;
    loop {
        let snapshot = operator.health(deadline);
        let states = snapshot.as_ref().ok().map(|snapshot| {
            let vm = snapshot.vm.as_ref().map_or(VmState::Unspecified, |vm| vm.state()).as_str_name();
            let health = snapshot.health.as_ref().map_or(HealthState::Unspecified, |health| health.state()).as_str_name();
            (vm, health)
        });
        if snapshot.as_ref().is_ok_and(healthy) {
            let (vm, health) = states.unwrap_or_default();
            progress("vm_healthy", request_id, Some(started), vm, health);
            return Ok(());
        }
        if reported.is_none_or(|at| at.elapsed() >= PROGRESS_INTERVAL) {
            let (vm, health) = states.unwrap_or_default();
            progress("health_wait", request_id, Some(started), vm, health);
            reported = Some(Instant::now());
        }
        check()?;
        if deadline.sleep(POLL).is_err() {
            return Err(Error::new(Kind::Timeout, "Agent VMM VM did not become healthy"));
        }
    }
}

/// Stops the VM before the runtime that owns it is replaced: gracefully, then by force on the same boot.
pub fn stop_vm(context: &Context, paths: &Paths, request_id: &str, deadline: Deadline) -> Result<()> {
    let mut operator = context.operator(paths).map_err(|_| err("running Agent VMM Host is not readable; refusing to replace its runtime"))?;
    stop_vm_with(operator.as_mut(), request_id, deadline)
}

pub(crate) fn stop_vm_with(operator: &mut dyn Operator, request_id: &str, deadline: Deadline) -> Result<()> {
    const STEP: Duration = Duration::from_secs(20);
    let vm = operator.vm(deadline).map_err(|_| err("Agent VMM VM status could not be read before update"))?;
    if vm.state() == VmState::Stopped {
        return Ok(());
    }
    if let Err(graceful) = operator.stop_vm(&format!("{request_id}-graceful"), false, fence(&vm), deadline.within(STEP)) {
        let force = deadline.within(STEP);
        let latest = operator.vm(force).map_err(|_| err("VM state could not be read before forced stop"))?;
        if latest.boot_id != vm.boot_id {
            return Err(err("VM boot identity changed during update stop"));
        }
        operator.stop_vm(&format!("{request_id}-force"), true, fence(&latest), force).map_err(|forced| {
            err(format!("VM did not reach terminal state before update: graceful stop: {graceful}; force stop: {forced}"))
        })?;
    }
    match operator.vm(deadline) {
        Ok(vm) if vm.state() == VmState::Stopped => Ok(()),
        _ => Err(err("VM stop returned without a terminal stopped state; refusing to stop the Host")),
    }
}

pub fn set_registration_state(context: &Context, paths: &Paths, registration_id: &str, state: RemoteRegistrationState, request_id: &str, deadline: Deadline) -> Result<()> {
    let mut operator = context.operator(paths).map_err(|_| err("Agent VMM operator is not readable for registration state change"))?;
    let current = operator.registration(registration_id, deadline).map_err(|error| error.context("read managed registration"))?;
    if current.id != registration_id {
        return Err(err("managed registration identity changed"));
    }
    // An enabled registration that is disconnected is set again so that the connector restarts.
    let reconnect = state == RemoteRegistrationState::Enabled && current.connection_state() != RemoteConnectionState::Connected;
    if current.state() == state && !reconnect {
        return Ok(());
    }
    let request = hostv1::SetRemoteRegistrationStateRequest {
        request_id: request_id.to_string(),
        expected_revision: current.revision,
        state: state as i32,
        registration_id: registration_id.to_string(),
    };
    operator.set_registration_state(request, deadline).map_err(|error| error.context("set managed registration state"))
}

pub fn wait_registration_connected(context: &Context, paths: &Paths, registration_id: &str, deadline: Deadline) -> Result<()> {
    let timeout = || Error::new(Kind::Timeout, "managed registration did not reconnect before the repair deadline: context deadline exceeded");
    let mut operator = context.operator(paths).map_err(|_| err("Agent VMM operator is not readable for registration connection observation"))?;
    loop {
        if operator.registration(registration_id, deadline).is_ok_and(|remote| remote.id == registration_id && remote.connection_state() == RemoteConnectionState::Connected) {
            return Ok(());
        }
        deadline.sleep(POLL).map_err(|_| timeout())?;
    }
}

pub(crate) fn revision_conflict(error: &Error) -> bool {
    error.rpc_reason() == Some("REGISTRATION_REVISION_CONFLICT")
}

/// Revokes the registration. A missing or already revoked registration is the requested result.
pub fn revoke_registration(context: &Context, paths: &Paths, registration_id: &str, request_id: &str, deadline: Deadline) -> Result<()> {
    let deadline = deadline.within(Duration::from_secs(120));
    let mut operator = context.operator(paths)?;
    let mut last = None;
    for _ in 0..3 {
        let current = match operator.registration(registration_id, deadline) {
            Ok(current) => current,
            Err(error) if error.rpc_code() == Some(RpcCode::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if current.state() == RemoteRegistrationState::Revoked {
            return Ok(());
        }
        let begin = hostv1::BeginRemoteRevocationRequest { registration_id: registration_id.to_string(), expected_revision: current.revision, request_id: request_id.to_string() };
        let result = operator.begin_revocation(begin, deadline).and_then(|begun| {
            let complete = hostv1::CompleteRemoteRevocationRequest { request_id: request_id.to_string(), registration_id: registration_id.to_string(), expected_revision: begun.revision };
            operator.complete_revocation(complete, deadline)
        });
        match result {
            Err(error) if revision_conflict(&error) => last = Some(error),
            other => return other,
        }
    }
    Err(last.unwrap_or_else(|| err("registration revision conflict")))
}
