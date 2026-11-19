//! Test doubles: a Host operator, a service manager, and a registration service.

use crate::error::{Deadline, Error, Kind, Result, RpcCode};
use crate::operator::Operator;
use crate::{CommandOutput, Context, HttpResponse, Paths, ServiceConfig, ServiceMode, System};
use agent_vmm_proto::host::v1 as hostv1;
use agent_vmm_proto::trust::v1::DeviceIdentity;
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};

pub fn rpc(code: RpcCode, reason: Option<&str>) -> Error {
    Error::new(Kind::Rpc { code, reason: reason.map(str::to_string) }, format!("rpc error: code = {}", code.name()))
}

/// The state of the fake Host. Each call appends its name to `calls`.
pub struct Host {
    pub reachable: bool,
    pub vm: hostv1::VmState,
    pub healthy: bool,
    pub identity_ready: bool,
    pub registrations: BTreeMap<String, hostv1::RemoteRegistration>,
    pub applied: Vec<hostv1::ApplyRemoteRegistrationRequest>,
    pub local: Vec<u8>,
    pub calls: Vec<String>,
    /// Errors that the named call returns, one per call.
    pub failures: BTreeMap<&'static str, VecDeque<Error>>,
}

impl Default for Host {
    fn default() -> Self {
        Self {
            reachable: true,
            vm: hostv1::VmState::Running,
            healthy: true,
            identity_ready: true,
            registrations: BTreeMap::new(),
            applied: Vec::new(),
            local: Vec::new(),
            calls: Vec::new(),
            failures: BTreeMap::new(),
        }
    }
}

impl Host {
    pub fn registration(id: &str, state: hostv1::RemoteRegistrationState, connected: bool) -> hostv1::RemoteRegistration {
        let connection = if connected { hostv1::RemoteConnectionState::Connected } else { hostv1::RemoteConnectionState::Disconnected };
        hostv1::RemoteRegistration { id: id.into(), revision: 1, state: state as i32, connection_state: connection as i32, ..Default::default() }
    }
}

struct FakeOperator(Arc<Mutex<Host>>);

impl FakeOperator {
    fn call(&self, name: &'static str) -> Result<std::sync::MutexGuard<'_, Host>> {
        let mut host = self.0.lock().unwrap();
        host.calls.push(name.to_string());
        if !host.reachable {
            return Err(rpc(RpcCode::Unavailable, None));
        }
        match host.failures.get_mut(name).and_then(VecDeque::pop_front) {
            Some(error) => Err(error),
            None => Ok(host),
        }
    }

    fn vm_of(host: &Host) -> hostv1::Vm {
        hostv1::Vm { state: host.vm as i32, boot_id: "boot-1".into(), error: String::new() }
    }
}

impl Operator for FakeOperator {
    fn health(&mut self, _: Deadline) -> Result<hostv1::GetSystemHealthSummaryResponse> {
        let host = self.call("health")?;
        let state = if host.healthy { hostv1::HealthState::Healthy } else { hostv1::HealthState::Degraded };
        Ok(hostv1::GetSystemHealthSummaryResponse { vm: Some(Self::vm_of(&host)), health: Some(hostv1::HealthStatus { state: state as i32, components: Vec::new() }), ..Default::default() })
    }

    fn registration(&mut self, id: &str, _: Deadline) -> Result<hostv1::RemoteRegistration> {
        self.call("registration")?.registrations.get(id).cloned().ok_or_else(|| rpc(RpcCode::NotFound, None))
    }

    fn vm(&mut self, _: Deadline) -> Result<hostv1::Vm> {
        Ok(Self::vm_of(&*self.call("vm")?))
    }

    fn vm_impact(&mut self, _: Deadline) -> Result<hostv1::Vm> {
        Ok(Self::vm_of(&*self.call("vm_impact")?))
    }

    fn start_vm(&mut self, _: &str, _: hostv1::VmMutationFence, _: Deadline) -> Result<()> {
        self.call("start_vm")?.vm = hostv1::VmState::Running;
        Ok(())
    }

    fn stop_vm(&mut self, _: &str, _: bool, _: hostv1::VmMutationFence, _: Deadline) -> Result<()> {
        self.call("stop_vm")?.vm = hostv1::VmState::Stopped;
        Ok(())
    }

    fn device_identity(&mut self, _: Deadline) -> Result<hostv1::GetDeviceIdentityResponse> {
        let host = self.call("device_identity")?;
        let state = if host.identity_ready { hostv1::DeviceIdentityState::Ready } else { hostv1::DeviceIdentityState::NeedsRepair };
        let identity = DeviceIdentity { device_id: "device-a".into(), root_public_key: vec![2; 33], root_key_revision: 1, ..Default::default() };
        Ok(hostv1::GetDeviceIdentityResponse { identity: Some(identity), state: state as i32 })
    }

    fn sign_recovery(&mut self, _: &[u8], _: Deadline) -> Result<hostv1::SignRegistrationRecoveryResponse> {
        drop(self.call("sign_recovery")?);
        let identity = DeviceIdentity { device_id: "device-a".into(), root_public_key: vec![2; 33], root_key_revision: 1, ..Default::default() };
        Ok(hostv1::SignRegistrationRecoveryResponse { identity: Some(identity), signature: vec![7; 64] })
    }

    fn apply_registration(&mut self, request: hostv1::ApplyRemoteRegistrationRequest, _: Deadline) -> Result<hostv1::RemoteRegistration> {
        let mut host = self.call("apply_registration")?;
        let id = request.registration.as_ref().map(|registration| registration.registration_id.clone()).unwrap_or_default();
        host.applied.push(request);
        let registration = Host::registration(&id, hostv1::RemoteRegistrationState::Enabled, true);
        host.registrations.insert(id, registration.clone());
        Ok(registration)
    }

    fn set_registration_state(&mut self, request: hostv1::SetRemoteRegistrationStateRequest, _: Deadline) -> Result<()> {
        let mut host = self.call("set_registration_state")?;
        let registration = host.registrations.get_mut(&request.registration_id).ok_or_else(|| rpc(RpcCode::NotFound, None))?;
        if registration.revision != request.expected_revision {
            return Err(rpc(RpcCode::Aborted, Some("REGISTRATION_REVISION_CONFLICT")));
        }
        registration.state = request.state;
        registration.revision += 1;
        Ok(())
    }

    fn begin_revocation(&mut self, request: hostv1::BeginRemoteRevocationRequest, _: Deadline) -> Result<hostv1::RemoteRegistration> {
        let mut host = self.call("begin_revocation")?;
        let registration = host.registrations.get_mut(&request.registration_id).ok_or_else(|| rpc(RpcCode::NotFound, None))?;
        registration.revision += 1;
        Ok(registration.clone())
    }

    fn complete_revocation(&mut self, request: hostv1::CompleteRemoteRevocationRequest, _: Deadline) -> Result<()> {
        let mut host = self.call("complete_revocation")?;
        let registration = host.registrations.get_mut(&request.registration_id).ok_or_else(|| rpc(RpcCode::NotFound, None))?;
        registration.state = hostv1::RemoteRegistrationState::Revoked as i32;
        Ok(())
    }

    fn local(&mut self, _: &str, _: &[u8], _: Deadline) -> Result<Vec<u8>> {
        Ok(self.call("local")?.local.clone())
    }
}

type Command = Box<dyn FnMut(&str, &[&str]) -> CommandOutput + Send>;

pub struct Fake {
    pub host: Arc<Mutex<Host>>,
    /// Every command line that lifecycle ran.
    pub commands: Mutex<Vec<String>>,
    command: Mutex<Command>,
    /// The body of each registration request, with its URL and `Authorization` value.
    pub posts: Mutex<Vec<(String, String, serde_json::Value)>>,
    pub responses: Mutex<VecDeque<(u16, serde_json::Value)>>,
    pub euid: u32,
}

pub fn output(code: i32, stdout: &str, stderr: &str) -> CommandOutput {
    CommandOutput { code: Some(code), stdout: stdout.into(), stderr: stderr.into() }
}

/// Answers the service manager of either platform for a Host job that runs or is not loaded.
pub fn service_manager(running: bool) -> impl FnMut(&str, &[&str]) -> CommandOutput + Send {
    move |program, arguments| match (program, arguments.first().copied()) {
        ("/bin/launchctl", Some("print")) if running && arguments[1].ends_with("-host") && !arguments[1].starts_with("system/") => output(0, "state = running\n\tpid = 4242\n", ""),
        // The login domain of the user exists.
        ("/bin/launchctl", Some("print")) if arguments[1].starts_with("gui/") && arguments[1].matches('/').count() == 1 => output(0, "", ""),
        ("/bin/launchctl", Some("print" | "bootout")) => output(113, "", "Could not find service"),
        ("systemctl", Some("show")) if running => output(0, "LoadState=loaded\nActiveState=active\nMainPID=4242\nExecMainCode=0\nExecMainStatus=0\n", ""),
        ("systemctl", Some("show")) => output(0, "LoadState=not-found\nActiveState=inactive\nMainPID=0\n", ""),
        ("/usr/sbin/lsof", _) => output(1, "", ""),
        _ => output(0, "", ""),
    }
}

impl Fake {
    pub fn new() -> Arc<Self> {
        Self::with(service_manager(true))
    }

    pub fn with(command: impl FnMut(&str, &[&str]) -> CommandOutput + Send + 'static) -> Arc<Self> {
        Arc::new(Self {
            host: Arc::default(),
            commands: Mutex::default(),
            command: Mutex::new(Box::new(command)),
            posts: Mutex::default(),
            responses: Mutex::default(),
            euid: rustix::process::geteuid().as_raw(),
        })
    }

    pub fn context(self: &Arc<Self>) -> Context {
        Context { system: self.clone() }
    }

    pub fn respond(&self, status: u16, body: serde_json::Value) {
        self.responses.lock().unwrap().push_back((status, body));
    }

    pub fn calls(&self) -> Vec<String> {
        self.host.lock().unwrap().calls.clone()
    }
}

impl System for Fake {
    fn operator(&self, _: &Path) -> Result<Box<dyn Operator>> {
        Ok(Box::new(FakeOperator(self.host.clone())))
    }

    fn run(&self, program: &str, arguments: &[&str], _: Deadline) -> Result<CommandOutput> {
        self.commands.lock().unwrap().push(format!("{program} {}", arguments.join(" ")));
        Ok((self.command.lock().unwrap())(program, arguments))
    }

    fn process_running(&self, pid: u32) -> bool {
        pid == 4242
    }

    fn post(&self, url: &str, authorization: &str, body: &[u8], _: u64) -> Result<HttpResponse> {
        self.posts.lock().unwrap().push((url.into(), authorization.into(), serde_json::from_slice(body).unwrap()));
        let (status, body) = self.responses.lock().unwrap().pop_front().ok_or_else(|| crate::error::err("connection refused"))?;
        Ok(HttpResponse { status, body: serde_json::to_vec(&body).unwrap() })
    }

    fn euid(&self) -> u32 {
        self.euid
    }

    fn interrupted(&self) -> bool {
        false
    }
}

/// An installed Host of the current user below `root`, in the layout of the build platform.
pub fn installed(root: &Path) -> Paths {
    let service = ServiceConfig { version: 1, mode: ServiceMode::Agent, user: "tester".into(), group: "staff".into(), uid: rustix::process::geteuid().as_raw(), gid: rustix::process::getegid().as_raw(), ..ServiceConfig::default() };
    let data = root.join("Library/Application Support/AgentVMM");
    let app = root.join("Library/Application Support/Agent VMM Host/current").join(crate::HOST_APP_NAME);
    let label = crate::HOST_BUNDLE_IDENTIFIER;
    let paths = Paths {
        home: root.to_path_buf(),
        source: app.clone(),
        logs: root.join("Library/Logs/AgentVMM"),
        service_logs: root.join("Library/Logs/AgentVMM"),
        service_config_file: data.join("service.json"),
        application_data: data,
        agent_plist: root.join("Library/LaunchAgents").join(format!("{label}.plist")),
        daemon_plist: root.join("LaunchDaemons").join(format!("{label}.plist")),
        host_plist: root.join("Library/LaunchAgents").join(format!("{label}.plist")),
        install_root: root.join("opt"),
        unit: root.join("etc/agent-vmm-host.service"),
        app,
        service,
        correlation_request_id: None,
    };
    // The files that make the Host count as installed on either platform.
    for file in [paths.app.join("Contents/MacOS/agent-vmm-host"), paths.app.join("agent-vmm"), paths.host_plist.clone(), paths.unit.clone()] {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, b"").unwrap();
    }
    paths
}
