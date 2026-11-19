//! Linux: one system service of the fixed `agent-vmm` account, managed by systemd.

mod release;

pub use release::release_id as release_identity;

use crate::account;
use crate::error::{Deadline, Error, Kind, Result, err};
use crate::statefs;
use crate::update::UpdateActions;
use crate::{Context, HOST_STOP_TIMEOUT, HostServiceFacts, Paths, ServiceConfig, ServiceMode, ServiceRequest, new_operation_id, operator, release_id};
use rustix::fs::{Gid, Mode, OFlags, Uid};
use std::collections::BTreeMap;
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt, chown};
use std::path::{Path, PathBuf};

const SERVICE_USER: &str = "agent-vmm";
const UNIT_NAME: &str = "agent-vmm-host.service";
const INSTALL_ROOT: &str = "opt/agent-vmm";
const STATE_DIRECTORY: &str = "var/lib/agent-vmm";
const UNIT_TEMPLATE: &str = include_str!("../../../templates/agent-vmm-host.service");

/// The fixed layout below `root`. `app` is the current release link.
pub fn paths_under(root: &Path, source: &Path, service: ServiceConfig) -> Paths {
    let data = root.join(STATE_DIRECTORY);
    let install_root = root.join(INSTALL_ROOT);
    Paths {
        correlation_request_id: None,
        home: PathBuf::new(),
        app: install_root.join("current"),
        source: source.to_path_buf(),
        install_root,
        unit: root.join("etc/systemd/system").join(UNIT_NAME),
        logs: data.join("logs"),
        service_logs: root.join("var/log/agent-vmm"),
        service_config_file: data.join("service.json"),
        application_data: data,
        service,
        agent_plist: PathBuf::new(),
        daemon_plist: PathBuf::new(),
        host_plist: PathBuf::new(),
    }
}

/// The root of the fixed layout.
#[cfg(not(feature = "e2e-isolation"))]
fn root() -> PathBuf {
    PathBuf::from("/")
}

/// A test build moves the layout away from the system directories.
#[cfg(feature = "e2e-isolation")]
fn root() -> PathBuf {
    let root = PathBuf::from(std::env::var_os("AGENT_VMM_E2E_ROOT").unwrap_or_default());
    let canonical = root.canonicalize().is_ok_and(|canonical| canonical == root);
    assert!(root.is_absolute() && canonical && root != Path::new("/"), "an e2e lifecycle build requires a canonical AGENT_VMM_E2E_ROOT other than /");
    root
}

pub fn check_mutation_user(euid: u32) -> Result<()> {
    if euid != 0 {
        return Err(err("Linux lifecycle mutations install or change the system service; run them as root with sudo"));
    }
    Ok(())
}

/// Linux has no shared-Host maintenance journal and no publication command.
pub fn execute_platform_command(_: &Context, _: &str, _: &ServiceRequest, _: &[String], _: &mut dyn Read) -> Option<Result<crate::cli::Reply>> {
    None
}

pub fn preflight_data_directory() -> Result<PathBuf> {
    Ok(root().join(STATE_DIRECTORY))
}

fn command(context: &Context, program: &str, arguments: &[&str], deadline: Deadline) -> Result<String> {
    let output = context.system.run(program, arguments, deadline)?;
    if output.success() {
        return Ok(output.stdout);
    }
    Err(err(format!("{program} {}: {}", arguments.join(" "), output.combined())))
}

fn systemctl(context: &Context, arguments: &[&str], deadline: Deadline) -> Result<()> {
    command(context, "systemctl", arguments, deadline).map(|_| ())
}

/// The release directory that contains the running `agent-vmm`.
fn service_source(request: &ServiceRequest) -> Result<PathBuf> {
    if request.mode.is_some() || request.user.is_some() || request.app.is_some() || request.shared_host {
        return Err(err("Linux uses the fixed agent-vmm system service; service selection options are not accepted"));
    }
    let executable = std::env::current_exe().map_err(|_| err("agent-vmm location is unavailable"))?.canonicalize().map_err(|_| err("agent-vmm location is invalid"))?;
    executable.parent().map(Path::to_path_buf).ok_or_else(|| err("agent-vmm location is invalid"))
}

fn service_config(state: &Path) -> Result<ServiceConfig> {
    let account = account::lookup(Some(SERVICE_USER)).map_err(|_| err("the agent-vmm service user is unavailable"))?;
    if account.uid == 0 || account.group.is_empty() {
        return Err(err("the agent-vmm service user must be a non-root user with a primary group"));
    }
    let disable_pairing = ServiceConfig::read(&state.join("service.json")).is_ok_and(|configured| configured.disable_pairing);
    Ok(ServiceConfig { version: 1, mode: ServiceMode::Daemon, user: account.name, group: account.group, uid: account.uid, gid: account.gid, release_id: release_id().to_string(), disable_pairing })
}

/// Creates the system user that runs the Host.
fn ensure_service_account(context: &Context, deadline: Deadline) -> Result<()> {
    if account::lookup(Some(SERVICE_USER)).is_ok() {
        return Ok(());
    }
    let home = root().join(STATE_DIRECTORY).display().to_string();
    command(context, "useradd", &["--system", "--user-group", "--home-dir", &home, "--no-create-home", "--shell", "/usr/sbin/nologin", SERVICE_USER], deadline).map(|_| ())
}

fn prepare_state_directories(paths: &Paths) -> Result<()> {
    for directory in [&paths.application_data, &paths.logs] {
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(directory).map_err(|error| err(format!("create {}: {error}", directory.display())))?;
        std::fs::set_permissions(directory, std::fs::Permissions::from_mode(0o700))?;
        chown(directory, Some(paths.service.uid), Some(paths.service.gid)).map_err(|_| err("service user directory ownership could not be applied"))?;
    }
    // The service log directory belongs to root. The service group can read it.
    std::fs::DirBuilder::new().recursive(true).mode(0o750).create(&paths.service_logs)?;
    let directory = rustix::fs::open(&paths.service_logs, OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty())?;
    if rustix::fs::fstat(&directory)?.st_uid != 0 {
        return Err(err("service log directory must be owned by root"));
    }
    rustix::fs::fchown(&directory, Some(Uid::ROOT), Some(Gid::from_raw(paths.service.gid)))?;
    Ok(rustix::fs::fchmod(&directory, Mode::from_raw_mode(0o750))?)
}

/// Prepares the service account and the state directory before a mutation,
/// because the operation journal lives in that directory.
pub fn lifecycle_paths(context: &Context, request: &ServiceRequest) -> Result<Paths> {
    let source = service_source(request)?;
    ensure_service_account(context, Deadline::after(HOST_STOP_TIMEOUT))?;
    let root = root();
    let paths = paths_under(&root, &source, service_config(&root.join(STATE_DIRECTORY))?);
    prepare_state_directories(&paths)?;
    Ok(paths)
}

pub fn observation_paths(_: &Context, request: &ServiceRequest) -> Result<Paths> {
    let source = service_source(request)?;
    let root = root();
    let fallback = ServiceConfig { version: 1, mode: ServiceMode::Daemon, user: SERVICE_USER.into(), ..ServiceConfig::default() };
    Ok(paths_under(&root, &source, service_config(&root.join(STATE_DIRECTORY)).unwrap_or(fallback)))
}

pub fn host_job(_: &Paths) -> String {
    UNIT_NAME.to_string()
}

pub fn host_installed(paths: &Paths) -> bool {
    paths.app.join("agent-vmm").is_file() && paths.unit.is_file()
}

fn render_unit(paths: &Paths) -> String {
    let values = [("User", paths.service.user.clone()), ("Group", paths.service.group.clone()), ("Current", paths.app.display().to_string()), ("State", paths.application_data.display().to_string()), ("ServiceLogs", paths.service_logs.display().to_string())];
    values.iter().fold(UNIT_TEMPLATE.to_string(), |unit, (key, value)| unit.replace(&format!("{{{{{key}}}}}"), value))
}

fn parse_observation(output: &str) -> Result<HostServiceFacts> {
    let properties: BTreeMap<&str, &str> = output.lines().filter_map(|line| line.trim().split_once('=')).collect();
    let property = |key: &str| properties.get(key).copied().unwrap_or_default();
    if property("LoadState") == "not-found" {
        return Err(Error::new(Kind::ServiceNotFound, "host service not found"));
    }
    let pid: u32 = property("MainPID").parse().ok().filter(|_| property("LoadState") == "loaded").ok_or_else(|| err("systemctl show reported an invalid Host service state"))?;
    Ok(HostServiceFacts {
        loaded: matches!(property("ActiveState"), "active" | "activating" | "deactivating" | "reloading"),
        running: pid > 0,
        pid,
        last_exit_status: property("ExecMainStatus").parse().ok().filter(|_| property("ExecMainCode") != "0"),
    })
}

pub fn observe_host_service(context: &Context, paths: &Paths, deadline: Deadline) -> Result<HostServiceFacts> {
    parse_observation(&command(context, "systemctl", &["show", &host_job(paths), "--property=LoadState,ActiveState,MainPID,ExecMainCode,ExecMainStatus"], deadline)?)
}

fn observe_or_absent(context: &Context, paths: &Paths, deadline: Deadline) -> Result<Option<HostServiceFacts>> {
    match observe_host_service(context, paths, deadline) {
        Ok(facts) => Ok(Some(facts)),
        Err(error) if error.kind == Kind::ServiceNotFound => Ok(None),
        Err(error) => Err(error.context("host service status could not be read")),
    }
}

/// Publishes the source release, makes it current, and starts the unit. The
/// system service keeps the fixed daemon policy: personal mesh pairing stays disabled.
pub fn reconcile_service(context: &Context, paths: &Paths, _: bool, deadline: Deadline) -> Result<()> {
    let id = release::install(paths, &paths.source)?;
    let desired = render_unit(paths);
    let facts = observe_or_absent(context, paths, deadline)?.unwrap_or_default();
    let euid = context.system.euid();
    let persist = || paths.service.persist(&paths.service_config_file, true, release_id(), euid).map_err(|_| err("service configuration could not be persisted"));
    let release_current = release::current(paths).as_deref() == Some(id.as_str());
    if std::fs::read(&paths.unit).is_ok_and(|installed| installed == desired.as_bytes()) && release_current && facts.running {
        return persist();
    }
    if !release_current && facts.loaded {
        drain_service(context, paths, deadline)?;
    } else if facts.loaded {
        systemctl(context, &["stop", &host_job(paths)], deadline).map_err(|error| error.context("host service could not be stopped before reload"))?;
    }
    release::switch(paths, &id)?;
    if let Some(directory) = paths.unit.parent() {
        std::fs::DirBuilder::new().recursive(true).mode(0o755).create(directory)?;
    }
    statefs::replace_file(&paths.unit, desired.as_bytes(), 0o644).map_err(|_| err("service definition could not be installed"))?;
    for arguments in [&["daemon-reload"][..], &["enable", UNIT_NAME], &["start", UNIT_NAME]] {
        systemctl(context, arguments, deadline).map_err(|error| error.context("host service could not be started"))?;
    }
    persist()
}

/// Stops the workload VMs through the Host, then stops the unit.
pub fn drain_service(context: &Context, paths: &Paths, deadline: Deadline) -> Result<()> {
    let facts = observe_or_absent(context, paths, deadline)?.unwrap_or_default();
    if facts.running {
        operator::stop_vm(context, paths, &new_operation_id("update-drain"), deadline)?;
    }
    if facts.loaded {
        systemctl(context, &["stop", &host_job(paths)], deadline).map_err(|error| error.context("host service could not be drained"))?;
    }
    Ok(())
}

pub fn remove_service(context: &Context, paths: &Paths, purge: bool, deadline: Deadline) -> Result<()> {
    drain_service(context, paths, deadline)?;
    if observe_or_absent(context, paths, deadline)?.is_some() {
        systemctl(context, &["disable", "--now", &host_job(paths)], deadline).map_err(|error| error.context("host service could not be stopped before uninstall"))?;
    }
    match std::fs::remove_file(&paths.unit) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(err("host service definition could not be removed")),
        _ => {}
    }
    systemctl(context, &["daemon-reload"], deadline)?;
    let _ = std::fs::remove_file(&paths.service_config_file);
    if purge {
        crate::journal::Journal::open(context, paths).and_then(|journal| journal.remove_host_data()).map_err(|_| err("application data could not be removed"))?;
    }
    match std::fs::remove_dir_all(&paths.install_root) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(err("installed releases could not be removed")),
        _ => {}
    }
    deadline.check()
}

pub fn validate_prepared_update(paths: &Paths, source: &Path) -> Result<()> {
    let hidden = source.file_name().is_some_and(|name| name.to_string_lossy().starts_with('.'));
    if source.parent() != Some(paths.install_root.as_path()) || !hidden {
        return Err(err(format!("prepared update must be a hidden directory in {}", paths.install_root.display())));
    }
    Ok(())
}

struct Update<'a> {
    context: &'a Context,
    paths: &'a Paths,
    release_id: &'a str,
}

impl UpdateActions for Update<'_> {
    fn installed_release(&self) -> Result<String> {
        release::release_id(&self.paths.app)
    }

    fn drain(&mut self, _: &Path) -> Result<()> {
        drain_service(self.context, self.paths, Deadline::after(HOST_STOP_TIMEOUT))
    }

    fn replace(&mut self, source: &Path) -> Result<()> {
        let id = release::install(self.paths, source)?;
        if !release::release_id(&self.paths.install_root.join(&id)).is_ok_and(|release| release == self.release_id) {
            return Err(err("copied Host release does not match the selected target"));
        }
        release::switch(self.paths, &id)
    }

    fn repair(&mut self) -> Result<()> {
        // The installed release is the source of the service from here on.
        let current = Paths { source: self.paths.app.clone(), ..self.paths.clone() };
        reconcile_service(self.context, &current, true, Deadline::after(HOST_STOP_TIMEOUT))
    }

    fn remove(&mut self, source: &Path) -> Result<()> {
        match std::fs::remove_dir_all(source) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        release::prune(self.paths)
    }
}

/// Installs the prepared release and switches the current link. The state directory is never part of a release.
pub fn update_application(context: &Context, paths: &Paths, source: &Path, release_id: &str, request_id: &str) -> Result<()> {
    crate::update::run(context, paths, source, release_id, request_id, &mut Update { context, paths, release_id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_properties_describe_the_host_job() {
        let running = parse_observation("LoadState=loaded\nActiveState=active\nMainPID=42\nExecMainCode=0\nExecMainStatus=0\n").unwrap();
        assert_eq!(running, HostServiceFacts { loaded: true, running: true, pid: 42, last_exit_status: None });
        let failed = parse_observation("LoadState=loaded\nActiveState=failed\nMainPID=0\nExecMainCode=1\nExecMainStatus=3\n").unwrap();
        assert_eq!(failed, HostServiceFacts { loaded: false, running: false, pid: 0, last_exit_status: Some(3) });
        assert_eq!(parse_observation("LoadState=not-found\nActiveState=inactive\nMainPID=0\n").unwrap_err().kind, Kind::ServiceNotFound);
        assert!(parse_observation("LoadState=masked\nMainPID=0\n").is_err());
    }

    #[test]
    fn the_unit_runs_the_current_release_as_the_service_user() {
        let service = ServiceConfig { user: "agent-vmm".into(), group: "agent-vmm".into(), ..ServiceConfig::default() };
        let unit = render_unit(&paths_under(Path::new("/"), Path::new("/tmp/release"), service));
        assert!(unit.contains("User=agent-vmm\nGroup=agent-vmm\nWorkingDirectory=/var/lib/agent-vmm\n"));
        assert!(unit.contains("ExecStart=/opt/agent-vmm/current/agent-vmm host --bundle /opt/agent-vmm/current/guest --instance /var/lib/agent-vmm/instance "));
        assert!(unit.contains("StandardError=append:/var/log/agent-vmm/host.stderr.log") && !unit.contains("{{"));
    }
}
