//! Installation, stop and removal of the launchd jobs.

use super::carrier;
use super::disk::{state_disk_owners, wait_state_disk_release};
use super::launchd::{self, Job, alternate_domain, domain, observe, stop_job, wait_process_exit};
use crate::error::{Deadline, Kind, Result, err};
use crate::journal::Journal;
use crate::statefs;
use crate::{Context, Paths, ServiceConfig, ServiceMode, new_operation_id, operator, release_id};
use rustix::fs::{Gid, Mode, OFlags, Uid};
use std::os::unix::fs::{DirBuilderExt, chown};
use std::path::Path;
use std::time::Duration;

const DAEMON_LOGS: &str = "/Library/Logs/AgentVMM";

/// What a stop covers. A Host stop leaves the carrier and its VM running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Host,
    Runtime,
}

fn absent(path: &Path) -> bool {
    matches!(path.symlink_metadata(), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
}

fn create_dir(path: &Path, mode: u32, message: &str) -> Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(mode).create(path).map_err(|_| err(message))
}

/// The system log directory belongs to root. The service group can read it.
fn prepare_daemon_logs(gid: u32) -> Result<()> {
    match std::fs::DirBuilder::new().mode(0o750).create(DAEMON_LOGS) {
        Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => return Err(error.into()),
        _ => {}
    }
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let directory = rustix::fs::open(DAEMON_LOGS, flags, Mode::empty())?;
    if rustix::fs::fstat(&directory)?.st_uid != 0 {
        return Err(err("daemon log directory must be owned by root"));
    }
    rustix::fs::fchown(&directory, Some(Uid::ROOT), Some(Gid::from_raw(gid)))?;
    Ok(rustix::fs::fchmod(&directory, Mode::from_raw_mode(0o750))?)
}

fn prepare_state_directories(paths: &Paths, euid: u32) -> Result<()> {
    create_dir(&paths.application_data, 0o700, "application data directory could not be created")?;
    create_dir(&paths.logs, 0o700, "log directory could not be created")?;
    if euid == 0 {
        if paths.service.mode == ServiceMode::Daemon {
            prepare_daemon_logs(paths.service.gid)?;
        }
        for directory in [&paths.application_data, &paths.logs] {
            chown(directory, Some(paths.service.uid), Some(paths.service.gid)).map_err(|_| err("service user directory ownership could not be applied"))?;
        }
    }
    Ok(())
}

/// True when launchd runs `job` from exactly the wanted definition, in the domain of the service mode only.
fn definition_current(context: &Context, paths: &Paths, job: Job, desired: &str, deadline: Deadline) -> bool {
    let installed = std::fs::read(job.plist(paths)).is_ok_and(|current| current == desired.as_bytes());
    let label = job.label();
    let running = || observe(context, &format!("{}/{label}", domain(paths)), deadline).is_ok_and(|facts| facts.running);
    let alternate = || launchd::run(context, &["print", &format!("{}/{label}", alternate_domain(paths))], deadline);
    installed && absent(&job.alternate_plist(paths)) && running() && alternate().is_err_and(|error| error.kind == Kind::ServiceNotFound)
}

fn config_current(paths: &Paths, disable_pairing: bool) -> bool {
    let wanted = ServiceConfig { disable_pairing, release_id: release_id().to_string(), ..paths.service.clone() };
    ServiceConfig::read(&paths.service_config_file).is_ok_and(|configured| configured == wanted)
}

fn install_job(context: &Context, paths: &Paths, job: Job, definition: &str, deadline: Deadline) -> Result<()> {
    let daemon = paths.service.mode == ServiceMode::Daemon;
    statefs::replace_file(&job.plist(paths), definition.as_bytes(), if daemon { 0o644 } else { 0o600 }).map_err(|_| err("service definition could not be installed"))?;
    if daemon || context.system.euid() == 0 {
        let _ = std::fs::remove_file(job.alternate_plist(paths));
    }
    let target = format!("{}/{}", domain(paths), job.label());
    // launchd keeps a disabled override after the definition is removed and
    // refuses to bootstrap a disabled service, so enable comes first.
    launchd::run(context, &["enable", &target], deadline).map_err(|error| error.context("host service could not be enabled"))?;
    let plist = job.plist(paths);
    launchd::run(context, &["bootstrap", &domain(paths), &plist.display().to_string()], deadline).map_err(|error| error.context("host service could not be loaded"))
}

fn stop_everywhere(context: &Context, paths: &Paths, job: Job, deadline: Deadline) -> Result<()> {
    for domain in [domain(paths), alternate_domain(paths)] {
        stop_job(context, &format!("{domain}/{}", job.label()), deadline).map_err(|error| error.context("host service could not be stopped before reload"))?;
    }
    Ok(())
}

/// Makes launchd run the carrier and the Host of the installed release. A job
/// that already runs from the wanted definition is left alone.
pub fn reconcile_service(context: &Context, paths: &Paths, disable_pairing: bool, deadline: Deadline) -> Result<()> {
    deadline.check()?;
    let euid = context.system.euid();
    let daemon = paths.service.mode == ServiceMode::Daemon;
    // An omitted option keeps the restrictive policy of the administrator. An explicit option can only tighten it.
    let disable_pairing = disable_pairing || paths.service.disable_pairing || daemon;
    if !paths.app.is_dir() {
        return Err(err("Agent VMM application bundle is missing"));
    }
    if !daemon && euid != 0 && !absent(&paths.daemon_plist) {
        return Err(err("switching to the Agent VMM login agent requires an administrator to remove the existing system LaunchDaemon definition"));
    }
    let carrier_definition = Job::Carrier.definition(paths, disable_pairing);
    let host_definition = Job::Host.definition(paths, disable_pairing);
    let carrier_current = definition_current(context, paths, Job::Carrier, &carrier_definition, deadline) && carrier::started_from(context, paths, &paths.app);
    // The Host is the client of the carrier, so a new carrier needs a new Host.
    let host_current = carrier_current && definition_current(context, paths, Job::Host, &host_definition, deadline);
    if daemon && euid != 0 {
        // The root executor owns the system definitions. Equal definitions are the fence.
        if !host_current {
            return Err(err("installing or updating the Agent VMM daemon requires administrator access"));
        }
        prepare_state_directories(paths, euid)?;
        return paths.service.persist(&paths.service_config_file, disable_pairing, release_id(), euid).map_err(|_| err("service configuration could not be persisted"));
    }
    if host_current && (daemon || config_current(paths, disable_pairing)) {
        return Ok(());
    }
    let definitions = paths.host_plist.parent().ok_or_else(|| err("launchd service directory is invalid"))?;
    create_dir(definitions, if daemon { 0o755 } else { 0o700 }, "launchd service directory could not be created")?;
    prepare_state_directories(paths, euid)?;
    if !host_current {
        stop_everywhere(context, paths, Job::Host, deadline)?;
    }
    if !carrier_current {
        stop_everywhere(context, paths, Job::Carrier, deadline)?;
        install_job(context, paths, Job::Carrier, &carrier_definition, deadline)?;
        carrier::record(context, paths)?;
    }
    if !host_current {
        install_job(context, paths, Job::Host, &host_definition, deadline)?;
    }
    deadline.check()?;
    paths.service.persist(&paths.service_config_file, disable_pairing, release_id(), euid).map_err(|_| err("service configuration could not be persisted"))
}

/// Stops the jobs of `scope` in both launchd domains and waits for their
/// processes. A runtime stop also waits until the state disk is closed.
pub fn stop(context: &Context, paths: &Paths, scope: Scope, deadline: Deadline) -> Result<()> {
    deadline.check()?;
    let jobs: &[Job] = if scope == Scope::Host { &[Job::Host] } else { &[Job::Host, Job::Carrier] };
    let mut targets = Vec::new();
    let mut pids = Vec::new();
    let mut host_running = false;
    for job in jobs {
        for domain in [domain(paths), alternate_domain(paths)] {
            let target = format!("{domain}/{}", job.label());
            let facts = match observe(context, &target, deadline) {
                Ok(facts) => facts,
                Err(error) if error.kind == Kind::ServiceNotFound => continue,
                Err(error) => return Err(error.context("host service status could not be read before stop")),
            };
            if facts.running {
                pids.push(facts.pid);
                host_running |= *job == Job::Host;
            }
            targets.push((domain, target));
        }
    }
    let attached = scope == Scope::Runtime && (host_running || !state_disk_owners(context, paths, deadline)?.is_empty());
    if attached {
        // Ask for an orderly VM stop. An older or damaged Host cannot answer;
        // launchd still terminates the processes, and the disk release below is the proof.
        if let Err(error) = operator::stop_vm(context, paths, &new_operation_id("lifecycle-stop"), deadline.within(Duration::from_secs(40))) {
            eprintln!("VM stop request failed: {error}");
        }
    }
    if context.system.euid() != 0 && targets.iter().any(|(domain, _)| domain == "system") {
        return Err(err("administrator must stop the Host with agent-vmm-service-executor stop, then retry this request"));
    }
    for (_, target) in &targets {
        stop_job(context, target, deadline).map_err(|error| error.context("host service could not be stopped"))?;
    }
    for pid in pids {
        wait_process_exit(context, pid, deadline).map_err(|error| error.context("host process did not exit after launchd removed the service"))?;
    }
    if scope == Scope::Runtime {
        wait_state_disk_release(context, paths, deadline)?;
    }
    deadline.check()
}

pub fn drain_service(context: &Context, paths: &Paths, deadline: Deadline) -> Result<()> {
    stop(context, paths, Scope::Runtime, deadline)
}

/// Removes the definitions of both service modes and the installed Host.
pub fn remove_software(paths: &Paths) -> Result<()> {
    for job in [Job::Host, Job::Carrier] {
        for definition in [job.plist(paths), job.alternate_plist(paths)] {
            match std::fs::remove_file(&definition) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(err("host service definition could not be removed")),
                _ => {}
            }
        }
    }
    remove_tree(&paths.app).map_err(|_| err("application bundle could not be removed"))
}

pub fn remove_tree(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
        _ => Ok(()),
    }
}

pub fn reset_data(context: &Context, paths: &Paths) -> Result<()> {
    match Journal::open(context, paths) {
        Ok(journal) => journal.remove_host_data()?,
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error),
    }
    remove_tree(&paths.logs)
}

pub fn remove_service(context: &Context, paths: &Paths, purge: bool, deadline: Deadline) -> Result<()> {
    if paths.service.mode == ServiceMode::Daemon && context.system.euid() != 0 {
        return Err(err("uninstalling the Agent VMM daemon requires administrator access"));
    }
    stop(context, paths, Scope::Runtime, deadline).map_err(|error| error.context("host service could not be stopped before uninstall"))?;
    let _ = std::fs::remove_file(&paths.service_config_file);
    if purge {
        reset_data(context, paths).map_err(|_| err("application data could not be removed"))?;
    }
    remove_software(paths)?;
    deadline.check()
}
