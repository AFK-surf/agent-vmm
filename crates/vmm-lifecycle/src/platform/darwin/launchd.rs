//! `launchctl` calls and what they observe.

use super::paths::{carrier_label, host_label};
use super::plist;
use crate::error::{Deadline, Error, Kind, Result, err};
use crate::{Context, HostServiceFacts, Paths, ServiceMode};
use std::path::PathBuf;
use std::time::Duration;

const LAUNCHCTL: &str = "/bin/launchctl";
const POLL: Duration = Duration::from_millis(25);

fn absent(action: &str, message: &str) -> bool {
    let unknown = ["Could not find service", "Could not find domain", "Domain does not support specified action"].iter().any(|text| message.contains(text));
    (action == "print" || action == "bootout") && (unknown || action == "bootout" && message.contains("No such process"))
}

/// Runs `launchctl`. A job that is not loaded is `Kind::ServiceNotFound`.
pub fn run(context: &Context, arguments: &[&str], deadline: Deadline) -> Result<()> {
    let action = arguments[0];
    let output = context.system.run(LAUNCHCTL, arguments, deadline).map_err(|error| error.context(format!("launchctl {action} was interrupted")))?;
    if output.success() {
        return Ok(());
    }
    let message = output.combined();
    if absent(action, &message) {
        return Err(Error::new(Kind::ServiceNotFound, format!("host service not found: {message}")));
    }
    Err(err(format!("launchctl {action} failed: {message}")))
}

pub fn parse_observation(output: &str) -> Result<HostServiceFacts> {
    let mut facts = HostServiceFacts { loaded: true, ..HostServiceFacts::default() };
    for line in output.lines() {
        let Some((key, value)) = line.trim().split_once('=') else { continue };
        match key.trim() {
            "pid" => facts.pid = value.trim().parse().ok().filter(|pid| *pid > 0).ok_or_else(|| err("launchctl print reported an invalid Host PID"))?,
            "last exit code" | "last exit status" => facts.last_exit_status = value.trim().parse().ok().or(facts.last_exit_status),
            _ => {}
        }
    }
    Ok(facts)
}

/// Reads one job. `target` is `<domain>/<label>`.
pub fn observe(context: &Context, target: &str, deadline: Deadline) -> Result<HostServiceFacts> {
    let output = context.system.run(LAUNCHCTL, &["print", target], deadline)?;
    if !output.success() {
        let message = output.combined();
        let kind = if absent("print", &message) { Kind::ServiceNotFound } else { Kind::Other };
        return Err(Error::new(kind, format!("launchctl print failed: {message}")));
    }
    let mut facts = parse_observation(&output.stdout)?;
    if facts.pid > 0 && !context.system.process_running(facts.pid) {
        facts.pid = 0;
    }
    facts.running = facts.pid > 0;
    Ok(facts)
}

/// The two launchd jobs of one Host installation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Job {
    Host,
    Carrier,
}

impl Job {
    pub fn label(self) -> String {
        match self {
            Self::Host => host_label(),
            Self::Carrier => carrier_label(),
        }
    }

    fn file(self, host: &std::path::Path) -> PathBuf {
        host.with_file_name(format!("{}.plist", self.label()))
    }

    /// The definition in the launchd domain of the service mode.
    pub fn plist(self, paths: &Paths) -> PathBuf {
        self.file(&paths.host_plist)
    }

    /// The definition that the other service mode would install.
    pub fn alternate_plist(self, paths: &Paths) -> PathBuf {
        self.file(if paths.service.mode == ServiceMode::Daemon { &paths.agent_plist } else { &paths.daemon_plist })
    }

    pub fn definition(self, paths: &Paths, disable_pairing: bool) -> String {
        let label = self.label();
        let service = &paths.service;
        let params = plist::Params { label: &label, home: &paths.home, app: &paths.app, user: &service.user, group: &service.group, daemon: service.mode == ServiceMode::Daemon, disable_pairing };
        match self {
            Self::Host => plist::host(&params),
            Self::Carrier => plist::carrier(&params),
        }
    }
}

pub fn domain(paths: &Paths) -> String {
    if paths.service.mode == ServiceMode::Daemon { "system".into() } else { format!("gui/{}", paths.service.uid) }
}

pub fn alternate_domain(paths: &Paths) -> String {
    if paths.service.mode == ServiceMode::Daemon { format!("gui/{}", paths.service.uid) } else { "system".into() }
}

pub fn host_job(paths: &Paths) -> String {
    format!("{}/{}", domain(paths), host_label())
}

pub fn host_installed(paths: &Paths) -> bool {
    paths.app.join("Contents/MacOS/agent-vmm-host").is_file() && paths.host_plist.is_file()
}

pub fn observe_host_service(context: &Context, paths: &Paths, deadline: Deadline) -> Result<HostServiceFacts> {
    observe(context, &host_job(paths), deadline)
}

/// Removes one job from launchd and waits until launchd no longer lists it.
pub fn stop_job(context: &Context, target: &str, deadline: Deadline) -> Result<()> {
    let loaded = |context: &Context| match run(context, &["print", target], deadline) {
        Ok(()) => Ok(true),
        Err(error) if error.kind == Kind::ServiceNotFound => Ok(false),
        Err(error) => Err(error.context("host service status could not be read")),
    };
    let bootout = run(context, &["bootout", target], deadline);
    deadline.check()?;
    if !loaded(context)? {
        return Ok(());
    }
    if let Err(error) = bootout {
        return Err(error.context("launchctl bootout failed while the host service remains loaded"));
    }
    while loaded(context)? {
        deadline.sleep(POLL).map_err(|error| error.context("host service did not finish stopping"))?;
    }
    Ok(())
}

pub fn wait_process_exit(context: &Context, pid: u32, deadline: Deadline) -> Result<()> {
    while context.system.process_running(pid) {
        deadline.sleep(POLL)?;
    }
    Ok(())
}
