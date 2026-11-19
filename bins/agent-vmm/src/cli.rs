//! The managed commands: one bounded observation or one request to an owner,
//! and one result envelope on standard output.

use crate::envelope::{self, Envelope};
use crate::helper;
use crate::options::{self, Options};
use crate::runtime::{OBSERVATION_BUDGET, Runtime, Session};
use crate::{mutation, observe, update};
use std::io::Write;
use vmm_lifecycle::Deadline;

pub const USAGE: &str = "usage: agent-vmm inspect|diagnose|logs|operation|repair|update ... | agent-vmm vm start|status|stop ... | agent-vmm cleanup preview|execute ... | agent-vmm host-socket set|remove|list ... | agent-vmm appliance install|update|status|recover|stop|start|activate|restart|revoke|remove ...";

pub fn is_managed(command: &str) -> bool {
    matches!(command, "inspect" | "diagnose" | "logs" | "operation" | "repair" | "update" | "vm" | "cleanup" | "host-socket")
}

fn help(command: &str) -> String {
    let text = match command {
        "inspect" => "usage: agent-vmm inspect [--service-type agent|daemon|auto] [--service-user USER] [--registration-id ID] [--environment-id ID] [--workload-id ID] [--json]",
        "diagnose" => "usage: agent-vmm diagnose [inspect options] [--json]",
        "logs" => "usage: agent-vmm logs (--component host|guest [--operation-id ID] | --environment-id ID --container-id ID [--include-container-output]) [--after-cursor CURSOR] [--limit 1..100] [--json]",
        "operation" => "usage: agent-vmm operation get|follow --owner lifecycle|host --operation-id ID [--json]",
        "host-socket" => "usage: agent-vmm host-socket set [--environment-id ID] --port PORT --path PATH --request-id ID | agent-vmm host-socket remove [--environment-id ID] --port PORT --request-id ID | agent-vmm host-socket list [--environment-id ID] [--operator-socket PATH] [--json]\nThe containers of the environment (without --environment-id: of every environment) reach the Host Unix socket at PATH over TCP at 100.127.255.1:PORT.",
        "vm" => "usage: agent-vmm vm status [--operator-socket PATH] [--json] | agent-vmm vm start|stop --request-id ID [--operator-socket PATH] [--force (stop only)] [--json]\nvm start and stop acknowledge an action on the whole VM, including workloads that are not individually listed.",
        "repair" => "usage: agent-vmm repair --request-id ID [--registration-id ID] [--json]",
        "update" => "usage: agent-vmm update --request-id ID --target-release-id ID --artifact-url HTTPS_URL --artifact-sha256 HEX --artifact-size BYTES [--service-type agent|daemon|auto] [--service-user USER] [--json]",
        "cleanup" => "usage: agent-vmm cleanup preview --environment-id ID --kind reset|discard [--operator-socket PATH] [--json] | agent-vmm cleanup execute --environment-id ID --kind reset|discard --plan-revision REVISION --request-id ID [--confirm-volume ID ...] [--confirm-irreversible (discard only)] [--operator-socket PATH] [--json]",
        _ => USAGE,
    };
    format!("{text}\n")
}

/// The issue code of a command that failed without an envelope.
fn failure_code(command: &str) -> &'static str {
    match command {
        "inspect" | "diagnose" => "observation_failed",
        "logs" => "log_read_failed",
        "operation" => "operation_observation_failed",
        "vm" => "vm_operation_failed",
        "repair" => "repair_failed",
        "update" => "update_failed",
        "host-socket" => "host_socket_failed",
        _ => "cleanup_failed",
    }
}

fn subcommand_requirement(command: &str) -> &'static str {
    match command {
        "operation" => "operation requires get or follow",
        "vm" => "vm requires start, status, or stop",
        "cleanup" => "cleanup requires preview or execute",
        _ => "host-socket requires set, remove, or list",
    }
}

/// The helper and the socket of a command that names none.
fn apply_defaults(command: &str, options: &mut Options) -> Result<(), String> {
    let owner_command = matches!(command, "vm" | "cleanup" | "host-socket");
    if options.lifecycle.is_empty() && !owner_command {
        let executable = std::env::current_exe().unwrap_or_default();
        options.lifecycle = helper::default_lifecycle_helper(&executable).display().to_string();
    }
    if owner_command && options.operator_socket.is_empty() {
        if std::env::var_os("HOME").is_none_or(|home| home.is_empty()) {
            return Err("service user home is unavailable".into());
        }
        options.operator_socket = helper::default_operator_socket();
    }
    Ok(())
}

/// Runs one managed command and returns its exit status. A failure prints an envelope on standard error.
pub fn run(arguments: &[String], stdout: &mut dyn Write, stderr: &mut dyn Write, runtime: &dyn Runtime) -> i32 {
    let Some(command) = arguments.first().map(String::as_str).filter(|command| is_managed(command)) else {
        let _ = writeln!(stderr, "{USAGE}");
        return 2;
    };
    let asks_help = |arguments: &[String]| matches!(arguments, [flag] if flag == "--help" || flag == "-h");
    let invalid = |stderr: &mut dyn Write, message: &str| {
        let _ = writeln!(stderr, "{}", envelope::failure(command, "invalid_arguments", message, "validation"));
        2
    };
    let mut remaining = &arguments[1..];
    if asks_help(remaining) {
        let _ = write!(stdout, "{}", help(command));
        return 0;
    }
    let mut subcommand = "";
    let choices = options::subcommands(command);
    if !choices.is_empty() {
        let Some(chosen) = remaining.first().map(String::as_str).filter(|chosen| choices.contains(chosen)) else {
            return invalid(stderr, subcommand_requirement(command));
        };
        (subcommand, remaining) = (chosen, &remaining[1..]);
        if command == "operation" && asks_help(remaining) {
            let _ = write!(stdout, "{}", help(command));
            return 0;
        }
    }
    let mut options = match Options::parse(command, subcommand, remaining) {
        Ok(options) => options,
        Err(message) => return invalid(stderr, &message),
    };
    if let Err(message) = apply_defaults(command, &mut options) {
        return invalid(stderr, &message);
    }
    let session = Session { runtime, budget: Deadline::after(OBSERVATION_BUDGET) };
    let single = |envelope: Result<Envelope, String>| envelope.map(|envelope| vec![envelope]);
    let result = match (command, subcommand) {
        ("inspect" | "diagnose", _) => single(observe::inspect(session, command, &options)),
        ("logs", _) => single(observe::logs(session, &options)),
        ("operation", _) => {
            // Each observation of a followed operation is one JSON line.
            let mut emit = |envelope: &Envelope| envelope.clone().render(true).and_then(|line| writeln!(stdout, "{line}").map_err(|error| error.to_string()));
            observe::operation(session, subcommand == "follow", &options, &mut emit)
        }
        ("vm", action) => single(mutation::vm(session, action, &options)),
        ("repair", _) => single(mutation::repair(session, &options)),
        ("update", _) => single(update::update(session, &options)),
        ("host-socket", action) => single(mutation::host_socket(session, action, &options)),
        (_, action) => single(mutation::cleanup(session, action, &options)),
    };
    let envelopes = match result {
        Ok(envelopes) => envelopes,
        Err(message) => {
            let stage = if command == "update" { "validation" } else { "unknown" };
            let _ = writeln!(stderr, "{}", envelope::failure(command, failure_code(command), &message, stage));
            return 1;
        }
    };
    for envelope in envelopes {
        let written = envelope.render(options.json).and_then(|line| writeln!(stdout, "{line}").map_err(|error| error.to_string()));
        if let Err(message) = written {
            let _ = writeln!(stderr, "{}", envelope::failure(command, "encoding_failed", &message, "validation"));
            return 1;
        }
    }
    0
}
