//! The options of the managed commands. Each command accepts a fixed set.

use std::path::Path;

pub const DEFAULT_LIST_LIMIT: usize = 50;
pub const MAX_LIST_LIMIT: usize = 100;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Options {
    pub json: bool,
    pub service_type: String,
    pub service_user: String,
    pub service_app: String,
    pub lifecycle: String,
    pub operator_socket: String,
    pub registration_id: String,
    pub environment_id: String,
    pub workload_id: String,
    pub container_id: String,
    pub component: String,
    pub owner: String,
    pub operation_id: String,
    pub after_cursor: String,
    pub limit: usize,
    pub follow: bool,
    pub include_content: bool,
    pub request_id: String,
    pub force: bool,
    pub cleanup_kind: String,
    pub plan_revision: u64,
    pub confirm_volumes: Vec<String>,
    pub confirm_cleanup: bool,
    pub artifact_url: String,
    pub artifact_sha256: String,
    pub artifact_size: u64,
    pub target_release: String,
    pub port: u32,
    pub path: String,
}

const OBSERVE: &[&str] = &["operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "registration-id", "environment-id", "workload-id"];

/// The options of a command, or `None` for an unknown command.
fn accepted(command: &str, subcommand: &str) -> Option<&'static [&'static str]> {
    Some(match (command, subcommand) {
        ("inspect" | "diagnose", "") => OBSERVE,
        ("logs", "") => &["json", "service-type", "service-user", "service-app", "lifecycle-helper", "operator-socket", "registration-id", "environment-id", "container-id", "component", "operation-id", "after-cursor", "limit", "include-container-output"],
        ("operation", "get") => &["operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "owner", "operation-id"],
        ("operation", "follow") => &["operator-socket", "json", "service-type", "service-user", "service-app", "lifecycle-helper", "owner", "operation-id", "follow"],
        ("vm", "start") => &["json", "operator-socket", "request-id"],
        ("vm", "status") => &["json", "operator-socket"],
        ("vm", "stop") => &["json", "operator-socket", "request-id", "force"],
        ("repair", "") => &["json", "service-type", "service-user", "service-app", "lifecycle-helper", "request-id", "registration-id"],
        ("update", "") => &["json", "service-type", "service-user", "lifecycle-helper", "request-id", "target-release-id", "artifact-url", "artifact-sha256", "artifact-size"],
        ("host-socket", "set") => &["json", "operator-socket", "environment-id", "port", "path", "request-id"],
        ("host-socket", "remove") => &["json", "operator-socket", "environment-id", "port", "request-id"],
        ("host-socket", "list") => &["json", "operator-socket", "environment-id"],
        ("cleanup", "preview") => &["json", "operator-socket", "environment-id", "kind"],
        ("cleanup", "execute") => &["json", "operator-socket", "environment-id", "kind", "request-id", "plan-revision", "confirm-volume", "confirm-irreversible"],
        _ => return None,
    })
}

/// The subcommands of a command. A command without subcommands has none.
pub fn subcommands(command: &str) -> &'static [&'static str] {
    match command {
        "operation" => &["get", "follow"],
        "vm" => &["start", "status", "stop"],
        "cleanup" => &["preview", "execute"],
        "host-socket" => &["set", "remove", "list"],
        _ => &[],
    }
}

pub fn identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    (1..=128).contains(&value.len()) && bytes.next().is_some_and(|first| first.is_ascii_alphanumeric()) && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'+' | b'-'))
}

fn boolean(name: &str) -> bool {
    matches!(name, "json" | "follow" | "include-container-output" | "force" | "confirm-irreversible")
}

impl Options {
    fn set(&mut self, name: &str, value: &str) -> Result<(), String> {
        let number = |value: &str| value.parse::<u64>().map_err(|_| format!("--{name} is not a number"));
        let flag = |value: &str| match value {
            "" | "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
            _ => Err(format!("--{name} is not a boolean")),
        };
        match name {
            "json" => self.json = flag(value)?,
            "follow" => self.follow = flag(value)?,
            "include-container-output" => self.include_content = flag(value)?,
            "force" => self.force = flag(value)?,
            "confirm-irreversible" => self.confirm_cleanup = flag(value)?,
            "service-type" => self.service_type = value.into(),
            "service-user" => self.service_user = value.into(),
            "service-app" => self.service_app = value.into(),
            "lifecycle-helper" => self.lifecycle = value.into(),
            "operator-socket" => self.operator_socket = value.into(),
            "registration-id" => self.registration_id = value.into(),
            "environment-id" => self.environment_id = value.into(),
            "workload-id" => self.workload_id = value.into(),
            "container-id" => self.container_id = value.into(),
            "component" => self.component = value.into(),
            "owner" => self.owner = value.into(),
            "operation-id" => self.operation_id = value.into(),
            "after-cursor" => self.after_cursor = value.into(),
            "request-id" => self.request_id = value.into(),
            "kind" => self.cleanup_kind = value.into(),
            "artifact-url" => self.artifact_url = value.into(),
            "artifact-sha256" => self.artifact_sha256 = value.into(),
            "target-release-id" => self.target_release = value.into(),
            "path" => self.path = value.into(),
            "limit" => self.limit = number(value)? as usize,
            "plan-revision" => self.plan_revision = number(value)?,
            "artifact-size" => self.artifact_size = number(value)?,
            "port" => self.port = u32::try_from(number(value)?).map_err(|_| "--port is out of range".to_string())?,
            "confirm-volume" => {
                if !identifier(value) || self.confirm_volumes.iter().any(|current| current == value) {
                    return Err("--confirm-volume is invalid or repeated".into());
                }
                self.confirm_volumes.push(value.into());
            }
            _ => return Err("unknown or incomplete argument".into()),
        }
        Ok(())
    }

    /// Parses `--name value`, `--name=value` and boolean `--name` for one command.
    pub fn parse(command: &str, subcommand: &str, arguments: &[String]) -> Result<Self, String> {
        let incomplete = || "unknown or incomplete argument".to_string();
        let accepted = accepted(command, subcommand).ok_or_else(incomplete)?;
        let mut options = Self { limit: DEFAULT_LIST_LIMIT, ..Self::default() };
        let mut index = 0;
        while index < arguments.len() {
            let argument = arguments[index].strip_prefix("--").or_else(|| arguments[index].strip_prefix('-')).ok_or_else(incomplete)?;
            let (name, inline) = argument.split_once('=').map_or((argument, None), |(name, value)| (name, Some(value)));
            if !accepted.contains(&name) {
                return Err(incomplete());
            }
            let value = match inline {
                Some(value) => value,
                None if boolean(name) => "",
                None => {
                    index += 1;
                    arguments.get(index).ok_or_else(incomplete)?
                }
            };
            options.set(name, value)?;
            index += 1;
        }
        options.validate(command, subcommand)?;
        Ok(options)
    }

    fn validate(&self, command: &str, subcommand: &str) -> Result<(), String> {
        let refuse = |message: &str| Err(message.to_string());
        if !(1..=MAX_LIST_LIMIT).contains(&self.limit) {
            return refuse("--limit must be between 1 and 100");
        }
        if !matches!(self.service_type.as_str(), "" | "agent" | "daemon" | "auto") {
            return refuse("--service-type must be agent, daemon, or auto");
        }
        for (name, value) in [("registration-id", &self.registration_id), ("environment-id", &self.environment_id), ("workload-id", &self.workload_id), ("container-id", &self.container_id), ("operation-id", &self.operation_id), ("request-id", &self.request_id)] {
            if !value.is_empty() && !identifier(value) {
                return Err(format!("--{name} is invalid"));
            }
        }
        if self.after_cursor.len() > 4096 {
            return refuse("--after-cursor exceeds 4096 bytes");
        }
        match command {
            "logs" => {
                let local = matches!(self.component.as_str(), "host" | "guest");
                let container = !self.environment_id.is_empty() && !self.container_id.is_empty();
                if local == container || !self.component.is_empty() && !local {
                    return refuse("logs requires either --component host|guest or both --environment-id and --container-id");
                }
                if container && !self.operation_id.is_empty() {
                    return refuse("container logs do not accept --operation-id");
                }
                if local && self.include_content {
                    return refuse("--include-container-output applies only to container logs");
                }
                if local && (!self.registration_id.is_empty() || !self.environment_id.is_empty()) {
                    return refuse("local component logs do not accept controller scope identifiers");
                }
            }
            "operation" if self.operation_id.is_empty() || !matches!(self.owner.as_str(), "lifecycle" | "host") => {
                return refuse("operation requires --owner lifecycle|host and --operation-id");
            }
            "inspect" | "diagnose" if !self.workload_id.is_empty() && self.registration_id.is_empty() => {
                return refuse("--workload-id requires the controller-provided --registration-id");
            }
            "vm" if (subcommand == "status") != self.request_id.is_empty() => {
                return refuse(if subcommand == "status" { "vm status does not accept --request-id" } else { "vm mutation requires --request-id" });
            }
            "repair" if self.request_id.is_empty() => return refuse("repair requires --request-id"),
            "cleanup" => {
                if self.environment_id.is_empty() || !matches!(self.cleanup_kind.as_str(), "reset" | "discard") {
                    return refuse("cleanup requires --environment-id and --kind reset|discard");
                }
                let execute = subcommand == "execute";
                if execute && (self.plan_revision == 0 || self.request_id.is_empty()) {
                    return refuse("cleanup execute requires --plan-revision and --request-id");
                }
                if execute && self.cleanup_kind == "discard" && !self.confirm_cleanup {
                    return refuse("cleanup discard requires --confirm-irreversible");
                }
                if execute && self.cleanup_kind == "reset" && (self.confirm_cleanup || !self.confirm_volumes.is_empty()) {
                    return refuse("cleanup reset does not accept discard confirmations");
                }
            }
            "update" => {
                let digest = self.artifact_sha256.len() == 64 && self.artifact_sha256.bytes().all(|byte| byte.is_ascii_hexdigit());
                if self.request_id.is_empty() || !identifier(&self.target_release) || self.artifact_url.is_empty() || self.artifact_size == 0 || !digest {
                    return refuse("update requires --request-id, --target-release-id, --artifact-url, --artifact-sha256, and --artifact-size");
                }
            }
            "host-socket" => {
                if subcommand != "list" && (self.port == 0 || self.port > 65535 || self.request_id.is_empty()) {
                    return Err(format!("host-socket {subcommand} requires --port and --request-id"));
                }
                if subcommand == "set" && !Path::new(&self.path).is_absolute() {
                    return refuse("host-socket set requires an absolute --path");
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The service options that the lifecycle helper receives.
    pub fn service_arguments(&self) -> Vec<String> {
        let mut arguments = Vec::new();
        for (flag, value) in [("--service-type", &self.service_type), ("--service-user", &self.service_user), ("--service-app", &self.service_app)] {
            if !value.is_empty() {
                arguments.extend([flag.to_string(), value.clone()]);
            }
        }
        arguments
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(command: &str, subcommand: &str, arguments: &[&str]) -> Result<Options, String> {
        Options::parse(command, subcommand, &arguments.iter().map(|argument| argument.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn the_invocations_of_the_provisioner_are_accepted() {
        let inspect = parse("inspect", "", &["--json", "--lifecycle-helper", "/h/agent-vmm-lifecycle", "--service-type", "daemon", "--service-user", "worker"]).unwrap();
        assert!(inspect.json && inspect.lifecycle == "/h/agent-vmm-lifecycle");
        assert_eq!(inspect.service_arguments(), ["--service-type", "daemon", "--service-user", "worker"]);
        let digest = "a".repeat(64);
        let update = parse("update", "", &["--json", "--request-id", "r1", "--target-release-id", "agent-vmm-0123", "--artifact-url", "https://x/y.zip", "--artifact-sha256", &digest, "--artifact-size", "42", "--lifecycle-helper", "/h"]).unwrap();
        assert_eq!((update.artifact_size, update.target_release.as_str()), (42, "agent-vmm-0123"));
        assert!(parse("update", "", &["--json", "--request-id", "r1"]).is_err());
        assert!(parse("update", "", &["--operator-socket", "/s"]).is_err(), "an update never names a socket");
    }

    #[test]
    fn each_command_refuses_the_options_of_another() {
        assert!(parse("inspect", "", &["--request-id", "r1"]).is_err());
        assert!(parse("inspect", "", &["--workload-id", "w1"]).is_err());
        assert!(parse("inspect", "", &["positional"]).is_err());
        assert!(parse("vm", "status", &["--request-id", "r1"]).is_err());
        assert!(parse("vm", "start", &[]).is_err());
        assert!(parse("vm", "start", &["--request-id", "r1", "--force"]).is_err());
        assert!(parse("vm", "stop", &["--request-id=r1", "--force"]).unwrap().force);
        assert!(parse("logs", "", &["--component", "host", "--environment-id", "e1"]).is_err());
        assert!(parse("logs", "", &["--environment-id", "e1", "--container-id", "c1", "--limit", "101"]).is_err());
        assert_eq!(parse("logs", "", &["--environment-id", "e1", "--container-id", "c1", "--limit", "7"]).unwrap().limit, 7);
        assert!(parse("cleanup", "execute", &["--environment-id", "e1", "--kind", "discard", "--plan-revision", "2", "--request-id", "r1"]).is_err());
        let discard = parse("cleanup", "execute", &["--environment-id", "e1", "--kind", "discard", "--plan-revision", "2", "--request-id", "r1", "--confirm-irreversible", "--confirm-volume", "v1", "--confirm-volume", "v2"]).unwrap();
        assert_eq!(discard.confirm_volumes, ["v1", "v2"]);
        assert!(parse("host-socket", "set", &["--port", "70000", "--path", "/s", "--request-id", "r1"]).is_err());
        assert!(parse("operation", "get", &["--owner", "cloud", "--operation-id", "o1"]).is_err());
    }
}
