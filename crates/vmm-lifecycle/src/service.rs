use crate::error::{Result, err};
use crate::{HOST_APP_NAME, statefs};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceMode {
    #[default]
    Agent,
    Daemon,
    Auto,
}

impl ServiceMode {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "agent" => Some(Self::Agent),
            "daemon" => Some(Self::Daemon),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// The service identity options. They can follow the command at any position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ServiceRequest {
    pub mode: Option<ServiceMode>,
    pub user: Option<String>,
    pub app: Option<PathBuf>,
    pub action: String,
    pub shared_host: bool,
}

impl ServiceRequest {
    /// Separates the service options from the arguments of the command.
    pub fn extract(action: &str, arguments: &[String]) -> Result<(Self, Vec<String>)> {
        let mut request = Self { action: action.to_string(), ..Self::default() };
        let mut remaining = Vec::with_capacity(arguments.len());
        let mut index = 0;
        while index < arguments.len() {
            let value = arguments.get(index + 1);
            match arguments[index].as_str() {
                "--shared-host" => {
                    if request.shared_host {
                        return Err(err("--shared-host may be specified only once"));
                    }
                    request.shared_host = true;
                }
                "--service-type" => {
                    if request.mode.is_some() {
                        return Err(err("--service-type may be specified only once"));
                    }
                    let value = value.ok_or_else(|| err("--service-type requires agent, daemon, or auto"))?;
                    request.mode =
                        Some(ServiceMode::parse(value).ok_or_else(|| err("--service-type must be agent, daemon, or auto"))?);
                    index += 1;
                }
                "--service-user" => {
                    if request.user.is_some() {
                        return Err(err("--service-user may be specified only once"));
                    }
                    let value = value.filter(|value| !value.trim().is_empty());
                    request.user = Some(value.ok_or_else(|| err("--service-user requires a user name"))?.clone());
                    index += 1;
                }
                "--service-app" => {
                    let invalid = || err("--service-app requires an absolute Agent VMM Host.app path");
                    if request.app.is_some() {
                        return Err(err("--service-app may be specified only once"));
                    }
                    let app = PathBuf::from(value.filter(|value| !value.trim().is_empty()).ok_or_else(invalid)?);
                    if !app.is_absolute() || app.file_name().is_none_or(|name| name != HOST_APP_NAME) {
                        return Err(invalid());
                    }
                    request.app = Some(app);
                    index += 1;
                }
                _ => remaining.push(arguments[index].clone()),
            }
            index += 1;
        }
        if request.shared_host && request.app.is_some() {
            return Err(err("--shared-host and --service-app are mutually exclusive"));
        }
        Ok((request, remaining))
    }
}

/// The owner of the Host service, as `service.json` records it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub version: u32,
    pub mode: ServiceMode,
    pub user: String,
    #[serde(default)]
    pub group: String,
    #[serde(default)]
    pub uid: u32,
    #[serde(default)]
    pub gid: u32,
    #[serde(default)]
    pub release_id: String,
    #[serde(default, rename = "disable_personal_mesh_pairing")]
    pub disable_pairing: bool,
}

impl ServiceConfig {
    pub fn read(path: &Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        let config: Self = serde_json::from_slice(&data).map_err(|_| err("Agent VMM service configuration is invalid"))?;
        if config.version != 1 || config.mode == ServiceMode::Auto || config.user.is_empty() || config.group.is_empty() || config.uid == 0 {
            return Err(err("Agent VMM service configuration is invalid"));
        }
        Ok(config)
    }

    /// Records the owner and the pairing policy of the installed service.
    pub fn persist(&self, path: &Path, disable_pairing: bool, release_id: &str, euid: u32) -> Result<()> {
        let config = Self { disable_pairing, release_id: release_id.to_string(), ..self.clone() };
        let mut data = serde_json::to_vec(&config)?;
        data.push(b'\n');
        statefs::replace_file(path, &data, 0o600)?;
        if euid == 0 {
            std::os::unix::fs::chown(path, Some(self.uid), Some(self.gid))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn service_options_are_accepted_at_any_position() {
        let (request, remaining) = ServiceRequest::extract(
            "registration-claim",
            &arguments(&["--disable-personal-mesh-pairing", "--request-id", "r1", "--descriptor-stdin", "--service-type", "daemon", "--service-user", "worker"]),
        )
        .unwrap();
        assert_eq!(request.mode, Some(ServiceMode::Daemon));
        assert_eq!(request.user.as_deref(), Some("worker"));
        assert_eq!(remaining, arguments(&["--disable-personal-mesh-pairing", "--request-id", "r1", "--descriptor-stdin"]));
    }

    #[test]
    fn service_options_reject_repeats_and_conflicts() {
        for values in [
            &["--service-type", "agent", "--service-type", "auto"][..],
            &["--service-type", "system"],
            &["--service-type"],
            &["--service-user", " "],
            &["--shared-host", "--shared-host"],
            &["--service-app", "relative/Agent VMM Host.app"],
            &["--service-app", "/tmp/Other.app"],
            &["--shared-host", "--service-app", "/tmp/Agent VMM Host.app"],
        ] {
            assert!(ServiceRequest::extract("status", &arguments(values)).is_err(), "{values:?}");
        }
    }
}
