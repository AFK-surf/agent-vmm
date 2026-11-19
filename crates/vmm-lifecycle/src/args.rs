//! Arguments of the lifecycle commands, after the service options are removed.

use crate::error::{PreparationFailure, Result, err};
use crate::valid_id;
use agent_vmm_proto::host::v1::RemoteRegistrationState;
use std::path::PathBuf;

/// Returns the value of `arguments[*index]` and moves past it.
fn value<'a>(arguments: &'a [String], index: &mut usize, flag: &str) -> Result<&'a str> {
    *index += 1;
    arguments.get(*index).map(String::as_str).ok_or_else(|| err(format!("{flag} requires a value")))
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct InstallArgs {
    pub disable_pairing: bool,
    pub request_id: String,
    /// A registration descriptor follows on standard input.
    pub descriptor_stdin: bool,
    pub registration_id: String,
}

/// Parses `install`, `repair` and `registration-claim`. The Linux installer of
/// Comma passes `install --registration-stdin`, so that spelling is accepted.
pub fn parse_install(action: &str, arguments: &[String]) -> Result<InstallArgs> {
    let mut result = InstallArgs::default();
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--disable-personal-mesh-pairing" => result.disable_pairing = true,
            "--descriptor-stdin" | "--registration-stdin" | "--operation-stdin" if action != "repair" => result.descriptor_stdin = true,
            "--resume-operation" if action == "install" => return Err(err("upgrade_required: use registration-claim")),
            "--request-id" => result.request_id = value(arguments, &mut index, "--request-id")?.to_string(),
            "--registration-id" if action == "repair" => result.registration_id = value(arguments, &mut index, "--registration-id")?.to_string(),
            _ => return Err(err("install and repair accept --disable-personal-mesh-pairing, --request-id, repair --registration-id, and install --registration-stdin")),
        }
        index += 1;
    }
    if !result.registration_id.is_empty() && !valid_id(&result.registration_id) {
        return Err(err("--registration-id is invalid"));
    }
    if !result.request_id.is_empty() && !valid_id(&result.request_id) {
        return Err(err("--request-id is invalid"));
    }
    Ok(result)
}

/// Parses `drain`: only `--request-id` is accepted.
pub fn parse_request_id(arguments: &[String]) -> Result<String> {
    match arguments {
        [] => Ok(String::new()),
        [flag, id] if flag == "--request-id" && valid_id(id) => Ok(id.clone()),
        _ => Err(err("only --request-id is accepted")),
    }
}

pub fn parse_uninstall(arguments: &[String]) -> Result<(bool, String)> {
    let (mut purge, mut request_id) = (false, String::new());
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--purge" => purge = true,
            "--request-id" => request_id = value(arguments, &mut index, "--request-id")?.to_string(),
            _ => return Err(err("uninstall accepts --purge and --request-id")),
        }
        index += 1;
    }
    Ok((purge, request_id))
}

pub fn parse_status(arguments: &[String]) -> Result<Option<String>> {
    match arguments {
        [] => Ok(None),
        [flag, id] if flag == "--registration-id" && valid_id(id) => Ok(Some(id.clone())),
        _ => Err(err("status accepts --registration-id ID")),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RegistrationStateArgs {
    pub registration_id: String,
    pub state: RemoteRegistrationState,
    pub request_id: String,
}

/// Parses the registration state commands. `fixed` is the state that the command name selects.
pub fn parse_registration_state(arguments: &[String], fixed: Option<RemoteRegistrationState>) -> Result<RegistrationStateArgs> {
    let mut result = RegistrationStateArgs { registration_id: String::new(), state: fixed.unwrap_or(RemoteRegistrationState::Unspecified), request_id: String::new() };
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--registration-id" => result.registration_id = value(arguments, &mut index, "--registration-id")?.to_string(),
            "--request-id" => result.request_id = value(arguments, &mut index, "--request-id")?.to_string(),
            "--state" if fixed.is_none() => {
                result.state = match value(arguments, &mut index, "--state")? {
                    "enabled" => RemoteRegistrationState::Enabled,
                    "draining" => RemoteRegistrationState::Draining,
                    _ => return Err(err("--state must be enabled or draining")),
                }
            }
            _ => return Err(err("registration-state accepts --registration-id, --state, and --request-id")),
        }
        index += 1;
    }
    if !valid_id(&result.registration_id) || result.state == RemoteRegistrationState::Unspecified {
        return Err(err("registration-state requires a valid --registration-id and --state"));
    }
    if !result.request_id.is_empty() && !valid_id(&result.request_id) {
        return Err(err("--request-id is invalid"));
    }
    Ok(result)
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct UpdateArgs {
    pub source_app: Option<PathBuf>,
    pub target_release_id: String,
    pub request_id: String,
    pub artifact_sha256: String,
    pub artifact_size: u64,
    pub preparation_failure_stdin: bool,
}

pub fn parse_update(arguments: &[String]) -> Result<UpdateArgs> {
    let mut result = UpdateArgs::default();
    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        match flag {
            "--preparation-failure-stdin" => result.preparation_failure_stdin = true,
            "--source-app" => result.source_app = Some(PathBuf::from(value(arguments, &mut index, flag)?)),
            "--target-release-id" => result.target_release_id = value(arguments, &mut index, flag)?.to_string(),
            "--request-id" => result.request_id = value(arguments, &mut index, flag)?.to_string(),
            "--artifact-sha256" => result.artifact_sha256 = value(arguments, &mut index, flag)?.to_ascii_lowercase(),
            "--artifact-size" => result.artifact_size = value(arguments, &mut index, flag)?.parse().ok().filter(|size| *size > 0).ok_or_else(|| err("update artifact size is invalid"))?,
            _ => return Err(err("unknown update argument")),
        }
        index += 1;
    }
    if !valid_id(&result.target_release_id) || !valid_id(&result.request_id) {
        return Err(err("update requires a valid --target-release-id and --request-id"));
    }
    if result.preparation_failure_stdin == result.source_app.is_some() {
        return Err(err("update requires exactly one of --source-app or --preparation-failure-stdin"));
    }
    if result.source_app.as_ref().is_some_and(|source| !source.is_absolute()) {
        return Err(err("update requires an absolute --source-app"));
    }
    if result.artifact_sha256.is_empty() != (result.artifact_size == 0) {
        return Err(err("update artifact SHA-256 and size must be supplied together"));
    }
    let digest = &result.artifact_sha256;
    if !digest.is_empty() && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())) {
        return Err(err("update artifact SHA-256 is invalid"));
    }
    if result.preparation_failure_stdin && result.artifact_sha256.is_empty() {
        return Err(err("update preparation failure requires artifact SHA-256 and size"));
    }
    Ok(result)
}

pub fn decode_preparation_failure(input: &[u8]) -> Result<PreparationFailure> {
    let failure: PreparationFailure = serde_json::from_slice(input).map_err(|_| err("update preparation failure is invalid"))?;
    let stage = matches!(failure.stage.as_str(), "target_selection" | "download" | "preparation");
    if failure.version != 1 || !stage || !valid_id(&failure.code) || failure.message.trim().is_empty() || failure.message.len() > 512 || failure.message.contains(['\r', '\n']) {
        return Err(err("update preparation failure fields are invalid"));
    }
    Ok(failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn install_accepts_the_descriptor_spellings_of_every_caller() {
        for flag in ["--descriptor-stdin", "--registration-stdin"] {
            let parsed = parse_install("install", &arguments(&[flag, "--request-id", "r1"])).unwrap();
            assert_eq!(parsed, InstallArgs { descriptor_stdin: true, request_id: "r1".into(), ..InstallArgs::default() });
        }
        let claim = parse_install("registration-claim", &arguments(&["--disable-personal-mesh-pairing", "--request-id", "r1", "--descriptor-stdin"])).unwrap();
        assert!(claim.disable_pairing && claim.descriptor_stdin);
        assert!(parse_install("repair", &arguments(&["--descriptor-stdin"])).is_err());
        assert!(parse_install("install", &arguments(&["--registration-id", "reg"])).is_err());
        assert_eq!(parse_install("repair", &arguments(&["--registration-id", "reg", "--request-id", "r"])).unwrap().registration_id, "reg");
        assert!(parse_install("install", &arguments(&["--request-id"])).is_err());
        assert!(parse_install("install", &arguments(&["--request-id", "a.b"])).is_err());
    }

    #[test]
    fn update_requires_one_source_and_a_complete_artifact() {
        let digest = "A".repeat(64);
        let base = ["--target-release-id", "rel-1", "--request-id", "r1"];
        let with = |extra: &[&str]| parse_update(&arguments(&[&base[..], extra].concat()));
        assert_eq!(with(&["--source-app", "/opt/x", "--artifact-sha256", &digest, "--artifact-size", "7"]).unwrap().artifact_sha256, "a".repeat(64));
        assert!(with(&[]).is_err());
        assert!(with(&["--source-app", "relative"]).is_err());
        assert!(with(&["--source-app", "/opt/x", "--artifact-size", "7"]).is_err());
        assert!(with(&["--source-app", "/opt/x", "--preparation-failure-stdin"]).is_err());
        assert!(with(&["--preparation-failure-stdin"]).is_err(), "a preparation failure names its artifact");
        assert!(with(&["--preparation-failure-stdin", "--artifact-sha256", &digest, "--artifact-size", "7"]).is_ok());
    }

    #[test]
    fn registration_state_commands_fix_or_read_the_state() {
        let fixed = parse_registration_state(&arguments(&["--registration-id", "reg", "--request-id", "r"]), Some(RemoteRegistrationState::Draining)).unwrap();
        assert_eq!(fixed.state, RemoteRegistrationState::Draining);
        assert!(parse_registration_state(&arguments(&["--registration-id", "reg", "--state", "enabled"]), Some(RemoteRegistrationState::Enabled)).is_err());
        assert_eq!(parse_registration_state(&arguments(&["--registration-id", "reg", "--state", "enabled"]), None).unwrap().state, RemoteRegistrationState::Enabled);
        assert!(parse_registration_state(&arguments(&["--registration-id", "reg"]), None).is_err());
        assert!(parse_registration_state(&arguments(&["--registration-id", "a/b"]), Some(RemoteRegistrationState::Enabled)).is_err());
    }
}
