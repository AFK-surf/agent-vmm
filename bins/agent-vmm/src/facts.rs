//! Reads the lifecycle status report and derives the state, the stage and the
//! freshness that an envelope reports.

use crate::envelope::{Issue, bounded};
use crate::options::Options;
use serde_json::{Map, Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use vmm_lifecycle::timefmt;

pub type Facts = Map<String, Value>;

const STALE_AFTER: Duration = Duration::from_secs(30);

pub fn string<'a>(facts: &'a Facts, key: &str) -> &'a str {
    facts.get(key).and_then(Value::as_str).unwrap_or_default()
}

pub fn boolean(facts: &Facts, key: &str) -> bool {
    optional_boolean(facts, key).unwrap_or(false)
}

pub fn optional_boolean(facts: &Facts, key: &str) -> Option<bool> {
    facts.get(key).and_then(Value::as_bool)
}

pub fn number(facts: &Facts, key: &str) -> i64 {
    let value = facts.get(key);
    value.and_then(Value::as_i64).or_else(|| value.and_then(Value::as_f64).map(|number| number as i64)).unwrap_or(0)
}

pub fn object<'a>(facts: &'a Facts, key: &str) -> Option<&'a Facts> {
    facts.get(key).and_then(Value::as_object)
}

/// The first value that is not blank, without its surrounding spaces.
pub fn first<'a>(values: &[&'a str]) -> &'a str {
    values.iter().map(|value| value.trim()).find(|value| !value.is_empty()).unwrap_or_default()
}

/// The lower-case name of an enum value without the prefix of its type.
pub fn enum_name(name: &str, prefix: &str) -> String {
    name.strip_prefix(prefix).unwrap_or(name).to_lowercase()
}

/// The identifiers that the caller named.
pub fn explicit_target(options: &Options) -> Facts {
    let mut target = Facts::new();
    for (key, value) in [("registration_id", &options.registration_id), ("environment_id", &options.environment_id), ("workload_id", &options.workload_id), ("container_id", &options.container_id)] {
        if !value.is_empty() {
            target.insert(key.into(), json!(value));
        }
    }
    target
}

pub fn target_from_status(status: &Facts, options: &Options) -> Facts {
    let mut target = explicit_target(options);
    target.insert("service_user".into(), json!(string(status, "serviceUser")));
    if number(status, "serviceUid") != 0 {
        target.insert("service_uid".into(), json!(number(status, "serviceUid")));
    }
    target.insert("service_job".into(), json!(string(status, "hostJob")));
    if !string(status, "serviceApp").is_empty() {
        target.insert("install_location".into(), json!(string(status, "serviceApp")));
    }
    target
}

pub fn validate_status_target(status: &Facts) -> Result<(), String> {
    if string(status, "serviceUser").is_empty() || number(status, "serviceUid") <= 0 || string(status, "hostJob").is_empty() || string(status, "serviceApp").is_empty() {
        return Err("lifecycle status did not identify one service UID, job, and installation location".into());
    }
    Ok(())
}

pub fn state(status: &Facts) -> &'static str {
    [("hostHealthy", "healthy"), ("hostReadable", "unhealthy"), ("hostRunning", "host_unreadable"), ("hostLoaded", "exited"), ("hostInstalled", "stopped")]
        .iter()
        .find(|(key, _)| boolean(status, key))
        .map_or("not_installed", |(_, state)| *state)
}

pub fn stage_and_issue(status: &Facts) -> (String, Option<Issue>) {
    if boolean(status, "hostHealthy") {
        return ("healthy".into(), None);
    }
    if let Some(startup) = object(status, "startup") {
        let stage = first(&[string(startup, "failureStage"), string(startup, "currentStage"), "unknown"]).to_string();
        let issue = boolean(startup, "failureConfirmed").then(|| Issue::new("startup_failed", &bounded(string(startup, "failureMessage"), 1024), "guest", Some(false)));
        return (stage, issue);
    }
    let issue = string(status, "hostObservationIssue");
    if !issue.is_empty() {
        return ("service_observation".into(), Some(Issue::new("service_observation_failed", &bounded(issue, 1024), "host", Some(true))));
    }
    ("unknown".into(), None)
}

pub fn freshness_from_time(value: Option<SystemTime>, now: SystemTime) -> &'static str {
    match value {
        None => "unknown",
        Some(value) if now.duration_since(value).is_ok_and(|age| age > STALE_AFTER) => "stale",
        Some(_) => "current",
    }
}

pub fn freshness_from_millis(value: i64, now: SystemTime) -> &'static str {
    let time = u64::try_from(value).ok().filter(|millis| *millis != 0).map(|millis| UNIX_EPOCH + Duration::from_millis(millis));
    freshness_from_time(time, now)
}

pub fn freshness(status: &Facts, now: SystemTime) -> &'static str {
    let snapshot = timefmt::parse(string(status, "snapshotObservedAt"));
    let startup = object(status, "startup").and_then(|startup| timefmt::parse(string(startup, "observedAt")));
    let readable = boolean(status, "hostReadable");
    if let Some(observed) = snapshot.filter(|_| readable).or(startup).or(snapshot) {
        return freshness_from_time(Some(observed), now);
    }
    if boolean(status, "hostObserved") || readable || boolean(status, "hostHealthy") {
        return "current";
    }
    object(status, "operation").map_or("unknown", |operation| freshness_from_millis(number(operation, "updatedAt"), now))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(value: Value) -> Facts {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn the_state_names_the_first_layer_that_does_not_answer() {
        assert_eq!(state(&facts(json!({"hostInstalled": true, "hostLoaded": true, "hostRunning": true, "hostReadable": true, "hostHealthy": true}))), "healthy");
        assert_eq!(state(&facts(json!({"hostInstalled": true, "hostLoaded": true, "hostRunning": true, "hostReadable": true}))), "unhealthy");
        assert_eq!(state(&facts(json!({"hostInstalled": true, "hostLoaded": true, "hostRunning": true}))), "host_unreadable");
        assert_eq!(state(&facts(json!({"hostInstalled": true, "hostLoaded": true}))), "exited");
        assert_eq!(state(&facts(json!({"hostInstalled": true}))), "stopped");
        assert_eq!(state(&facts(json!({}))), "not_installed");
    }

    #[test]
    fn a_confirmed_startup_failure_is_the_issue() {
        let status = facts(json!({"startup": {"currentStage": "guest_boot", "failureStage": "guest_agent", "failureConfirmed": true, "failureMessage": "token=abc"}}));
        let (stage, issue) = stage_and_issue(&status);
        assert_eq!(stage, "guest_agent");
        assert_eq!(issue, Some(Issue::new("startup_failed", "token=[REDACTED]", "guest", Some(false))));
        let (stage, issue) = stage_and_issue(&facts(json!({"startup": {"currentStage": "guest_boot"}})));
        assert_eq!((stage.as_str(), issue), ("guest_boot", None));
        let (stage, issue) = stage_and_issue(&facts(json!({"hostObservationIssue": "launchctl failed"})));
        assert_eq!((stage.as_str(), issue.map(|issue| issue.code)), ("service_observation", Some("service_observation_failed".into())));
    }

    #[test]
    fn freshness_follows_the_newest_owner_observation() {
        let now = UNIX_EPOCH + Duration::from_secs(1_795_000_100);
        let at = |seconds: u64| timefmt::format_system(UNIX_EPOCH + Duration::from_secs(seconds));
        assert_eq!(freshness(&facts(json!({"hostReadable": true, "snapshotObservedAt": at(1_795_000_090)})), now), "current");
        assert_eq!(freshness(&facts(json!({"hostReadable": true, "snapshotObservedAt": at(1_795_000_000)})), now), "stale");
        assert_eq!(freshness(&facts(json!({"snapshotObservedAt": at(1_795_000_000), "startup": {"observedAt": at(1_795_000_095)}})), now), "current");
        assert_eq!(freshness(&facts(json!({"hostObserved": true})), now), "current");
        assert_eq!(freshness(&facts(json!({"operation": {"updatedAt": 1_795_000_000_000i64}})), now), "stale");
        assert_eq!(freshness(&facts(json!({})), now), "unknown");
    }

    #[test]
    fn a_status_must_identify_its_service() {
        let status = facts(json!({"serviceUser": "worker", "serviceUid": 501, "hostJob": "gui/501/com.afk-surf.agent-vmm-host", "serviceApp": "/Users/worker/Agent VMM Host.app"}));
        assert_eq!(validate_status_target(&status), Ok(()));
        let options = Options { registration_id: "registration-1".into(), ..Options::default() };
        assert_eq!(Value::Object(target_from_status(&status, &options)), json!({"registration_id": "registration-1", "service_user": "worker", "service_uid": 501, "service_job": "gui/501/com.afk-surf.agent-vmm-host", "install_location": "/Users/worker/Agent VMM Host.app"}));
        assert!(validate_status_target(&facts(json!({"serviceUser": "worker", "hostJob": "x", "serviceApp": "/a"}))).is_err());
    }
}
