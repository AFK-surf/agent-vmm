//! Startup stages that the Host and the guest print to their logs. They locate
//! a startup failure while the Host cannot answer.

use crate::journal::LifecycleOperation;
use crate::{Paths, statefs, timefmt};
use serde::{Deserialize, Serialize};

const LOG_READ_LIMIT: u64 = 1 << 20;
const LINE_LIMIT: usize = 8192;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupEvidence {
    #[serde(skip_serializing_if = "String::is_empty")]
    pub operation_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_successful_stage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub current_stage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub current_state: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_stage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub failure_message: String,
    pub failure_confirmed: bool,
    pub source: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub observed_at: String,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Record {
    operation_id: String,
    request_id: String,
    stage: String,
    state: String,
    error: String,
}

const HOST_MARKER: &str = "vmm_start=";
const GUEST_MARKER: &str = "guest_startup=";

pub fn read(paths: &Paths, operation: Option<&LifecycleOperation>) -> Option<StartupEvidence> {
    let mut result = StartupEvidence { request_id: operation.map(|operation| operation.request_id.clone()).unwrap_or_default(), ..StartupEvidence::default() };
    let sources = [
        (paths.service_logs.join("host.stderr.log"), HOST_MARKER, "host_start_log"),
        (paths.logs.join("serial.log.previous"), GUEST_MARKER, "guest_serial_log"),
        (paths.logs.join("serial.log"), GUEST_MARKER, "guest_serial_log"),
    ];
    for (path, marker, name) in sources {
        let Ok((data, modified)) = statefs::read_tail(&path, LOG_READ_LIMIT) else { continue };
        if apply(&mut result, &String::from_utf8_lossy(&data), marker, operation) {
            result.source = name.to_string();
            result.observed_at = timefmt::format_system(modified);
        }
    }
    (!result.current_stage.is_empty() || result.failure_confirmed).then_some(result)
}

fn apply(result: &mut StartupEvidence, data: &str, marker: &str, operation: Option<&LifecycleOperation>) -> bool {
    let guest = marker == GUEST_MARKER;
    // Guest stages belong to the Host start that is already identified and still open.
    if guest && (result.operation_id.is_empty() || result.failure_confirmed || result.current_stage == "wait_guest_health" && result.current_state == "succeeded") {
        return false;
    }
    let mut guest_current = String::new();
    let mut applied = false;
    for line in data.lines().filter(|line| line.len() <= LINE_LIMIT) {
        let Some(index) = line.find(marker) else { continue };
        let Ok(record) = serde_json::from_str::<Record>(&line[index + marker.len()..]) else { continue };
        if record.stage.is_empty() || operation.is_some_and(|operation| !record.request_id.is_empty() && record.request_id != operation.request_id) {
            continue;
        }
        let other_start = !result.operation_id.is_empty() && !record.operation_id.is_empty() && record.operation_id != result.operation_id;
        if other_start && !guest && operation.is_none() {
            *result = StartupEvidence::default();
            guest_current.clear();
        } else if other_start {
            continue;
        }
        applied = true;
        if !record.operation_id.is_empty() {
            result.operation_id = record.operation_id;
        }
        if !record.request_id.is_empty() {
            result.request_id = record.request_id;
        }
        if guest && record.state == "started" {
            if !guest_current.is_empty() && guest_current != record.stage {
                result.last_successful_stage = guest_current;
            }
            guest_current = record.stage.clone();
        }
        if record.state == "succeeded" {
            result.last_successful_stage = record.stage.clone();
        }
        if record.state == "failed" && !record.error.is_empty() {
            result.failure_stage = record.stage.clone();
            result.failure_message = record.error;
            result.failure_confirmed = true;
        }
        result.current_stage = record.stage;
        result.current_state = record.state;
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_stages_extend_the_identified_host_start() {
        let mut result = StartupEvidence::default();
        let host = "t vmm_start={\"operation_id\":\"op-1\",\"request_id\":\"r1\",\"stage\":\"boot\",\"state\":\"succeeded\"}\nnoise\nvmm_start={\"operation_id\":\"op-1\",\"stage\":\"wait_guest_health\",\"state\":\"started\"}\n";
        assert!(apply(&mut result, host, HOST_MARKER, None));
        let guest = "guest_startup={\"stage\":\"mount\",\"state\":\"started\"}\nguest_startup={\"stage\":\"network\",\"state\":\"started\"}\nguest_startup={\"stage\":\"network\",\"state\":\"failed\",\"error\":\"no link\"}\n";
        assert!(apply(&mut result, guest, GUEST_MARKER, None));
        assert_eq!((result.operation_id.as_str(), result.request_id.as_str()), ("op-1", "r1"));
        assert_eq!((result.last_successful_stage.as_str(), result.failure_stage.as_str(), result.failure_message.as_str()), ("mount", "network", "no link"));
        assert!(result.failure_confirmed);
        assert!(!apply(&mut result, guest, GUEST_MARKER, None), "a confirmed failure is final");
    }

    #[test]
    fn records_of_another_request_are_ignored() {
        let operation = LifecycleOperation { request_id: "r2".into(), ..LifecycleOperation::default() };
        let mut result = StartupEvidence::default();
        let host = "vmm_start={\"operation_id\":\"op-1\",\"request_id\":\"r1\",\"stage\":\"boot\",\"state\":\"failed\",\"error\":\"old\"}\n";
        assert!(!apply(&mut result, host, HOST_MARKER, Some(&operation)));
        assert!(!apply(&mut result, "guest_startup={\"stage\":\"mount\",\"state\":\"started\"}\n", GUEST_MARKER, Some(&operation)));
    }
}
