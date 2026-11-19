//! The result envelope of the managed commands. Every command prints one
//! envelope: what it observed, how fresh that is, and what the caller can do next.

use regex::Regex;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::sync::OnceLock;
use std::time::SystemTime;

pub const VERSION: u32 = 1;
pub const LOG_BYTES: usize = 1 << 20;
pub const DIAGNOSE_BYTES: usize = 2 << 20;

pub const CONTINUE_NORMAL_WORK: &str = "continue_normal_work";
pub const EXCLUSIVE_TRANSITION: &str = "exclusive_transition_in_progress";
pub const RETRY_SAME_REQUEST_AT: &str = "retry_same_request_at";
pub const ADMINISTRATOR_ACTION: &str = "administrator_action_required";
pub const TERMINAL_ACTION: &str = "terminal_action_required";

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Issue {
    pub code: String,
    pub message: String,
    pub layer: String,
    pub retryable: Option<bool>,
}

impl Issue {
    pub fn new(code: &str, message: &str, layer: &str, retryable: Option<bool>) -> Self {
        Self { code: code.into(), message: message.into(), layer: layer.into(), retryable }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NextAction {
    pub action: String,
    pub permission: String,
}

pub fn next(action: &str, permission: &str) -> Vec<NextAction> {
    vec![NextAction { action: action.into(), permission: permission.into() }]
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct LogRecord {
    pub cursor: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub timestamp: String,
    pub stream: String,
    pub data: String,
    #[serde(skip_serializing_if = "is_false")]
    pub truncated: bool,
    #[serde(rename = "dropped_records", skip_serializing_if = "is_zero")]
    pub dropped: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Envelope {
    pub version: u32,
    pub command: String,
    pub target: Map<String, Value>,
    pub source: Vec<String>,
    pub observed_at: String,
    pub freshness: String,
    pub state: String,
    pub stage: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub disposition: String,
    pub issue: Option<Issue>,
    pub operation_ref: Option<Value>,
    pub next_action: Vec<NextAction>,
    pub partial: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub cursor: String,
    #[serde(skip_serializing_if = "is_false")]
    pub gap: bool,
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub facts: Map<String, Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<LogRecord>,
}

/// The time as Go `time.RFC3339Nano` prints it in UTC.
pub fn rfc3339(time: SystemTime) -> String {
    vmm_lifecycle::timefmt::format_system(time)
}

impl Envelope {
    pub fn new(command: &str, now: SystemTime, target: Map<String, Value>) -> Self {
        Self {
            version: VERSION,
            command: command.into(),
            target,
            source: Vec::new(),
            observed_at: rfc3339(now),
            freshness: "unknown".into(),
            state: "unknown".into(),
            stage: "unknown".into(),
            disposition: String::new(),
            issue: None,
            operation_ref: None,
            next_action: Vec::new(),
            partial: false,
            cursor: String::new(),
            gap: false,
            facts: Map::new(),
            logs: Vec::new(),
        }
    }

    pub fn operation(&mut self, owner: &str, id: &str) {
        self.operation_ref = Some(json!({"owner": owner, "id": id}));
    }

    /// The line for standard output. Secrets are redacted before anything is printed.
    pub fn render(mut self, json_output: bool) -> Result<String, String> {
        redact_map(&mut self.facts);
        for record in &mut self.logs {
            record.data = redact_text(&record.data);
        }
        if let Some(issue) = &mut self.issue {
            issue.message = redact_text(&issue.message);
        }
        if self.command == "logs" {
            fit_logs(&mut self);
        }
        let data = serde_json::to_string(&self).map_err(|error| error.to_string())?;
        if self.command == "diagnose" && data.len() + 1 > DIAGNOSE_BYTES {
            return Err("diagnostic output exceeded 2 MiB".into());
        }
        if self.command == "logs" && data.len() + 1 > LOG_BYTES {
            return Err("log output exceeded 1 MiB".into());
        }
        if json_output {
            return Ok(data);
        }
        Ok(if self.stage == "unknown" { format!("{}: {}", self.command, self.state) } else { format!("{}: {} ({})", self.command, self.state, self.stage) })
    }
}

/// The envelope that a failed command prints on standard error.
pub fn failure(command: &str, code: &str, message: &str, stage: &str) -> String {
    let mut envelope = Envelope::new(command, SystemTime::now(), Map::new());
    envelope.state = "failed".into();
    envelope.stage = stage.into();
    envelope.issue = Some(Issue::new(code, &bounded(message, 1024), "cli", None));
    envelope.partial = true;
    serde_json::to_string(&envelope).unwrap_or_default()
}

fn secret_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    const NAMES: &str = "token|secret|credential|password|api[_-]?key|access[_-]?key(?:_id)?|private[_-]?key|authorization|(?:set-)?cookie";
    PATTERNS.get_or_init(|| {
        [
            r"(?i)(authorization\s*:\s*)[^\r\n]+".to_string(),
            r"(?i)((?:set-)?cookie\s*:\s*)[^\r\n]+".to_string(),
            format!(r#"(?i)("(?:{NAMES})"\s*:\s*)"[^"]*""#),
            format!(r"(?i)('(?:{NAMES})'\s*:\s*)'[^']*'"),
            format!(r#"(?i)(["']?(?:{NAMES})["']?\s*[:=]\s*)[^\s,"']+"#),
        ]
        .iter()
        .map(|pattern| Regex::new(pattern).expect("the redaction patterns are valid"))
        .collect()
    })
}

pub fn redact_text(value: &str) -> String {
    secret_patterns().iter().fold(value.to_string(), |text, pattern| pattern.replace_all(&text, "${1}[REDACTED]").into_owned())
}

fn sensitive_key(key: &str) -> bool {
    let normalized = key.to_lowercase().replace('-', "_");
    ["token", "secret", "credential", "password", "api_key", "access_key", "private_key", "authorization", "cookie", "task_body", "prompt", "private_content"].iter().any(|part| normalized.contains(part))
}

fn redact_value(value: &mut Value) {
    match value {
        Value::String(text) => *text = redact_text(text),
        Value::Object(map) => redact_map(map),
        Value::Array(items) => items.iter_mut().for_each(redact_value),
        _ => {}
    }
}

pub fn redact_map(map: &mut Map<String, Value>) {
    for (key, value) in map.iter_mut() {
        if sensitive_key(key) {
            *value = Value::String("[REDACTED]".into());
        } else {
            redact_value(value);
        }
    }
}

/// Redacts and trims a message, and cuts it at a character boundary.
pub fn bounded(value: &str, limit: usize) -> String {
    let text = redact_text(value.trim());
    let mut end = limit.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// Shortens log records until the envelope fits its bound. A local log keeps its end; a container log keeps its start.
fn fit_logs(envelope: &mut Envelope) {
    let fits = |envelope: &Envelope| serde_json::to_string(envelope).is_ok_and(|data| data.len() < LOG_BYTES);
    if fits(envelope) {
        return;
    }
    envelope.gap = true;
    let original: Vec<Vec<char>> = envelope.logs.iter().map(|record| record.data.chars().collect()).collect();
    for record in &mut envelope.logs {
        record.truncated |= !record.data.is_empty();
        record.data.clear();
    }
    let local = matches!(envelope.source.as_slice(), [source] if source == "guest_log" || source == "host_log");
    let part = |text: &[char], length: usize| -> String { if local { text[text.len() - length..].iter().collect() } else { text[..length].iter().collect() } };
    for step in 0..original.len() {
        let index = if local { original.len() - 1 - step } else { step };
        let text = &original[index];
        let (mut low, mut high) = (0, text.len());
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            envelope.logs[index].data = part(text, middle);
            if fits(envelope) { low = middle } else { high = middle - 1 }
        }
        envelope.logs[index].data = part(text, low);
        envelope.logs[index].truncated = low < text.len();
        if low < text.len() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn secrets_never_reach_the_output() {
        assert_eq!(redact_text("Authorization: Bearer abc\nnext"), "Authorization: [REDACTED]\nnext");
        assert_eq!(redact_text(r#"{"token":"abc","name":"x"}"#), r#"{"token":[REDACTED],"name":"x"}"#);
        assert_eq!(redact_text("api_key=abc123, other=1"), "api_key=[REDACTED], other=1");
        let mut facts = json!({"ticket_token": "abc", "nested": {"note": "password: hunter2", "items": ["cookie: a=b"]}, "count": 3}).as_object().unwrap().clone();
        redact_map(&mut facts);
        assert_eq!(Value::Object(facts), json!({"ticket_token": "[REDACTED]", "nested": {"note": "password: [REDACTED]", "items": ["cookie: [REDACTED]"]}, "count": 3}));
    }

    #[test]
    fn an_envelope_keeps_the_keys_that_callers_read() {
        let mut envelope = Envelope::new("inspect", UNIX_EPOCH + Duration::from_secs(1_795_000_000), Map::new());
        envelope.facts.insert("hostHealthy".into(), json!(true));
        let value: Value = serde_json::from_str(&envelope.clone().render(true).unwrap()).unwrap();
        assert_eq!(value, json!({
            "version": 1, "command": "inspect", "target": {}, "source": [], "observed_at": "2026-11-18T11:06:40Z", "freshness": "unknown", "state": "unknown", "stage": "unknown",
            "issue": null, "operation_ref": null, "next_action": [], "partial": false, "facts": {"hostHealthy": true},
        }));
        envelope.state = "healthy".into();
        assert_eq!(envelope.clone().render(false).unwrap(), "inspect: healthy");
        envelope.stage = "healthy".into();
        assert_eq!(envelope.render(false).unwrap(), "inspect: healthy (healthy)");
        let failed: Value = serde_json::from_str(&failure("update", "update_failed", " token=abc ", "download")).unwrap();
        assert_eq!((failed["state"].as_str(), failed["stage"].as_str(), failed["issue"]["message"].as_str(), failed["partial"].as_bool()), (Some("failed"), Some("download"), Some("token=[REDACTED]"), Some(true)));
    }

    #[test]
    fn a_log_envelope_is_cut_to_its_bound() {
        let mut envelope = Envelope::new("logs", SystemTime::now(), Map::new());
        envelope.source = vec!["host_log".into()];
        envelope.logs = vec![LogRecord { cursor: "a".into(), stream: "stderr".into(), data: "x".repeat(LOG_BYTES), ..LogRecord::default() }, LogRecord { cursor: "b".into(), stream: "stderr".into(), data: "tail".into(), ..LogRecord::default() }];
        let rendered = envelope.render(true).unwrap();
        assert!(rendered.len() < LOG_BYTES);
        let value: Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!((value["gap"].as_bool(), value["logs"][1]["data"].as_str(), value["logs"][0]["truncated"].as_bool()), (Some(true), Some("tail"), Some(true)));
        assert!(value["logs"][1].get("truncated").is_none(), "the newest record of a local log is kept whole");
    }
}
