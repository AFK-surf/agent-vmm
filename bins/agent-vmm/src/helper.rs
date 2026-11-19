//! Runs the lifecycle helper and names the platform layout that the commands read.

use crate::runtime::HelperOutput;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use vmm_lifecycle::Deadline;

#[cfg(target_os = "macos")]
mod platform {
    use std::path::{Path, PathBuf};

    pub const SERVICE_SOURCE: &str = "launchd";

    pub fn service_data_directory(home: &Path) -> PathBuf {
        home.join("Library/Application Support/AgentVMM")
    }

    pub fn service_log_directory(home: &Path) -> PathBuf {
        home.join("Library/Logs/AgentVMM")
    }

    pub fn service_output_log_directory(home: &Path) -> PathBuf {
        service_log_directory(home)
    }

    pub fn default_lifecycle_helper(executable: &Path) -> PathBuf {
        executable.parent().unwrap_or(Path::new("")).join("agent-vmm-lifecycle")
    }

    pub fn helper_arguments(arguments: &[String]) -> Vec<String> {
        arguments.to_vec()
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::path::{Path, PathBuf};

    pub const SERVICE_SOURCE: &str = "systemd";

    pub fn service_data_directory(_: &Path) -> PathBuf {
        PathBuf::from("/var/lib/agent-vmm")
    }

    pub fn service_log_directory(_: &Path) -> PathBuf {
        PathBuf::from("/var/lib/agent-vmm/logs")
    }

    pub fn service_output_log_directory(_: &Path) -> PathBuf {
        PathBuf::from("/var/log/agent-vmm")
    }

    /// On Linux this program is the helper: `agent-vmm lifecycle ...`.
    pub fn default_lifecycle_helper(executable: &Path) -> PathBuf {
        executable.to_path_buf()
    }

    pub fn helper_arguments(arguments: &[String]) -> Vec<String> {
        std::iter::once("lifecycle".to_string()).chain(arguments.iter().cloned()).collect()
    }
}

pub use platform::*;

/// The data directory of an operator who runs the Host as an own user.
pub fn platform_data_directory(home: &Path) -> PathBuf {
    if cfg!(target_os = "macos") { home.join("Library/Application Support/AgentVMM") } else { home.join(".local/state/agent-vmm") }
}

pub fn home() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
}

pub fn default_operator_socket() -> String {
    service_data_directory(&home()).join("operator.sock").display().to_string()
}

/// These commands change the installation. A caller that stops waiting must not stop them.
fn owns_a_transition(arguments: &[String]) -> bool {
    matches!(arguments.first().map(String::as_str), Some("repair" | "host-repair" | "update" | "host-update" | "host-maintain" | "install" | "install-managed" | "maintenance"))
}

type Collected = (Arc<Mutex<Vec<u8>>>, Option<std::thread::JoinHandle<()>>);

fn collect(stream: Option<impl Read + Send + 'static>) -> Collected {
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let target = buffer.clone();
    let reader = stream.map(|mut stream| {
        std::thread::spawn(move || {
            let mut chunk = [0u8; 8192];
            while let Ok(count @ 1..) = stream.read(&mut chunk) {
                target.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).extend_from_slice(&chunk[..count]);
            }
        })
    });
    (buffer, reader)
}

fn snapshot(buffer: &Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    buffer.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
}

/// Runs the helper until it exits or the budget ends. Output that arrived before the end of the budget is returned in both cases.
pub fn run(path: &str, arguments: &[String], input: Option<&[u8]>, budget: Deadline) -> HelperOutput {
    execute(path, &helper_arguments(arguments), owns_a_transition(arguments), input, budget)
}

fn execute(program: &str, arguments: &[String], detached: bool, input: Option<&[u8]>, budget: Deadline) -> HelperOutput {
    const TIMEOUT: &str = "context deadline exceeded";
    const POLL: Duration = Duration::from_millis(5);
    if detached && budget.expired() {
        return HelperOutput { error: Some(TIMEOUT.into()), ..HelperOutput::default() };
    }
    let mut command = Command::new(program);
    command.args(arguments).stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() }).stdout(Stdio::piped()).stderr(Stdio::piped());
    if detached {
        // An interrupt of this command's terminal does not reach the helper; the helper ignores a hangup.
        command.process_group(0);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return HelperOutput { error: Some(error.to_string()), ..HelperOutput::default() },
    };
    if let (Some(mut stdin), Some(input)) = (child.stdin.take(), input) {
        let input = input.to_vec();
        std::thread::spawn(move || {
            let _ = stdin.write_all(&input);
        });
    }
    let ((stdout, stdout_reader), (stderr, stderr_reader)) = (collect(child.stdout.take()), collect(child.stderr.take()));
    let mut error = loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break None,
            Ok(Some(status)) => break Some(status.code().map_or_else(|| status.to_string(), |code| format!("exit status {code}"))),
            Ok(None) if budget.expired() => {
                if !detached {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                break Some(TIMEOUT.into());
            }
            Ok(None) => std::thread::sleep(POLL),
            Err(error) => break Some(error.to_string()),
        }
    };
    // The output is complete when every holder of the streams closed them.
    let reading = || [&stdout_reader, &stderr_reader].iter().any(|reader| reader.as_ref().is_some_and(|reader| !reader.is_finished()));
    while error.as_deref() != Some(TIMEOUT) && reading() {
        if budget.expired() {
            error = Some(TIMEOUT.into());
        }
        std::thread::sleep(POLL);
    }
    HelperOutput { stdout: snapshot(&stdout), stderr: snapshot(&stderr), error }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn shell(script: &str, detached: bool, input: Option<&[u8]>, budget: Duration) -> HelperOutput {
        execute("/bin/sh", &arguments(&["-c", script]), detached, input, Deadline::after(budget))
    }

    #[test]
    fn a_transition_outlives_the_budget_and_an_observation_does_not() {
        assert!(owns_a_transition(&arguments(&["host-update", "--request-id", "r1"])));
        assert!(owns_a_transition(&arguments(&["host-repair"])));
        assert!(!owns_a_transition(&arguments(&["status"])));
        assert!(!owns_a_transition(&arguments(&["operation", "--operation-id", "o1"])));
        let first = helper_arguments(&arguments(&["status"]));
        assert_eq!(first.last().map(String::as_str), Some("status"));
        assert_eq!(first.len(), if cfg!(target_os = "linux") { 2 } else { 1 });
    }

    #[test]
    fn the_output_and_the_exit_of_a_helper_are_reported() {
        let long = Duration::from_secs(20);
        let ok = shell("cat; echo out; echo err >&2", false, Some(b"in:"), long);
        assert_eq!((String::from_utf8_lossy(&ok.stdout).as_ref(), String::from_utf8_lossy(&ok.stderr).as_ref(), ok.error), ("in:out\n", "err\n", None));
        let failed = shell("echo out; exit 3", true, None, long);
        assert_eq!((String::from_utf8_lossy(&failed.stdout).as_ref(), failed.error.as_deref()), ("out\n", Some("exit status 3")));
        let missing = run("/nonexistent/agent-vmm-lifecycle", &arguments(&["status"]), None, Deadline::after(long));
        assert!(missing.error.is_some() && missing.stdout.is_empty());
    }

    #[test]
    fn an_observation_that_exceeds_the_budget_is_stopped() {
        let started = std::time::Instant::now();
        let output = shell("echo started; exec sleep 30", false, None, Duration::from_millis(500));
        assert_eq!((output.error.as_deref(), String::from_utf8_lossy(&output.stdout).as_ref()), (Some("context deadline exceeded"), "started\n"));
        assert!(started.elapsed() < Duration::from_secs(10));
        let refused = shell("echo started", true, None, Duration::ZERO);
        assert!(refused.stdout.is_empty() && refused.error.is_some(), "a transition is not started after the budget");
    }
}
