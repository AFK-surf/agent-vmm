//! Installation, update and removal share one recoverable step sequence.

use crate::error::{Deadline, Error, Kind, Result};
use crate::statefs::hex;
use crate::{Context, INIT_TIMEOUT, Paths, START_FAILURE_FILE, journal, operator};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Only the macOS maintenance uses this item.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub const TARGET_OLDER_THAN_DATA: &str = "target_older_than_data";
pub const HOST_START_FAILED: &str = "host_start_failed";

fn is_false(value: &bool) -> bool {
    !*value
}

/// The committed effects of one cutover. A retry skips a step that is recorded here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CutoverProgress {
    #[serde(skip_serializing_if = "is_false")]
    pub data_cleared: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub software_removed: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub software_installed: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub effects_verified: bool,
}

/// The effects of one cutover. `save` records the stage before each effect.
pub trait Steps {
    fn save(&mut self, stage: &str, progress: &CutoverProgress) -> Result<()>;
    fn stop(&mut self) -> Result<()>;
    fn reset(&mut self) -> Result<()>;
    fn replace(&mut self) -> Result<()>;
    fn remove(&mut self) -> Result<()>;
    fn restore(&mut self) -> Result<()>;
    fn verify(&mut self) -> Result<()>;
}

pub fn cutover(progress: &mut CutoverProgress, reset: bool, remove: bool, steps: &mut dyn Steps) -> Result<()> {
    if progress.effects_verified {
        return Ok(());
    }
    if !progress.software_installed && !progress.software_removed {
        steps.save("stop", progress)?;
        steps.stop()?;
    }
    if reset && !progress.data_cleared {
        steps.save("reset", progress)?;
        steps.reset()?;
        progress.data_cleared = true;
        steps.save("reset", progress)?;
    }
    if remove {
        if !progress.software_removed {
            steps.save("replace", progress)?;
            steps.remove()?;
            progress.software_removed = true;
            steps.save("replace", progress)?;
        }
    } else {
        if !progress.software_installed {
            steps.save("replace", progress)?;
            steps.replace()?;
            progress.software_installed = true;
            steps.save("replace", progress)?;
        }
        steps.save("restore", progress)?;
        steps.restore()?;
    }
    steps.save("settled", progress)?;
    steps.verify()?;
    progress.effects_verified = true;
    steps.save("settled", progress)
}

/// What a Host that refused its database wrote before it exited.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct StartFailure {
    pub code: String,
    pub data_schema: i64,
    pub current_schema: i64,
}

pub fn read_start_failure(context: &Context, paths: &Paths) -> Option<StartFailure> {
    let data = journal::Journal::open(context, paths).ok()?.dir().read(START_FAILURE_FILE, 4 << 10).ok()?;
    serde_json::from_slice::<StartFailure>(&data).ok().filter(|failure| !failure.code.is_empty())
}

fn schema_failure(failure: &StartFailure) -> Error {
    let message = format!("Host refused its database: {} (data schema {}, current schema {})", failure.code, failure.data_schema, failure.current_schema);
    Error::new(Kind::Maintenance(HOST_START_FAILED.into()), message)
}

/// Loads the service and waits for a healthy VM. A Host that refuses its
/// database ends the wait at once, because no retry changes the refusal.
pub fn restore_host(context: &Context, paths: &Paths, request_id: &str, repair: &mut dyn FnMut() -> Result<()>) -> Result<()> {
    match std::fs::remove_file(paths.application_data.join(START_FAILURE_FILE)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }
    repair()?;
    let mut refused = || read_start_failure(context, paths).map_or(Ok(()), |failure| Err(schema_failure(&failure)));
    refused()?;
    let started = operator::ensure_vm_started_until(context, paths, request_id, Deadline::after(INIT_TIMEOUT), &mut refused);
    refused()?;
    started
}

pub fn restore_request_id(id: &str, attempt: u32) -> String {
    format!("maintenance-restore-{}-{attempt}", hex(&Sha256::digest(id.as_bytes())[..16]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorded {
        log: Vec<String>,
        fail: Option<&'static str>,
    }

    impl Recorded {
        fn effect(&mut self, name: &'static str) -> Result<()> {
            self.log.push(name.into());
            if self.fail == Some(name) { Err(crate::error::err(format!("{name} failed"))) } else { Ok(()) }
        }
    }

    impl Steps for Recorded {
        fn save(&mut self, stage: &str, _: &CutoverProgress) -> Result<()> {
            self.log.push(format!("save:{stage}"));
            Ok(())
        }
        fn stop(&mut self) -> Result<()> {
            self.effect("stop")
        }
        fn reset(&mut self) -> Result<()> {
            self.effect("reset")
        }
        fn replace(&mut self) -> Result<()> {
            self.effect("replace")
        }
        fn remove(&mut self) -> Result<()> {
            self.effect("remove")
        }
        fn restore(&mut self) -> Result<()> {
            self.effect("restore")
        }
        fn verify(&mut self) -> Result<()> {
            self.effect("verify")
        }
    }

    #[test]
    fn a_retry_resumes_after_the_committed_effects() {
        let mut progress = CutoverProgress::default();
        let mut first = Recorded { fail: Some("restore"), ..Recorded::default() };
        assert!(cutover(&mut progress, true, false, &mut first).is_err());
        assert_eq!(first.log, ["save:stop", "stop", "save:reset", "reset", "save:reset", "save:replace", "replace", "save:replace", "save:restore", "restore"]);
        assert_eq!(progress, CutoverProgress { data_cleared: true, software_installed: true, ..CutoverProgress::default() });

        let mut second = Recorded::default();
        cutover(&mut progress, true, false, &mut second).unwrap();
        assert_eq!(second.log, ["save:restore", "restore", "save:settled", "verify", "save:settled"]);
        assert!(progress.effects_verified);
        let mut third = Recorded::default();
        cutover(&mut progress, true, false, &mut third).unwrap();
        assert!(third.log.is_empty());
    }

    #[test]
    fn removal_never_restores_the_host() {
        let mut progress = CutoverProgress::default();
        let mut steps = Recorded::default();
        cutover(&mut progress, false, true, &mut steps).unwrap();
        assert_eq!(steps.log, ["save:stop", "stop", "save:replace", "remove", "save:replace", "save:settled", "verify", "save:settled"]);
    }

    #[test]
    fn restore_requests_are_stable_per_attempt() {
        let id = restore_request_id("request-1", 2);
        assert!(id.starts_with("maintenance-restore-") && id.ends_with("-2") && id.len() == 54, "{id}");
    }
}
