//! `update` replaces the installed Host with a prepared release through the cutover sequence.

use crate::cutover::{self, CutoverProgress, Steps};
use crate::error::{Result, err};
use crate::{Context, Paths, journal, now_millis, platform};
use std::path::Path;

/// The platform effects of an update.
pub trait UpdateActions {
    /// The release identity of the installed Host.
    fn installed_release(&self) -> Result<String>;
    /// Stops the Host before `source` replaces it.
    fn drain(&mut self, source: &Path) -> Result<()>;
    fn replace(&mut self, source: &Path) -> Result<()>;
    fn repair(&mut self) -> Result<()>;
    fn remove(&mut self, source: &Path) -> Result<()>;
}

struct Update<'a> {
    context: &'a Context,
    paths: &'a Paths,
    source: &'a Path,
    release_id: &'a str,
    request_id: &'a str,
    attempt: u32,
    actions: &'a mut dyn UpdateActions,
}

impl Steps for Update<'_> {
    fn save(&mut self, stage: &str, progress: &CutoverProgress) -> Result<()> {
        let mut current = journal::read_request(self.context, self.paths, self.request_id)
            .filter(|current| current.target_release_id == self.release_id)
            .ok_or_else(|| err("lifecycle update target is not owned by this request"))?;
        if stage == "restore" && current.stage != stage {
            current.attempt = 1;
        }
        current.stage = stage.to_string();
        current.cutover = Some(*progress);
        current.updated_at = now_millis();
        journal::persist(self.context, self.paths, &current)?;
        self.attempt = current.attempt;
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        self.actions.drain(self.source)
    }

    fn reset(&mut self) -> Result<()> {
        Err(err("an update preserves the Host data"))
    }

    fn replace(&mut self) -> Result<()> {
        self.actions.replace(self.source)
    }

    fn remove(&mut self) -> Result<()> {
        Err(err("an update keeps the Host installed"))
    }

    fn restore(&mut self) -> Result<()> {
        let id = cutover::restore_request_id(self.request_id, self.attempt);
        cutover::restore_host(self.context, self.paths, &id, &mut || self.actions.repair())
    }

    fn verify(&mut self) -> Result<()> {
        self.actions.remove(self.source).map_err(|error| error.context("remove retired Host bundle"))
    }
}

pub fn run(context: &Context, paths: &Paths, source: &Path, release_id: &str, request_id: &str, actions: &mut dyn UpdateActions) -> Result<()> {
    platform::validate_prepared_update(paths, source)?;
    let operation = journal::read_request(context, paths, request_id).ok_or_else(|| err("lifecycle update is not owned by this request"))?;
    let mut progress = operation.cutover.unwrap_or_default();
    if actions.installed_release().is_ok_and(|installed| installed == release_id) {
        progress.software_installed = true;
    }
    let mut update = Update { context, paths, source, release_id, request_id, attempt: operation.attempt, actions };
    if operation.stage == "settled" {
        update.verify()?;
        progress.effects_verified = true;
        return update.save("settled", &progress);
    }
    cutover::cutover(&mut progress, false, false, &mut update)
}
