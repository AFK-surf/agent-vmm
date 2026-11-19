//! The carrier process owns the VM and holds only the virtualization
//! entitlement. A Host update keeps it running while the carrier executable
//! and the guest bundle of the new release are the ones it started from.

use super::launchd::{self, Job};
use crate::error::{Deadline, Result};
use crate::journal::Journal;
use crate::statefs::hex;
use crate::{Context, Paths};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

const RECORD: &str = "carrier.json";
pub const CARRIER_EXECUTABLE: &str = "Contents/Helpers/agent-vmm-carrier";
const GUEST_MANIFEST: &str = "Contents/Resources/Guest/manifest.json";

/// What a running carrier was started from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    version: u32,
    carrier_sha256: String,
    guest_sha256: String,
}

fn digest(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    std::io::copy(&mut std::fs::File::open(path)?, &mut hasher)?;
    Ok(hex(&hasher.finalize()))
}

/// The carrier identity of the release in `app`.
pub fn identity(app: &Path) -> Result<Identity> {
    Ok(Identity { version: 1, carrier_sha256: digest(&app.join(CARRIER_EXECUTABLE))?, guest_sha256: digest(&app.join(GUEST_MANIFEST))? })
}

fn recorded(context: &Context, paths: &Paths) -> Option<Identity> {
    Journal::open(context, paths).ok()?.read(RECORD).ok()
}

/// Records the identity after launchd loaded the carrier of the installed release.
pub fn record(context: &Context, paths: &Paths) -> Result<()> {
    Journal::open_or_create(context, paths)?.write(RECORD, &identity(&paths.app)?)
}

/// True when the loaded carrier was started from the release in `app`.
pub fn started_from(context: &Context, paths: &Paths, app: &Path) -> bool {
    identity(app).is_ok_and(|identity| recorded(context, paths) == Some(identity))
}

/// True when a replacement of the Host by the release in `app` can leave the carrier and its VM running.
pub fn survives(context: &Context, paths: &Paths, app: &Path, deadline: Deadline) -> bool {
    let target = format!("{}/{}", launchd::domain(paths), Job::Carrier.label());
    started_from(context, paths, app) && launchd::observe(context, &target, deadline).is_ok_and(|facts| facts.running)
}
