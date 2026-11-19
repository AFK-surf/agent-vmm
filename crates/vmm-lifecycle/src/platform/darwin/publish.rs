//! `publish-host` installs the bundle that contains the helper at the shared
//! Host location when no Host is there. It never replaces an installed Host.

use super::actions::{canonical, copy_bundle, host_release_id, verify_signature};
use super::paths::{shared_host_app, validate_maintenance_path};
use super::service::remove_tree;
use crate::error::{Kind, Result, err};
use crate::statefs::random_hex;
use crate::{Context, HOST_APP_NAME, Paths, journal};
use rustix::fs::{CWD, RenameFlags};
use serde::Serialize;
use std::os::unix::fs::DirBuilderExt;
use std::path::Path;

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Publication {
    version: u32,
    action: &'static str,
    target_app: String,
    release_id: String,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<serde_json::Value>,
}

/// The checks and the copy of a publication. A test replaces `codesign` and `ditto`.
pub trait Bundles {
    /// Returns the release identity of a canonical, correctly signed bundle.
    fn validate(&self, app: &Path) -> Result<String>;
    fn copy(&self, source: &Path, target: &Path) -> Result<()>;
}

pub struct Production<'a>(pub &'a Context);

impl Bundles for Production<'_> {
    fn validate(&self, app: &Path) -> Result<String> {
        if !canonical(app) {
            return Err(err("publication requires a canonical bundle without symlink ancestors"));
        }
        let id = host_release_id(app)?;
        verify_signature(self.0, app)?;
        Ok(id)
    }

    fn copy(&self, source: &Path, target: &Path) -> Result<()> {
        copy_bundle(self.0, source, target)
    }
}

/// Holds the owner lock through verification and publication, so no
/// maintenance can run between the check and the rename.
pub fn publish(context: &Context, paths: &Paths, source: &Path, bundles: &dyn Bundles) -> (Publication, Result<()>) {
    let mut result = Publication { version: 1, action: "publish-host", target_app: paths.app.display().to_string(), ..Publication::default() };
    let outcome = (|| -> Result<&'static str> {
        if paths.app != shared_host_app(&paths.home) || !source.is_absolute() || source.file_name().is_none_or(|name| name != HOST_APP_NAME) || source == paths.app {
            return Err(err("publication requires a separate Agent VMM Host.app source and the standard shared target"));
        }
        let _lock = journal::lock_mutation(context, paths).inspect_err(|error| {
            if error.kind == Kind::HostUninstalled {
                result.error = Some(serde_json::json!({"code": "host_uninstalled", "reason": "owner_uninstalled"}));
            }
        })?;
        validate_maintenance_path(&paths.home, &paths.app)?;
        result.release_id = host_release_id(source).map_err(|error| error.context("invalid publication source"))?;
        match paths.app.symlink_metadata() {
            Ok(_) if bundles.validate(&paths.app).is_ok_and(|id| id == result.release_id) => return Ok("already_present"),
            Ok(_) => return Err(err("shared Host target already exists; use update or maintenance to replace it")),
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            Err(_) => {}
        }
        let parent = paths.app.parent().ok_or_else(|| err("shared Host location is invalid"))?;
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)?;
        let staging = parent.join(format!(".host-publication-{}", random_hex(8)));
        std::fs::DirBuilder::new().mode(0o700).create(&staging)?;
        let stage = staging.join(HOST_APP_NAME);
        let published = (|| -> Result<()> {
            bundles.copy(source, &stage)?;
            if !bundles.validate(&stage).is_ok_and(|id| id == result.release_id) {
                return Err(err("copied publication bundle failed release or signature verification"));
            }
            rustix::fs::renameat_with(CWD, &stage, CWD, &paths.app, RenameFlags::NOREPLACE)
                .map_err(|error| err(format!("shared Host publication failed without replacing the existing target: {error}")))?;
            Ok(std::fs::File::open(parent)?.sync_all()?)
        })();
        let _ = remove_tree(&staging);
        published.map(|()| "published")
    })();
    match outcome {
        Ok(outcome) => {
            result.outcome = outcome.into();
            (result, Ok(()))
        }
        Err(error) => (result, Err(error)),
    }
}
