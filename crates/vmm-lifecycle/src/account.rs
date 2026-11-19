//! Operating system accounts.

use crate::error::{Result, err};
use std::path::PathBuf;
use uzers::os::unix::UserExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub name: String,
    pub uid: u32,
    pub gid: u32,
    pub group: String,
    pub home: PathBuf,
    /// The groups of the user, with the primary group.
    pub groups: Vec<u32>,
}

/// Looks up `name`, or the effective user when no name is given.
pub fn lookup(name: Option<&str>) -> Result<Account> {
    let user = match name {
        Some(name) => uzers::get_user_by_name(name),
        None => uzers::get_user_by_uid(uzers::get_effective_uid()),
    }
    .ok_or_else(|| err("service user is unavailable"))?;
    let gid = user.primary_group_id();
    let group = uzers::get_group_by_gid(gid).map(|group| group.name().to_string_lossy().into_owned()).unwrap_or_default();
    Ok(Account {
        name: user.name().to_string_lossy().into_owned(),
        uid: user.uid(),
        gid,
        group,
        home: user.home_dir().to_path_buf(),
        groups: user.groups().unwrap_or_default().iter().map(|group| group.gid()).collect(),
    })
}
