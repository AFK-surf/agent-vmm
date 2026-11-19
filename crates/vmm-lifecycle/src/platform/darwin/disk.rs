//! The VM state disk. Its data can be removed only after every process closed it.

use crate::error::{Deadline, Error, Kind, Result, err};
use crate::preflight::STATE_DISKS;
use crate::{Context, Paths};
use std::path::Path;
use std::time::Duration;

const LSOF: &str = "/usr/sbin/lsof";

/// The process identifiers in `lsof -F` output. With `expected`, only the owners of that file name count.
fn owners(output: &str, expected: Option<&str>) -> Vec<u32> {
    let mut owners = Vec::new();
    let mut pid = 0;
    for line in output.lines().filter(|line| line.len() >= 2) {
        let (field, value) = line.split_at(1);
        match (field, expected) {
            ("p", None) => {
                pid = value.parse().unwrap_or(0);
                owners.extend((pid > 0).then_some(pid));
            }
            ("p", Some(_)) => pid = value.parse().unwrap_or(0),
            ("n", Some(expected)) if pid > 0 && value.strip_suffix(" (deleted)").unwrap_or(value) == expected => owners.push(pid),
            _ => {}
        }
    }
    owners
}

fn inspect(context: &Context, arguments: &[&str], deadline: Deadline) -> Result<String> {
    let output = context.system.run(LSOF, arguments, deadline.within(Duration::from_secs(10)))?;
    // lsof exits with 1 and prints nothing when no process matches.
    if !output.stderr.trim().is_empty() || !matches!(output.code, Some(0 | 1)) {
        return Err(err("open-file inspection failed"));
    }
    Ok(output.stdout)
}

fn open_owners(context: &Context, path: &Path, deadline: Deadline) -> Result<Vec<u32>> {
    let name = path.to_str().ok_or_else(|| err("state disk path is invalid"))?;
    if path.symlink_metadata().is_ok() {
        return Ok(owners(&inspect(context, &["-Fpn", "--", name], deadline)?, None));
    }
    // A removed disk can stay open. List the unlinked open files and match the name.
    let parent = path.parent().and_then(|parent| parent.canonicalize().ok());
    let expected = parent.zip(path.file_name()).map(|(parent, file)| parent.join(file));
    let expected = expected.as_deref().unwrap_or(path).display().to_string();
    let output = inspect(context, &["+L1", "-Fpn"], deadline)?;
    if path.symlink_metadata().is_ok() {
        return Err(err("state disk changed during attachment inspection"));
    }
    Ok(owners(&output, Some(&expected)))
}

/// The processes that hold a state disk of this installation open.
pub fn state_disk_owners(context: &Context, paths: &Paths, deadline: Deadline) -> Result<Vec<u32>> {
    let mut all = Vec::new();
    for disk in STATE_DISKS {
        all.extend(open_owners(context, &paths.application_data.join(disk), deadline)?);
    }
    Ok(all)
}

pub fn wait_state_disk_release(context: &Context, paths: &Paths, deadline: Deadline) -> Result<()> {
    let deadline = deadline.within(Duration::from_secs(5));
    loop {
        let owners = state_disk_owners(context, paths, deadline).map_err(|error| error.context("verify VM state disk ownership"))?;
        if owners.is_empty() {
            return Ok(());
        }
        if deadline.sleep(Duration::from_millis(25)).is_err() {
            return Err(Error::new(Kind::Timeout, format!("VM state disk is still open by process IDs {owners:?}")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_file_listings_name_the_owning_processes() {
        assert_eq!(owners("p12\nn/a/pool.raw\np34\nn/a/pool.raw\n", None), [12, 34]);
        let unlinked = "p12\nn/a/other\np34\nn/a/pool.raw (deleted)\nn/a/pool.raw\np56\nn/b/pool.raw\n";
        assert_eq!(owners(unlinked, Some("/a/pool.raw")), [34, 34]);
        assert!(owners("", None).is_empty());
    }
}
