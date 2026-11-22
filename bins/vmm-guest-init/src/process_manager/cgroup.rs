//! Per-execution and per-process cgroup v2 isolation, killing, and OOM detection.

use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const REMOVE_ATTEMPTS: usize = 10;
const REMOVE_RETRY_DELAY: Duration = Duration::from_millis(20);

#[derive(Debug)]
enum Mode {
    /// Dedicated child cgroup created for one execution or process.
    Dedicated(PathBuf),
    /// Degraded fallback when child cgroups cannot be created (e.g. non-cgroupfs tests).
    Shared {
        dir: PathBuf,
        baseline_oom_kill: u64,
    },
}

#[derive(Debug)]
pub struct SubCgroup {
    mode: Mode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OomStats {
    pub oom_killed: bool,
    pub memory_peak_bytes: Option<u64>,
}

pub(crate) fn read_oom_kill(dir: &Path) -> Option<u64> {
    let text = fs::read_to_string(dir.join("memory.events")).ok()?;
    text.lines()
        .find_map(|line| line.strip_prefix("oom_kill "))
        .and_then(|value| value.trim().parse::<u64>().ok())
}

pub(crate) fn read_memory_peak(dir: &Path) -> Option<u64> {
    fs::read_to_string(dir.join("memory.peak"))
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

fn cstring_path(path: &Path) -> Option<CString> {
    CString::new(path.as_os_str().as_bytes()).ok()
}

impl SubCgroup {
    /// Creates a child cgroup `<parent>/<name>` when cgroup v2 child creation works,
    /// otherwise falls back to monitoring `<parent>` in degraded mode.
    pub fn create(parent: Option<&Path>, name: &str) -> Option<Self> {
        let parent = parent?;
        let sub_dir = parent.join(name);
        let created = fs::create_dir(&sub_dir).is_ok();
        if (created || sub_dir.is_dir()) && sub_dir.join("cgroup.procs").exists() {
            return Some(Self {
                mode: Mode::Dedicated(sub_dir),
            });
        }
        if created {
            let _ = fs::remove_dir(&sub_dir);
        }
        if parent.join("cgroup.procs").exists() || parent.join("memory.events").exists() {
            let baseline_oom_kill = read_oom_kill(parent).unwrap_or(0);
            return Some(Self {
                mode: Mode::Shared {
                    dir: parent.to_path_buf(),
                    baseline_oom_kill,
                },
            });
        }
        None
    }

    /// Returns the `cgroup.procs` path that the spawned child should join in `pre_exec`.
    pub fn procs_path(&self) -> Option<CString> {
        match &self.mode {
            Mode::Dedicated(dir) => cstring_path(&dir.join("cgroup.procs")),
            Mode::Shared { dir, .. } => {
                let procs = dir.join("cgroup.procs");
                procs.exists().then(|| cstring_path(&procs)).flatten()
            }
        }
    }

    /// Writes `"1"` to `<sub_cgroup>/cgroup.kill` to terminate all processes in the child cgroup,
    /// including descendants that detached from the process group.
    pub fn kill(&self) {
        if let Mode::Dedicated(dir) = &self.mode {
            let _ = fs::write(dir.join("cgroup.kill"), "1");
        }
    }

    /// Reads OOM and peak memory stats, optionally kills remaining descendants, and removes
    /// the dedicated child cgroup directory.
    pub fn finish(&self, timed_out: bool, kill_remaining: bool) -> OomStats {
        match &self.mode {
            Mode::Dedicated(dir) => {
                let oom_killed = read_oom_kill(dir).unwrap_or(0) > 0;
                let memory_peak_bytes = read_memory_peak(dir);
                if kill_remaining {
                    let _ = fs::write(dir.join("cgroup.kill"), "1");
                }
                remove_cgroup_dir(dir, kill_remaining);
                OomStats {
                    oom_killed,
                    memory_peak_bytes,
                }
            }
            Mode::Shared {
                dir,
                baseline_oom_kill,
            } => {
                let after = read_oom_kill(dir).unwrap_or(*baseline_oom_kill);
                let oom_killed = !timed_out && after > *baseline_oom_kill;
                let memory_peak_bytes = read_memory_peak(dir);
                OomStats {
                    oom_killed,
                    memory_peak_bytes,
                }
            }
        }
    }
}

fn remove_cgroup_dir(dir: &Path, retry: bool) {
    let attempts = if retry { REMOVE_ATTEMPTS } else { 2 };
    for attempt in 0..attempts {
        match fs::remove_dir(dir) {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(_) if attempt + 1 < attempts => {
                std::thread::sleep(REMOVE_RETRY_DELAY);
            }
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedicated_sub_cgroup_reads_oom_and_peak_and_cleans_up() {
        let root = tempfile::tempdir().unwrap();
        let sub = root.path().join("exec-1");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("cgroup.procs"), "").unwrap();
        fs::write(sub.join("cgroup.kill"), "").unwrap();
        fs::write(
            sub.join("memory.events"),
            "low 0\nhigh 0\nmax 2\noom 1\noom_kill 1\n",
        )
        .unwrap();
        fs::write(sub.join("memory.peak"), "67108864\n").unwrap();

        let cg = SubCgroup::create(Some(root.path()), "exec-1").unwrap();
        assert!(cg.procs_path().is_some());
        cg.kill();
        assert_eq!(fs::read_to_string(sub.join("cgroup.kill")).unwrap(), "1");
        let stats = cg.finish(false, true);
        assert!(stats.oom_killed);
        assert_eq!(stats.memory_peak_bytes, Some(67_108_864));
    }

    #[test]
    fn shared_fallback_detects_oom_delta_and_ignores_timeout() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("cgroup.procs"), "").unwrap();
        fs::write(root.path().join("memory.events"), "oom_kill 2\n").unwrap();
        fs::write(root.path().join("memory.peak"), "12345\n").unwrap();

        let cg = SubCgroup::create(Some(root.path()), "exec-2").unwrap();
        // No change -> not OOM killed.
        assert_eq!(
            cg.finish(false, false),
            OomStats {
                oom_killed: false,
                memory_peak_bytes: Some(12345),
            }
        );
        // Increment oom_kill -> OOM detected unless timed_out.
        fs::write(root.path().join("memory.events"), "oom_kill 3\n").unwrap();
        assert!(cg.finish(false, false).oom_killed);
        assert!(!cg.finish(true, true).oom_killed);
    }
}
