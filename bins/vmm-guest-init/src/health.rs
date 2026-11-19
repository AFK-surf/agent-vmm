//! Samples the resource usage of the workload from cgroup v2 and sysfs.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use vmm_guest_proto::rpc::ResourceUsage;

use crate::{sys, unix_nanos};

/// The sources of a sample. A missing source yields `None`, never zero.
#[derive(Debug, Clone, Default)]
pub struct Sampler {
    /// The workload cgroup directory.
    pub cgroup_dir: Option<PathBuf>,
    /// The `statistics` directory of the network interface.
    pub network_statistics_dir: Option<PathBuf>,
    /// A path on the writable root layer.
    pub overlay_path: Option<PathBuf>,
    /// The mount path of each volume, by volume name.
    pub volume_paths: Vec<(String, PathBuf)>,
}

fn read_number(dir: Option<&Path>, name: &str) -> Option<u64> {
    fs::read_to_string(dir?.join(name))
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// Sums the values of `key` in a flat or nested keyed cgroup file.
fn sum_keyed(dir: Option<&Path>, name: &str, key: &str) -> Option<u64> {
    let text = fs::read_to_string(dir?.join(name)).ok()?;
    let values = text.split_ascii_whitespace().filter_map(|field| {
        field
            .strip_prefix(key)?
            .strip_prefix('=')
            .and_then(|value| value.parse::<u64>().ok())
    });
    Some(values.sum())
}

fn used_bytes(path: &Path) -> Option<u64> {
    sys::used_bytes(&File::open(path).ok()?).ok()
}

impl Sampler {
    pub fn sample(&self) -> ResourceUsage {
        let cgroup = self.cgroup_dir.as_deref();
        let network = self.network_statistics_dir.as_deref();
        let cpu_usec = cgroup
            .and_then(|dir| fs::read_to_string(dir.join("cpu.stat")).ok())
            .and_then(|text| {
                let line = text
                    .lines()
                    .find_map(|line| line.strip_prefix("usage_usec "))?;
                line.trim().parse::<u64>().ok()
            });
        let overlay_used_bytes = self.overlay_path.as_deref().and_then(used_bytes);
        let volume_used_bytes: Vec<(String, u64)> = self
            .volume_paths
            .iter()
            .filter_map(|(name, path)| Some((name.clone(), used_bytes(path)?)))
            .collect();
        let volumes_total: u64 = volume_used_bytes.iter().map(|(_, used)| used).sum();
        let disk_used_bytes = overlay_used_bytes
            .filter(|_| volume_used_bytes.len() == self.volume_paths.len())
            .map(|overlay| overlay + volumes_total);
        ResourceUsage {
            sampled_unix_nanos: unix_nanos(),
            cpu_nanos: cpu_usec.map(|usec| usec.saturating_mul(1000)),
            memory_bytes: read_number(cgroup, "memory.current"),
            pids: read_number(cgroup, "pids.current"),
            disk_read_bytes: sum_keyed(cgroup, "io.stat", "rbytes"),
            disk_write_bytes: sum_keyed(cgroup, "io.stat", "wbytes"),
            disk_used_bytes,
            overlay_used_bytes,
            volume_used_bytes,
            network_rx_bytes: read_number(network, "rx_bytes"),
            network_tx_bytes: read_number(network, "tx_bytes"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_cgroup_and_interface_counters_and_marks_missing_sources() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, text: &str| fs::write(dir.path().join(name), text).unwrap();
        write(
            "cpu.stat",
            "usage_usec 1500\nuser_usec 1000\nsystem_usec 500\n",
        );
        write("memory.current", "4096\n");
        write("pids.current", "7\n");
        write(
            "io.stat",
            "254:0 rbytes=100 wbytes=20 rios=3\n254:16 rbytes=5 wbytes=1 dbytes=0\n",
        );
        write("rx_bytes", "11\n");

        let mut sampler = Sampler {
            cgroup_dir: Some(dir.path().into()),
            network_statistics_dir: Some(dir.path().into()),
            overlay_path: Some(dir.path().into()),
            volume_paths: vec![("data".into(), dir.path().into())],
        };
        let usage = sampler.sample();
        assert_eq!(usage.cpu_nanos, Some(1_500_000));
        assert_eq!((usage.memory_bytes, usage.pids), (Some(4096), Some(7)));
        assert_eq!(
            (usage.disk_read_bytes, usage.disk_write_bytes),
            (Some(105), Some(21))
        );
        assert_eq!(
            (usage.network_rx_bytes, usage.network_tx_bytes),
            (Some(11), None)
        );
        let overlay = usage.overlay_used_bytes.unwrap();
        let [(name, volume)] = &usage.volume_used_bytes[..] else {
            panic!("{usage:?}");
        };
        assert_eq!(
            (name.as_str(), usage.disk_used_bytes),
            ("data", Some(overlay + volume))
        );

        // A volume that the Guest cannot read is absent, and the sum is unknown.
        let absent = dir.path().join("absent");
        sampler.volume_paths.push(("absent".into(), absent));
        let usage = sampler.sample();
        assert_eq!(
            (usage.volume_used_bytes.len(), usage.disk_used_bytes),
            (1, None)
        );

        let missing = Sampler::default().sample();
        assert_eq!(
            (missing.cpu_nanos, missing.pids, missing.disk_used_bytes),
            (None, None, None)
        );
    }
}
