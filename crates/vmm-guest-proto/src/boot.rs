//! Boot configuration that dom0 serves once to each Guest at `/v1/boot`.

use std::collections::BTreeSet;
use std::fmt;
use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::Secret;

pub const BOOT_CONFIG_VERSION: u32 = 1;
pub const BOOT_PATH: &str = "/v1/boot";
pub const MAX_BOOT_CONFIG_BYTES: usize = 256 * 1024;
pub const DEFAULT_GATEWAY: Ipv4Addr = Ipv4Addr::new(10, 77, 0, 1);
pub const DEFAULT_BOOT_PORT: u16 = 8182;
pub const DEFAULT_RPC_PORT: u16 = 8183;
pub const SLOT_PREFIX_LEN: u8 = 16;

/// Kernel command-line key for the network slot of the VM.
pub const CMDLINE_SLOT: &str = "vmm.slot";
/// Kernel command-line key for the dom0 boot configuration port.
pub const CMDLINE_BOOT_PORT: &str = "vmm.boot_port";

pub const MIN_LOG_LIMIT_BYTES: u64 = 4096;
pub const MAX_LOG_LIMIT_BYTES: u64 = 64 << 20;

/// Address of a slot on the isolated `10.77.0.0/16` switch. dom0 owns `.0.1`.
pub fn slot_address(slot: u32) -> Option<Ipv4Addr> {
    if slot > 65532 {
        return None;
    }
    let host = slot + 2;
    Some(Ipv4Addr::new(10, 77, (host >> 8) as u8, host as u8))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootConfig {
    pub version: u32,
    pub instance_id: String,
    /// dom0 sends this token with each RPC. The Guest compares it in constant time.
    pub rpc_auth_token: Secret,
    #[serde(default = "default_rpc_port")]
    pub rpc_port: u16,
    pub network: NetworkConfig,
    pub root: RootConfig,
    #[serde(default)]
    pub scratch: Option<ScratchConfig>,
    #[serde(default)]
    pub volumes: Vec<VolumeMount>,
    #[serde(default)]
    pub host_shares: Vec<HostShareMount>,
    /// Root of workspace RPCs that select no volume.
    #[serde(default)]
    pub workspace_path: Option<String>,
    pub workload: WorkloadSpec,
}

fn default_rpc_port() -> u16 {
    DEFAULT_RPC_PORT
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub address: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Ipv4Addr,
    #[serde(default)]
    pub dns: Vec<Ipv4Addr>,
    #[serde(default)]
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootConfig {
    /// EROFS image device, such as `vda`. It is always mounted read-only.
    pub device: String,
    /// When true the workload root rejects writes. When false `overlay` holds its upper layer.
    pub read_only_root: bool,
    #[serde(default)]
    pub overlay: Option<EncryptedDisk>,
}

/// A block device opened with plain dm-crypt `aes-xts-plain64`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptedDisk {
    pub device: String,
    /// Hex key of 256 or 512 bits.
    pub key: Secret,
    #[serde(default)]
    pub allow_discards: bool,
    /// dom0 sets this only for a disk that holds no data. The Guest formats it,
    /// and dom0 clears the flag after the first successful `Health`.
    /// Plain dm-crypt cannot detect a wrong key, so the Guest never infers this.
    #[serde(default)]
    pub format: bool,
}

/// Volatile disk, formatted at each boot. It backs `/tmp` and, for a read-only
/// root, the mount points that the image lacks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScratchConfig {
    pub device: String,
    #[serde(default)]
    pub key: Option<Secret>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeMount {
    pub name: String,
    pub disk: EncryptedDisk,
    pub mount_path: String,
    /// Applied to the volume root by PID 1 when `disk.format` is set.
    pub owner_uid: u32,
    pub owner_gid: u32,
    pub mode: u32,
    /// The disk is a read-only block device. The Guest mounts it without its
    /// journal, so dom0 must attach a filesystem that was unmounted cleanly.
    #[serde(default)]
    pub read_only: bool,
}

/// A `9p2000.L` export that dom0 serves over TCP at the gateway address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostShareMount {
    /// The 9p attach name.
    pub name: String,
    pub port: u16,
    pub mount_path: String,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    #[serde(default)]
    pub entrypoint: Vec<String>,
    #[serde(default)]
    pub cmd: Vec<String>,
    /// `KEY=VALUE` entries, also the base environment of `Exec` and `ProcessStart`.
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub workdir: Option<String>,
    /// `name`, `uid`, `name:group`, or `uid:gid`. Absent selects root.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub pids_max: Option<u64>,
    pub log_limit_bytes: u64,
    /// Required when `entrypoint` and `cmd` are both empty: PID 1 starts no
    /// main process and stays up for RPC callers.
    #[serde(default)]
    pub keep_alive_idle: bool,
    /// Retains `CAP_SETGID`, `CAP_SETUID`, and `CAP_SETPCAP` as ambient
    /// capabilities across non-root UID drops so rootless BuildKit's
    /// `newuidmap`/`newgidmap` can establish sub-ID mappings without setuid
    /// bits or file capabilities. Set only for disposable build VMs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub subid_caps: bool,
}

impl WorkloadSpec {
    pub fn argv(&self) -> Vec<String> {
        self.entrypoint.iter().chain(&self.cmd).cloned().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootConfigError(String);

impl fmt::Display for BootConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid boot configuration: {}", self.0)
    }
}

impl std::error::Error for BootConfigError {}

fn invalid<T>(message: impl Into<String>) -> Result<T, BootConfigError> {
    Err(BootConfigError(message.into()))
}

impl BootConfig {
    /// Parses and validates a `/v1/boot` body.
    pub fn from_json(body: &[u8]) -> Result<Self, BootConfigError> {
        if body.len() > MAX_BOOT_CONFIG_BYTES {
            return invalid("body exceeds the size limit");
        }
        let config: Self =
            serde_json::from_slice(body).map_err(|error| BootConfigError(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn to_json(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(serde_json::to_vec(self).expect("boot configuration serializes to JSON"))
    }

    pub fn validate(&self) -> Result<(), BootConfigError> {
        if self.version != BOOT_CONFIG_VERSION {
            return invalid(format!("unsupported version {}", self.version));
        }
        if !valid_id(&self.instance_id) {
            return invalid("instance_id");
        }
        let token = self.rpc_auth_token.expose();
        if !(32..=256).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return invalid("rpc_auth_token must be 32 to 256 printable ASCII bytes");
        }
        if self.rpc_port == 0 {
            return invalid("rpc_port");
        }
        self.network.validate()?;

        let mut devices = BTreeSet::new();
        let mut claim_device = |device: &str| {
            let name = device.as_bytes();
            let valid = (3..=4).contains(&name.len())
                && name.starts_with(b"vd")
                && name[2..].iter().all(u8::is_ascii_lowercase);
            if valid && devices.insert(device.to_owned()) {
                Ok(())
            } else {
                invalid(format!("device {device:?} is invalid or used twice"))
            }
        };
        claim_device(&self.root.device)?;
        match (&self.root.overlay, self.root.read_only_root) {
            (Some(overlay), false) => {
                claim_device(&overlay.device)?;
                valid_disk_key(&overlay.key)?;
            }
            (None, true) => {}
            (None, false) => return invalid("a writable root requires root.overlay"),
            (Some(_), true) => return invalid("a read-only root takes no root.overlay"),
        }
        if let Some(scratch) = &self.scratch {
            claim_device(&scratch.device)?;
            if let Some(key) = &scratch.key {
                valid_disk_key(key)?;
            }
        }

        let mut names = BTreeSet::new();
        let mut mount_paths = BTreeSet::new();
        for volume in &self.volumes {
            claim_device(&volume.disk.device)?;
            valid_disk_key(&volume.disk.key)?;
            if !valid_id(&volume.name) || !names.insert(volume.name.as_str()) {
                return invalid(format!("volume name {:?}", volume.name));
            }
            if volume.mode > 0o7777 {
                return invalid(format!("volume {:?} mode", volume.name));
            }
            if volume.read_only && volume.disk.format {
                return invalid(format!(
                    "volume {:?} is read-only and unformatted",
                    volume.name
                ));
            }
            valid_mount_path(&volume.mount_path)?;
            if !mount_paths.insert(volume.mount_path.as_str()) {
                return invalid(format!("mount path {:?} is used twice", volume.mount_path));
            }
        }
        for share in &self.host_shares {
            if !valid_id(&share.name) || share.port == 0 {
                return invalid(format!("host share {:?}", share.name));
            }
            valid_mount_path(&share.mount_path)?;
            if !mount_paths.insert(share.mount_path.as_str()) {
                return invalid(format!("mount path {:?} is used twice", share.mount_path));
            }
        }
        if let Some(path) = &self.workspace_path {
            valid_mount_path(path)?;
        }
        self.workload.validate()
    }
}

impl NetworkConfig {
    fn validate(&self) -> Result<(), BootConfigError> {
        if !(1..=32).contains(&self.prefix_len) || self.dns.len() > 3 {
            return invalid("network");
        }
        if let Some(hostname) = &self.hostname
            && (hostname.len() > 63 || !valid_id(hostname))
        {
            return invalid("network.hostname");
        }
        Ok(())
    }
}

impl WorkloadSpec {
    fn validate(&self) -> Result<(), BootConfigError> {
        let argv = self.argv();
        if argv.is_empty() && !self.keep_alive_idle {
            return invalid("an empty workload command requires keep_alive_idle");
        }
        if argv.iter().any(|arg| arg.contains('\0')) || argv.first().is_some_and(String::is_empty) {
            return invalid("workload command");
        }
        if !self.env.iter().all(|entry| valid_env_entry(entry)) {
            return invalid("workload.env entries must be KEY=VALUE");
        }
        if let Some(workdir) = &self.workdir
            && (!workdir.starts_with('/') || workdir.contains('\0'))
        {
            return invalid("workload.workdir must be absolute");
        }
        if self
            .user
            .as_ref()
            .is_some_and(|user| user.is_empty() || user.contains('\0'))
        {
            return invalid("workload.user");
        }
        if self.pids_max == Some(0) {
            return invalid("workload.pids_max");
        }
        if !(MIN_LOG_LIMIT_BYTES..=MAX_LOG_LIMIT_BYTES).contains(&self.log_limit_bytes) {
            return invalid("workload.log_limit_bytes");
        }
        Ok(())
    }
}

/// `KEY=VALUE` with a non-empty key and no NUL byte.
pub fn valid_env_entry(entry: &str) -> bool {
    !entry.contains('\0')
        && entry
            .split_once('=')
            .is_some_and(|(key, _)| !key.is_empty())
}

fn valid_id(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && value.as_bytes()[0].is_ascii_alphanumeric()
}

fn valid_disk_key(key: &Secret) -> Result<(), BootConfigError> {
    let key = key.expose();
    if matches!(key.len(), 64 | 128) && key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        invalid("a disk key must be 256 or 512 bits of hex")
    }
}

/// An absolute, normalized path that does not shadow the kernel filesystems.
fn valid_mount_path(path: &str) -> Result<(), BootConfigError> {
    let normalized = path.len() <= 1024
        && !path.contains('\0')
        && path.strip_prefix('/').is_some_and(|rest| {
            !rest.is_empty()
                && rest
                    .split('/')
                    .all(|part| !part.is_empty() && part != "." && part != "..")
        });
    let reserved = ["/proc", "/sys", "/dev"]
        .iter()
        .any(|root| path == *root || path.starts_with(&format!("{root}/")));
    if normalized && !reserved {
        Ok(())
    } else {
        invalid(format!("mount path {path:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk(device: &str) -> EncryptedDisk {
        EncryptedDisk {
            device: device.into(),
            key: Secret::new("ab".repeat(64)),
            allow_discards: true,
            format: false,
        }
    }

    fn sample() -> BootConfig {
        BootConfig {
            version: BOOT_CONFIG_VERSION,
            instance_id: "vm-01".into(),
            rpc_auth_token: Secret::new("t".repeat(43)),
            rpc_port: DEFAULT_RPC_PORT,
            network: NetworkConfig {
                address: slot_address(5).unwrap(),
                prefix_len: SLOT_PREFIX_LEN,
                gateway: DEFAULT_GATEWAY,
                dns: vec![DEFAULT_GATEWAY],
                hostname: Some("sandbox".into()),
            },
            root: RootConfig {
                device: "vda".into(),
                read_only_root: false,
                overlay: Some(disk("vdb")),
            },
            scratch: Some(ScratchConfig {
                device: "vdc".into(),
                key: None,
            }),
            volumes: vec![VolumeMount {
                name: "workspace".into(),
                disk: disk("vdd"),
                mount_path: "/workspace".into(),
                owner_uid: 1000,
                owner_gid: 1000,
                mode: 0o700,
                read_only: false,
            }],
            host_shares: vec![HostShareMount {
                name: "share-1".into(),
                port: 5640,
                mount_path: "/mnt/host".into(),
                read_only: true,
            }],
            workspace_path: Some("/workspace".into()),
            workload: WorkloadSpec {
                entrypoint: vec![],
                cmd: vec![],
                env: vec!["PATH=/usr/bin:/bin".into()],
                workdir: Some("/workspace".into()),
                user: Some("1000:1000".into()),
                pids_max: Some(4096),
                log_limit_bytes: 1 << 20,
                keep_alive_idle: true,
                subid_caps: false,
            },
        }
    }

    #[test]
    fn json_round_trip_keeps_secrets_out_of_debug_output() {
        let config = sample();
        let json = config.to_json();
        assert!(
            !std::str::from_utf8(&json).unwrap().contains("subid_caps"),
            "default subid_caps=false must be omitted from boot JSON"
        );
        let parsed = BootConfig::from_json(&json).unwrap();
        assert_eq!(parsed, config);
        let debug = format!("{parsed:?}");
        assert!(
            !debug.contains(&"ab".repeat(8)) && !debug.contains(&"t".repeat(8)),
            "{debug}"
        );
    }

    #[test]
    fn slot_addresses_follow_the_dom0_switch_layout() {
        assert_eq!(slot_address(0), Some(Ipv4Addr::new(10, 77, 0, 2)));
        assert_eq!(slot_address(254), Some(Ipv4Addr::new(10, 77, 1, 0)));
        assert_eq!(slot_address(65533), None);
    }

    #[test]
    fn rejects_configurations_that_pid1_cannot_assemble() {
        type Mutation = fn(&mut BootConfig);
        let cases: &[(&str, Mutation)] = &[
            ("writable root without overlay", |c| c.root.overlay = None),
            ("overlay on a read-only root", |c| {
                c.root.read_only_root = true
            }),
            ("device used twice", |c| {
                c.volumes[0].disk.device = "vdb".into()
            }),
            ("device path", |c| c.root.device = "../vda".into()),
            ("short key", |c| c.volumes[0].disk.key = Secret::new("abcd")),
            ("escaping mount path", |c| {
                c.volumes[0].mount_path = "/a/../b".into()
            }),
            ("mount over /proc", |c| {
                c.host_shares[0].mount_path = "/proc/x".into()
            }),
            ("duplicate mount path", |c| {
                c.host_shares[0].mount_path = "/workspace".into()
            }),
            ("idle without keep_alive_idle", |c| {
                c.workload.keep_alive_idle = false
            }),
            ("env without key", |c| c.workload.env = vec!["=x".into()]),
            ("short token", |c| c.rpc_auth_token = Secret::new("short")),
            ("unbounded log", |c| c.workload.log_limit_bytes = u64::MAX),
            ("formatting a read-only volume", |c| {
                c.volumes[0].read_only = true;
                c.volumes[0].disk.format = true;
            }),
        ];
        for (name, mutate) in cases {
            let mut config = sample();
            mutate(&mut config);
            assert!(config.validate().is_err(), "{name}");
        }
        assert!(BootConfig::from_json(br#"{"version":1,"unknown":true}"#).is_err());
    }
}
