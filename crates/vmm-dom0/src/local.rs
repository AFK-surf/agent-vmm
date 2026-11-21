//! Linux host initialization shared by the embedded backend and the control daemon.
use crate::{
    backend::{LocalDom0Backend, Options},
    boot_server::BootServer,
    dns::DnsServer,
    extent::{DEFAULT_EMERGENCY_BYTES, FilesystemHeadroom, Headroom, Pool, UnlimitedHeadroom},
    keys::MasterKey,
    network::NftablesDriver,
};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
    net::SocketAddr,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{net::TcpListener, task::JoinSet};
use vmm_core::backend::Dom0Backend;
use vmm_l4hctl::{DiskExtent, HypervisorCtl, LinuxL4hCtl};
use zeroize::Zeroizing;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dom0Config {
    state_dir: PathBuf,
    #[serde(default)]
    backing_filesystem: Option<PathBuf>,
    disk: u64,
    first_sector: u64,
    sectors: u64,
    master_key: PathBuf,
    initrd: PathBuf,
    interface: String,
    switch: u64,
    helper_root: PathBuf,
    helper_digest: String,
    #[serde(default)]
    builder_archive: Option<PathBuf>,
    #[serde(default)]
    builder_digest: Option<String>,
    #[serde(default)]
    emergency_bytes: Option<u64>,
    #[serde(default)]
    host_shares: BTreeMap<String, PathBuf>,
    pub socket: Option<PathBuf>,
    #[serde(default)]
    pub carrier_quic_listen: Option<SocketAddr>,
    pub tls: Option<TlsConfig>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub listen: SocketAddr,
    pub client_ca: PathBuf,
    pub certificate: PathBuf,
    pub key: PathBuf,
}

pub struct LocalDom0Handle {
    pub backend: Arc<LocalDom0Backend>,
    // Dropping the set aborts the listeners and their connection tasks.
    listeners: JoinSet<io::Result<()>>,
}

impl LocalDom0Handle {
    pub(crate) fn serve(
        backend: Arc<LocalDom0Backend>,
        boot_listener: TcpListener,
        dns: DnsServer,
    ) -> Self {
        let boot = backend.boots.clone();
        let mut listeners = JoinSet::new();
        listeners.spawn(async move { boot.serve(boot_listener).await });
        listeners.spawn(dns.serve(backend.clone()));
        Self { backend, listeners }
    }
    /// Wait for either guest listener to stop, then stop the other listener.
    pub async fn wait(&mut self) -> Result<()> {
        if let Some(first) = self.listeners.join_next().await {
            self.listeners.shutdown().await;
            first??;
        }
        Ok(())
    }
}

pub async fn open(config_path: &Path, init_pool: bool) -> Result<LocalDom0Handle> {
    Dom0Config::read(config_path)?.open(init_pool).await
}

impl Dom0Config {
    pub fn read(path: &Path) -> Result<Self> {
        Ok(serde_json::from_reader(File::open(path)?)?)
    }

    pub async fn open(self, init_pool: bool) -> Result<LocalDom0Handle> {
        let builder = trusted_builder(
            self.builder_archive.as_deref(),
            self.builder_digest.as_deref(),
            Path::new("/usr/lib/agent-vmm"),
        )?;
        private_dir(&self.state_dir)?;
        let mut secret = File::open(&self.master_key)?;
        if secret.metadata()?.permissions().mode() & 0o077 != 0 {
            return Err("master key is group- or world-accessible".into());
        }
        let mut key = Zeroizing::new([0u8; 32]);
        secret.read_exact(key.as_mut())?;
        if secret.read(&mut [0_u8])? != 0 {
            return Err("master key must contain exactly 32 bytes".into());
        }
        let ctl = Arc::new(Mutex::new(LinuxL4hCtl::open()?));
        let sectors = if self.sectors == 0 {
            let disks = ctl
                .lock()
                .map_err(|_| "l4hctl mutex poisoned")?
                .blk_disks()?;
            let disk = disks
                .into_iter()
                .find(|d| d.id == self.disk)
                .ok_or_else(|| format!("pool disk {} not found", self.disk))?;
            let avail = disk
                .sectors
                .checked_sub(self.first_sector)
                .ok_or("pool disk smaller than first_sector")?;
            avail - (avail % crate::extent::BLOCK_SECTORS)
        } else {
            self.sectors
        };
        let db = self.state_dir.join("pool.db");
        if !init_pool && !db.try_exists()? {
            return Err(
                "missing pool allocation database; --init-pool provisions an empty disk".into(),
            );
        }
        let headroom: Arc<dyn Headroom> = match self.backing_filesystem {
            Some(path) => Arc::new(FilesystemHeadroom(path)),
            None => Arc::new(UnlimitedHeadroom),
        };
        let pool = Arc::new(Pool::open(
            &db,
            ctl.clone(),
            self.disk,
            DiskExtent {
                sector: self.first_sector,
                sectors,
            },
            headroom,
            self.emergency_bytes.unwrap_or(DEFAULT_EMERGENCY_BYTES),
        )?);
        let mut options = Options::new(
            std::fs::read(self.initrd)?.into(),
            self.state_dir.join("images"),
        );
        options.dns_port = 53;
        options.upstream_dns = Some(crate::dns::public_resolver(
            &std::fs::read_to_string("/etc/resolv.conf").unwrap_or_default(),
        ));
        let dns_address = std::net::SocketAddrV4::new(options.gateway, options.dns_port);
        options.switch = Some(self.switch);
        let digest: [u8; 32] = hex::decode(&self.helper_digest)?
            .try_into()
            .map_err(|_| "invalid helper digest")?;
        options.helper_image = Some(digest);
        options.builder_image = builder.as_ref().map(|(_, d)| *d);
        options.host_shares = self.host_shares;

        let boot_address = (options.gateway, options.boot_port);
        let fw = NftablesDriver::new(self.interface.clone())?;
        let backend = Arc::new(LocalDom0Backend::open(
            pool,
            ctl,
            options,
            Arc::new(MasterKey(key)),
            Arc::new(fw),
            BootServer::default(),
        )?);
        configure_gateway(&self.interface)?;
        let dns = DnsServer::bind(dns_address).await?;
        let listener = TcpListener::bind(boot_address).await?;
        backend.recover_transients().await?;
        if !backend.image_installed(digest)? {
            let image = backend.stage_directory(&self.helper_root, digest)?;
            backend.install_image_artifact(&image).await?;
        }
        if let Some((archive, digest)) = builder
            && !backend.image_installed(digest)?
        {
            let image = backend.stage_image(&archive, digest)?;
            backend.install_image_artifact(&image).await?;
        }
        Ok(LocalDom0Handle::serve(backend, listener, dns))
    }
}

fn trusted_builder(
    archive: Option<&Path>,
    digest: Option<&str>,
    release_dir: &Path,
) -> Result<Option<(PathBuf, vmm_core::Digest)>> {
    let (path, text) = match (archive, digest) {
        (Some(path), Some(digest)) => (path.to_owned(), digest.to_owned()),
        (None, None) => {
            let path = release_dir.join("buildkit.oci.tar");
            let metadata = release_dir.join("buildkit.digest");
            if !path.try_exists()? && !metadata.try_exists()? {
                return Ok(None);
            }
            (path, std::fs::read_to_string(metadata)?)
        }
        _ => return Err("builder_archive and builder_digest must be configured together".into()),
    };
    if !path.metadata()?.is_file() {
        return Err("builder_archive must be a regular OCI archive".into());
    }
    let hash = text.trim();
    let hash = hash.strip_prefix("sha256:").unwrap_or(hash);
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid trusted builder manifest digest".into());
    }
    let mut digest = [0; 32];
    hex::decode_to_slice(hash, &mut digest)?;
    Ok(Some((path, digest)))
}

pub fn private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)?;
    if std::fs::symlink_metadata(path)?.permissions().mode() & 0o077 != 0 {
        return Err(format!("{} must be a private directory (0700)", path.display()).into());
    }
    Ok(())
}

fn configure_gateway(interface: &str) -> Result<()> {
    use std::process::Command;
    for args in [
        &["address", "replace", "10.77.0.1/16", "dev", interface][..],
        &["link", "set", "dev", interface, "up"],
        &["route", "replace", "local", "100.127.254.0/24", "dev", "lo"],
        &["route", "replace", "local", "100.127.255.1/32", "dev", "lo"],
    ] {
        let output = Command::new("ip").args(args).output()?;
        if !output.status.success() {
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
            .into());
        }
    }
    std::fs::write("/proc/sys/net/ipv4/ip_forward", "1\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_archive_and_digest_form_one_release_artifact() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let dir = tmp.path();
        let archive = dir.join("buildkit.oci.tar");
        let meta = dir.join("buildkit.digest");
        let hash = "ab".repeat(32);
        assert!(trusted_builder(None, None, dir)?.is_none());
        std::fs::write(&meta, format!("sha256:{hash}\n"))?;
        assert!(trusted_builder(None, None, dir).is_err());
        std::fs::write(&archive, [])?;
        let expected = Some((archive.clone(), [0xab; 32]));
        assert_eq!(trusted_builder(None, None, dir)?, expected);
        assert_eq!(trusted_builder(Some(&archive), Some(&hash), dir)?, expected);
        assert!(trusted_builder(Some(&archive), None, dir).is_err());
        assert!(trusted_builder(None, Some(&hash), dir).is_err());
        std::fs::write(&meta, "not a digest")?;
        assert!(trusted_builder(None, None, dir).is_err());
        std::fs::remove_file(meta)?;
        assert!(trusted_builder(None, None, dir).is_err());
        Ok(())
    }
}
