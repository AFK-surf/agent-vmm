#![forbid(unsafe_code)]
use crate::{
    bundle::Bundle,
    server,
    sparse::open_pool,
    vz::{Settings, VzHypervisor},
};
use clap::Parser;
use objc2_foundation::NSProcessInfo;
use std::{
    fs::{self, DirBuilder, OpenOptions, Permissions},
    io,
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    net::{UnixListener, UnixStream},
    signal::unix::{SignalKind, signal},
    sync::Mutex,
};

#[derive(Parser)]
struct Args {
    #[arg(long,default_value_t=64<<30)]
    state_size: u64,
    #[arg(long)]
    bundle: PathBuf,
    #[arg(long)]
    instance: PathBuf,
    #[arg(long)]
    socket: PathBuf,
    #[arg(long)]
    serial_log: PathBuf,
    #[arg(long)]
    cpus: Option<usize>,
    #[arg(long)]
    memory_bytes: Option<u64>,
}
fn private_dir(path: &Path) -> io::Result<()> {
    DirBuilder::new().recursive(true).mode(0o700).create(path)?;
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.mode() & 0o077 != 0 {
        return Err(io::Error::other(format!(
            "expected private directory: {}",
            path.display()
        )));
    }
    Ok(())
}
struct SocketPath(PathBuf, u64);
impl Drop for SocketPath {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.0)
            .is_ok_and(|m| m.ino() == self.1 && m.file_type().is_socket())
        {
            let _ = fs::remove_file(&self.0);
        }
    }
}
async fn run_args(args: Args) -> io::Result<()> {
    private_dir(&args.instance)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(args.instance.join("carrier.lock"))?;
    lock.try_lock().map_err(io::Error::other)?;
    let bundle = Bundle::verify(&args.bundle)?;
    let parent = |p: &Path| {
        p.parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| io::Error::other("missing parent directory"))
    };
    private_dir(&parent(&args.serial_log)?)?;
    private_dir(&parent(&args.socket)?)?;
    let pool = args.instance.join("pool.raw");
    drop(open_pool(&pool, args.state_size)?);
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&args.serial_log)?;
    match fs::symlink_metadata(&args.socket) {
        Ok(meta) if meta.file_type().is_socket() => match UnixStream::connect(&args.socket).await {
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                fs::remove_file(&args.socket)?
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "carrier socket in use",
                ));
            }
        },
        Ok(_) => return Err(io::Error::other("carrier socket path occupied")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let host = NSProcessInfo::processInfo();
    let available = host.physicalMemory().saturating_sub(4 << 30);
    let memory = args
        .memory_bytes
        .unwrap_or((host.physicalMemory() / 2).min(available) & !((1 << 20) - 1));
    const MAX_L4H_CPUS: usize = 8;
    let cpus = args
        .cpus
        .unwrap_or((host.processorCount() / 2).clamp(1, MAX_L4H_CPUS));
    if cpus == 0
        || cpus > host.processorCount().min(MAX_L4H_CPUS)
        || memory < 512 << 20
        || memory > available
        || !memory.is_multiple_of(1 << 20)
    {
        return Err(io::Error::other(
            "invalid carrier CPU or memory configuration",
        ));
    }
    let settings = Settings {
        bundle,
        pool,
        serial_log: args.serial_log,
        cpus,
        memory,
    };
    let listener = UnixListener::bind(&args.socket)?;
    let _socket = SocketPath(
        args.socket.clone(),
        fs::symlink_metadata(&args.socket)?.ino(),
    );
    fs::set_permissions(&args.socket, Permissions::from_mode(0o600))?;
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    server::serve(
        listener,
        Arc::new(Mutex::new(VzHypervisor::new(settings))),
        async move {
            tokio::select! {_=term.recv()=>{},_=int.recv()=>{}}
        },
    )
    .await
}

pub fn run() -> io::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(run_args(Args::parse()))
}
