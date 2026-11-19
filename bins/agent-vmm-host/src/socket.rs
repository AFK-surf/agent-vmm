use std::{
    fs::{self, DirBuilder, File},
    io::{self, ErrorKind},
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::net::{UnixListener, UnixStream};

pub(crate) fn private_directory(path: &Path) -> io::Result<()> {
    DirBuilder::new().mode(0o700).recursive(true).create(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir()
        || metadata.mode() & 0o777 != 0o700
        || metadata.uid() != rustix::process::geteuid().as_raw()
    {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "host socket directory must be private and owned by the service account",
        ));
    }
    Ok(())
}

/// Remove only the directory entry created by this listener.
pub(crate) struct SocketPath {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Drop for SocketPath {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path)
            .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == (self.dev, self.ino))
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

pub(crate) async fn listen(path: &Path) -> io::Result<(UnixListener, SocketPath)> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "socket directory is required"))?;
    private_directory(parent)?;
    match UnixListener::bind(path) {
        Ok(listener) => return finish_bind(path, listener),
        Err(e) if e.kind() == ErrorKind::AddrInUse => {}
        Err(e) => return Err(e),
    }
    // Serialize stale-socket recovery with other starting hosts.
    let directory = File::open(parent)?;
    directory.try_lock().map_err(io::Error::from)?;
    match fs::symlink_metadata(path) {
        Ok(m) if !m.file_type().is_socket() => {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                "socket path occupied by another file",
            ));
        }
        Ok(_) => {
            match tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(path)).await {
                Ok(Err(e)) if e.kind() == ErrorKind::ConnectionRefused => fs::remove_file(path)?,
                Ok(Err(e)) if e.kind() == ErrorKind::NotFound => {}
                Ok(Err(e)) => return Err(e),
                _ => return Err(io::Error::new(ErrorKind::AddrInUse, "socket is in use")),
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    finish_bind(path, UnixListener::bind(path)?)
}

fn finish_bind(path: &Path, listener: UnixListener) -> io::Result<(UnixListener, SocketPath)> {
    let metadata = fs::symlink_metadata(path)?;
    let guard = SocketPath {
        path: path.to_owned(),
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok((listener, guard))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn protects_active_listener_and_recovers_abandoned_socket() {
        let tmp = tempfile::tempdir_in("/tmp").unwrap();
        let path = tmp.path().join("private/operator.sock");
        let (first, cleanup) = listen(&path).await.unwrap();
        assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
        assert_eq!(
            listen(&path).await.err().unwrap().kind(),
            ErrorKind::AddrInUse
        );
        let _ = first.accept().await.unwrap();
        drop(cleanup);
        drop(first);
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        let (second, next_cleanup) = listen(&path).await.unwrap();
        drop(second);
        drop(next_cleanup);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn preserves_other_files_and_rejects_nonprivate_directories() {
        let tmp = tempfile::tempdir_in("/tmp").unwrap();
        let dir = tmp.path().join("state");
        private_directory(&dir).unwrap();
        let path = dir.join("operator.sock");
        fs::write(&path, b"other").unwrap();
        assert!(listen(&path).await.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"other");
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(listen(&dir.join("session.sock")).await.is_err());
    }
}
