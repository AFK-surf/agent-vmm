use anyhow::{Context, Result};
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::mpsc,
};
use vmm_data::{
    Command,
    node::{Channel, DataNode, Tunnel},
};
use zeroize::Zeroizing;

#[tokio::main]
async fn main() -> Result<()> {
    let (socket, service_socket, mesh_control_socket) = socket_arguments()?;
    let host_exited = watch_lifeline();
    if let Some(parent) = socket.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if socket.exists() {
        tokio::fs::remove_file(&socket).await?;
    }

    let node = DataNode::bind().await?;
    let listener = UnixListener::bind(&socket).context("bind local control socket")?;
    let socket_identity = file_identity(&socket)?;
    let (tx, rx) = DataNode::incoming();

    tokio::select! {
        result = tokio::signal::ctrl_c() => { result?; },
        _ = host_exited => {}
        _ = serve_ipc(listener, node.clone()) => {}
        _ = node.serve(tx) => {}
        _ = serve_sockets(rx, service_socket, mesh_control_socket) => {}
    }
    node.shutdown().await;
    remove_owned_socket(&socket, socket_identity);
    Ok(())
}

fn file_identity(path: &Path) -> std::io::Result<(u64, u64)> {
    let metadata = std::fs::symlink_metadata(path)?;
    Ok((metadata.dev(), metadata.ino()))
}

/// A replacement worker may already have bound the same path; remove only the
/// socket that this worker bound.
fn remove_owned_socket(path: &Path, identity: (u64, u64)) {
    if file_identity(path).is_ok_and(|current| current == identity) {
        let _ = std::fs::remove_file(path);
    }
}

/// The Host holds the write end of stdin. EOF means that the Host exited, so
/// the worker must not outlive it.
fn watch_lifeline() -> tokio::sync::oneshot::Receiver<()> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut byte = [0u8; 1];
        loop {
            match stdin.read(&mut byte) {
                Ok(0) => break,
                Err(error) if error.kind() != std::io::ErrorKind::Interrupted => break,
                _ => {}
            }
        }
        let _ = sender.send(());
    });
    receiver
}

fn socket_arguments() -> Result<(PathBuf, PathBuf, PathBuf)> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [
            control_flag,
            control,
            service_flag,
            service,
            mesh_flag,
            mesh,
        ] if control_flag == "--socket"
            && service_flag == "--service-socket"
            && mesh_flag == "--mesh-control-socket" =>
        {
            let control = PathBuf::from(control);
            let service = PathBuf::from(service);
            let mesh = PathBuf::from(mesh);
            ensure_absolute(&control, "control socket")?;
            ensure_absolute(&service, "service socket")?;
            ensure_absolute(&mesh, "mesh control socket")?;
            Ok((control, service, mesh))
        }
        _ => anyhow::bail!(
            "usage: agent-vmm-data --socket <absolute-path> --service-socket <absolute-path> --mesh-control-socket <absolute-path>"
        ),
    }
}

fn ensure_absolute(path: &std::path::Path, label: &str) -> Result<()> {
    anyhow::ensure!(path.is_absolute(), "{label} must be absolute");
    Ok(())
}

async fn serve_sockets(mut rx: mpsc::Receiver<Tunnel>, service: PathBuf, mesh: PathBuf) {
    while let Some(tunnel) = rx.recv().await {
        let path = match tunnel.channel {
            Channel::Service => &service,
            Channel::MeshControl => &mesh,
        }
        .clone();
        tokio::spawn(async move {
            if let Ok(socket) = UnixStream::connect(path).await {
                let _ = tunnel.bridge(socket).await;
            }
        });
    }
}

async fn serve_ipc(listener: UnixListener, node: DataNode) {
    while let Ok((stream, _)) = listener.accept().await {
        let node = node.clone();
        tokio::spawn(async move {
            let _ = handle_ipc(stream, node).await;
        });
    }
}

async fn handle_ipc(mut stream: UnixStream, node: DataNode) -> Result<()> {
    let mut input = Zeroizing::new(Vec::with_capacity(1024));
    {
        let mut reader = BufReader::new(&mut stream).take(4097);
        reader.read_until(b'\n', &mut input).await?;
    }
    anyhow::ensure!(
        input.ends_with(b"\n"),
        "local command must end with newline"
    );
    let (channel, addr) = match serde_json::from_slice::<Command>(&input)? {
        Command::OpenServiceTunnel { endpoint_addr } => (Channel::Service, endpoint_addr),
        Command::OpenMeshControlTunnel { endpoint_addr } => (Channel::MeshControl, endpoint_addr),
        command => {
            let response = node.command(command).await?;
            stream.write_all(&response).await?;
            stream.write_all(b"\n").await?;
            return Ok(());
        }
    };
    let tunnel = node.open(channel, addr).await?;
    stream.write_all(b"{\"ready\":true}\n").await?;
    tunnel.bridge(stream).await
}

#[cfg(test)]
mod tests {
    use super::{file_identity, remove_owned_socket};

    #[test]
    fn socket_removal_spares_a_replacement_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.sock");
        let previous = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let previous_identity = file_identity(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let replacement = std::os::unix::net::UnixListener::bind(&path).unwrap();
        remove_owned_socket(&path, previous_identity);
        assert!(path.exists());
        remove_owned_socket(&path, file_identity(&path).unwrap());
        assert!(!path.exists());
        drop((previous, replacement));
    }
}
