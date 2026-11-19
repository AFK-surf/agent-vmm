use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::Path,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::Semaphore,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        RootCertStore, ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        server::WebPkiClientVerifier,
    },
};
use vmm_dom0::{
    local::{Dom0Config, Result, TlsConfig, private_dir},
    remote::serve_connection,
};

fn acceptor(c: &TlsConfig) -> Result<TlsAcceptor> {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(&c.client_ca)? {
        roots.add(cert?)?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
    let chain = CertificateDer::pem_file_iter(&c.certificate)?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let tls = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, PrivateKeyDer::from_pem_file(&c.key)?)?;
    Ok(TlsAcceptor::from(Arc::new(tls)))
}
pub async fn run() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args
        .next()
        .ok_or("usage: l4h-dom0d CONFIG.json [--init-pool]")?;
    let init = match args.next() {
        None => false,
        Some(flag) if flag == "--init-pool" && args.next().is_none() => true,
        _ => return Err("usage: l4h-dom0d CONFIG.json [--init-pool]".into()),
    };
    let mut config = Dom0Config::read(Path::new(&path))?;
    let socket = config.socket.take();
    if socket.is_none() && config.vsock_control_port.is_none() {
        return Err("configure a Unix or vsock control listener".into());
    }
    let control = config
        .vsock_control_port
        .map(crate::vsock::Listener::bind)
        .transpose()?;
    let relay = config
        .vsock_relay_port
        .map(crate::vsock::Listener::bind)
        .transpose()?;
    let tls_config = config.tls.take();
    let mut handle = config.open(init).await?;
    let backend = handle.backend.clone();
    let tls = match tls_config {
        Some(t) => Some((TcpListener::bind(t.listen).await?, acceptor(&t)?)),
        None => None,
    };
    let unix = match socket {
        Some(path) => Some(bind_unix(&path).await?),
        None => None,
    };
    let control_server = async {
        let permits = Arc::new(Semaphore::new(128));
        loop {
            let permit = permits.clone().acquire_owned().await?;
            let b = backend.clone();
            tokio::select! {
                s=optional(unix.as_ref().map(UnixListener::accept))=>{let (s,_)=s?;tokio::spawn(async move{let _permit=permit;let _=serve_connection(s,b.as_ref()).await;});},
                s=optional(control.as_ref().map(crate::vsock::Listener::accept))=>{
                    let stream=s?;
                    tokio::spawn(async move {let _permit=permit;let _=serve_connection(stream,b.as_ref()).await;});
                }
                s=async{match &tls{Some((tcp,a))=>tcp.accept().await.map(|(s,_)|(s,a.clone())),None=>std::future::pending().await}}=>{
                    let (s,a)=s?;
                    tokio::spawn(async move{
                        let _permit=permit;
                        if let Ok(Ok(s))=tokio::time::timeout(Duration::from_secs(10),a.accept(s)).await{let _=serve_connection(s,b.as_ref()).await;}
                    });
                }
            }
        }
    };
    let relay_server = async {
        let listener = match relay {
            Some(listener) => listener,
            None => return std::future::pending().await,
        };
        let permits = Arc::new(Semaphore::new(1024));
        loop {
            let permit = permits.clone().acquire_owned().await?;
            let stream = listener.accept().await?;
            tokio::spawn(async move {
                let _permit = permit;
                let _ =
                    crate::relay::serve_guest_relay(stream, tokio::net::TcpStream::connect).await;
            });
        }
    };
    tokio::select! {
        result = handle.wait() => result,
        result = control_server => result,
        result = relay_server => result,
    }
}

async fn optional<F: Future>(future: Option<F>) -> F::Output {
    match future {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

async fn bind_unix(socket: &Path) -> Result<UnixListener> {
    private_dir(socket.parent().ok_or("socket directory")?)?;
    // The exclusive backend instance lock is already held. An active listener
    // at this path belongs to a different installation.
    if let Ok(m) = std::fs::symlink_metadata(socket) {
        if !m.file_type().is_socket() {
            return Err("control socket path occupied".into());
        }
        match tokio::net::UnixStream::connect(socket).await {
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                std::fs::remove_file(socket)?
            }
            _ => return Err("control socket in use".into()),
        }
    }
    let unix = UnixListener::bind(socket)?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    Ok(unix)
}
