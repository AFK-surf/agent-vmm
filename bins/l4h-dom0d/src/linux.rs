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
    let socket = config
        .socket
        .take()
        .ok_or("l4h-dom0d requires a control socket in CONFIG.json")?;
    private_dir(socket.parent().ok_or("socket directory")?)?;
    let tls_config = config.tls.take();
    let mut handle = config.open(init).await?;
    let backend = handle.backend.clone();
    let tls = match tls_config {
        Some(t) => Some((TcpListener::bind(t.listen).await?, acceptor(&t)?)),
        None => None,
    };
    // The exclusive backend instance lock is already held. An active listener
    // at this path belongs to a different installation.
    if let Ok(m) = std::fs::symlink_metadata(&socket) {
        if !m.file_type().is_socket() {
            return Err("control socket path occupied".into());
        }
        match tokio::net::UnixStream::connect(&socket).await {
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                std::fs::remove_file(&socket)?
            }
            _ => return Err("control socket in use".into()),
        }
    }
    let unix = UnixListener::bind(&socket)?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))?;
    let server = async {
        let permits = Arc::new(Semaphore::new(128));
        loop {
            let permit = permits.clone().acquire_owned().await?;
            let b = backend.clone();
            tokio::select! {
                s=unix.accept()=>{let (s,_)=s?;tokio::spawn(async move{let _permit=permit;let _=serve_connection(s,b.as_ref()).await;});},
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
    tokio::select! {r=handle.wait()=>r,r=server=>r}
}
