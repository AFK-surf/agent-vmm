//! Each export owns a listener during one VM incarnation. The TCP peer and
//! attach name must both match before the connection obtains a root fid.
mod protocol;
use std::{
    collections::BTreeMap,
    fs::File,
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
    sync::Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, watch},
    task::JoinSet,
};
use vmm_core::{Error, Result};

#[derive(Default)]
pub(crate) struct Shares(Mutex<BTreeMap<(String, String), Export>>);
struct Export {
    port: u16,
    end: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

impl Shares {
    pub async fn export(
        &self,
        instance: &str,
        name: &str,
        root: &Path,
        read_only: bool,
        source: Ipv4Addr,
        gateway: SocketAddrV4,
    ) -> Result<u16> {
        let mut map = self.0.lock().await;
        let key = (instance.to_owned(), name.to_owned());
        if let Some(export) = map.get(&key) {
            return Ok(export.port);
        }
        let root = Arc::new(File::open(root)?);
        if !root.metadata()?.is_dir() {
            return Err(Error::Invalid("share directory"));
        }
        let listener = TcpListener::bind(gateway).await?;
        let port = listener.local_addr()?.port();
        let (end, mut stopped) = watch::channel(false);
        let name = name.to_owned();
        let task = tokio::spawn(async move {
            let mut conns = JoinSet::new();
            loop {
                tokio::select! {biased;
                    _=stopped.changed()=>break,
                    Some(_)=conns.join_next(), if !conns.is_empty()=>{},
                    accepted=listener.accept(), if conns.len()<64=>{
                        let Ok((s,peer))=accepted else{break;};
                        if peer.ip()==source{
                            conns.spawn(handle(s,root.clone(),name.clone(),read_only,stopped.clone()));
                        }
                    }
                }
            }
            drop(listener);
            while conns.join_next().await.is_some() {}
        });
        map.insert(key, Export { port, end, task });
        Ok(port)
    }

    pub async fn revoke(&self, instance: &str) {
        let mut map = self.0.lock().await;
        let keys = map
            .keys()
            .filter(|(i, _)| i == instance)
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some(export) = map.remove(&key) {
                export.end.send_replace(true);
                let _ = export.task.await;
            }
        }
    }
}

async fn handle(
    mut stream: TcpStream,
    root: Arc<File>,
    name: String,
    ro: bool,
    mut stop: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let mut session = protocol::Session::new(root, name, ro);
    loop {
        let size = tokio::select! {_ = stop.changed()=>return Ok(()), n=stream.read_u32_le()=>n?};
        if !(7..=session.msize).contains(&size) {
            return Ok(());
        }
        let mut frame = vec![0; size as usize - 4];
        tokio::select! {_ = stop.changed()=>return Ok(()), bytes=stream.read_exact(&mut frame)=>{bytes?;}}
        let (next, reply) = tokio::task::spawn_blocking(move || {
            let reply = session.process(&frame);
            (session, reply)
        })
        .await
        .map_err(std::io::Error::other)?;
        session = next;
        tokio::select! {_ = stop.changed()=>return Ok(()), result=stream.write_all(&reply)=>result?}
    }
}

#[cfg(test)]
mod tests;
