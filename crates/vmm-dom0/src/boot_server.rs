use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
    time::timeout,
};
use vmm_core::{Error, Result};
use vmm_guest_proto::boot::{BootConfig, MAX_BOOT_CONFIG_BYTES};
use zeroize::Zeroizing;

struct Entry {
    instance: String,
    before_send: Box<dyn Fn() -> Result<()> + Send + Sync>,
    body: Mutex<Option<Zeroizing<Vec<u8>>>>,
}

#[derive(Clone, Default)]
pub struct BootServer {
    entries: Arc<Mutex<HashMap<Ipv4Addr, Arc<Entry>>>>,
}

impl BootServer {
    pub async fn register(&self, config: &BootConfig) -> Result<()> {
        self.register_with(config, || Ok(())).await
    }

    pub(crate) async fn register_with(
        &self,
        config: &BootConfig,
        before_send: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) -> Result<()> {
        config
            .validate()
            .map_err(|_| Error::Invalid("boot configuration"))?;
        // Use fixed storage so an internal Vec growth cannot leave an old key
        // or RPC token in an allocation that will never be zeroized.
        let mut body = Zeroizing::new(vec![0; MAX_BOOT_CONFIG_BYTES]);
        let mut cursor = io::Cursor::new(body.as_mut_slice());
        serde_json::to_writer(&mut cursor, config)
            .map_err(|_| Error::Invalid("boot configuration size"))?;
        let len = cursor.position() as usize;
        body.truncate(len);
        let mut entries = self.entries.lock().await;
        let std::collections::hash_map::Entry::Vacant(slot) = entries.entry(config.network.address)
        else {
            return Err(Error::Conflict("boot slot occupied"));
        };
        slot.insert(Arc::new(Entry {
            before_send: Box::new(before_send),
            instance: config.instance_id.clone(),
            body: Mutex::new(Some(body)),
        }));
        Ok(())
    }

    pub async fn cancel(&self, ip: Ipv4Addr, instance: &str) {
        let mut entries = self.entries.lock().await;
        if entries.get(&ip).is_some_and(|e| e.instance == instance) {
            let entry = entries.remove(&ip).expect("entry checked under lock");
            entry.body.lock().await.take();
        }
    }

    pub async fn serve(&self, listener: TcpListener) -> io::Result<()> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                connection = listener.accept(), if connections.len() < 128 => {
                    let (stream, peer) = connection?;
                    if let IpAddr::V4(ip) = peer.ip() {
                        let server = self.clone();
                        connections.spawn(async move {
                            let _ = timeout(Duration::from_secs(5), server.handle(stream, ip)).await;
                        });
                    }
                }
            }
        }
    }

    async fn handle(&self, mut stream: TcpStream, ip: Ipv4Addr) -> io::Result<()> {
        let mut request = [0; 8192];
        let mut len = 0;
        while !request[..len].windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut request[len..]).await?;
            if n == 0 {
                return Ok(());
            }
            len += n;
        }
        let line = request[..len].split(|&b| b == b'\n').next();
        let entry = match line {
            Some(b"GET /v1/boot HTTP/1.1\r" | b"GET /v1/boot HTTP/1.0\r") => {
                self.entries.lock().await.get(&ip).cloned()
            }
            _ => None,
        };
        if let Some(entry) = entry {
            let mut body = entry.body.lock().await;
            if let Some(data) = body.as_ref() {
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
                    data.len()
                );
                (entry.before_send)().map_err(io::Error::other)?;
                stream.write_all(header.as_bytes()).await?;
                stream.write_all(data).await?;
                body.take();
                drop(body);
                let mut entries = self.entries.lock().await;
                if entries.get(&ip).is_some_and(|e| Arc::ptr_eq(e, &entry)) {
                    entries.remove(&ip);
                }
                return Ok(());
            }
        }
        stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
    }
}
