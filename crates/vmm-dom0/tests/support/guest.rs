use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, Shutdown, SocketAddrV4, TcpListener as StdListener},
    os::fd::OwnedFd,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpSocket, TcpStream},
};
use vmm_guest_init::{
    health::Sampler,
    process_manager::ProcessManager,
    rpc_server::Server,
    runtime::{Runtime, System},
    spawn::SpawnContext,
    workspace_server::WorkspaceServer,
};
use vmm_guest_proto::{boot::BootConfig, rpc::PowerOffReply};
use vmm_l4hctl::{HypervisorCtl, MockHypervisorCtl};

pub const SLOT: Ipv4Addr = Ipv4Addr::new(127, 77, 0, 2);
pub type Control = Arc<Mutex<MockHypervisorCtl>>;

pub async fn connect_from_slot(addr: SocketAddrV4) -> TcpStream {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(SocketAddrV4::new(SLOT, 0).into()).unwrap();
    socket.connect(addr.into()).await.unwrap()
}
async fn fetch(port: u16) -> (u16, Vec<u8>) {
    let mut stream = connect_from_slot(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await;
    stream
        .write_all(b"GET /v1/boot HTTP/1.1\r\nHost: gateway\r\n\r\n")
        .await
        .unwrap();
    let mut reply = Vec::new();
    stream.read_to_end(&mut reply).await.unwrap();
    let end = reply.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let code = std::str::from_utf8(&reply[9..12]).unwrap().parse().unwrap();
    (code, reply.split_off(end))
}

#[derive(Default)]
pub struct Observed {
    pub boots: Vec<(String, bool, bool, bool, Option<u64>)>,
    pub prepared: Vec<u64>,
}
struct Machine {
    id: u64,
    ctl: Control,
    observed: Arc<Mutex<Observed>>,
}
impl System for Machine {
    fn prepare_power_off(&self) -> PowerOffReply {
        assert!(
            self.ctl
                .lock()
                .unwrap()
                .list_vms()
                .unwrap()
                .iter()
                .any(|v| v.id == self.id)
        );
        self.observed.lock().unwrap().prepared.push(self.id);
        PowerOffReply {
            trims: vec![],
            clean_unmount: true,
        }
    }
    fn power_off(&self) {}
}
struct Serving {
    id: u64,
    listener: std::net::TcpStream,
    thread: Option<std::thread::JoinHandle<std::io::Error>>,
}
impl Drop for Serving {
    fn drop(&mut self) {
        self.listener.shutdown(Shutdown::Both).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}
/// A booted l4h VM is represented by the real Guest RPC server. Only the
/// block mounts, PID 1 poweroff, and hypervisor are replaced by test fixtures.
pub async fn guests(ctl: Control, port: u16, root: PathBuf, observed: Arc<Mutex<Observed>>) {
    let mut serving: Option<Serving> = None;
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let inventory = ctl.lock().unwrap().list_vms().unwrap();
        assert!(inventory.len() <= 1);
        let vm = inventory.first().map(|v| v.id);
        if serving.as_ref().map(|s| s.id) == vm {
            continue;
        }
        drop(serving.take());
        let Some(id) = vm else { continue };
        let (status, body) = fetch(port).await;
        assert_eq!(status, 200);
        let config = BootConfig::from_json(&body).unwrap();
        assert_eq!(fetch(port).await.0, 404);
        assert_eq!(config.network.dns, [Ipv4Addr::LOCALHOST]);
        let volume = &config.volumes[0];
        {
            let hv = ctl.lock().unwrap();
            let disks = hv.volumes(id).unwrap();
            assert!(disks[0].read_only);
            assert_eq!(disks.last().unwrap().read_only, volume.read_only);
            if config.root.overlay.is_some() {
                assert!(!disks[1].read_only);
            }
        }
        let overlay = config.root.overlay.as_ref();
        observed.lock().unwrap().boots.push((
            volume.name.clone(),
            volume.disk.format,
            volume.read_only,
            overlay.is_some_and(|o| o.format),
            config.workload.pids_max,
        ));
        let volume_path = root.join(&volume.name);
        std::fs::create_dir_all(&volume_path).unwrap();
        let context = SpawnContext {
            etc_dir: root.clone(),
            cgroup_procs: None,
            base_env: vec![],
            default_workdir: root.display().to_string(),
            default_user: None,
            grant_subid_caps: false,
        };
        let paths = BTreeMap::from([(volume.name.clone(), volume_path)]);
        let sampler = Sampler {
            overlay_path: overlay.map(|_| root.clone()),
            volume_paths: paths.clone().into_iter().collect(),
            ..Default::default()
        };
        let runtime = Runtime::new(
            config.instance_id,
            config.rpc_auth_token,
            ProcessManager::new(context, 4096),
            WorkspaceServer::new(None, paths),
            sampler,
            Box::new(Machine {
                id,
                ctl: ctl.clone(),
                observed: observed.clone(),
            }),
        );
        let listener = StdListener::bind((SLOT, config.rpc_port)).unwrap();
        let peer = OwnedFd::from(listener.try_clone().unwrap()).into();
        let server = Server::new(Arc::new(runtime), None);
        serving = Some(Serving {
            id,
            listener: peer,
            thread: Some(std::thread::spawn(move || server.serve(listener))),
        });
    }
}
