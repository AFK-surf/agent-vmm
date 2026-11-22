//! A Guest runtime on a temporary directory, served on a loopback port.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempfile::TempDir;
use vmm_guest_init::health::Sampler;
use vmm_guest_init::process_manager::ProcessManager;
use vmm_guest_init::rpc_server::Server;
use vmm_guest_init::runtime::{Runtime, System};
use vmm_guest_init::spawn::SpawnContext;
use vmm_guest_init::workspace_server::WorkspaceServer;
use vmm_guest_proto::Secret;
use vmm_guest_proto::client::{Client, ClientError};
use vmm_guest_proto::rpc::{
    CommandSpec, ErrorCode, ExecRequest, PowerOffReply, ProcessStartRequest, TrimReport,
    WorkspaceTarget,
};

pub const TOKEN: &str = "test-token-0123456789abcdef0123456789abcdef";
pub const INSTANCE: &str = "vm-test";

/// Records the machine operations of `PowerOff` in call order.
#[derive(Clone, Default)]
pub struct Recorder(pub Arc<Mutex<Vec<&'static str>>>);

impl System for Recorder {
    fn prepare_power_off(&self) -> PowerOffReply {
        self.0.lock().unwrap().push("prepare");
        PowerOffReply {
            trims: vec![TrimReport {
                mount_path: "/".into(),
                trimmed_bytes: Some(4096),
                error: None,
            }],
            clean_unmount: true,
        }
    }

    fn power_off(&self) {
        self.0.lock().unwrap().push("power_off");
    }
}

pub struct Guest {
    pub client: Client,
    pub address: SocketAddr,
    pub machine: Recorder,
    dir: TempDir,
}

impl Guest {
    /// Starts a Guest in shell-idle mode, or with the shell script `main` as its main process.
    pub fn start(main: Option<&str>, log_limit_bytes: usize) -> Self {
        Self::start_argv(main.map(|script| shell(script).argv), log_limit_bytes)
    }

    pub fn start_argv(main: Option<Vec<String>>, log_limit_bytes: usize) -> Self {
        Self::start_with_cgroup(main, log_limit_bytes, None)
    }

    pub fn start_with_cgroup(
        main: Option<Vec<String>>,
        log_limit_bytes: usize,
        cgroup_procs: Option<CString>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        for name in ["workspace", "volume", "etc", "outside"] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
        }
        let context = SpawnContext {
            etc_dir: dir.path().join("etc"),
            cgroup_procs,
            base_env: vec![
                format!("PATH={}", std::env::var("PATH").unwrap()),
                "GUEST_BASE=base".into(),
            ],
            default_workdir: dir.path().to_str().unwrap().into(),
            default_user: None,
            grant_subid_caps: false,
        };
        let processes = ProcessManager::new(context, log_limit_bytes);
        if let Some(argv) = main {
            processes.start_main(argv);
        }
        let volumes = BTreeMap::from([("data".to_owned(), dir.path().join("volume"))]);
        let machine = Recorder::default();
        let runtime = Arc::new(Runtime::new(
            INSTANCE.into(),
            Secret::new(TOKEN),
            processes,
            WorkspaceServer::new(Some(dir.path().join("workspace")), volumes),
            Sampler::default(),
            Box::new(machine.clone()),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = Server::new(runtime, None);
        std::thread::spawn(move || server.serve(listener));
        Self {
            client: Client::new(address, Secret::new(TOKEN)),
            address,
            machine,
            dir,
        }
    }

    pub fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }
}

pub fn shell(script: &str) -> CommandSpec {
    CommandSpec {
        argv: vec!["sh".into(), "-c".into(), script.into()],
        env: Vec::new(),
        workdir: None,
        user: None,
    }
}

pub fn exec(script: &str) -> ExecRequest {
    ExecRequest {
        command: shell(script),
        stdin: false,
        tty: None,
        timeout_ms: 60_000,
        max_output_bytes: 0,
    }
}

pub fn process(request_id: &str, script: &str) -> ProcessStartRequest {
    ProcessStartRequest {
        request_id: request_id.into(),
        command: shell(script),
        output_limit_bytes: 0,
    }
}

pub fn target(path: &str) -> WorkspaceTarget {
    WorkspaceTarget {
        volume: None,
        path: path.into(),
    }
}

/// The error code of a call that the Guest must refuse.
pub fn refused<T: std::fmt::Debug>(result: Result<T, ClientError>) -> ErrorCode {
    match result {
        Err(ClientError::Rpc(error)) => error.code,
        other => panic!("expected an RPC error, got {other:?}"),
    }
}

/// Waits for a condition that another thread or process establishes.
pub fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
