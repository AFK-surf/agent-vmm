#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::{
        fs::{DirBuilderExt, PermissionsExt},
        net::UnixStream,
    },
    process::{Child, Command},
    time::{Duration, Instant},
};

struct Host(Child);
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn systemd_command_serves_offline_and_loads_the_instance_config() {
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let root = dir.path().join("state");
    fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
    let socket = root.join("operator.sock");
    let instance = root.join("instance");

    let command = || {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_agent-vmm"));
        cmd.arg("host");
        for (flag, name) in [
            ("--bundle", "guest"),
            ("--instance", "instance"),
            ("--serial-log", "logs/serial.log"),
            ("--operator-socket", "operator.sock"),
            ("--remote-compute-socket", "remote-compute.sock"),
            ("--database", "host.sqlite"),
            ("--session-directory", "sessions"),
        ] {
            cmd.arg(flag).arg(root.join(name));
        }
        cmd.arg("--disable-personal-mesh-pairing");
        cmd
    };
    let mut host = Host(command().spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    while UnixStream::connect(&socket).is_err() {
        assert!(host.0.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    rustix::process::kill_process(
        rustix::process::Pid::from_child(&host.0),
        rustix::process::Signal::TERM,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = host.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        fs::metadata(&instance).unwrap().permissions().mode() & 0o777,
        0o700
    );
    fs::write(instance.join("dom0.json"), b"{").unwrap();
    let out = command().output().unwrap();
    assert!(!out.status.success());
    assert!(!socket.exists());
}
