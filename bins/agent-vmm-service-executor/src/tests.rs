use super::*;
use crate::system::Stat;
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::io;

const EXECUTOR: &str = "/Users/worker/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/Helpers/agent-vmm-service-executor";

#[derive(Clone)]
struct Entry {
    uid: u32,
    mode: u32,
    symlink: bool,
    regular: bool,
    data: String,
}

/// An in-memory machine: files with owners and modes, one account, and launchd.
struct Machine {
    euid: u32,
    executable: &'static str,
    files: RefCell<BTreeMap<PathBuf, Entry>>,
    account: Owner,
    /// The loaded jobs and the process of each.
    jobs: RefCell<BTreeMap<String, i32>>,
    holders: RefCell<BTreeMap<PathBuf, Vec<i32>>>,
    /// After a stop of the carrier, the virtualization service holds the pool for this number of pauses.
    late_release: Cell<Option<u32>>,
    calls: RefCell<Vec<String>>,
}

/// The process of the system virtualization service that holds the disks of a VM.
const VIRTUALIZATION_PID: i32 = 88;

fn worker() -> Owner {
    Owner { user: "worker".into(), group: "staff".into(), uid: 501, gid: 20, home: "/Users/worker".into() }
}

impl Machine {
    /// A machine where root runs a protected executor and the worker owns an installed Host.
    fn new() -> Self {
        let machine = Self { euid: 0, executable: "/usr/local/libexec/agent-vmm-service-executor", files: RefCell::default(), account: worker(), jobs: RefCell::default(), holders: RefCell::default(), late_release: Cell::new(None), calls: RefCell::default() };
        for directory in ["/", "/usr", "/usr/local", "/usr/local/libexec", "/Library", "/Library/Application Support", "/Library/LaunchDaemons", "/Users"] {
            machine.add(directory, 0, 0o755, false);
        }
        machine.add(machine.executable, 0, 0o755, true);
        let app = worker().home.join(APP);
        for directory in app.join("Contents/MacOS").ancestors().take_while(|path| *path != Path::new("/Users")) {
            machine.add(directory.to_str().unwrap(), 501, 0o755, false);
        }
        machine.add(app.join("Contents/Helpers").to_str().unwrap(), 501, 0o755, false);
        machine.add(app.join("Contents/MacOS/agent-vmm-host").to_str().unwrap(), 501, 0o755, true);
        machine.add(app.join("Contents/Helpers/agent-vmm-carrier").to_str().unwrap(), 501, 0o755, true);
        machine
    }

    fn add(&self, path: &str, uid: u32, mode: u32, regular: bool) {
        self.files.borrow_mut().insert(path.into(), Entry { uid, mode, symlink: false, regular, data: String::new() });
    }

    fn change(&self, path: &Path, change: impl FnOnce(&mut Entry)) {
        change(self.files.borrow_mut().get_mut(path).unwrap());
    }

    fn run(&self, arguments: &[&str]) -> Outcome {
        run(self, &arguments.iter().map(|argument| argument.to_string()).collect::<Vec<_>>())
    }

    fn configured() -> Self {
        let machine = Self::new();
        machine.run(&["configure", "--service-user", "worker"]).unwrap();
        machine
    }

    fn launches(&self) -> Vec<String> {
        self.calls.take()
    }
}

impl System for Machine {
    fn euid(&self) -> u32 {
        self.euid
    }

    fn executable(&self) -> io::Result<PathBuf> {
        Ok(self.executable.into())
    }

    fn lstat(&self, path: &Path) -> io::Result<Stat> {
        let files = self.files.borrow();
        let entry = files.get(path).ok_or(io::ErrorKind::NotFound)?;
        Ok(Stat { uid: entry.uid, mode: entry.mode, symlink: entry.symlink, regular: entry.regular })
    }

    fn read(&self, path: &Path) -> io::Result<String> {
        Ok(self.files.borrow().get(path).ok_or(io::ErrorKind::NotFound)?.data.clone())
    }

    fn replace_file(&self, path: &Path, data: &[u8], mode: u32) -> io::Result<()> {
        let entry = Entry { uid: 0, mode, symlink: false, regular: true, data: String::from_utf8(data.to_vec()).unwrap() };
        self.files.borrow_mut().insert(path.into(), entry);
        Ok(())
    }

    fn create_dir(&self, path: &Path, mode: u32) -> io::Result<()> {
        self.files.borrow_mut().entry(path.into()).or_insert(Entry { uid: 0, mode, symlink: false, regular: false, data: String::new() });
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.files.borrow_mut().remove(path);
        Ok(())
    }

    fn account(&self, name: &str) -> Option<Owner> {
        (name == self.account.user).then(|| self.account.clone())
    }

    fn launchctl(&self, arguments: &[&str]) -> Launch {
        self.calls.borrow_mut().push(arguments.join(" "));
        let mut jobs = self.jobs.borrow_mut();
        match arguments {
            ["print", target] => jobs.get(*target).map_or(Launch::NotFound, |pid| Launch::Done(format!("state = running\n\tpid = {pid}\n"))),
            ["bootstrap", "system", plist] => {
                let label = Path::new(plist).file_stem().unwrap().to_str().unwrap();
                jobs.insert(format!("system/{label}"), 4242);
                Launch::Done(String::new())
            }
            ["bootout", target] => {
                if target.ends_with("carrier") && self.late_release.get().is_some() {
                    self.holders.borrow_mut().insert(worker().home.join(STATE_DISKS[0]), vec![VIRTUALIZATION_PID]);
                }
                jobs.remove(*target).map_or(Launch::NotFound, |_| Launch::Done(String::new()))
            }
            ["enable" | "kickstart", _] => Launch::Done(String::new()),
            _ => Launch::Failed("unexpected launchctl call".into()),
        }
    }

    fn process_running(&self, pid: i32) -> bool {
        self.jobs.borrow().values().any(|job| *job == pid)
    }

    fn open_owners(&self, path: &Path) -> io::Result<Vec<i32>> {
        Ok(self.holders.borrow().get(path).cloned().unwrap_or_default())
    }

    fn prepare_logs(&self, _: u32) -> io::Result<()> {
        Ok(())
    }

    fn pause(&self) {
        match self.late_release.get() {
            Some(0) => self.holders.borrow_mut().clear(),
            Some(pauses) => self.late_release.set(Some(pauses - 1)),
            None => {}
        }
    }
}

#[test]
fn only_root_runs_a_protected_executor() {
    let user = Machine { euid: 501, ..Machine::new() };
    assert_eq!(user.run(&["status"]).unwrap_err(), "administrator access is required");
    // An executor inside a bundle of the service user could be replaced by that user.
    let replaceable = Machine { executable: EXECUTOR, ..Machine::new() };
    replaceable.add(EXECUTOR, 501, 0o755, true);
    assert!(replaceable.run(&["status"]).unwrap_err().contains("is replaceable"));
    let writable = Machine::new();
    writable.change(Path::new("/usr/local/libexec"), |entry| entry.mode = 0o775);
    assert!(writable.run(&["status"]).unwrap_err().contains("is replaceable"));
}

#[test]
fn every_value_comes_from_the_owner_file_and_fixed_constants() {
    let machine = Machine::configured();
    for arguments in [&["install", "/tmp/evil.plist"][..], &["start", "--label", "other"], &["stop", "host"], &["status", "carrier", "extra"], &["run"], &[]] {
        assert_eq!(machine.run(arguments).unwrap_err(), USAGE, "{arguments:?}");
    }
    assert!(machine.launches().is_empty(), "a refused action reaches launchd with nothing");
    assert_eq!(Machine::configured().run(&["configure", "--service-user", "root"]).unwrap_err(), "service user does not exist");
    assert_eq!(Machine::new().run(&["status"]).unwrap_err(), format!("{OWNER_FILE}: entity not found"));
}

#[test]
fn configure_records_the_account_and_later_actions_require_the_same_account() {
    let machine = Machine::configured();
    let saved = machine.files.borrow()[Path::new(OWNER_FILE)].clone();
    assert_eq!((saved.mode, saved.uid), (0o600, 0));
    assert_eq!(Owner::decode(&saved.data), Some(worker()));

    let renamed = Machine { account: Owner { uid: 502, ..worker() }, ..Machine::new() };
    renamed.files.borrow_mut().extend(machine.files.borrow().clone());
    assert_eq!(renamed.run(&["status"]).unwrap_err(), "service owner config no longer matches the OS account");

    machine.change(Path::new(OWNER_FILE), |entry| entry.mode = 0o666);
    assert!(machine.run(&["status"]).unwrap_err().contains("is replaceable"));

    let shared = Machine::new();
    shared.change(Path::new("/Users/worker"), |entry| entry.mode = 0o777);
    assert!(shared.run(&["configure", "--service-user", "worker"]).unwrap_err().contains("controlled by another identity"));
}

#[test]
fn install_loads_the_fixed_definition_of_each_job() {
    let machine = Machine::configured();
    machine.run(&["install"]).unwrap();
    let host = "system/com.afk-surf.agent-vmm-host";
    assert_eq!(machine.launches(), [format!("print {host}"), format!("enable {host}"), "bootstrap system /Library/LaunchDaemons/com.afk-surf.agent-vmm-host.plist".to_string()]);
    let definition = machine.files.borrow()[Path::new("/Library/LaunchDaemons/com.afk-surf.agent-vmm-host.plist")].clone();
    assert_eq!(definition.mode, 0o644);
    for expected in ["<string>/Users/worker/Library/Application Support/Agent VMM Host/current/Agent VMM Host.app/Contents/MacOS/agent-vmm-host</string>", "<key>UserName</key>\n  <string>worker</string>", "<string>--disable-personal-mesh-pairing</string>", "<string>Background</string>"] {
        assert!(definition.data.contains(expected), "{expected}");
    }
    assert_eq!(machine.run(&["install"]).unwrap_err(), "fixed job must be stopped before its definition is replaced");

    machine.run(&["install", "carrier"]).unwrap();
    let carrier = machine.files.borrow()[Path::new("/Library/LaunchDaemons/com.afk-surf.agent-vmm-carrier.plist")].clone();
    assert!(carrier.data.contains("/Contents/Helpers/agent-vmm-carrier</string>") && carrier.data.contains("<string>worker</string>"));
    machine.launches();
    machine.run(&["start"]).unwrap();
    assert_eq!(machine.launches(), [format!("print {host}")], "a running job is not started again");
    machine.run(&["status", "carrier"]).unwrap();
}

#[test]
fn a_program_that_another_identity_controls_is_not_installed() {
    let program = worker().home.join(APP).join("Contents/MacOS/agent-vmm-host");
    type Change = fn(&mut Entry);
    let changes: [(&Path, Change); 4] = [
        (program.as_path(), |entry| entry.mode = 0o775),
        (program.as_path(), |entry| entry.uid = 0),
        (program.parent().unwrap(), |entry| entry.uid = 502),
        (program.parent().unwrap(), |entry| entry.symlink = true),
    ];
    for (path, change) in changes {
        let machine = Machine::configured();
        machine.change(path, change);
        assert!(machine.run(&["install"]).is_err());
        assert!(machine.jobs.borrow().is_empty());
    }
}

const CARRIER: &str = "system/com.afk-surf.agent-vmm-carrier";

/// A machine where both jobs run and the carrier process has the given identifier.
fn running(carrier_pid: i32) -> Machine {
    let machine = Machine::configured();
    machine.run(&["install", "carrier"]).unwrap();
    machine.run(&["install"]).unwrap();
    machine.jobs.borrow_mut().insert(CARRIER.into(), carrier_pid);
    machine
}

#[test]
fn a_host_stop_leaves_the_carrier_and_its_vm_running() {
    let machine = running(77);
    // The carrier and the virtualization service hold the disks of the running VM.
    for disk in STATE_DISKS {
        machine.holders.borrow_mut().insert(worker().home.join(disk), vec![77, VIRTUALIZATION_PID]);
    }
    machine.run(&["stop"]).unwrap();
    assert_eq!(machine.jobs.borrow().keys().collect::<Vec<_>>(), [CARRIER], "the Host stops and the carrier keeps the VM");
    machine.run(&["stop"]).unwrap();
}

#[test]
fn a_carrier_stop_never_detaches_a_running_vm() {
    const ATTACHED: &str = "VM state disk is still attached; complete the service-user VM stop first";
    // The holder of a disk is the virtualization service, not the carrier process.
    for (disk, holders) in [(STATE_DISKS[0], vec![VIRTUALIZATION_PID]), (STATE_DISKS[1], vec![VIRTUALIZATION_PID]), (STATE_DISKS[0], vec![77])] {
        let machine = running(77);
        machine.holders.borrow_mut().insert(worker().home.join(disk), holders);
        assert_eq!(machine.run(&["stop", "carrier"]).unwrap_err(), ATTACHED);
        assert_eq!(machine.run(&["remove", "carrier"]).unwrap_err(), ATTACHED);
        assert!(machine.jobs.borrow().contains_key(CARRIER) && !machine.launches().iter().any(|call| call.starts_with("bootout")));
    }

    // The service user stopped the VM: the carrier stops and its definition is removed.
    let machine = running(77);
    machine.run(&["remove", "carrier"]).unwrap();
    assert!(!machine.jobs.borrow().contains_key(CARRIER));
    assert!(!machine.files.borrow().contains_key(Path::new("/Library/LaunchDaemons/com.afk-surf.agent-vmm-carrier.plist")));
}

#[test]
fn a_carrier_stop_ends_when_the_state_disks_are_closed() {
    let machine = running(77);
    machine.late_release.set(Some(3));
    machine.run(&["stop", "carrier"]).unwrap();
    assert!(machine.holders.borrow().is_empty(), "the stop waited for the virtualization service");

    let machine = running(77);
    machine.late_release.set(Some(u32::MAX));
    assert_eq!(machine.run(&["stop", "carrier"]).unwrap_err(), format!("VM state disk is still open by process IDs [{VIRTUALIZATION_PID}]"));
}

#[test]
fn the_owner_file_round_trips_and_refuses_other_content() {
    let owner = Owner { user: "o\"brien\\".into(), home: "/Users/o brien".into(), ..worker() };
    assert_eq!(Owner::decode(&owner.encode()), Some(owner));
    assert_eq!(Owner::decode(" {\"uid\": 501, \"gid\": 20, \"user\": \"w\\u0041\", \"group\": \"g\", \"home\": \"\\/h\"} "), Some(Owner { user: "wA".into(), group: "g".into(), uid: 501, gid: 20, home: "/h".into() }));
    for invalid in ["", "{}", "{\"user\":\"w\",\"group\":\"g\",\"uid\":\"501\",\"gid\":20,\"home\":\"/h\"}", "{\"user\":\"w\",\"group\":\"g\",\"uid\":501,\"gid\":20,\"home\":\"/h\",\"program\":\"/x\"}", "{\"user\":\"w\",\"group\":\"g\",\"uid\":501,\"gid\":20,\"home\":\"/h\"} x"] {
        assert_eq!(Owner::decode(invalid), None, "{invalid}");
    }
}
