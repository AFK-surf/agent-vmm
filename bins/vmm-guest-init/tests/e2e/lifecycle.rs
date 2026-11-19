use std::time::Duration;

use vmm_guest_proto::rpc::{ErrorCode, MainState, PowerOffRequest, ProcessState, QuiesceRequest};

use crate::support::{Guest, eventually, exec, process, refused};

/// Children of this process that exited and were not collected.
fn zombie_children() -> Vec<String> {
    let me = std::process::id().to_string();
    let mut zombies = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // The fields after the parenthesized command are the state and the parent.
        let Some((_, rest)) = stat.rsplit_once(") ") else {
            continue;
        };
        let mut fields = rest.split(' ');
        if fields.next() == Some("Z") && fields.next() == Some(me.as_str()) {
            zombies.push(stat);
        }
    }
    zombies
}

#[test]
fn orphaned_processes_are_reaped() {
    // Orphans go to this process, as they go to PID 1 in the Guest.
    // SAFETY: `prctl` with this option takes integer arguments only.
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) },
        0
    );
    let guest = Guest::start(None, 4096);
    let before = guest.client.health([0; 16]).unwrap().orphans_reaped;

    // Each shell exits at once and leaves a child behind that exits later.
    for _ in 0..5 {
        let output = guest
            .client
            .exec(exec("sleep 0.3 >/dev/null 2>&1 & exit 0"), b"")
            .unwrap();
        assert_eq!(output.result.exit.code, 0);
    }
    eventually("the orphans to be reaped", || {
        guest.client.health([0; 16]).unwrap().orphans_reaped >= before + 5
    });
    eventually("no zombie to remain", || zombie_children().is_empty());
}

#[test]
fn quiesce_closes_admission_only_when_no_exec_runs() {
    let guest = Guest::start(None, 4096);
    let quiesce = |cancel: bool, minimum_idle_ms: u64| {
        guest
            .client
            .quiesce(QuiesceRequest {
                cancel,
                minimum_idle_ms,
            })
            .unwrap()
    };

    let mut session = guest
        .client
        .exec_stream(exec("echo busy; sleep 300"))
        .unwrap();
    session.next_event().unwrap();
    let busy = quiesce(false, 0);
    assert!(!busy.quiesced);
    assert_eq!((busy.active_execs, busy.idle_ms), (1, 0));
    drop(session);
    eventually("the exec to end", || quiesce(true, 0).active_execs == 0);

    // The Guest was idle for less than an hour, so this request changes nothing.
    assert!(!quiesce(false, 3_600_000).quiesced);
    guest
        .client
        .process_start(process("background", "sleep 300"))
        .unwrap();
    let idle = quiesce(false, 0);
    assert!(idle.quiesced);
    assert_eq!((idle.active_execs, idle.running_processes), (0, 1));
    assert!(guest.client.health([0; 16]).unwrap().quiesced);
    assert_eq!(
        refused(guest.client.exec(exec("true"), b"")),
        ErrorCode::Unavailable
    );
    let start = guest.client.process_start(process("refused", "true"));
    assert_eq!(refused(start), ErrorCode::Unavailable);

    assert!(!quiesce(true, 0).quiesced);
    assert_eq!(
        guest
            .client
            .exec(exec("true"), b"")
            .unwrap()
            .result
            .exit
            .code,
        0
    );
    guest
        .client
        .power_off(PowerOffRequest { grace_ms: 100 })
        .unwrap();
}

#[test]
fn power_off_stops_the_workload_before_the_machine() {
    let guest = Guest::start(Some("trap '' TERM; while :; do sleep 1; done"), 4096);
    let polite = guest
        .client
        .process_start(process("polite", "sleep 300"))
        .unwrap()
        .process;
    assert!(matches!(
        guest.client.wait_main_exit(0).unwrap(),
        MainState::Running { .. }
    ));

    let reply = guest
        .client
        .power_off(PowerOffRequest { grace_ms: 300 })
        .unwrap();
    assert_eq!(reply.trims.len(), 1);
    assert_eq!(reply.trims[0].trimmed_bytes, Some(4096));
    assert!(reply.clean_unmount);
    // The machine operations ran in order, and only after each process ended.
    eventually("the machine to power off", || {
        *guest.machine.0.lock().unwrap() == ["prepare", "power_off"]
    });
    let MainState::Exited { exit, .. } = guest.client.wait_main_exit(0).unwrap() else {
        panic!("the main process survived");
    };
    assert_eq!(exit.signal, Some(9), "the main process ignored SIGTERM");
    let listed = guest.client.process_list().unwrap();
    assert_eq!(listed[0].process_id, polite.process_id);
    let ProcessState::Exited(exit) = listed[0].state else {
        panic!("the process survived")
    };
    assert_eq!(exit.signal, Some(15));

    assert_eq!(
        refused(guest.client.exec(exec("true"), b"")),
        ErrorCode::Unavailable
    );
    let again = guest.client.power_off(PowerOffRequest { grace_ms: 0 });
    assert_eq!(refused(again), ErrorCode::FailedPrecondition);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(guest.machine.0.lock().unwrap().len(), 2);
}
