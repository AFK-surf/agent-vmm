use std::time::{Duration, Instant};

use vmm_guest_proto::client::ExecEvent;
use vmm_guest_proto::rpc::{ErrorCode, ExitStatus, Signal, TtySize};

use crate::support::{Guest, eventually, exec, refused};

#[test]
fn exec_returns_output_and_exit_code() {
    let guest = Guest::start(None, 4096);
    let output = guest
        .client
        .exec(exec("echo out; echo err >&2; exit 7"), b"")
        .unwrap();
    assert_eq!(output.stdout, b"out\n");
    assert_eq!(output.stderr, b"err\n");
    assert_eq!(
        output.result.exit,
        ExitStatus {
            code: 7,
            signal: None
        }
    );
    assert!(!output.result.timed_out && !output.result.stdout_truncated);

    std::fs::create_dir(guest.path("work")).unwrap();
    let mut request = exec("echo $GUEST_BASE $EXTRA; pwd");
    request.command.env = vec!["EXTRA=more".into()];
    request.command.workdir = Some(guest.path("work").to_str().unwrap().into());
    let output = guest.client.exec(request, b"").unwrap();
    let expected = format!("base more\n{}\n", guest.path("work").display());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);

    let output = guest.client.exec(exec("kill -TERM $$"), b"").unwrap();
    assert_eq!(
        output.result.exit,
        ExitStatus {
            code: 143,
            signal: Some(15)
        }
    );
}

#[test]
fn exec_rejects_what_it_cannot_start() {
    let guest = Guest::start(None, 4096);
    let mut request = exec("true");
    request.command.argv = vec!["/nonexistent/program".into()];
    assert_eq!(
        refused(guest.client.exec(request, b"")),
        ErrorCode::InvalidArgument
    );

    let mut request = exec("true");
    request.command.workdir = Some("/nonexistent/workdir".into());
    assert_eq!(
        refused(guest.client.exec(request, b"")),
        ErrorCode::InvalidArgument
    );

    let mut request = exec("true");
    request.command.user = Some("nobody-in-this-image".into());
    assert_eq!(
        refused(guest.client.exec(request, b"")),
        ErrorCode::InvalidArgument
    );

    let mut request = exec("true");
    request.command.env = vec!["NO_EQUALS_SIGN".into()];
    assert_eq!(
        refused(guest.client.exec(request, b"")),
        ErrorCode::InvalidArgument
    );
}

#[test]
fn exec_relays_stdin_in_both_directions_at_once() {
    let guest = Guest::start(None, 4096);
    let mut request = exec("cat");
    request.stdin = true;
    let output = guest.client.exec(request.clone(), b"hello").unwrap();
    assert_eq!(output.stdout, b"hello");

    // Larger than every pipe and socket buffer on the path.
    let input: Vec<u8> = (0..4 << 20).map(|index| (index % 251) as u8).collect();
    let output = guest.client.exec(request, &input).unwrap();
    assert_eq!(output.result.exit.code, 0);
    assert!(output.stdout == input, "the echoed stream differs");

    // A process that never reads its stdin still completes.
    let mut request = exec("echo ignored");
    request.stdin = true;
    let output = guest.client.exec(request, &input).unwrap();
    assert_eq!(output.stdout, b"ignored\n");
}

#[test]
fn exec_kills_the_process_group_at_the_timeout() {
    let guest = Guest::start(None, 4096);
    let mut request = exec("echo started; sleep 300 & sleep 300");
    request.timeout_ms = 300;
    let started = Instant::now();
    let output = guest.client.exec(request, b"").unwrap();
    assert!(started.elapsed() < Duration::from_secs(20));
    assert!(output.result.timed_out);
    assert_eq!(
        output.result.exit,
        ExitStatus {
            code: 137,
            signal: Some(9)
        }
    );
    assert_eq!(output.stdout, b"started\n");
}

#[test]
fn exec_truncates_output_at_the_limit_and_still_reports_the_exit() {
    let guest = Guest::start(None, 4096);
    let mut request = exec("head -c 300000 /dev/zero; head -c 10 /dev/zero >&2; exit 3");
    request.max_output_bytes = 1000;
    let output = guest.client.exec(request, b"").unwrap();
    assert_eq!(output.stdout, vec![0u8; 1000]);
    assert_eq!(output.stderr, vec![0u8; 10]);
    assert!(output.result.stdout_truncated && !output.result.stderr_truncated);
    assert_eq!(output.result.exit.code, 3);
}

#[test]
fn exec_returns_when_a_background_child_keeps_the_output_open() {
    let guest = Guest::start(None, 4096);
    let started = Instant::now();
    let output = guest
        .client
        .exec(exec("sleep 8 & echo parent"), b"")
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(output.stdout, b"parent\n");
    assert_eq!(output.result.exit.code, 0);
}

#[test]
fn exec_on_a_terminal_has_a_window_size_and_takes_signals() {
    let guest = Guest::start(None, 4096);
    let mut request = exec("test -t 0 && test -t 1 && stty size");
    request.tty = Some(TtySize {
        rows: 33,
        columns: 101,
    });
    let output = guest.client.exec(request, b"").unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "33 101");
    assert_eq!(output.result.exit.code, 0);

    let mut session = guest
        .client
        .exec_stream(exec("echo ready; sleep 300"))
        .unwrap();
    let mut input = session.input().unwrap();
    assert_eq!(
        session.next_event().unwrap(),
        ExecEvent::Stdout(b"ready\n".to_vec())
    );
    input.signal(Signal::Term).unwrap();
    loop {
        if let ExecEvent::Exit(result) = session.next_event().unwrap() {
            assert_eq!(result.exit.signal, Some(15));
            break;
        }
    }
}

#[test]
fn a_disconnected_exec_client_kills_the_process() {
    let guest = Guest::start(None, 4096);
    let mut session = guest
        .client
        .exec_stream(exec("echo $$; sleep 300"))
        .unwrap();
    let ExecEvent::Stdout(pid) = session.next_event().unwrap() else {
        panic!("expected the PID");
    };
    let pid = String::from_utf8(pid).unwrap().trim().to_owned();
    assert!(std::path::Path::new(&format!("/proc/{pid}")).exists());
    drop(session);
    eventually("the process to be killed and reaped", || {
        !std::path::Path::new(&format!("/proc/{pid}")).exists()
    });
}
