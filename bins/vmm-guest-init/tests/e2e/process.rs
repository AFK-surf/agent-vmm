use vmm_guest_proto::rpc::{
    ErrorCode, ProcessState, ProcessStopRequest, ProcessTailRequest, ProcessWriteRequest, Signal,
};

use crate::support::{Guest, process, refused};

fn tail(guest: &Guest, process_id: &str, since_offset: u64) -> (String, u64, ProcessState) {
    let request = ProcessTailRequest {
        process_id: process_id.into(),
        since_offset,
        max_bytes: 0,
        wait_ms: 20_000,
    };
    let reply = guest.client.process_tail(request).unwrap();
    (
        String::from_utf8(reply.data).unwrap(),
        reply.next_offset,
        reply.state,
    )
}

fn write(guest: &Guest, process_id: &str, data: &str, close_stdin: bool) {
    let request = ProcessWriteRequest {
        process_id: process_id.into(),
        data: data.into(),
        close_stdin,
    };
    assert_eq!(
        guest.client.process_write(request).unwrap().bytes_written,
        data.len() as u64
    );
}

#[test]
fn a_repeated_start_request_returns_the_first_process() {
    let guest = Guest::start(None, 4096);
    let script = "echo ready; while read line; do echo \"got $line\"; done; echo end";
    let first = guest
        .client
        .process_start(process("request-1", script))
        .unwrap();
    assert!(!first.replayed);
    assert_eq!(first.process.state, ProcessState::Running);

    let replay = guest
        .client
        .process_start(process("request-1", script))
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.process.process_id, first.process.process_id);
    assert_eq!(replay.process.pid, first.process.pid);
    let conflict = guest
        .client
        .process_start(process("request-1", "echo other"));
    assert_eq!(refused(conflict), ErrorCode::AlreadyExists);
    assert_eq!(
        refused(guest.client.process_start(process("", script))),
        ErrorCode::InvalidArgument
    );

    let listed = guest.client.process_list().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].request_id, "request-1");

    let id = &first.process.process_id;
    let (output, offset, state) = tail(&guest, id, 0);
    assert_eq!((output.as_str(), state), ("ready\n", ProcessState::Running));
    write(&guest, id, "one\n", false);
    let (output, offset, _) = tail(&guest, id, offset);
    assert_eq!(output, "got one\n");
    write(&guest, id, "two\n", true);

    // The process ends when its stdin closes. A replay after the exit starts nothing.
    let mut rest = String::new();
    let mut position = offset;
    let exit = loop {
        let (output, next, state) = tail(&guest, id, position);
        rest.push_str(&output);
        position = next;
        if let ProcessState::Exited(exit) = state {
            break exit;
        }
    };
    assert_eq!(rest, "got two\nend\n");
    assert_eq!(exit.code, 0);
    let closed = ProcessWriteRequest {
        process_id: id.clone(),
        data: b"x".to_vec(),
        close_stdin: false,
    };
    assert_eq!(
        refused(guest.client.process_write(closed)),
        ErrorCode::FailedPrecondition
    );
    let replay = guest
        .client
        .process_start(process("request-1", script))
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.process.state, ProcessState::Exited(exit));
    assert_eq!(guest.client.process_list().unwrap().len(), 1);
}

#[test]
fn stop_signals_the_group_and_kills_what_ignores_the_signal() {
    let guest = Guest::start(None, 4096);
    let polite = guest
        .client
        .process_start(process("polite", "sleep 300"))
        .unwrap()
        .process;
    let stubborn_script = "trap '' TERM; echo armed; while :; do sleep 1; done";
    let stubborn = guest
        .client
        .process_start(process("stubborn", stubborn_script))
        .unwrap()
        .process;
    assert_eq!(tail(&guest, &stubborn.process_id, 0).0, "armed\n");

    let stop = |process_id: &str| ProcessStopRequest {
        process_id: process_id.into(),
        signal: Signal::Term,
        grace_ms: 300,
    };
    let stopped = guest.client.process_stop(stop(&polite.process_id)).unwrap();
    let ProcessState::Exited(exit) = stopped.state else {
        panic!("still running")
    };
    assert_eq!(exit.signal, Some(15));
    let stopped = guest
        .client
        .process_stop(stop(&stubborn.process_id))
        .unwrap();
    let ProcessState::Exited(exit) = stopped.state else {
        panic!("still running")
    };
    assert_eq!(exit.signal, Some(9));

    // Stopping an exited process changes nothing, and an unknown one is an error.
    let again = guest.client.process_stop(stop(&polite.process_id)).unwrap();
    assert_eq!(again.state, stopped_state(15));
    assert_eq!(
        refused(guest.client.process_stop(stop("999"))),
        ErrorCode::NotFound
    );
    let unknown = ProcessTailRequest {
        process_id: "x".into(),
        since_offset: 0,
        max_bytes: 0,
        wait_ms: 0,
    };
    assert_eq!(
        refused(guest.client.process_tail(unknown)),
        ErrorCode::NotFound
    );
}

fn stopped_state(signal: i32) -> ProcessState {
    ProcessState::Exited(vmm_guest_proto::rpc::ExitStatus {
        code: 128 + signal,
        signal: Some(signal),
    })
}

#[test]
fn tail_keeps_the_newest_output_and_reports_the_skipped_range() {
    let guest = Guest::start(None, 4096);
    let mut request = process(
        "noisy",
        "head -c 5000 /dev/zero | tr '\\0' a; echo; echo last",
    );
    request.output_limit_bytes = 1000;
    let id = guest
        .client
        .process_start(request)
        .unwrap()
        .process
        .process_id;

    let mut position = 0;
    let mut output = Vec::new();
    loop {
        let request = ProcessTailRequest {
            process_id: id.clone(),
            since_offset: position,
            max_bytes: 300,
            wait_ms: 20_000,
        };
        let reply = guest.client.process_tail(request).unwrap();
        assert!(reply.data.len() <= 300);
        output.extend_from_slice(&reply.data);
        position = reply.next_offset;
        if reply.state != ProcessState::Running && reply.data.is_empty() {
            break;
        }
    }
    assert_eq!(position, 5006);
    assert!(output.ends_with(b"a\nlast\n"));
    let listed = guest.client.process_list().unwrap();
    assert_eq!(listed[0].output_end_offset, 5006);

    // After the exit only the last 1000 bytes remain: a read from 0 starts at 4006.
    let request = ProcessTailRequest {
        process_id: id,
        since_offset: 0,
        max_bytes: 0,
        wait_ms: 0,
    };
    let reply = guest.client.process_tail(request).unwrap();
    assert_eq!((reply.offset, reply.next_offset), (4006, 5006));
    assert_eq!(reply.data.len(), 1000);
}
