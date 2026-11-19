use vmm_guest_proto::rpc::{ErrorCode, LogRecord, LogStream, MainState, ReadLogsRequest, Signal};

use crate::support::{Guest, eventually, refused};

const MAIN: &str = "trap 'echo extra; echo warning >&2' USR1
i=0
while [ $i -lt 2000 ]; do echo \"line $i 0123456789012345678901234567890123456789\"; i=$((i+1)); done
while :; do sleep 0.1; done";
const LAST_LINE: &str = "line 1999 0123456789012345678901234567890123456789\n";

fn logs(guest: &Guest, request: ReadLogsRequest) -> (Vec<LogRecord>, u64, bool) {
    let mut records = Vec::new();
    let end = guest
        .client
        .read_logs(request, |record| records.push(record))
        .unwrap();
    (records, end.next_cursor, end.ended)
}

fn all() -> ReadLogsRequest {
    ReadLogsRequest {
        after_cursor: None,
        since_unix_nanos: None,
        tail_records: 0,
        follow: false,
    }
}

fn text(records: &[LogRecord], stream: LogStream) -> String {
    let matching = records.iter().filter(|record| record.stream == stream);
    String::from_utf8(matching.flat_map(|record| record.data.clone()).collect()).unwrap()
}

#[test]
fn the_ring_keeps_the_newest_output_and_cursors_continue_after_it() {
    let guest = Guest::start(Some(MAIN), 4096);
    assert!(matches!(
        guest.client.health([1; 16]).unwrap().main,
        MainState::Running { .. }
    ));
    eventually("the main process output", || {
        text(&logs(&guest, all()).0, LogStream::Stdout).ends_with(LAST_LINE)
    });

    // About 100 kB went through a 4 kB ring: the oldest records are gone and counted.
    let (records, cursor, ended) = logs(&guest, all());
    assert!(!ended);
    let retained: usize = records.iter().map(|record| record.data.len()).sum();
    assert!(retained <= 4096, "{retained}");
    assert!(records[0].dropped_records > 0);
    assert_eq!(records[0].dropped_records, records[0].cursor - 1);
    assert!(
        records[1..]
            .iter()
            .all(|record| record.dropped_records == 0)
    );
    assert!(
        records
            .windows(2)
            .all(|pair| pair[1].cursor == pair[0].cursor + 1)
    );
    assert_eq!(records.last().unwrap().cursor, cursor);
    let stdout = text(&records, LogStream::Stdout);
    assert!(!stdout.contains("line 0 "));

    // Nothing is new after the cursor until the process writes again.
    let after = |cursor| ReadLogsRequest {
        after_cursor: Some(cursor),
        ..all()
    };
    assert_eq!(logs(&guest, after(cursor)), (Vec::new(), cursor, false));
    guest.client.send_signal(Signal::Usr1).unwrap();
    eventually("the new records", || {
        logs(&guest, after(cursor)).0.len() == 2
    });
    let (records, next, _) = logs(&guest, after(cursor));
    assert_eq!(text(&records, LogStream::Stdout), "extra\n");
    assert_eq!(text(&records, LogStream::Stderr), "warning\n");
    assert_eq!(
        (records[0].cursor, records[1].cursor, next),
        (cursor + 1, cursor + 2, cursor + 2)
    );
    assert!(records.iter().all(|record| record.dropped_records == 0));

    let tail = ReadLogsRequest {
        tail_records: 1,
        ..all()
    };
    let (records, tail_cursor, _) = logs(&guest, tail);
    assert_eq!((records.len(), records[0].cursor), (1, next));
    assert_eq!(tail_cursor, next);
    let future = ReadLogsRequest {
        since_unix_nanos: Some(i64::MAX),
        ..all()
    };
    assert_eq!(logs(&guest, future), (Vec::new(), next, false));
    let past = records[0].timestamp_unix_nanos;
    let since = ReadLogsRequest {
        since_unix_nanos: Some(past),
        tail_records: 1,
        ..all()
    };
    assert_eq!(logs(&guest, since).0.len(), 1);

    // A follower stays connected until the main process output ends.
    let follower = guest.client.clone();
    let follow = ReadLogsRequest {
        after_cursor: Some(next),
        follow: true,
        ..all()
    };
    let following = std::thread::spawn(move || {
        let mut seen = Vec::new();
        let end = follower
            .read_logs(follow, |record| seen.push(record))
            .unwrap();
        (seen, end.ended)
    });
    guest.client.send_signal(Signal::Usr1).unwrap();
    eventually("the followed records", || {
        logs(&guest, after(next)).0.len() == 2
    });
    assert!(matches!(
        guest.client.wait_main_exit(50).unwrap(),
        MainState::Running { .. }
    ));
    guest.client.send_signal(Signal::Term).unwrap();
    let MainState::Exited { exit, .. } = guest.client.wait_main_exit(20_000).unwrap() else {
        panic!("the main process still runs");
    };
    assert_eq!(exit.signal, Some(15));
    let (seen, ended) = following.join().unwrap();
    assert!(ended);
    assert_eq!(text(&seen, LogStream::Stdout), "extra\n");
    assert_eq!(
        refused(guest.client.send_signal(Signal::Term)),
        ErrorCode::FailedPrecondition
    );
}

#[test]
fn shell_idle_mode_has_no_main_process_and_an_ended_empty_log() {
    let guest = Guest::start(None, 4096);
    assert_eq!(guest.client.wait_main_exit(10).unwrap(), MainState::Idle);
    assert_eq!(
        logs(
            &guest,
            ReadLogsRequest {
                follow: true,
                ..all()
            }
        ),
        (Vec::new(), 0, true)
    );
    assert_eq!(
        refused(guest.client.send_signal(Signal::Term)),
        ErrorCode::FailedPrecondition
    );
}

#[test]
fn a_main_process_that_cannot_start_is_reported() {
    let guest = Guest::start_argv(Some(vec!["/nonexistent/entrypoint".into()]), 4096);
    let MainState::FailedToStart { message } = guest.client.wait_main_exit(10).unwrap() else {
        panic!("expected a start failure");
    };
    assert!(message.contains("/nonexistent/entrypoint"), "{message}");
}
