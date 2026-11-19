use std::net::TcpStream;

use vmm_guest_proto::Secret;
use vmm_guest_proto::client::Client;
use vmm_guest_proto::frame::{ClientFrame, ServerFrame, read_frame, write_frame};
use vmm_guest_proto::rpc::{Call, ErrorCode, HealthRequest, Hello, MainState};

use crate::support::{Guest, INSTANCE, TOKEN, exec, refused};

#[test]
fn health_proves_the_token_and_a_wrong_token_is_refused() {
    let guest = Guest::start(None, 4096);
    let health = guest.client.health([9; 16]).unwrap();
    assert_eq!(health.instance_id, INSTANCE);
    assert_eq!(health.main, MainState::Idle);
    assert!(!health.quiesced);
    assert_eq!(health.usage.cpu_nanos, None);
    assert!(health.usage.sampled_unix_nanos > 0);

    for token in ["", "wrong", &TOKEN[1..], &format!("{TOKEN}x")] {
        let intruder = Client::new(guest.address, Secret::new(token));
        assert_eq!(
            refused(intruder.health([9; 16])),
            ErrorCode::Unauthenticated,
            "{token:?}"
        );
        assert_eq!(
            refused(intruder.exec(exec("true"), b"")),
            ErrorCode::Unauthenticated
        );
        assert_eq!(refused(intruder.process_list()), ErrorCode::Unauthenticated);
    }
}

#[test]
fn another_protocol_version_is_refused() {
    let guest = Guest::start(None, 4096);
    let mut stream = TcpStream::connect(guest.address).unwrap();
    let hello = Hello {
        version: vmm_guest_proto::PROTOCOL_VERSION + 1,
        token: Secret::new(TOKEN),
        call: Call::Health(HealthRequest { nonce: [0; 16] }),
    };
    write_frame(&mut stream, &ClientFrame::Hello(Box::new(hello))).unwrap();
    let Some(ServerFrame::Done(Err(error))) = read_frame(&mut stream).unwrap() else {
        panic!("expected a refusal");
    };
    assert_eq!(error.code, ErrorCode::UnsupportedVersion);
}
