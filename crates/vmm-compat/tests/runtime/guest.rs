use async_trait::async_trait;
use std::{net::SocketAddr, time::Duration};
use tonic::Status;
use vmm_compat::{
    RuntimeDriver,
    guest::{Duplex, GuestDriver},
};
use vmm_core::{BootIdentity, HostShareMount};
use vmm_guest_proto::{
    Secret,
    client::Client,
    frame::{ClientFrame, ServerFrame, read_frame, write_frame},
    rpc as g,
};

pub struct Guest(SocketAddr);

impl Guest {
    pub async fn start() -> (Self, tokio::task::JoinHandle<()>) {
        let server = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let task = tokio::spawn(async move {
            for _ in 0..3 {
                let (socket, _) = tokio::time::timeout(Duration::from_secs(8), server.accept())
                    .await
                    .unwrap()
                    .unwrap();
                tokio::task::spawn_blocking(move || {
                    let mut socket = socket.into_std().unwrap();
                    socket.set_nonblocking(false).unwrap();
                    socket
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let Some(ClientFrame::Hello(hello)) = read_frame(&mut socket).unwrap() else {
                        panic!("missing hello")
                    };
                    assert_eq!(hello.token, Secret::new("fixture"));
                    let done = match hello.call {
                        g::Call::Exec(r) => {
                            assert_eq!(r.command.argv, ["cat"]);
                            let Some(ClientFrame::Data(input)) = read_frame(&mut socket).unwrap()
                            else {
                                panic!("missing stdin")
                            };
                            assert!(matches!(
                                read_frame(&mut socket).unwrap(),
                                Some(ClientFrame::Resize(g::TtySize {
                                    rows: 25,
                                    columns: 80
                                }))
                            ));
                            assert!(matches!(
                                read_frame(&mut socket).unwrap(),
                                Some(ClientFrame::End)
                            ));
                            write_frame(&mut socket, &ServerFrame::Stdout(input)).unwrap();
                            write_frame(&mut socket, &ServerFrame::Stderr(b"diagnostic".to_vec()))
                                .unwrap();
                            g::Reply::Exec(g::ExecResult {
                                exit: g::ExitStatus {
                                    code: 3,
                                    signal: None,
                                },
                                timed_out: false,
                                stdout_truncated: false,
                                stderr_truncated: false,
                            })
                        }
                        g::Call::ReadLogs(r) => {
                            assert_eq!(r.tail_records, 2);
                            write_frame(
                                &mut socket,
                                &ServerFrame::Log(g::LogRecord {
                                    cursor: 5,
                                    timestamp_unix_nanos: 7,
                                    stream: g::LogStream::Stderr,
                                    data: b"retained".to_vec(),
                                    dropped_records: 4,
                                }),
                            )
                            .unwrap();
                            g::Reply::Logs(g::LogsEnd {
                                next_cursor: 5,
                                ended: true,
                            })
                        }
                        _ => panic!("unexpected guest call"),
                    };
                    write_frame(&mut socket, &ServerFrame::Done(Ok(done))).unwrap();
                })
                .await
                .unwrap();
            }
        });
        (Self(addr), task)
    }
}

#[async_trait]
impl GuestDriver for Guest {
    async fn client(&self, _: &BootIdentity) -> Result<Client, Status> {
        Ok(Client::new(self.0, Secret::new("fixture")))
    }
    async fn connect_port(&self, _: &BootIdentity, _: u16) -> Result<Box<dyn Duplex>, Status> {
        Err(Status::unavailable("fixture has no forwarded service"))
    }
}
#[async_trait]
impl RuntimeDriver for Guest {
    async fn authorize_shares(&self, _: &str, shares: &[HostShareMount]) -> Result<(), Status> {
        assert!(shares.is_empty());
        Ok(())
    }
    async fn health(&self, boot: &BootIdentity) -> Result<g::HealthReply, Status> {
        Ok(g::HealthReply {
            instance_id: boot.instance_id.clone(),
            boot_token_proof: [0; 32],
            main: g::MainState::Idle,
            quiesced: false,
            uptime_ms: 0,
            orphans_reaped: 0,
            usage: Default::default(),
        })
    }
}
