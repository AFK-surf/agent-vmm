use super::*;
use futures::stream;
use std::io::Cursor;

#[test]
fn wire_boundaries_and_status_details() {
    let mut data = Vec::new();
    Frame::Message(Bytes::new()).write_sync(&mut data).unwrap();
    Frame::End.write_sync(&mut data).unwrap();
    assert_eq!(data, [1, 0, 0, 0, 0, 2, 0, 0, 0, 0]);
    data.clear();
    Frame::Status(Status::with_details(
        Code::OutOfRange,
        "range",
        Bytes::from_static(&[255, 1]),
    ))
    .write_sync(&mut data)
    .unwrap();
    assert_eq!(data[0], 3);
    assert_eq!(
        u32::from_be_bytes(data[1..5].try_into().unwrap()) as usize,
        data.len() - 5
    );
    assert_eq!(
        &data[5..],
        br#"{"code":11,"message":"range","details_b64":"/wE="}"#
    );
    let Frame::Status(status) = Frame::read_sync(&mut data.as_slice()).unwrap() else {
        panic!()
    };
    assert_eq!(status.details(), &[255, 1]);
    for prefix in [
        [0, 0, 1, 0, 1],
        [1, 1, 0, 0, 1],
        [2, 0, 0, 0, 1],
        [4, 0, 0, 0, 0],
    ] {
        assert!(Frame::read_sync(&mut Cursor::new(prefix)).is_err());
    }
}

struct Echo;
#[async_trait]
impl Service for Echo {
    fn name(&self) -> &'static str {
        "test.Echo"
    }
    async fn call(&self, method: &str, request: Request<Body>) -> Result<Body, Status> {
        if method != "Echo" {
            return Err(Status::unimplemented("unknown echo method"));
        }
        Ok(request.into_inner())
    }
}

#[tokio::test]
async fn simultaneous_calls_in_both_directions() {
    let (left, right) = tokio::io::duplex(16 * 1024);
    let a = MuxSession::start(left, Mode::Client, Router::new(Echo));
    let b = MuxSession::start(right, Mode::Server, Router::new(Echo));
    async fn echo(client: Client, text: String) {
        let messages = vec![String::new(), text.clone()];
        let mut response = client
            .call::<String, String, _>("/test.Echo/Echo", Request::new(stream::iter(messages)))
            .await
            .unwrap()
            .into_inner();
        drop(client);
        assert_eq!(response.message().await.unwrap(), Some(String::new()));
        assert_eq!(response.message().await.unwrap(), Some(text));
        assert!(response.message().await.unwrap().is_none());
    }
    let clients = (a.client(), b.client());
    drop((a, b));
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            echo(clients.0, "a".repeat(1 << 20)),
            echo(clients.1, "b".repeat(1 << 20))
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn closing_idle_unix_stream_releases_slot_but_half_close_preserves_it() {
    struct Quiet;
    #[async_trait]
    impl Service for Quiet {
        fn name(&self) -> &'static str {
            "test.Quiet"
        }
        async fn call(&self, method: &str, request: Request<Body>) -> Result<Body, Status> {
            let _: Request<()> = request.unary().await?;
            if method == "Wait" {
                return Ok(Body::new(
                    stream::once(async { Ok(Bytes::new()) }).chain(stream::pending()),
                ));
            }
            Ok(Response::new(()).unary_body())
        }
    }
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let path = dir.path().join("rpc");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(Router::new(Quiet).serve_unix(listener, 1, std::future::pending()));
    let io = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (mut reader, mut writer) = io.into_split();
    Frame::Header(RequestHeader {
        v: 1,
        method: "/test.Quiet/Wait".into(),
        metadata: Metadata::default(),
        timeout_ms: None,
    })
    .write(&mut writer)
    .await
    .unwrap();
    Frame::Message(Bytes::new())
        .write(&mut writer)
        .await
        .unwrap();
    Frame::End.write(&mut writer).await.unwrap();
    writer.shutdown().await.unwrap();
    assert!(matches!(
        Frame::read_body(&mut reader, true, MAX_FRAME_BYTES)
            .await
            .unwrap(),
        Frame::Message(_)
    ));
    let next = tokio::spawn(async move {
        Client::unix(path)
            .call::<(), (), _>("/test.Quiet/Next", Request::new(stream::iter([()])))
            .await
            .unwrap()
            .into_inner()
            .single()
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    assert!(!next.is_finished());
    drop((reader, writer));
    tokio::time::timeout(std::time::Duration::from_secs(3), next)
        .await
        .unwrap()
        .unwrap();
    server.abort();
}

#[test]
fn sync_deadline_covers_all_reads_of_a_slow_response() {
    use std::{
        io::{Read, Write},
        os::unix::net::UnixListener,
        time::Duration,
    };
    let dir = tempfile::tempdir_in("/tmp").unwrap();
    let path = dir.path().join("slow");
    let listener = UnixListener::bind(&path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut io, _) = listener.accept().unwrap();
        io.read_to_end(&mut Vec::new()).unwrap();
        let mut response = Vec::new();
        Frame::Message(Bytes::new())
            .write_sync(&mut response)
            .unwrap();
        Frame::Status(Status::ok(""))
            .write_sync(&mut response)
            .unwrap();
        for chunk in response.chunks(5) {
            if io.write_all(chunk).is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    });
    let result = sync_unary::<(), ()>(&path, "/test.Slow/Read", &(), Duration::from_millis(60));
    assert_eq!(result.unwrap_err().code(), Code::DeadlineExceeded);
    server.join().unwrap();
}
