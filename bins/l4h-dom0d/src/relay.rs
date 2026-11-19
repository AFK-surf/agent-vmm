#![forbid(unsafe_code)]
use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, copy_bidirectional},
    time::timeout,
};

pub async fn serve_guest_relay<S, D, F>(
    mut stream: S,
    dial: impl FnOnce(SocketAddrV4) -> F,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
    F: Future<Output = io::Result<D>>,
{
    let mut header = [0; 6];
    timeout(Duration::from_secs(5), stream.read_exact(&mut header)).await??;
    let [a, b, c, d, p, q] = header;
    let port = u16::from_be_bytes([p, q]);
    if [a, b] != [10, 77] || (c == 0 && d <= 1) || (c == 255 && d == 255) || port == 0 {
        stream.write_u8(1).await?;
        return Ok(());
    }
    let target = SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), port);
    let Ok(Ok(mut tcp)) = timeout(Duration::from_secs(5), dial(target)).await else {
        stream.write_u8(1).await?;
        return Ok(());
    };
    stream.write_u8(0).await?;
    copy_bidirectional(&mut stream, &mut tcp).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};
    async fn unexpected_dial(_: SocketAddrV4) -> io::Result<DuplexStream> {
        panic!("rejected target was dialed")
    }

    #[tokio::test]
    async fn rejects_reserved_or_external_addresses_before_dial() {
        for ip in [
            [127, 0, 0, 1],
            [10, 77, 0, 1],
            [10, 77, 0, 0],
            [10, 77, 255, 255],
            [10, 78, 0, 2],
        ] {
            let (mut client, server) = duplex(64);
            client.write_all(&ip).await.unwrap();
            client.write_u16(22).await.unwrap();
            serve_guest_relay(server, unexpected_dial).await.unwrap();
            assert_eq!(client.read_u8().await.unwrap(), 1);
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        }
    }
    #[tokio::test]
    async fn acknowledges_tcp_connect_and_preserves_half_close() {
        for ip in [[10, 77, 1, 0], [10, 77, 0, 255], [10, 77, 255, 254]] {
            confirm_relay(ip).await;
        }
    }
    async fn confirm_relay(ip: [u8; 4]) {
        use tokio::net::{TcpListener, TcpStream};
        let tcp = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = tcp.local_addr().unwrap();
        let (mut client, server) = duplex(128);
        let relay = tokio::spawn(serve_guest_relay(server, move |dst| async move {
            assert_eq!(dst, SocketAddrV4::new(Ipv4Addr::from(ip), 8181));
            TcpStream::connect(addr).await
        }));
        client.write_all(&ip).await.unwrap();
        client.write_u16(8181).await.unwrap();
        let (mut target, _) = tcp.accept().await.unwrap();
        assert_eq!(client.read_u8().await.unwrap(), 0);
        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap();
        let mut buf = Vec::new();
        target.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"request");
        target.write_all(b"answer").await.unwrap();
        target.shutdown().await.unwrap();
        buf.clear();
        client.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"answer");
        relay.await.unwrap().unwrap();
    }
}
