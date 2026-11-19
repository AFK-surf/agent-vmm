use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn s(data: &mut Vec<u8>, text: &str) {
    data.extend((text.len() as u16).to_le_bytes());
    data.extend(text.as_bytes());
}
async fn call(sock: &mut TcpStream, op: u8, data: &[u8]) -> (u8, Vec<u8>) {
    let mut request = ((7 + data.len()) as u32).to_le_bytes().to_vec();
    request.push(op);
    request.extend(12_u16.to_le_bytes());
    request.extend(data);
    sock.write_all(&request).await.unwrap();
    let n = sock.read_u32_le().await.unwrap();
    let mut response = vec![0; n as usize - 4];
    sock.read_exact(&mut response).await.unwrap();
    assert_eq!(&response[1..3], &12_u16.to_le_bytes());
    (response[0], response.split_off(3))
}
fn walk(fid: u32, name: &str) -> Vec<u8> {
    let mut v = 1_u32.to_le_bytes().to_vec();
    v.extend(fid.to_le_bytes());
    v.extend(1_u16.to_le_bytes());
    s(&mut v, name);
    v
}
async fn attach(sock: &mut TcpStream, name: &str) -> u8 {
    let mut v = 1_u32.to_le_bytes().to_vec();
    v.extend(u32::MAX.to_le_bytes());
    s(&mut v, "root");
    s(&mut v, name);
    v.extend(0_u32.to_le_bytes());
    call(sock, 104, &v).await.0
}
#[tokio::test]
async fn grants_bind_the_peer_name_access_and_lifetime() -> vmm_core::Result<()> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("share");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("good"), b"ok")?;
    std::os::unix::fs::symlink(temp.path(), root.join("link"))?;
    let grants = Shares::default();
    let address = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    let port = grants
        .export("instance", "src", &root, true, Ipv4Addr::LOCALHOST, address)
        .await?;
    let mut socket = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await?;
    let mut version = (1_u32 << 20).to_le_bytes().to_vec();
    s(&mut version, "9P2000.L");
    assert_eq!(call(&mut socket, 100, &version).await.1, version);
    assert_eq!(attach(&mut socket, "other").await, 7);
    assert_eq!(attach(&mut socket, "src").await, 105);
    assert_eq!(call(&mut socket, 110, &walk(2, "good")).await.0, 111);
    assert_eq!(call(&mut socket, 110, &walk(3, "link")).await.0, 7);
    assert_eq!(call(&mut socket, 110, &walk(3, "..")).await.0, 7);
    let mut open = 2_u32.to_le_bytes().to_vec();
    open.extend(2_u32.to_le_bytes());
    assert_eq!(call(&mut socket, 12, &open).await.0, 7);
    open[4] = 0;
    assert_eq!(call(&mut socket, 12, &open).await.0, 13);
    let mut read = 2_u32.to_le_bytes().to_vec();
    read.extend(0_u64.to_le_bytes());
    read.extend(3_u32.to_le_bytes());
    assert_eq!(&call(&mut socket, 116, &read).await.1[4..], b"ok");
    let denied = grants
        .export(
            "another",
            "src",
            &root,
            false,
            Ipv4Addr::new(192, 0, 2, 8),
            address,
        )
        .await?;
    let mut wrong = TcpStream::connect((Ipv4Addr::LOCALHOST, denied)).await?;
    let mut byte = [0];
    assert_eq!(wrong.read(&mut byte).await?, 0);
    grants.revoke("instance").await;
    assert_eq!(socket.read(&mut byte).await?, 0);
    grants.revoke("another").await;
    Ok(())
}
