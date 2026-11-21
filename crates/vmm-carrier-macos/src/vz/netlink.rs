//! Point-to-point L2 Ethernet + ARP + IPv4 + UDP framing over a `UnixDatagram`
//! socketpair connected to `VZFileHandleNetworkDeviceAttachment`, exposing a
//! `quinn::AsyncUdpSocket` so `agent-vmm-carrier` talks QUIC directly to
//! `l4h-dom0d` on `169.254.77.2:8180`.

use quinn::{
    AsyncUdpSocket, UdpPoller,
    udp::{RecvMeta, Transmit},
};
use std::{
    fmt,
    future::Future,
    io::{self, IoSliceMut},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    os::fd::{AsRawFd, OwnedFd},
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU16, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::UnixDatagram,
    time::{Instant, Sleep},
};

pub const HOST_MAC: [u8; 6] = [0x02, 0x56, 0x4d, 0x4d, 0x00, 0x01];
pub const GUEST_MAC: [u8; 6] = [0x02, 0x56, 0x4d, 0x4d, 0x00, 0x02];
pub const GUEST_MAC_STR: &str = "02:56:4d:4d:00:02";

pub const HOST_IP: Ipv4Addr = Ipv4Addr::new(169, 254, 77, 1);
pub const GUEST_IP: Ipv4Addr = Ipv4Addr::new(169, 254, 77, 2);
pub const HOST_UDP_PORT: u16 = 41800;
pub const GUEST_QUIC_PORT: u16 = 8180;

const ETH_LEN: usize = 14;
const IPV4_LEN: usize = 20;
const UDP_LEN: usize = 8;
const ETH_TYPE_IPV4: u16 = 0x0800;
const ETH_TYPE_ARP: u16 = 0x0806;
const IP_PROTO_UDP: u8 = 17;

/// Create a `UnixDatagram` pair sized for `VZFileHandleNetworkDeviceAttachment`
/// (`SO_RCVBUF` = 4x `SO_SNDBUF`). Returns `(host_socket, guest_fd_for_vz)`.
pub(super) fn socket_pair() -> io::Result<(std::os::unix::net::UnixDatagram, OwnedFd)> {
    let (host, guest) = std::os::unix::net::UnixDatagram::pair()?;
    let snd_size: libc::c_int = 128 << 10;
    let rcv_size: libc::c_int = 512 << 10;
    for fd in [host.as_raw_fd(), guest.as_raw_fd()] {
        for (opt, val) in [(libc::SO_SNDBUF, snd_size), (libc::SO_RCVBUF, rcv_size)] {
            // Best-effort buffer sizing; macOS caps at kern.ipc.maxsockbuf.
            // SAFETY: fd is a valid socket and val is a live c_int.
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    opt,
                    (&raw const val).cast(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }
    host.set_nonblocking(true)?;
    Ok((host, guest.into()))
}

fn internet_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let (chunks, rem) = data.as_chunks::<2>();
    for &c in chunks {
        sum += u32::from(u16::from_be_bytes(c));
    }
    if let Some(&b) = rem.first() {
        sum += u32::from(b) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn udp_checksum(src: Ipv4Addr, dst: Ipv4Addr, udp_packet: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let src_octets = src.octets();
    let dst_octets = dst.octets();
    for &pair in src_octets
        .as_chunks::<2>()
        .0
        .iter()
        .chain(dst_octets.as_chunks::<2>().0.iter())
    {
        sum += u32::from(u16::from_be_bytes(pair));
    }
    sum += u32::from(IP_PROTO_UDP);
    sum += udp_packet.len() as u32;
    let (chunks, rem) = udp_packet.as_chunks::<2>();
    for &c in chunks {
        sum += u32::from(u16::from_be_bytes(c));
    }
    if let Some(&b) = rem.first() {
        sum += u32::from(b) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    let res = !(sum as u16);
    if res == 0 { 0xffff } else { res }
}

fn build_udp_frame(
    dst_mac: [u8; 6],
    ip_id: u16,
    src_port: u16,
    dst_ip: Ipv4Addr,
    dst_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let udp_len = (UDP_LEN + payload.len()) as u16;
    let ip_len = (IPV4_LEN + UDP_LEN + payload.len()) as u16;
    let mut frame = vec![0u8; ETH_LEN + IPV4_LEN + UDP_LEN + payload.len()];
    // Ethernet header
    frame[0..6].copy_from_slice(&dst_mac);
    frame[6..12].copy_from_slice(&HOST_MAC);
    frame[12..14].copy_from_slice(&ETH_TYPE_IPV4.to_be_bytes());
    // IPv4 header
    let ip = &mut frame[ETH_LEN..ETH_LEN + IPV4_LEN];
    ip[0] = 0x45;
    ip[2..4].copy_from_slice(&ip_len.to_be_bytes());
    ip[4..6].copy_from_slice(&ip_id.to_be_bytes());
    ip[6..8].copy_from_slice(&0x4000u16.to_be_bytes()); // DF
    ip[8] = 64; // TTL
    ip[9] = IP_PROTO_UDP;
    ip[12..16].copy_from_slice(&HOST_IP.octets());
    ip[16..20].copy_from_slice(&dst_ip.octets());
    let csum = internet_checksum(ip);
    ip[10..12].copy_from_slice(&csum.to_be_bytes());
    // UDP header + payload
    let udp = &mut frame[ETH_LEN + IPV4_LEN..];
    udp[0..2].copy_from_slice(&src_port.to_be_bytes());
    udp[2..4].copy_from_slice(&dst_port.to_be_bytes());
    udp[4..6].copy_from_slice(&udp_len.to_be_bytes());
    udp[UDP_LEN..].copy_from_slice(payload);
    let ucsum = udp_checksum(HOST_IP, dst_ip, udp);
    udp[6..8].copy_from_slice(&ucsum.to_be_bytes());
    frame
}

fn build_arp_reply(target_mac: [u8; 6], target_ip: [u8; 4]) -> [u8; 42] {
    let mut pkt = [0u8; 42];
    pkt[0..6].copy_from_slice(&target_mac);
    pkt[6..12].copy_from_slice(&HOST_MAC);
    pkt[12..14].copy_from_slice(&ETH_TYPE_ARP.to_be_bytes());
    // ARP payload (28 bytes)
    pkt[14..16].copy_from_slice(&1u16.to_be_bytes()); // Ethernet
    pkt[16..18].copy_from_slice(&ETH_TYPE_IPV4.to_be_bytes()); // IPv4
    pkt[18] = 6; // hlen
    pkt[19] = 4; // plen
    pkt[20..22].copy_from_slice(&2u16.to_be_bytes()); // Reply
    pkt[22..28].copy_from_slice(&HOST_MAC);
    pkt[28..32].copy_from_slice(&HOST_IP.octets());
    pkt[32..38].copy_from_slice(&target_mac);
    pkt[38..42].copy_from_slice(&target_ip);
    pkt
}

const ENOBUFS_BACKOFF: Duration = Duration::from_millis(1);

pub(super) struct NetlinkSocket {
    dgram: UnixDatagram,
    guest_mac: RwLock<[u8; 6]>,
    ip_id: AtomicU16,
    backoff_until: Mutex<Option<Instant>>,
}

impl NetlinkSocket {
    pub(super) fn new(host_socket: std::os::unix::net::UnixDatagram) -> io::Result<Arc<Self>> {
        let dgram = UnixDatagram::from_std(host_socket)?;
        Ok(Arc::new(Self {
            dgram,
            guest_mac: RwLock::new(GUEST_MAC),
            ip_id: AtomicU16::new(1),
            backoff_until: Mutex::new(None),
        }))
    }

    fn note_enobufs(&self) {
        if let Ok(mut until) = self.backoff_until.lock() {
            *until = Some(Instant::now() + ENOBUFS_BACKOFF);
        }
    }

    fn take_backoff_deadline(&self) -> Option<Instant> {
        self.backoff_until.lock().ok().and_then(|mut g| g.take())
    }
}

impl fmt::Debug for NetlinkSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetlinkSocket").finish()
    }
}

struct NetlinkPoller {
    socket: Arc<NetlinkSocket>,
    backoff: Option<Pin<Box<Sleep>>>,
}

impl fmt::Debug for NetlinkPoller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetlinkPoller").finish()
    }
}

impl UdpPoller for NetlinkPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(sleep) = self.backoff.as_mut() {
            match sleep.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => self.backoff = None,
            }
        }
        if let Some(deadline) = self.socket.take_backoff_deadline()
            && deadline > Instant::now()
        {
            let mut sleep = Box::pin(tokio::time::sleep_until(deadline));
            if sleep.as_mut().poll(cx).is_pending() {
                self.backoff = Some(sleep);
                return Poll::Pending;
            }
        }
        self.socket.dgram.poll_send_ready(cx)
    }
}

impl AsyncUdpSocket for NetlinkSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(NetlinkPoller {
            socket: self,
            backoff: None,
        })
    }

    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        let IpAddr::V4(dst_ip) = transmit.destination.ip() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "carrier netlink supports IPv4 only",
            ));
        };
        let dst_mac = self.guest_mac.read().map(|g| *g).unwrap_or(GUEST_MAC);
        let id = self.ip_id.fetch_add(1, Ordering::Relaxed);
        let frame = build_udp_frame(
            dst_mac,
            id,
            HOST_UDP_PORT,
            dst_ip,
            transmit.destination.port(),
            transmit.contents,
        );
        match self.dgram.try_send(&frame) {
            Ok(_) => Ok(()),
            // On macOS XNU, AF_UNIX SOCK_DGRAM returns ENOBUFS when the peer
            // socket buffer is transiently full; surface WouldBlock with a
            // short timer backoff so QUIC pauses sends cleanly instead of
            // dropping packets or busy-looping on kqueue writability.
            Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => {
                self.note_enobufs();
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            Err(e) => Err(e),
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if bufs.is_empty() || meta.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let mut raw = [0u8; 4096];
        loop {
            match self.dgram.poll_recv_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {}
            }
            let n = match self.dgram.try_recv(&mut raw) {
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Poll::Ready(Err(e)),
            };
            if n < ETH_LEN {
                continue;
            }
            let eth_type = u16::from_be_bytes([raw[12], raw[13]]);
            if eth_type == ETH_TYPE_ARP && n >= 42 {
                let op = u16::from_be_bytes([raw[20], raw[21]]);
                if op == 1 && raw[38..42] == HOST_IP.octets() {
                    let mut sender_mac = [0u8; 6];
                    sender_mac.copy_from_slice(&raw[22..28]);
                    if let Ok(mut guard) = self.guest_mac.write() {
                        *guard = sender_mac;
                    }
                    let mut sender_ip = [0u8; 4];
                    sender_ip.copy_from_slice(&raw[28..32]);
                    let reply = build_arp_reply(sender_mac, sender_ip);
                    let _ = self.dgram.try_send(&reply);
                }
                continue;
            }
            if eth_type != ETH_TYPE_IPV4 || n < ETH_LEN + IPV4_LEN {
                continue;
            }
            let ihl = ((raw[ETH_LEN] & 0x0f) as usize) * 4;
            if ihl < IPV4_LEN || n < ETH_LEN + ihl + UDP_LEN {
                continue;
            }
            if raw[ETH_LEN + 9] != IP_PROTO_UDP {
                continue;
            }
            let dst_ip = Ipv4Addr::new(
                raw[ETH_LEN + 16],
                raw[ETH_LEN + 17],
                raw[ETH_LEN + 18],
                raw[ETH_LEN + 19],
            );
            if dst_ip != HOST_IP {
                continue;
            }
            let src_ip = Ipv4Addr::new(
                raw[ETH_LEN + 12],
                raw[ETH_LEN + 13],
                raw[ETH_LEN + 14],
                raw[ETH_LEN + 15],
            );
            let udp_off = ETH_LEN + ihl;
            let src_port = u16::from_be_bytes([raw[udp_off], raw[udp_off + 1]]);
            let dst_port = u16::from_be_bytes([raw[udp_off + 2], raw[udp_off + 3]]);
            let udp_len = u16::from_be_bytes([raw[udp_off + 4], raw[udp_off + 5]]) as usize;
            if dst_port != HOST_UDP_PORT || udp_len < UDP_LEN || udp_off + udp_len > n {
                continue;
            }
            if let Ok(mut guard) = self.guest_mac.write() {
                guard.copy_from_slice(&raw[6..12]);
            }
            let payload = &raw[udp_off + UDP_LEN..udp_off + udp_len];
            let copy_len = payload.len().min(bufs[0].len());
            bufs[0][..copy_len].copy_from_slice(&payload[..copy_len]);
            meta[0] = RecvMeta {
                addr: SocketAddr::V4(SocketAddrV4::new(src_ip, src_port)),
                len: copy_len,
                stride: copy_len,
                ecn: None,
                dst_ip: Some(IpAddr::V4(HOST_IP)),
            };
            return Poll::Ready(Ok(1));
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(SocketAddr::V4(SocketAddrV4::new(HOST_IP, HOST_UDP_PORT)))
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_and_arp_framing_roundtrip() {
        let frame = build_udp_frame(GUEST_MAC, 42, HOST_UDP_PORT, GUEST_IP, GUEST_QUIC_PORT, b"hello");
        assert_eq!(frame.len(), ETH_LEN + IPV4_LEN + UDP_LEN + 5);
        assert_eq!(&frame[0..6], &GUEST_MAC);
        assert_eq!(&frame[6..12], &HOST_MAC);
        assert_eq!(u16::from_be_bytes([frame[12], frame[13]]), ETH_TYPE_IPV4);
        assert_eq!(internet_checksum(&frame[ETH_LEN..ETH_LEN + IPV4_LEN]), 0);
        assert_eq!(&frame[ETH_LEN + IPV4_LEN + UDP_LEN..], b"hello");

        let arp = build_arp_reply(GUEST_MAC, GUEST_IP.octets());
        assert_eq!(u16::from_be_bytes([arp[12], arp[13]]), ETH_TYPE_ARP);
        assert_eq!(u16::from_be_bytes([arp[20], arp[21]]), 2);
        assert_eq!(&arp[22..28], &HOST_MAC);
        assert_eq!(&arp[28..32], &HOST_IP.octets());
    }

    #[tokio::test]
    async fn enobufs_backoff_pends_and_wakes_poller() {
        use std::future::poll_fn;
        let (host, _guest) = socket_pair().unwrap();
        let sock = NetlinkSocket::new(host).unwrap();
        sock.note_enobufs();
        let mut poller = sock.clone().create_io_poller();
        let started = Instant::now();
        poll_fn(|cx| poller.as_mut().poll_writable(cx))
            .await
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_micros(500),
            "poller returned before ENOBUFS backoff elapsed"
        );
    }
}
