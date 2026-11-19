//! Source-scoped service names at the guest gateway.
use crate::backend::LocalDom0Backend;
use simple_dns::{
    CLASS, OPCODE, Packet, PacketFlag, QCLASS, QTYPE, RCODE, ResourceRecord, TYPE, rdata::RData,
};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    task::JoinSet,
    time::timeout,
};
use vmm_core::{EgressMode, Result};
use vmm_l4hctl::VmState;

pub struct DnsServer {
    udp: Arc<UdpSocket>,
    tcp: TcpListener,
}

impl DnsServer {
    pub async fn bind(addr: SocketAddrV4) -> io::Result<Self> {
        let tcp = TcpListener::bind(addr).await?;
        let udp = Arc::new(UdpSocket::bind(tcp.local_addr()?).await?);
        Ok(Self { udp, tcp })
    }
    pub fn port(&self) -> io::Result<u16> {
        Ok(self.tcp.local_addr()?.port())
    }
    pub async fn serve(self, backend: Arc<LocalDom0Backend>) -> io::Result<()> {
        let mut workers = JoinSet::new();
        let mut buf = vec![0; u16::MAX as usize];
        loop {
            tokio::select! {
                Some(_) = workers.join_next(), if !workers.is_empty() => {},
                packet = self.udp.recv_from(&mut buf), if workers.len() < 128 => {
                    let (n, peer) = packet?;
                    let IpAddr::V4(ip) = peer.ip() else { continue };
                    let udp = self.udp.clone();
                    let backend = backend.clone();
                    let query = buf[..n].to_vec();
                    workers.spawn(async move {
                        if let Ok(answer) = backend.answer_dns(ip, &query, false).await {
                            let _ = udp.send_to(&answer, peer).await;
                        }
                    });
                }
                connection = self.tcp.accept(), if workers.len() < 128 => {
                    let (mut stream, peer) = connection?;
                    let IpAddr::V4(ip) = peer.ip() else { continue };
                    let backend = backend.clone();
                    workers.spawn(async move {
                        let _ = timeout(Duration::from_secs(30), backend.serve_dns_tcp(&mut stream, ip)).await;
                    });
                }
            }
        }
    }
}

enum Resolution {
    Local(Option<Ipv4Addr>),
    Forward(SocketAddrV4),
    Refused,
}

impl LocalDom0Backend {
    async fn serve_dns_tcp(&self, stream: &mut TcpStream, ip: Ipv4Addr) -> io::Result<()> {
        loop {
            let wire = read_frame(stream).await?;
            let answer = self.answer_dns(ip, &wire, true).await?;
            write_frame(stream, &answer).await?;
        }
    }
    async fn resolve_dns(&self, ip: Ipv4Addr, name: &str) -> Result<Resolution> {
        let routes = self.routes.lock().await;
        let Some(source) = routes.sources.get(&ip) else {
            return Ok(Resolution::Refused);
        };
        let b = self.catalog().boot(source)?;
        if b.retiring
            || b.retired
            || !self
                .present(&b, &self.inventory()?)?
                .is_some_and(|v| v.state == VmState::Running)
        {
            return Ok(Resolution::Refused);
        }
        for r in routes.for_source(&b.identity) {
            let alias = &r.virtual_service_name;
            if !alias.is_empty()
                && (name.eq_ignore_ascii_case(alias)
                    || name.eq_ignore_ascii_case(&format!("{alias}.services.agent"))
                    || name.eq_ignore_ascii_case(&format!("{alias}.{}.agent", b.scope.id)))
            {
                return Ok(Resolution::Local(Some(r.vip)));
            }
        }
        if !name.contains('.') || name.ends_with(".agent") {
            return Ok(Resolution::Local(None));
        }
        if b.scope.egress_mode == EgressMode::PublicInternet
            && let Some(upstream) = self.cfg.upstream_dns
        {
            return Ok(Resolution::Forward(upstream));
        }
        Ok(Resolution::Refused)
    }
    async fn answer_dns(&self, ip: Ipv4Addr, wire: &[u8], tcp: bool) -> io::Result<Vec<u8>> {
        let header = wire.get(..12).ok_or(io::ErrorKind::InvalidData)?;
        let mut reply = Packet::new_reply(u16::from_be_bytes([header[0], header[1]]));
        let encode = |p: &Packet<'_>| p.build_bytes_vec_compressed().map_err(io::Error::other);
        // A standard query has a single question and at most one EDNS OPT.
        if header[4..10] != [0, 1, 0, 0, 0, 0] || u16::from_be_bytes([header[10], header[11]]) > 1 {
            *reply.rcode_mut() = RCODE::FormatError;
            return encode(&reply);
        }
        let Ok(query) = Packet::parse(wire) else {
            *reply.rcode_mut() = RCODE::FormatError;
            return encode(&reply);
        };
        let q = &query.questions[0];
        if query.has_flags(PacketFlag::RESPONSE)
            || query.opcode() != OPCODE::StandardQuery
            || !q.qname.is_valid()
            || q.qclass != QCLASS::CLASS(CLASS::IN)
            || query
                .additional_records
                .iter()
                .any(|r| !matches!(r.rdata, RData::OPT(_)))
        {
            *reply.rcode_mut() = RCODE::FormatError;
            return encode(&reply);
        }
        reply.questions.push(q.clone());
        if query.has_flags(PacketFlag::RECURSION_DESIRED) {
            reply.set_flags(PacketFlag::RECURSION_DESIRED);
        }
        match self
            .resolve_dns(ip, &q.qname.to_string().to_ascii_lowercase())
            .await
        {
            Ok(Resolution::Local(vip)) => {
                reply.set_flags(PacketFlag::AUTHORITATIVE_ANSWER);
                if let Some(vip) = vip {
                    if matches!(q.qtype, QTYPE::TYPE(TYPE::A) | QTYPE::ANY) {
                        reply.answers.push(ResourceRecord::new(
                            q.qname.clone(),
                            CLASS::IN,
                            1,
                            RData::A(vip.into()),
                        ));
                    }
                } else {
                    *reply.rcode_mut() = RCODE::NameError;
                }
            }
            Ok(Resolution::Refused) => *reply.rcode_mut() = RCODE::Refused,
            Ok(Resolution::Forward(addr)) => {
                match timeout(Duration::from_secs(4), forward(addr, wire, &query, tcp)).await {
                    Ok(Ok(answer)) => return Ok(answer),
                    _ => *reply.rcode_mut() = RCODE::ServerFailure,
                }
            }
            Err(_) => *reply.rcode_mut() = RCODE::ServerFailure,
        }
        encode(&reply)
    }
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut data = vec![0; stream.read_u16().await? as usize];
    stream.read_exact(&mut data).await?;
    Ok(data)
}
async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    stream
        .write_u16(u16::try_from(data.len()).map_err(io::Error::other)?)
        .await?;
    stream.write_all(data).await
}
async fn forward(
    addr: SocketAddrV4,
    wire: &[u8],
    query: &Packet<'_>,
    tcp: bool,
) -> io::Result<Vec<u8>> {
    let response = if tcp {
        let mut s = TcpStream::connect(addr).await?;
        write_frame(&mut s, wire).await?;
        read_frame(&mut s).await?
    } else {
        let s = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
        s.connect(addr).await?;
        s.send(wire).await?;
        let mut data = vec![0; u16::MAX as usize];
        let n = s.recv(&mut data).await?;
        data.truncate(n);
        data
    };
    let packet = Packet::parse(&response).map_err(io::Error::other)?;
    let [question] = packet.questions.as_slice() else {
        return Err(io::ErrorKind::InvalidData.into());
    };
    let sent = &query.questions[0];
    if !packet.has_flags(PacketFlag::RESPONSE)
        || packet.opcode() != query.opcode()
        || packet.id() != query.id()
        || !question.qname.eq(&sent.qname)
        || question.qtype != sent.qtype
        || question.qclass != sent.qclass
    {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(response)
}

/// Select an IPv4 resolver reachable under the public egress policy.
pub fn public_resolver(config: &str) -> SocketAddrV4 {
    let ip = config
        .lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            if words.next()? != "nameserver" {
                return None;
            }
            words.next()?.parse::<Ipv4Addr>().ok()
        })
        .find(|ip| {
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_documentation()
                && !matches!(
                    ip.octets(),
                    [0 | 224..=255, ..]
                        | [100, 64..=127, ..]
                        | [192, 0, 0 | 2, _]
                        | [192, 88, 99, _]
                        | [198, 18 | 19, ..]
                )
        })
        .unwrap_or(Ipv4Addr::new(1, 1, 1, 1));
    SocketAddrV4::new(ip, 53)
}
