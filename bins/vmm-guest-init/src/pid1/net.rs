//! Static IPv4 configuration through the interface `ioctl` calls.

use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use crate::sys::cvt;

const SIOCADDRT: u32 = 0x890b;
const SIOCGIFFLAGS: u32 = 0x8913;
const SIOCSIFFLAGS: u32 = 0x8914;
const SIOCSIFADDR: u32 = 0x8916;
const SIOCSIFNETMASK: u32 = 0x891c;
const RTF_UP: libc::c_ushort = 0x1;
const RTF_GATEWAY: libc::c_ushort = 0x2;
const RTF_HOST: libc::c_ushort = 0x4;

#[repr(C)]
union IfReqData {
    address: libc::sockaddr_in,
    flags: libc::c_short,
    /// The kernel reads the full `struct ifreq`.
    size: [u8; 24],
}

#[repr(C)]
struct IfReq {
    name: [u8; libc::IFNAMSIZ],
    data: IfReqData,
}

/// `struct rtentry` of 64-bit Linux.
#[repr(C)]
struct RtEntry {
    pad1: libc::c_ulong,
    destination: libc::sockaddr_in,
    gateway: libc::sockaddr_in,
    mask: libc::sockaddr_in,
    flags: libc::c_ushort,
    pad2: libc::c_short,
    pad3: libc::c_ulong,
    pad4: *mut libc::c_void,
    metric: libc::c_short,
    device: *const libc::c_char,
    mtu: libc::c_ulong,
    window: libc::c_ulong,
    irtt: libc::c_ushort,
}

fn sockaddr(address: Ipv4Addr) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: 0,
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(address.octets()),
        },
        sin_zero: [0; 8],
    }
}

fn netmask(prefix_len: u8) -> Ipv4Addr {
    Ipv4Addr::from(
        u32::MAX
            .checked_shl(32u32.saturating_sub(prefix_len.into()))
            .unwrap_or(0),
    )
}

fn interface_name(name: &str) -> io::Result<[u8; libc::IFNAMSIZ]> {
    let mut buffer = [0u8; libc::IFNAMSIZ];
    if name.len() >= buffer.len() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    buffer[..name.len()].copy_from_slice(name.as_bytes());
    Ok(buffer)
}

fn control_socket() -> io::Result<OwnedFd> {
    // SAFETY: `socket` takes no pointers and returns a new descriptor.
    let fd = cvt(unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) })?;
    // SAFETY: the kernel returned a new descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn interface_ioctl(socket: &OwnedFd, request: u32, argument: &mut IfReq) -> io::Result<()> {
    // SAFETY: each request used here reads or writes one `struct ifreq`.
    cvt(unsafe { libc::ioctl(socket.as_raw_fd(), request as _, argument as *mut IfReq) }).map(drop)
}

/// Sets the address of an interface, when one is given, and brings it up.
pub fn configure(name: &str, address: Option<(Ipv4Addr, u8)>) -> io::Result<()> {
    let socket = control_socket()?;
    let name = interface_name(name)?;
    if let Some((address, prefix_len)) = address {
        let mut request = IfReq {
            name,
            data: IfReqData {
                address: sockaddr(address),
            },
        };
        interface_ioctl(&socket, SIOCSIFADDR, &mut request)?;
        request.data = IfReqData {
            address: sockaddr(netmask(prefix_len)),
        };
        interface_ioctl(&socket, SIOCSIFNETMASK, &mut request)?;
    }
    let mut request = IfReq {
        name,
        data: IfReqData { size: [0; 24] },
    };
    interface_ioctl(&socket, SIOCGIFFLAGS, &mut request)?;
    // SAFETY: `SIOCGIFFLAGS` filled the flags member of the union.
    let flags = unsafe { request.data.flags };
    request.data.flags = flags | (libc::IFF_UP | libc::IFF_RUNNING) as libc::c_short;
    interface_ioctl(&socket, SIOCSIFFLAGS, &mut request)
}

fn add_route(
    socket: &OwnedFd,
    device: &[u8; libc::IFNAMSIZ],
    destination: Ipv4Addr,
    mask: Ipv4Addr,
    gateway: Option<Ipv4Addr>,
) -> io::Result<()> {
    let mut flags = RTF_UP;
    if gateway.is_some() {
        flags |= RTF_GATEWAY;
    }
    if mask == Ipv4Addr::BROADCAST {
        flags |= RTF_HOST;
    }
    let route = RtEntry {
        pad1: 0,
        destination: sockaddr(destination),
        gateway: sockaddr(gateway.unwrap_or(Ipv4Addr::UNSPECIFIED)),
        mask: sockaddr(mask),
        flags,
        pad2: 0,
        pad3: 0,
        pad4: std::ptr::null_mut(),
        metric: 0,
        device: device.as_ptr().cast(),
        mtu: 0,
        window: 0,
        irtt: 0,
    };
    // SAFETY: `SIOCADDRT` reads one `struct rtentry` whose device name is NUL-terminated.
    let result =
        unsafe { libc::ioctl(socket.as_raw_fd(), SIOCADDRT as _, &route as *const RtEntry) };
    match cvt(result) {
        Err(error) if error.raw_os_error() != Some(libc::EEXIST) => Err(error),
        _ => Ok(()),
    }
}

/// Adds the default route. A gateway outside the interface network gets a host route first.
pub fn add_default_route(
    name: &str,
    address: Ipv4Addr,
    prefix_len: u8,
    gateway: Ipv4Addr,
) -> io::Result<()> {
    let socket = control_socket()?;
    let device = interface_name(name)?;
    let mask = u32::from(netmask(prefix_len));
    if u32::from(address) & mask != u32::from(gateway) & mask {
        add_route(&socket, &device, gateway, Ipv4Addr::BROADCAST, None)?;
    }
    add_route(
        &socket,
        &device,
        Ipv4Addr::UNSPECIFIED,
        Ipv4Addr::UNSPECIFIED,
        Some(gateway),
    )
}

pub fn set_hostname(hostname: &str) -> io::Result<()> {
    // SAFETY: the pointer and the length describe the bytes of `hostname`.
    cvt(unsafe { libc::sethostname(hostname.as_ptr().cast(), hostname.len()) }).map(drop)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_layouts_match_the_kernel_structures() {
        assert_eq!(size_of::<IfReq>(), 40);
        assert_eq!(size_of::<RtEntry>(), 120);
        assert_eq!(std::mem::offset_of!(RtEntry, flags), 56);
        assert_eq!(std::mem::offset_of!(RtEntry, device), 88);
        assert_eq!(netmask(16), Ipv4Addr::new(255, 255, 0, 0));
        assert_eq!(netmask(32), Ipv4Addr::BROADCAST);
        assert_eq!(
            sockaddr(Ipv4Addr::new(10, 77, 0, 1))
                .sin_addr
                .s_addr
                .to_ne_bytes(),
            [10, 77, 0, 1]
        );
        assert!(interface_name("an-interface-name-that-is-too-long").is_err());
    }
}
