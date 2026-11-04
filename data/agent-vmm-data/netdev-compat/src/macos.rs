use std::{
    collections::BTreeMap,
    ffi::CStr,
    mem::{offset_of, size_of},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, UdpSocket},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use ipnet::{Ipv4Net, Ipv6Net};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Ipv6AddrFlags {
    pub deprecated: bool,
    pub temporary: bool,
    pub tentative: bool,
    pub duplicated: bool,
    pub permanent: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MacAddr([u8; 6]);
impl MacAddr {
    pub fn octets(&self) -> [u8; 6] {
        self.0
    }
}

#[derive(Clone, Debug, Default)]
pub struct Interface {
    pub name: String,
    pub index: u32,
    pub flags: u32,
    pub mac_addr: Option<MacAddr>,
    pub ipv4: Vec<Ipv4Net>,
    pub ipv6: Vec<Ipv6Net>,
    pub ipv6_scope_ids: Vec<u32>,
    pub ipv6_addr_flags: Vec<Ipv6AddrFlags>,
}

struct IfAddrs(*mut libc::ifaddrs);
impl Drop for IfAddrs {
    fn drop(&mut self) {
        // SAFETY: getifaddrs allocated this list, and this owner frees it once.
        unsafe { libc::freeifaddrs(self.0) };
    }
}

pub fn get_interfaces() -> Vec<Interface> {
    let mut head = std::ptr::null_mut();
    // SAFETY: head is a valid output pointer.
    if unsafe { libc::getifaddrs(&mut head) } != 0 {
        return Vec::new();
    }
    let addrs = IfAddrs(head);
    let socket = ipv6_socket();
    // SAFETY: the owned getifaddrs list remains valid for this traversal.
    unsafe { collect_interfaces(addrs.0, socket.as_ref()) }
}

unsafe fn collect_interfaces(
    mut next: *const libc::ifaddrs,
    socket: Option<&OwnedFd>,
) -> Vec<Interface> {
    let mut interfaces = BTreeMap::<String, Interface>::new();
    while !next.is_null() {
        // SAFETY: the caller supplies a valid, linked getifaddrs list.
        let entry = unsafe { &*next };
        next = entry.ifa_next;
        if entry.ifa_name.is_null() {
            continue;
        }
        // SAFETY: getifaddrs names are NUL-terminated and live with the list.
        let name = unsafe { CStr::from_ptr(entry.ifa_name) };
        let iface = interfaces
            .entry(name.to_string_lossy().into_owned())
            .or_insert_with(|| {
                Interface {
                    name: name.to_string_lossy().into_owned(),
                    // SAFETY: name is a valid interface name from getifaddrs.
                    index: unsafe { libc::if_nametoindex(name.as_ptr()) },
                    flags: entry.ifa_flags,
                    ..Interface::default()
                }
            });
        // SAFETY: addresses and masks are owned by this getifaddrs list.
        unsafe { add_address(iface, entry, socket) };
    }
    interfaces.into_values().collect()
}

unsafe fn add_address(iface: &mut Interface, entry: &libc::ifaddrs, socket: Option<&OwnedFd>) {
    if entry.ifa_addr.is_null() {
        return;
    }
    // SAFETY: getifaddrs provides at least the sockaddr header at this pointer.
    let family = unsafe { (*entry.ifa_addr).sa_family } as i32;
    // SAFETY: sa_len specifies the available bytes for this Darwin sockaddr.
    let address = unsafe { sockaddr_bytes(entry.ifa_addr) };
    // SAFETY: the optional mask follows the same getifaddrs lifetime.
    let mask = unsafe { sockaddr_bytes(entry.ifa_netmask) };
    match family {
        libc::AF_INET => {
            let Some(raw) = field::<4>(address, offset_of!(libc::sockaddr_in, sin_addr)) else {
                return;
            };
            let mask = mask_field::<4>(mask, offset_of!(libc::sockaddr_in, sin_addr));
            if let Ok(net) = Ipv4Net::with_netmask(raw.into(), mask.into()) {
                if !iface.ipv4.contains(&net) {
                    iface.ipv4.push(net);
                }
            }
        }
        libc::AF_INET6 => {
            let Some(raw) = field::<16>(address, offset_of!(libc::sockaddr_in6, sin6_addr)) else {
                return;
            };
            let scope = field::<4>(address, offset_of!(libc::sockaddr_in6, sin6_scope_id))
                .map(u32::from_ne_bytes)
                .unwrap_or(0);
            let ip = Ipv6Addr::from(raw);
            let scope = if scope == 0 && ip.is_unicast_link_local() {
                iface.index
            } else {
                scope
            };
            let mask = mask_field::<16>(mask, offset_of!(libc::sockaddr_in6, sin6_addr));
            if let Ok(net) = Ipv6Net::with_netmask(ip, mask.into()) {
                if !iface.ipv6.contains(&net) {
                    iface.ipv6.push(net);
                    iface.ipv6_scope_ids.push(scope);
                    iface
                        .ipv6_addr_flags
                        .push(ipv6_flags(socket, &iface.name, ip, scope));
                }
            }
        }
        libc::AF_LINK => {
            let offset = offset_of!(libc::sockaddr_dl, sdl_data);
            if let (Some(&name_len), Some(&mac_len)) = (
                address.get(offset_of!(libc::sockaddr_dl, sdl_nlen)),
                address.get(offset_of!(libc::sockaddr_dl, sdl_alen)),
            ) {
                if mac_len < 6 {
                    return;
                }
                if let Some(mac) = field::<6>(address, offset + usize::from(name_len)) {
                    iface.mac_addr = Some(MacAddr(mac));
                }
            }
        }
        _ => {}
    }
}

unsafe fn sockaddr_bytes<'a>(address: *const libc::sockaddr) -> &'a [u8] {
    if address.is_null() {
        return &[];
    }
    // SAFETY: the caller supplies a Darwin sockaddr whose sa_len owns this extent.
    unsafe { std::slice::from_raw_parts(address.cast(), usize::from((*address).sa_len)) }
}

fn field<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes.get(offset..offset + N)?.try_into().ok()
}

fn mask_field<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    let mut result = [0; N];
    // BSD masks can omit trailing zero bytes and can have sa_family == AF_UNSPEC.
    if let Some(bytes) = bytes.get(offset..) {
        let count = bytes.len().min(N);
        result[..count].copy_from_slice(&bytes[..count]);
    }
    result
}

fn ipv6_socket() -> Option<OwnedFd> {
    // SAFETY: socket has no borrowed inputs and returns a newly owned descriptor.
    let fd = unsafe { libc::socket(libc::AF_INET6, libc::SOCK_DGRAM, 0) };
    // SAFETY: a successful socket call returns an owned descriptor.
    (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) })
}

fn ipv6_flags(socket: Option<&OwnedFd>, name: &str, ip: Ipv6Addr, scope: u32) -> Ipv6AddrFlags {
    let Some(socket) = socket else {
        return Ipv6AddrFlags::default();
    };
    // SAFETY: zero is valid for all in6_ifreq fields and the union's sockaddr view.
    let mut request: libc::in6_ifreq = unsafe { std::mem::zeroed() };
    for (output, input) in request.ifr_name.iter_mut().zip(name.bytes()) {
        *output = input as libc::c_char;
    }
    let mut address: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
    address.sin6_len = size_of::<libc::sockaddr_in6>() as u8;
    address.sin6_family = libc::AF_INET6 as u8;
    address.sin6_addr.s6_addr = ip.octets();
    address.sin6_scope_id = scope;
    request.ifr_ifru.ifru_addr = address;
    // <netinet6/in6_var.h>: SIOCGIFAFLAG_IN6, absent from libc's constants.
    // SAFETY: the socket and request have the types required by this ioctl.
    if unsafe {
        libc::ioctl(
            socket.as_raw_fd(),
            0xC1206949 as libc::c_ulong,
            &mut request,
        )
    } < 0
    {
        return Ipv6AddrFlags::default();
    }
    // SAFETY: a successful SIOCGIFAFLAG_IN6 writes ifru_flags6.
    decode_ipv6_flags(unsafe { request.ifr_ifru.ifru_flags6 } as u32)
}

fn decode_ipv6_flags(raw: u32) -> Ipv6AddrFlags {
    Ipv6AddrFlags {
        deprecated: raw & 0x10 != 0,
        temporary: raw & 0x80 != 0,
        tentative: raw & 0x02 != 0,
        duplicated: raw & 0x04 != 0,
        permanent: false,
    }
}

pub fn get_local_ipaddr() -> Option<IpAddr> {
    [
        (
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V4(Ipv4Addr::new(10, 254, 254, 254)),
        ),
        (
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::new(
                0xfdff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
            )),
        ),
    ]
    .into_iter()
    .find_map(|(local, destination)| {
        let socket = UdpSocket::bind((local, 0)).ok()?;
        socket.connect((destination, 1)).ok()?;
        Some(socket.local_addr().ok()?.ip())
    })
}

#[cfg(test)]
mod tests;
