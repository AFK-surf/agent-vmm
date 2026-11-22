use super::*;

fn add<T>(iface: &mut Interface, address: &T, mask: Option<&[u8]>) {
    // SAFETY: zero initializes the optional pointers, which are valid for add_address.
    let mut entry: libc::ifaddrs = unsafe { std::mem::zeroed() };
    entry.ifa_addr = std::ptr::from_ref(address).cast_mut().cast();
    entry.ifa_netmask = mask.map_or(std::ptr::null_mut(), |mask| mask.as_ptr().cast_mut().cast());
    // SAFETY: address and mask outlive this call and provide valid Darwin sockaddr bytes.
    unsafe { add_address(iface, &entry, None) };
}

fn v6(ip: &str, scope: u32) -> libc::sockaddr_in6 {
    // SAFETY: all-zero sockaddr_in6 is valid before its fields are populated.
    let mut address: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
    address.sin6_len = size_of::<libc::sockaddr_in6>() as u8;
    address.sin6_family = libc::AF_INET6 as u8;
    address.sin6_addr.s6_addr = ip.parse::<Ipv6Addr>().unwrap().octets();
    address.sin6_scope_id = scope;
    address
}

#[test]
fn address_facts_preserve_masks_scopes_deduplication_and_mac() {
    let mut iface = Interface {
        index: 7,
        ..Interface::default()
    };
    // SAFETY: zero is a valid sockaddr_in initialization.
    let mut v4: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    v4.sin_len = size_of::<libc::sockaddr_in>() as u8;
    v4.sin_family = libc::AF_INET as u8;
    v4.sin_addr.s_addr = u32::from_ne_bytes([192, 0, 2, 9]);
    // Compact BSD masks omit trailing zeroes and can use AF_UNSPEC.
    let mask4 = [7, 0, 0, 0, 255, 255, 255];
    add(&mut iface, &v4, Some(&mask4));
    add(&mut iface, &v4, Some(&mask4));
    assert_eq!(iface.ipv4, ["192.0.2.9/24".parse::<Ipv4Net>().unwrap()]);

    let mask6 = [
        16, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255,
    ];
    let link = v6("fe80::1", 9);
    add(&mut iface, &link, Some(&mask6));
    add(&mut iface, &link, Some(&mask6));
    add(&mut iface, &v6("fe80::2", 0), Some(&mask6));
    add(&mut iface, &v6("2001:db8::1", 0), Some(&mask6));
    assert_eq!(
        iface.ipv6,
        ["fe80::1/64", "fe80::2/64", "2001:db8::1/64"].map(|s| s.parse::<Ipv6Net>().unwrap())
    );
    assert_eq!(iface.ipv6_scope_ids, [9, 7, 0]);
    assert_eq!(iface.ipv6_addr_flags, [Ipv6AddrFlags::default(); 3]);

    // A nine-byte BSD name exceeds sockaddr_dl.sdl_data's nominal capacity with a MAC.
    let mut link = [0_u8; 32];
    link[0] = 23;
    link[1] = libc::AF_LINK as u8;
    link[offset_of!(libc::sockaddr_dl, sdl_nlen)] = 9;
    link[offset_of!(libc::sockaddr_dl, sdl_alen)] = 6;
    link[8..17].copy_from_slice(b"bridge100");
    link[17..23].copy_from_slice(&[2, 3, 4, 5, 6, 7]);
    add(&mut iface, &link, None);
    assert_eq!(iface.mac_addr.unwrap().octets(), [2, 3, 4, 5, 6, 7]);
}

#[test]
fn absent_or_short_addresses_do_not_drop_the_interface_or_misalign_ipv6() {
    let mut iface = Interface {
        index: 1,
        ..Interface::default()
    };
    let short = [2, libc::AF_INET6 as u8];
    add(&mut iface, &short, None);
    // SAFETY: a null optional ifa_addr is valid input.
    let entry = unsafe { std::mem::zeroed() };
    // SAFETY: entry has no address and remains valid for the call.
    unsafe { add_address(&mut iface, &entry, None) };
    assert!(iface.ipv6.is_empty());
    assert!(iface.ipv6_scope_ids.is_empty());
    assert!(iface.ipv6_addr_flags.is_empty());
    add(&mut iface, &v6("::1", 0), None);
    assert_eq!(iface.ipv6[0], "::1/0".parse::<Ipv6Net>().unwrap());
    assert_eq!(iface.ipv6_scope_ids, [0]);
}

#[test]
fn darwin_ipv6_address_states_map_to_netwatch_flags() {
    assert_eq!(decode_ipv6_flags(0), Ipv6AddrFlags::default());
    assert_eq!(
        decode_ipv6_flags(0x96),
        Ipv6AddrFlags {
            deprecated: true,
            temporary: true,
            tentative: true,
            duplicated: true,
            permanent: false,
        }
    );
    assert!(decode_ipv6_flags(0x80).temporary);
    assert!(!decode_ipv6_flags(0x80).deprecated);
}

#[test]
fn live_interfaces_keep_bsd_identity_loopback_and_address_metadata() {
    let interfaces = get_interfaces();
    let loopback = interfaces.iter().find(|iface| iface.name == "lo0").unwrap();
    assert_ne!(loopback.index, 0);
    assert_ne!(loopback.flags & libc::IFF_LOOPBACK as u32, 0);
    assert!(loopback.ipv4.iter().any(|ip| ip.addr().is_loopback()));
    assert!(loopback.ipv6.iter().any(|ip| ip.addr().is_loopback()));
    for iface in interfaces {
        assert_eq!(iface.ipv6.len(), iface.ipv6_scope_ids.len());
        assert_eq!(iface.ipv6.len(), iface.ipv6_addr_flags.len());
    }
    let socket = ipv6_socket().unwrap();
    assert_eq!(
        ipv6_flags(Some(&socket), "lo0", Ipv6Addr::LOCALHOST, 0),
        Ipv6AddrFlags::default()
    );
}
