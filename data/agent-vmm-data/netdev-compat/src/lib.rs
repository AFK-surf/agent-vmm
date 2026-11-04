#[cfg(not(target_os = "macos"))]
pub use netdev_upstream::*;

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub use macos::*;

#[cfg(target_os = "macos")]
pub mod interface {
    pub use crate::get_interfaces;
    pub mod ipv6_addr_flags {
        pub use crate::Ipv6AddrFlags;
    }
}

#[cfg(target_os = "macos")]
pub mod net {
    pub mod ip {
        pub use crate::get_local_ipaddr;
    }
}
