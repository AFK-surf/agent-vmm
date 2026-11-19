//! Kernel command-line parameters that PID 1 needs before it has a network.

use std::net::Ipv4Addr;

use vmm_guest_proto::boot::{CMDLINE_BOOT_PORT, CMDLINE_SLOT, DEFAULT_BOOT_PORT, slot_address};

#[derive(Debug, PartialEq, Eq)]
pub struct Cmdline {
    /// Static address of the slot on the dom0 switch.
    pub address: Ipv4Addr,
    pub boot_port: u16,
}

pub fn parse(cmdline: &str) -> Result<Cmdline, String> {
    let value = |key: &str| {
        let mut values = cmdline
            .split_ascii_whitespace()
            .filter_map(|word| word.strip_prefix(key)?.strip_prefix('='));
        values.next_back()
    };
    let slot =
        value(CMDLINE_SLOT).ok_or(format!("the kernel command line has no {CMDLINE_SLOT}"))?;
    let address = slot.parse().ok().and_then(slot_address);
    let boot_port = match value(CMDLINE_BOOT_PORT) {
        None => Some(DEFAULT_BOOT_PORT),
        Some(port) => port.parse().ok().filter(|port| *port != 0),
    };
    Ok(Cmdline {
        address: address.ok_or(format!("invalid {CMDLINE_SLOT}={slot}"))?,
        boot_port: boot_port.ok_or(format!("invalid {CMDLINE_BOOT_PORT}"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_slot_address_and_the_boot_port() {
        let parsed = parse("console=hvc0 vmm.slot=258 quiet vmm.boot_port=9000").unwrap();
        assert_eq!(
            parsed,
            Cmdline {
                address: Ipv4Addr::new(10, 77, 1, 4),
                boot_port: 9000
            }
        );
        assert_eq!(parse("vmm.slot=0").unwrap().boot_port, DEFAULT_BOOT_PORT);
        for invalid in [
            "console=hvc0",
            "vmm.slot=x",
            "vmm.slot=70000",
            "vmm.slot=1 vmm.boot_port=0",
        ] {
            assert!(parse(invalid).is_err(), "{invalid}");
        }
    }
}
