use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::{Ipv4Addr, SocketAddrV4},
    process::{Command, Stdio},
    sync::Mutex,
};
use vmm_core::{EgressMode, Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub mode: EgressMode,
    pub dns: Option<SocketAddrV4>,
    pub local_ports: BTreeSet<SocketAddrV4>,
}

pub trait FirewallDriver: Send + Sync {
    fn replace(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> Result<()>;
}

#[derive(Default)]
pub struct MockFirewallDriver(pub Mutex<BTreeMap<Ipv4Addr, Policy>>);

impl FirewallDriver for MockFirewallDriver {
    fn replace(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> Result<()> {
        *self.0.lock().map_err(|_| Error::Interrupted)? = policies.clone();
        Ok(())
    }
}

pub struct NftablesDriver {
    interface: String,
}

impl NftablesDriver {
    pub fn new(interface: String) -> Result<Self> {
        if interface.is_empty()
            || interface.len() > 15
            || !interface
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        {
            return Err(Error::Invalid("guest gateway interface"));
        }
        Ok(Self { interface })
    }

    fn rules(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> String {
        use std::fmt::Write;
        let iface = &self.interface;
        let mut out = format!(
            "add table inet vmm_dom0\ndelete table inet vmm_dom0\ntable inet vmm_dom0 {{\n chain input {{ type filter hook input priority -5; policy accept;\n iifname != \"{iface}\" return\n ct direction reply ct state established accept\n"
        );
        for (src, policy) in policies {
            if let Some(dns) = policy.dns {
                writeln!(
                    out,
                    " ip saddr {src} ip daddr {} meta l4proto {{ tcp, udp }} th dport {} accept",
                    dns.ip(),
                    dns.port()
                )
                .unwrap();
            }
            for endpoint in &policy.local_ports {
                writeln!(
                    out,
                    " ip saddr {src} ip daddr {} tcp dport {} accept",
                    endpoint.ip(),
                    endpoint.port()
                )
                .unwrap();
            }
        }
        writeln!(out," drop\n }}\n chain forward {{ type filter hook forward priority -5; policy accept;\n iifname \"{iface}\" oifname \"{iface}\" drop").unwrap();
        writeln!(out," iifname \"{iface}\" ip daddr {{ 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.0.2.0/24, 192.88.99.0/24, 192.168.0.0/16, 198.18.0.0/15, 198.51.100.0/24, 203.0.113.0/24, 224.0.0.0/4, 240.0.0.0/4 }} drop").unwrap();
        for (src, policy) in policies {
            if policy.mode == EgressMode::PublicInternet {
                writeln!(out," iifname \"{iface}\" ip saddr {src} accept\n oifname \"{iface}\" ip daddr {src} ct direction reply ct state established,related accept").unwrap();
            }
        }
        writeln!(out," iifname \"{iface}\" drop\n oifname \"{iface}\" drop\n }}\n chain nat {{ type nat hook postrouting priority srcnat; policy accept;\n iifname \"{iface}\" ip saddr 10.77.0.0/16 masquerade\n }}\n}}").unwrap();
        out
    }
}

impl FirewallDriver for NftablesDriver {
    fn replace(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> Result<()> {
        let mut child = Command::new("nft")
            .args(["-f", "-"])
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let sent = child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(self.rules(policies).as_bytes());
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
            .into());
        }
        Ok(sent?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dns_rule_is_scoped_to_the_gateway_and_active_slot() {
        let ip = Ipv4Addr::new(10, 77, 0, 2);
        let mut policies = BTreeMap::from([(
            ip,
            Policy {
                mode: EgressMode::DenyAll,
                local_ports: BTreeSet::new(),
                dns: Some(SocketAddrV4::new(Ipv4Addr::new(10, 77, 0, 1), 53)),
            },
        )]);
        let fw = NftablesDriver::new("guest0".into()).unwrap();
        let rule =
            "ip saddr 10.77.0.2 ip daddr 10.77.0.1 meta l4proto { tcp, udp } th dport 53 accept";
        assert!(fw.rules(&policies).contains(rule));
        policies.get_mut(&ip).unwrap().dns = None;
        assert!(!fw.rules(&policies).contains("th dport 53"));
    }
}
