use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    net::{Ipv4Addr, SocketAddrV4},
    process::{Command, Stdio},
    sync::Mutex,
    time::{Duration, Instant},
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
    fn allow_fake_ip(&self, _ip: Ipv4Addr, _ttl: Duration) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct MockFirewallDriver(
    pub Mutex<BTreeMap<Ipv4Addr, Policy>>,
    pub Mutex<BTreeMap<Ipv4Addr, Duration>>,
);

impl FirewallDriver for MockFirewallDriver {
    fn replace(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> Result<()> {
        *self.0.lock().map_err(|_| Error::Interrupted)? = policies.clone();
        Ok(())
    }
    fn allow_fake_ip(&self, ip: Ipv4Addr, ttl: Duration) -> Result<()> {
        self.1.lock().map_err(|_| Error::Interrupted)?.insert(ip, ttl);
        Ok(())
    }
}

#[derive(Default)]
struct NftState {
    policies: BTreeMap<Ipv4Addr, Policy>,
    fake_ips: BTreeMap<Ipv4Addr, Instant>,
}

pub struct NftablesDriver {
    interface: String,
    state: Mutex<NftState>,
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
        Ok(Self {
            interface,
            state: Mutex::new(NftState::default()),
        })
    }

    fn rules(
        &self,
        policies: &BTreeMap<Ipv4Addr, Policy>,
        fake_ips: &BTreeMap<Ipv4Addr, Instant>,
        now: Instant,
    ) -> String {
        use std::fmt::Write;
        let iface = &self.interface;
        let mut out = String::from(
            "add table inet vmm_dom0\ndelete table inet vmm_dom0\ntable inet vmm_dom0 {\n set resolved_fake_ips {\n  type ipv4_addr\n  flags timeout\n",
        );
        let live_fake: Vec<_> = fake_ips
            .iter()
            .filter_map(|(&ip, &deadline)| {
                deadline
                    .checked_duration_since(now)
                    .filter(|rem| !rem.is_zero())
                    .map(|rem| (ip, rem.as_secs().max(1)))
            })
            .collect();
        if !live_fake.is_empty() {
            write!(out, "  elements = {{ ").unwrap();
            for (i, (ip, secs)) in live_fake.iter().enumerate() {
                if i > 0 {
                    write!(out, ", ").unwrap();
                }
                write!(out, "{ip} timeout {secs}s").unwrap();
            }
            writeln!(out, " }}").unwrap();
        }
        writeln!(
            out,
            " }}\n chain input {{ type filter hook input priority -5; policy accept;\n iifname != \"{iface}\" return\n ct direction reply ct state established accept"
        )
        .unwrap();
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
        writeln!(
            out,
            " drop\n }}\n chain forward {{ type filter hook forward priority -5; policy accept;\n iifname \"{iface}\" oifname \"{iface}\" drop"
        )
        .unwrap();
        for (src, policy) in policies {
            if policy.mode == EgressMode::PublicInternet {
                writeln!(
                    out,
                    " iifname \"{iface}\" ip saddr {src} ip daddr @resolved_fake_ips accept\n oifname \"{iface}\" ip daddr {src} ct direction reply ct state established,related accept"
                )
                .unwrap();
            }
        }
        writeln!(
            out,
            " iifname \"{iface}\" ip daddr {{ 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.0.0.0/24, 192.0.2.0/24, 192.88.99.0/24, 192.168.0.0/16, 198.18.0.0/15, 198.51.100.0/24, 203.0.113.0/24, 224.0.0.0/4, 240.0.0.0/4 }} drop"
        )
        .unwrap();
        for (src, policy) in policies {
            if policy.mode == EgressMode::PublicInternet {
                writeln!(out, " iifname \"{iface}\" ip saddr {src} accept").unwrap();
            }
        }
        writeln!(
            out,
            " iifname \"{iface}\" drop\n oifname \"{iface}\" drop\n }}\n chain nat {{ type nat hook postrouting priority srcnat; policy accept;\n iifname \"{iface}\" oifname != \"{iface}\" ip saddr 10.77.0.0/16 masquerade\n }}\n}}"
        )
        .unwrap();
        out
    }

    fn apply_script(script: &str) -> Result<()> {
        let mut child = Command::new("nft")
            .args(["-f", "-"])
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let sent = child
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(script.as_bytes());
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(std::io::Error::other(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            )
            .into());
        }
        Ok(sent?)
    }
    fn record_fake_ip(state: &mut NftState, ip: Ipv4Addr, ttl: Duration, now: Instant) -> bool {
        state.fake_ips.retain(|_, deadline| *deadline > now);
        let secs = ttl.as_secs().max(1);
        let half_life = Duration::from_secs(secs / 2);
        if state
            .fake_ips
            .get(&ip)
            .and_then(|deadline| deadline.checked_duration_since(now))
            .is_some_and(|remaining| remaining >= half_life)
        {
            return false;
        }
        state.fake_ips.insert(ip, now + Duration::from_secs(secs));
        true
    }
}

impl FirewallDriver for NftablesDriver {
    fn replace(&self, policies: &BTreeMap<Ipv4Addr, Policy>) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::Interrupted)?;
        let now = Instant::now();
        state.fake_ips.retain(|_, deadline| *deadline > now);
        state.policies = policies.clone();
        let script = self.rules(&state.policies, &state.fake_ips, now);
        Self::apply_script(&script)
    }

    fn allow_fake_ip(&self, ip: Ipv4Addr, ttl: Duration) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| Error::Interrupted)?;
        let now = Instant::now();
        if !Self::record_fake_ip(&mut state, ip, ttl, now) {
            return Ok(());
        }
        let script = self.rules(&state.policies, &state.fake_ips, now);
        Self::apply_script(&script)
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
        let now = Instant::now();
        let rule =
            "ip saddr 10.77.0.2 ip daddr 10.77.0.1 meta l4proto { tcp, udp } th dport 53 accept";
        assert!(fw.rules(&policies, &BTreeMap::new(), now).contains(rule));
        policies.get_mut(&ip).unwrap().dns = None;
        assert!(
            !fw.rules(&policies, &BTreeMap::new(), now)
                .contains("th dport 53")
        );
    }

    #[test]
    fn resolved_fake_ips_allowed_before_bogon_drop_only_for_public_internet() {
        let public_src = Ipv4Addr::new(10, 77, 0, 2);
        let denied_src = Ipv4Addr::new(10, 77, 0, 3);
        let policies = BTreeMap::from([
            (
                public_src,
                Policy {
                    mode: EgressMode::PublicInternet,
                    local_ports: BTreeSet::new(),
                    dns: None,
                },
            ),
            (
                denied_src,
                Policy {
                    mode: EgressMode::DenyAll,
                    local_ports: BTreeSet::new(),
                    dns: None,
                },
            ),
        ]);
        let fw = NftablesDriver::new("guest0".into()).unwrap();
        let now = Instant::now();
        let fake_ips = BTreeMap::from([
            (Ipv4Addr::new(198, 18, 14, 184), now + Duration::from_secs(300)),
            (Ipv4Addr::new(198, 18, 14, 185), now),
        ]);
        let script = fw.rules(&policies, &fake_ips, now);
        assert!(script.contains("elements = { 198.18.14.184 timeout 300s }"));
        assert!(!script.contains("198.18.14.185"));
        let allow_pos = script
            .find("iifname \"guest0\" ip saddr 10.77.0.2 ip daddr @resolved_fake_ips accept")
            .expect("public source fake-ip allow");
        let bogon_pos = script
            .find("198.18.0.0/15")
            .expect("bogon drop contains 198.18.0.0/15");
        assert!(allow_pos < bogon_pos);
        assert!(!script.contains("ip saddr 10.77.0.3 ip daddr @resolved_fake_ips"));
        assert!(
            script.contains("iifname \"guest0\" oifname != \"guest0\" ip saddr 10.77.0.0/16 masquerade")
        );
    }

    #[test]
    fn fake_ip_refresh_skips_within_half_life_and_renews_near_expiry() {
        let ip = Ipv4Addr::new(198, 18, 14, 184);
        let ttl = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut state = NftState::default();

        // First insert must trigger an nft apply.
        assert!(NftablesDriver::record_fake_ip(&mut state, ip, ttl, t0));
        assert_eq!(state.fake_ips.get(&ip), Some(&(t0 + ttl)));

        // Within half-life (t=10s, remaining 50s >= 30s): skip nft apply.
        assert!(!NftablesDriver::record_fake_ip(
            &mut state,
            ip,
            ttl,
            t0 + Duration::from_secs(10),
        ));
        assert_eq!(state.fake_ips.get(&ip), Some(&(t0 + ttl)));

        // Past half-life (t=31s, remaining 29s < 30s): refresh deadline and trigger nft apply.
        let t_refresh = t0 + Duration::from_secs(31);
        assert!(NftablesDriver::record_fake_ip(&mut state, ip, ttl, t_refresh));
        assert_eq!(state.fake_ips.get(&ip), Some(&(t_refresh + ttl)));
    }
}
