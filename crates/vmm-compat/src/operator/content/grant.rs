//! Compare approved network, system and tenant permissions with the immutable manifest.
use super::{a, h};
use std::{
    collections::{HashMap, HashSet},
    net::Ipv4Addr,
};
use tonic::Status;

fn within(small: Option<&h::ResourceLimits>, large: Option<&h::ResourceLimits>) -> bool {
    let (Some(small), Some(large)) = (small, large) else {
        return false;
    };
    let pids = small.pids.unwrap_or(0);
    let disk = small.disk_bytes.unwrap_or(0);
    pids > 0 && pids <= large.pids.unwrap_or(0) && disk > 0 && disk <= large.disk_bytes.unwrap_or(0)
}
fn frontend(a: &a::ControllerFrontend, r: &a::ControllerFrontend) -> bool {
    if a.transport != r.transport || a.exposure != r.exposure {
        return false;
    }
    let (Some(a), Some(r)) = (&a.bounds, &r.bounds) else {
        return false;
    };
    a.max_frame_bytes <= r.max_frame_bytes
        && (r.max_total_bytes == 0
            || (a.max_total_bytes != 0 && a.max_total_bytes <= r.max_total_bytes))
        && a.deadline_seconds <= r.deadline_seconds
        && a.idle_seconds <= r.idle_seconds
        && a.max_concurrent_streams <= r.max_concurrent_streams
}
pub(super) fn approved(
    m: &a::ControllerApplianceManifest,
    g: &a::ApprovedGrant,
    workload_disk: u64,
) -> Result<(), Status> {
    let bad = || Status::failed_precondition("approved grant exceeds manifest");
    let capacity = g.capacity.as_ref().ok_or_else(bad)?;
    let requested = m.requested_capacity.as_ref().ok_or_else(bad)?;
    if !within(g.system_limits.as_ref(), m.system_limits.as_ref())
        || !within(
            capacity.per_environment_limits.as_ref(),
            requested.per_environment_limits.as_ref(),
        )
        || g.lan_egress && !m.requested_lan_egress
        || g.host_tcp_endpoints.len() > 64
        || h::EgressMode::try_from(g.egress_mode).is_err()
        || h::EgressMode::try_from(capacity.max_egress_mode).is_err()
        || g.egress_mode > m.requested_egress_mode
        || capacity.max_egress_mode > requested.max_egress_mode
    {
        return Err(bad());
    }
    for (limits, disk) in [
        (g.system_limits.as_ref(), 4 << 30),
        (capacity.per_environment_limits.as_ref(), workload_disk),
    ] {
        if !within(
            limits,
            Some(&h::ResourceLimits {
                pids: Some(2048),
                disk_bytes: Some(disk),
            }),
        ) {
            return Err(Status::resource_exhausted(
                "approved resources exceed VM capacity",
            ));
        }
    }
    let mut endpoints = HashSet::new();
    for e in &g.host_tcp_endpoints {
        let addr = e.ip.parse::<Ipv4Addr>().map_err(|_| bad())?;
        if addr.is_unspecified()
            || addr.is_multicast()
            || addr.is_loopback()
            || addr.is_link_local()
            || addr.is_broadcast()
            || !(1..=65535).contains(&e.port)
            || !endpoints.insert((addr, e.port))
        {
            return Err(bad());
        }
    }
    let mut requested = HashMap::new();
    for f in &m.requested_frontends {
        if f.name.is_empty()
            || requested.insert(f.name.as_str(), f).is_some()
            || f.exposure() == a::FrontendExposure::LanHttp
                && f.transport() != a::FrontendTransport::Http
        {
            return Err(bad());
        }
    }
    for f in &g.frontends {
        if !requested
            .remove(f.name.as_str())
            .is_some_and(|r| frontend(f, r))
        {
            return Err(bad());
        }
    }
    Ok(())
}
