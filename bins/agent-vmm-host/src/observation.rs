use agent_vmm_proto::{host::v1::HealthState, remote::v1 as r};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, watch},
    time,
};
use vmm_compat::operator::{DiskSample, LocalOperator};
use vmm_remote_salix::Observation;

const ACTIVE_PERIOD: Duration = Duration::from_secs(5);
const IDLE_PERIOD: Duration = Duration::from_secs(30);

pub(super) async fn refresh(
    api: &LocalOperator,
    observations: watch::Sender<Observation>,
    has_active: impl Fn() -> bool,
    wake: Arc<Notify>,
) {
    loop {
        let period = if has_active() {
            ACTIVE_PERIOD
        } else {
            IDLE_PERIOD
        };
        let woke = tokio::select! {
            _ = time::sleep(period) => false,
            _ = wake.notified() => true,
        };
        if woke && !has_active() {
            continue;
        }
        observations.send_replace(sample(api).await);
    }
}

pub(super) async fn sample(api: &LocalOperator) -> Observation {
    let (health, disk, configured) = api.remote_observation().await;
    project(health, disk, configured)
}

fn project(health: HealthState, disk: Option<DiskSample>, configured: bool) -> Observation {
    let (status, issue, message) = match health {
        HealthState::Healthy if disk.is_some() || !configured => ("healthy", "", ""),
        HealthState::Healthy | HealthState::Degraded => {
            ("degraded", "host_degraded", "Host health is degraded.")
        }
        _ => (
            "unavailable",
            "host_unavailable",
            "Host health is unavailable.",
        ),
    };
    let mut components = vec![r::RemoteObservationComponent {
        component: "vmmd".into(),
        status: status.into(),
    }];
    if configured {
        components.push(r::RemoteObservationComponent {
            component: "state-disk".into(),
            status: if disk.is_some() { "healthy" } else { status }.into(),
        });
    }
    let usage = r::RemoteObservationUsage {
        sampled_unix_millis: vmm_remote_salix::unix_millis(),
        stale: disk.is_none(),
        issue: if disk.is_none() {
            "usage_unavailable"
        } else {
            ""
        }
        .into(),
        disk_bytes: disk.map_or(0, |sample| sample.used),
        ..Default::default()
    };
    Observation {
        health: Some(r::RemoteObservationHealth {
            status: status.into(),
            issue: issue.into(),
            message: message.into(),
            components,
        }),
        usage: Some(usage),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_configured_missing_disk_sample_degrades_a_healthy_host() {
        for (configured, expected) in [(false, "healthy"), (true, "degraded")] {
            let observed = project(HealthState::Healthy, None, configured);
            let health = observed.health.unwrap();
            assert_eq!(health.status, expected);
            assert_eq!(health.components.len(), 1 + usize::from(configured));
            assert_eq!(health.components[0].status, expected);
            let usage = observed.usage.unwrap();
            assert!(usage.stale);
            assert_eq!(usage.issue, "usage_unavailable");
        }
    }
}
