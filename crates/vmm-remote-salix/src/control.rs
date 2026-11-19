use crate::{
    Error, Gateway, RemoteConnection, Result,
    allocation::allocation_proto,
    command::Commands,
    gateway::{CONNECT_TIMEOUT, status_error},
    remote::{
        self, ConnectRequest, ControllerReady, HostHello, RemoteObservationRenewal,
        RemoteObservationSnapshot, connect_request, connect_response,
    },
    tunnel::Tunnels,
    unix_millis,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{future::pending, sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    time::{Instant, interval_at, sleep, timeout},
};
use tokio_stream::wrappers::ReceiverStream;
use vmm_core::{Channel, Core, Limits, Store};

pub(crate) const INVENTORY_LIMIT: usize = 64;
const OBSERVATION_FEATURE: &str = "remote-observation-v1";
const OBSERVATION_INTERVAL: Duration = Duration::from_secs(15);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

pub struct ConnectorConfig {
    pub connection: RemoteConnection,
    /// Local VM shape for each scope. The request supplies PID and disk limits.
    pub scope_defaults: Limits,
    pub runtime: mpsc::Sender<crate::RuntimeSession>,
    pub observe: Option<Arc<dyn Fn() -> Observation + Send + Sync>>,
}

#[derive(Default)]
pub struct Observation {
    pub health: Option<remote::RemoteObservationHealth>,
    pub usage: Option<remote::RemoteObservationUsage>,
    pub reclaim_candidate: Option<remote::WorkloadReclaimCandidate>,
}

pub struct Connector {
    endpoint: Option<tokio::sync::watch::Receiver<crate::trust::EndpointObservation>>,
    ready_epoch: AtomicU64,
    pub(crate) core: Core,
    pub(crate) gateway: Gateway,
    pub(crate) config: ConnectorConfig,
}

impl Connector {
    pub fn new(core: Core, gateway: Gateway, config: ConnectorConfig) -> Self {
        Self {
            endpoint: None,
            ready_epoch: AtomicU64::new(0),
            core,
            gateway,
            config,
        }
    }

    pub fn with_endpoint(
        mut self,
        endpoint: tokio::sync::watch::Receiver<crate::trust::EndpointObservation>,
    ) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn pool_revision(&self) -> u64 {
        self.config.connection.policy_revision
    }

    pub fn is_connected(&self) -> bool {
        self.ready_epoch.load(Ordering::Acquire) != 0
    }

    /// Runs until rejected credentials or an incompatible protocol require action, or the
    /// caller cancels the future. Closing the connection drops only its command and session paths.
    pub async fn run(&self) -> Result<()> {
        let mut delay = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            match self.connect_once().await {
                Err(
                    error @ (Error::Authentication(_)
                    | Error::ProtocolUnsupported(_)
                    | Error::Invalid(_)),
                ) => return Err(error),
                Err(Error::Core(vmm_core::Error::Missing("registration"))) => {
                    return Err(Error::Core(vmm_core::Error::Missing("registration")));
                }
                _ => {}
            }
            if started.elapsed() >= MAX_RECONNECT_DELAY {
                delay = Duration::from_secs(1);
            }
            // Independent registrations avoid reconnecting in lockstep.
            let mut random = [0];
            let _ = getrandom::fill(&mut random);
            sleep(delay.mul_f64(0.75 + f64::from(random[0]) / 1024.0)).await;
            delay = (delay * 2).min(MAX_RECONNECT_DELAY);
        }
    }

    /// Drives one epoch. EOF and transient gateway statuses can be retried by the caller.
    pub async fn connect_once(&self) -> Result<()> {
        let store = &self.core.store;
        let registration = self.gateway.registration_id();
        let epoch = store
            .registration(registration)?
            .connection_epoch
            .checked_add(1)
            .ok_or(Error::Invalid("connection epoch exhausted"))?;
        let lease = ChannelLease {
            ready_epoch: &self.ready_epoch,
            store,
            channel: store.connect_with_policy(
                registration,
                epoch,
                Some(self.config.connection.policy_revision),
            )?,
        };
        let channel = &lease.channel;
        let inventory = store.remote_inventory(channel, INVENTORY_LIMIT)?;
        let mut instance = [0; 16];
        getrandom::fill(&mut instance).map_err(|_| Error::Random)?;
        let mut endpoint = self.endpoint.clone();
        let hello = HostHello {
            protocol_version: "4".into(),
            connector_release: env!("CARGO_PKG_VERSION").into(),
            registration_id: registration.into(),
            connection_instance: instance.iter().map(|byte| format!("{byte:02x}")).collect(),
            pool_revision: self.config.connection.policy_revision,
            capacity: Some(self.capacity()),
            inventory: inventory
                .allocations
                .into_iter()
                .map(allocation_proto)
                .collect(),
            connection_epoch: epoch,
            inventory_watermark: inventory.watermark,
            supported_features: vec![OBSERVATION_FEATURE.into()],
            observation: Some(self.observation(1, inventory.watermark)),
            endpoint_observation: endpoint.as_mut().map(|rx| rx.borrow_and_update().clone()),
        };

        let (outbound, requests) = mpsc::channel(16);
        send(&outbound, connect_request::Payload::Hello(hello)).await?;
        let mut client = self.gateway.controller();
        let mut incoming = timeout(CONNECT_TIMEOUT, async {
            client
                .connect(ReceiverStream::new(requests))
                .await
                .map(|reply| reply.into_inner())
        })
        .await
        .map_err(|_| Error::Deadline)?
        .map_err(status_error)?;
        let ready = timeout(CONNECT_TIMEOUT, incoming.message())
            .await
            .map_err(|_| Error::Deadline)?
            .map_err(status_error)?
            .and_then(|frame| frame.payload);
        let Some(connect_response::Payload::ControllerReady(ready)) = ready else {
            return Err(Error::Protocol("missing ControllerReady"));
        };
        validate_ready(ready, channel, inventory.watermark)?;
        if store.remote_inventory(channel, 0)?.watermark != inventory.watermark {
            return Err(Error::Disconnected);
        }

        self.ready_epoch.store(epoch, Ordering::Release);
        let mut command_state = Commands::default();
        let mut tunnels = Tunnels::new(self, channel);
        let commands = async {
            loop {
                let payload = incoming
                    .message()
                    .await
                    .map_err(status_error)?
                    .ok_or(Error::Disconnected)?
                    .payload;
                let Some(connect_response::Payload::Command(command)) = payload else {
                    return Err(Error::Protocol("expected a controller command"));
                };
                command_state
                    .process(self, channel, command, &outbound, &mut tunnels)
                    .await?;
            }
        };
        let observations = async {
            let mut ticks =
                interval_at(Instant::now() + OBSERVATION_INTERVAL, OBSERVATION_INTERVAL);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            for sequence in 2.. {
                ticks.tick().await;
                let watermark = store.remote_inventory(channel, 0)?.watermark;
                let observation = self.observation(sequence, watermark);
                send(
                    &outbound,
                    connect_request::Payload::ObservationRenewal(RemoteObservationRenewal {
                        connection_epoch: epoch,
                        observation: Some(observation),
                    }),
                )
                .await?;
            }
            pending::<Result<()>>().await
        };
        let renew_endpoint = async {
            let Some(mut rx) = endpoint else {
                return pending::<Result<()>>().await;
            };
            loop {
                rx.changed().await.map_err(|_| Error::Disconnected)?;
                let observation = rx.borrow_and_update().clone();
                send(
                    &outbound,
                    connect_request::Payload::EndpointObservationRenewal(
                        remote::EndpointObservationRenewal {
                            connection_epoch: epoch,
                            endpoint_observation: Some(observation),
                        },
                    ),
                )
                .await?;
            }
        };
        tokio::try_join!(commands, observations, renew_endpoint).map(|((), (), ())| ())
    }

    fn capacity(&self) -> remote::Capacity {
        let pool = &self.config.connection.pool_policy;
        remote::Capacity {
            per_environment_limits: Some(remote::ResourceLimits {
                pids: Some(pool.per_environment_limits.pids),
                disk_bytes: Some(pool.per_environment_limits.disk_bytes),
            }),
            max_egress_mode: match pool.max_egress_mode {
                crate::EgressMode::DenyAll => remote::EgressMode::DenyAll,
                crate::EgressMode::PublicInternet => remote::EgressMode::PublicInternet,
            }
            .into(),
        }
    }

    fn observation(&self, sequence: u64, watermark: u64) -> RemoteObservationSnapshot {
        let sample = self
            .config
            .observe
            .as_ref()
            .map(|observe| observe())
            .unwrap_or_default();
        let now = unix_millis();
        RemoteObservationSnapshot {
            reclaim_candidate: sample.reclaim_candidate,
            sequence,
            observed_unix_millis: now,
            capacity: Some(self.capacity()),
            health: sample.health.or_else(|| {
                Some(remote::RemoteObservationHealth {
                    status: "unavailable".into(),
                    issue: "observation_unavailable".into(),
                    ..Default::default()
                })
            }),
            usage: sample.usage,
            inventory_watermark: watermark,
            inventory_observed_unix_millis: now,
        }
    }
}

fn validate_ready(ready: ControllerReady, channel: &Channel, watermark: u64) -> Result<()> {
    if ready.registration_id != channel.registration_id
        || ready.connection_epoch != channel.connection_epoch
        || ready.acknowledged_inventory_watermark != watermark
    {
        return Err(Error::Protocol(
            "ControllerReady does not acknowledge the HostHello fence",
        ));
    }
    if !ready
        .enabled_features
        .iter()
        .any(|name| name == OBSERVATION_FEATURE)
        || ready
            .enabled_features
            .iter()
            .any(|name| name != OBSERVATION_FEATURE)
    {
        return Err(Error::Protocol(
            "ControllerReady did not negotiate remote-observation-v1",
        ));
    }
    Ok(())
}

pub(crate) async fn send(
    outbound: &mpsc::Sender<ConnectRequest>,
    payload: connect_request::Payload,
) -> Result<()> {
    outbound
        .send(ConnectRequest {
            payload: Some(payload),
        })
        .await
        .map_err(|_| Error::Disconnected)
}

struct ChannelLease<'a> {
    ready_epoch: &'a AtomicU64,
    store: &'a Store,
    channel: Channel,
}

impl Drop for ChannelLease<'_> {
    fn drop(&mut self) {
        let _ = self.ready_epoch.compare_exchange(
            self.channel.connection_epoch,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let _ = self.store.disconnect(&self.channel);
    }
}
