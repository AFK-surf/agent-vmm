//! Service capabilities are independent of control-channel TLS and iroh endpoint identity.
use agent_vmm_proto::{
    service::v1 as s,
    trust::v1::{AuthorityClass, RouteCapability},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use vmm_compat::guest::GuestDriver;
use vmm_core::{ServiceExport, Store};
use vmm_signing::SignedMessage;

/// A local trust provider resolves an approved lease, including its source membership,
/// policy and destination boot. The wire request cannot install a lease or supply its key.
#[derive(Clone, PartialEq)]
pub(crate) struct TrustedLease {
    pub header: s::OpenServiceForwardHeader,
    pub issuer: Vec<u8>,
    pub target: ServiceExport,
}
pub(crate) trait RouteTrust: Send + Sync {
    fn lookup(&self, route: &str, lease: &str) -> Option<TrustedLease>;
}

pub(crate) struct ServiceForward {
    pub device: String,
    pub store: Arc<Store>,
    pub guest: Arc<dyn GuestDriver>,
    pub trust: Option<Arc<dyn RouteTrust>>,
    pub budgets: Mutex<HashMap<String, Arc<Budget>>>,
}

fn denied() -> Status {
    Status::permission_denied("service route has no current trusted lease")
}
fn exhausted() -> Status {
    Status::resource_exhausted("service route budget exhausted")
}

pub(crate) struct Budget {
    expires: SystemTime,
    generation: u64,
    used: Mutex<Usage>,
}
#[derive(Default)]
struct Usage {
    connections: u32,
    active: u32,
    bytes: u64,
}
struct ConnectionBudget {
    budget: Arc<Budget>,
    limit: u64,
}
impl Drop for ConnectionBudget {
    fn drop(&mut self) {
        if let Ok(mut used) = self.budget.used.lock() {
            used.active -= 1;
        }
    }
}
impl ConnectionBudget {
    fn consume(&self, bytes: usize) -> Result<(), Status> {
        let mut used = self.budget.used.lock().map_err(|_| exhausted())?;
        used.bytes = used
            .bytes
            .checked_add(bytes as u64)
            .filter(|v| *v <= self.limit)
            .ok_or_else(exhausted)?;
        Ok(())
    }
}

impl ServiceForward {
    fn admit(
        &self,
        header: &s::OpenServiceForwardHeader,
    ) -> Result<(TrustedLease, SystemTime, ConnectionBudget), Status> {
        let trust = self.trust.as_ref().ok_or_else(denied)?;
        let expected = trust
            .lookup(&header.route_id, &header.lease_id)
            .ok_or_else(denied)?;
        let capability = header.capability.as_ref().ok_or_else(denied)?;
        let authority = capability.authority.as_ref().ok_or_else(denied)?;
        let valid_issuer = match authority.authority_class() {
            AuthorityClass::ManagedController => {
                !capability.issuer_key_id.is_empty()
                    && capability.issuer_key_id == authority.key_id
                    && capability.issuer_device_id.is_empty()
                    && capability.source_membership_credential.is_some()
            }
            AuthorityClass::PersonalMesh => {
                capability.issuer_device_id == self.device
                    && capability.issuer_key_id.is_empty()
                    && authority.trust_domain_ref == authority.authority_id.as_bytes()
                    && capability.source_membership_credential.is_none()
            }
            AuthorityClass::Unspecified => false,
        };
        if !valid_issuer
            || authority.authority_id.is_empty()
            || authority.revision == 0
            || capability.policy_revision == 0
            || capability.nonce.len() != 32
            || capability.source_device_id.is_empty()
            || !matches!(capability.route_class.as_str(), "device" | "compute")
            || !capability.allowed_verbs.windows(2).all(|v| v[0] < v[1])
            || expected.header != *header
            || header.route_id.is_empty()
            || header.lease_id.is_empty()
            || header.route_generation == 0
            || header.route_generation != capability.route_generation
            || capability.destination_device_id != self.device
            || capability.destination_export_id != expected.target.id
            || capability.audience != "agent-vmm/service-tunnel/1"
            || capability.allowed_protocol != "tcp"
            || !capability.allowed_verbs.iter().any(|v| v == "connect")
            || capability.capability_id.is_empty()
        {
            return Err(denied());
        }
        SignedMessage::try_from(capability)
            .and_then(|m| m.verify(&expected.issuer))
            .map_err(|_| denied())?;
        let now = SystemTime::now();
        let start: SystemTime = capability
            .not_before
            .ok_or_else(denied)?
            .try_into()
            .map_err(|_| denied())?;
        let expires: SystemTime = capability
            .expires_at
            .ok_or_else(denied)?
            .try_into()
            .map_err(|_| denied())?;
        if start > now
            || expires <= now
            || expires.duration_since(start).map_err(|_| denied())? > Duration::from_secs(3600)
            || self
                .store
                .current_service_export(&expected.target.id)
                .map_err(|_| denied())?
                != expected.target
        {
            return Err(denied());
        }
        let slot = self.acquire_budget(capability, expires, now)?;
        Ok((expected, expires, slot))
    }
    fn acquire_budget(
        &self,
        cap: &RouteCapability,
        expires: SystemTime,
        now: SystemTime,
    ) -> Result<ConnectionBudget, Status> {
        let limits = cap.budget.ok_or_else(denied)?;
        let mut budgets = self.budgets.lock().map_err(|_| exhausted())?;
        budgets.retain(|_, budget| budget.expires > now);
        if !budgets.contains_key(&cap.capability_id) && budgets.len() >= 4096 {
            return Err(exhausted());
        }
        let budget = budgets
            .entry(cap.capability_id.clone())
            .or_insert_with(|| {
                Arc::new(Budget {
                    expires,
                    generation: cap.route_generation,
                    used: Mutex::default(),
                })
            })
            .clone();
        let mut used = budget.used.lock().map_err(|_| exhausted())?;
        if budget.generation != cap.route_generation
            || budget.expires != expires
            || used.connections >= limits.connection_limit
            || used.active >= limits.concurrency_limit
            || used.bytes >= limits.byte_limit
        {
            return Err(exhausted());
        }
        used.connections += 1;
        used.active += 1;
        drop(used);
        Ok(ConnectionBudget {
            budget,
            limit: limits.byte_limit,
        })
    }
}

#[tonic::async_trait]
impl s::service_route_service_server::ServiceRouteService for ServiceForward {
    type OpenServiceForwardStream = ReceiverStream<Result<s::OpenServiceForwardResponse, Status>>;
    async fn open_service_forward(
        &self,
        request: Request<Streaming<s::OpenServiceForwardRequest>>,
    ) -> Result<Response<Self::OpenServiceForwardStream>, Status> {
        use s::open_service_forward_request::Payload as In;
        use s::open_service_forward_response::Payload as Out;
        let mut input = request.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(10), input.message())
            .await
            .map_err(|_| Status::deadline_exceeded("service header deadline"))??;
        let Some(In::Header(header)) = first.and_then(|v| v.payload) else {
            return Err(Status::invalid_argument(
                "first service frame must be a route header",
            ));
        };
        let (lease, expires, budget) = self.admit(&header)?;
        let target = &lease.target;
        let deadline = tokio::time::Instant::now()
            + expires
                .duration_since(SystemTime::now())
                .map_err(|_| denied())?;
        let stream =
            tokio::time::timeout_at(deadline, self.guest.connect_port(&target.boot, target.port))
                .await
                .map_err(|_| denied())??;
        let store = self.store.clone();
        let trust = self.trust.clone().ok_or_else(denied)?;
        let current = move || {
            trust
                .lookup(&lease.header.route_id, &lease.header.lease_id)
                .as_ref()
                == Some(&lease)
                && store.current_service_export(&lease.target.id).ok().as_ref()
                    == Some(&lease.target)
        };
        if !current() {
            return Err(denied());
        }
        let (send, receive) = mpsc::channel(4);
        tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(stream);
            let upload = async {
                while let Some(message) = input.message().await? {
                    match message.payload {
                        Some(In::Data(bytes)) if !bytes.is_empty() && bytes.len() <= 65536 => {
                            budget.consume(bytes.len())?;
                            writer
                                .write_all(&bytes)
                                .await
                                .map_err(|_| Status::unavailable("guest write failed"))?;
                        }
                        Some(In::CloseWrite(true)) => break,
                        _ => return Err(Status::invalid_argument("invalid service data frame")),
                    }
                }
                writer
                    .shutdown()
                    .await
                    .map_err(|_| Status::unavailable("guest half-close failed"))
            };
            let download = async {
                let mut buffer = vec![0; 32768];
                loop {
                    let n = reader
                        .read(&mut buffer)
                        .await
                        .map_err(|_| Status::unavailable("guest read failed"))?;
                    budget.consume(n)?;
                    let payload = if n == 0 {
                        Out::CloseWrite(true)
                    } else {
                        Out::Data(buffer[..n].to_vec())
                    };
                    send.send(Ok(s::OpenServiceForwardResponse {
                        payload: Some(payload),
                    }))
                    .await
                    .map_err(|_| Status::cancelled("client disconnected"))?;
                    if n == 0 {
                        return Ok::<(), Status>(());
                    }
                }
            };
            let revoke = async {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if !current() {
                        break;
                    }
                }
            };
            let result = tokio::select! {
                r = async {tokio::try_join!(upload,download)} => r.map(|_|()),
                _ = tokio::time::sleep_until(deadline) => Err(denied()),
                _ = revoke => Err(denied()),
                _ = send.closed() => return,
            };
            drop(budget);
            if let Err(e) = result {
                let _ = send.send(Err(e)).await;
            }
        });
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}
