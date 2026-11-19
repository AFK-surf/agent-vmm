//! Disposable build VMs and execution-bound operation receipts.
mod input;
mod ledger;
mod worker;

use super::{ImageImports, base, grant};
use crate::{
    Containers, RuntimeContext, context::core_status, execution, guest::Output,
    runtime::ImageRuntime,
};
use agent_vmm_proto::host::v1 as h;
use async_trait::async_trait;
use ledger::Record;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, mpsc, watch};
use vmm_rpc::{Status, Streaming};
use vmm_core::{EgressMode, RetainedImage, Scope, Store};
use vmm_guest_proto::client::Client;

#[async_trait]
pub trait BuildBackend: Send + Sync {
    async fn acquire(
        &self,
        id: &str,
        scope: &Scope,
        network: EgressMode,
        quota: u64,
    ) -> Result<BuildGuest, Status>;
    /// Fence any racing acquire, then wait for reclamation of this lease's VM and disks.
    async fn settle(&self, id: &str) -> Result<(), Status>;
}
pub struct BuildGuest {
    pub client: Client,
    pub instance_id: String,
    pub lease: Box<dyn GuestLease>,
}
#[async_trait]
pub trait GuestLease: Send + Sync {
    async fn close(self: Box<Self>) -> Result<(), Status>;
}

#[derive(Clone)]
pub struct Builds {
    backend: Arc<dyn BuildBackend>,
    containers: Containers,
    imports: ImageImports,
    active: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
}
impl Builds {
    pub fn new(
        backend: Arc<dyn BuildBackend>,
        containers: Containers,
        imports: ImageImports,
    ) -> Result<Self, Status> {
        ledger::initialize(&containers)?;
        Ok(Self {
            backend,
            containers,
            imports,
            active: Default::default(),
        })
    }
    pub async fn recover(&self, store: &Store) -> Result<(), Status> {
        let active = self.active.lock().await;
        for mut record in ledger::pending(&self.containers)? {
            if !active.contains_key(&record.lease) {
                self.recover_one(store, &mut record).await?;
            }
        }
        Ok(())
    }
    async fn recover_one(&self, store: &Store, r: &mut Record) -> Result<(), Status> {
        if let Some(bytes) = store
            .published_build(&r.scope, &r.receipt.request_id)
            .map_err(core_status)?
        {
            r.receipt = h::ImageOperationReceipt::decode(bytes.as_slice())
                .map_err(|_| Status::data_loss("invalid published build receipt"))?;
        } else {
            self.backend.settle(&r.lease).await?;
            r.failed(&Status::cancelled("interrupted build reclaimed"), true);
        }
        ledger::save(&self.containers, r)
    }

    async fn operation(
        &self,
        ctx: &RuntimeContext,
        q: h::ObserveImageOperationRequest,
        cancel: bool,
    ) -> Result<h::ImageOperationReceipt, Status> {
        if q.kind() != h::ImageOperationKind::Build
            || q.execution_id.is_empty()
            || q.container_id.is_empty()
            || q.expected_instance_id.is_empty()
        {
            return Err(Status::invalid_argument(
                "build observation requires exact execution and container instance",
            ));
        }
        let active = self.active.lock().await;
        let mut record = ledger::lookup(&self.containers, &ctx.scope.id, &q.request_id)?
            .ok_or_else(|| Status::not_found("image operation not found"))?;
        let r = &record.receipt;
        if r.input_sha256 != q.input_sha256
            || r.execution_id != q.execution_id
            || r.container_id != q.container_id
            || r.expected_instance_id != q.expected_instance_id
        {
            return Err(Status::aborted(
                "image operation differs from accepted input",
            ));
        }
        if record.active() {
            if let Some(signal) = active.get(&record.lease) {
                if cancel {
                    signal.send_replace(true);
                }
            } else {
                self.recover_one(&ctx.core.store, &mut record).await?;
            }
        }
        Ok(record.receipt)
    }
}

fn result(r: &h::ImageOperationReceipt) -> h::BuildImageResponse {
    h::BuildImageResponse {
        event: Some(h::build_image_response::Event::Result(
            h::BuildImageResult {
                image: r.image_reference.clone(),
                digest: r.manifest_digest.clone(),
            },
        )),
    }
}

async fn interrupted(
    ctx: &RuntimeContext,
    deadline: i64,
    mut cancel: watch::Receiver<bool>,
) -> Status {
    loop {
        if *cancel.borrow() {
            return Status::cancelled("build cancelled");
        }
        match ctx.core.store.session_is_current(&ctx.session) {
            Ok(true) => {}
            Ok(false) => return Status::unauthenticated("build session revoked"),
            Err(e) => return core_status(e),
        }
        let now = match execution::now() {
            Ok(n) => n,
            Err(e) => return e,
        };
        if now >= deadline {
            return Status::deadline_exceeded("build deadline expired");
        }
        let duration =
            Duration::from_nanos((deadline - now) as u64).min(Duration::from_millis(250));
        tokio::select! {
            _=tokio::time::sleep(duration)=>{},
            changed=cancel.changed()=>if changed.is_err(){return Status::cancelled("host stopped")},
        }
    }
}

impl Builds {
    async fn run(
        &self,
        ctx: RuntimeContext,
        mut r: Record,
        header: h::BuildImageHeader,
        stream: Streaming<h::BuildImageRequest>,
        tx: mpsc::Sender<Result<h::BuildImageResponse, Status>>,
        cancel: watch::Receiver<bool>,
    ) {
        let mut acquiring = false;
        let mut prepared = false;
        let mut worker = None;
        let deadline = header.execution_deadline_unix_nano;
        let interrupt = interrupted(&ctx, deadline, cancel.clone());
        tokio::pin!(interrupt);
        let do_work = async {
            let upload = input::receive(&self.imports.directory, &header, stream).await?;
            let base_file = if header.base_image_reference.is_empty() {
                None
            } else {
                let (_, artifact) =
                    grant::resolve(&ctx, &header.base_image_reference).map_err(core_status)?;
                if super::oci::digest(&header.base_manifest_digest)? != artifact.manifest_digest {
                    return Err(Status::aborted(
                        "base image digest differs from admitted reference",
                    ));
                }
                Some(base::open(
                    &self.imports.directory,
                    artifact.manifest_digest,
                )?)
            };
            let network = if header.network_mode() == h::BuildNetworkMode::None {
                EgressMode::DenyAll
            } else {
                ctx.scope.egress_mode.clone()
            };
            acquiring = true;
            let guest = self
                .backend
                .acquire(
                    &r.lease,
                    &ctx.scope,
                    network,
                    ctx.scope.limits.disk_quota_bytes,
                )
                .await?;
            r.receipt.runtime_instance_id = guest.instance_id;
            ledger::save(&self.containers, &r)?;
            let dir = self.imports.directory.clone();
            let notify = tx.clone();
            let task = worker.insert(tokio::task::spawn_blocking(move || {
                worker::run(guest.client, header, upload, base_file, &dir, notify)
            }));
            let joined = task.await;
            worker.take();
            let archive = joined.map_err(|_| Status::internal("build worker interrupted"))??;
            guest.lease.close().await?;
            prepared = true;
            Ok::<_, Status>(archive)
        };
        let archive =
            tokio::select! {biased; status=&mut interrupt=>Err(status),archive=do_work=>archive};
        let settled = if acquiring && !prepared {
            self.backend.settle(&r.lease).await.is_ok()
        } else {
            true
        };
        if let Some(task) = worker {
            let _ = task.await;
        }
        let finish = async {
            let archive = archive?;
            r.receipt.manifest_digest = format!("sha256:{}", hex::encode(archive.digest));
            r.receipt.state = h::ImageOperationState::Succeeded as i32;
            r.receipt.writer_settled = true;
            let output = RetainedImage {
                reference: archive.header.image_reference.clone(),
                digest: archive.digest,
                generation: uuid::Uuid::new_v4().to_string(),
                build_request: r.receipt.request_id.clone(),
                build_receipt: r.receipt.encode_to_vec(),
                logical_bytes: archive.logical_bytes,
            };
            ImageImports::publish_output(
                ctx.clone(),
                self.containers.clone(),
                archive,
                self.imports.staging.clone(),
                Some(super::staging::BuildPublication {
                    context: &ctx,
                    output: &output,
                    cancelled: cancel.clone(),
                    deadline,
                }),
            )
            .await?;
            Ok::<_, Status>(())
        }
        .await;
        let mut active = self.active.lock().await;
        let response = match finish {
            Ok(()) => Ok(result(&r.receipt)),
            Err(e) => {
                match ctx
                    .core
                    .store
                    .published_build(&r.scope, &r.receipt.request_id)
                {
                    Ok(Some(bytes)) => match h::ImageOperationReceipt::decode(bytes.as_slice()) {
                        Ok(published) => {
                            r.receipt = published;
                            Ok(result(&r.receipt))
                        }
                        Err(_) => Err(Status::data_loss("invalid published build receipt")),
                    },
                    Ok(None) => {
                        let e = if *cancel.borrow() {
                            Status::cancelled("build cancelled")
                        } else if execution::now().is_ok_and(|now| now >= deadline) {
                            Status::deadline_exceeded("build deadline expired")
                        } else {
                            e
                        };
                        r.failed(&e, settled);
                        Err(e)
                    }
                    Err(e) => {
                        let e = core_status(e);
                        r.failed(&e, false);
                        Err(e)
                    }
                }
            }
        };
        let response = ledger::save(&self.containers, &r).and(response);
        active.remove(&r.lease);
        drop(active);
        let _ = tx.send(response).await;
    }
}

#[async_trait]
impl ImageRuntime for Builds {
    async fn build_image(
        &self,
        ctx: &RuntimeContext,
        mut stream: Streaming<h::BuildImageRequest>,
    ) -> Result<Output<h::BuildImageResponse>, Status> {
        let Some(h::build_image_request::Event::Header(mut header)) =
            stream.message().await?.and_then(|m| m.event)
        else {
            return Err(Status::invalid_argument("first build event must be header"));
        };
        let canonical = input::validate(&header)?;
        let record = Record::new(ctx, &header, canonical.clone());
        let mut active = self.active.lock().await;
        let (tx, rx) = mpsc::channel(16);
        if let Some(mut prior) =
            ledger::lookup(&self.containers, &record.scope, &header.request_id)?
        {
            if prior.active() && !active.contains_key(&prior.lease) {
                self.recover_one(&ctx.core.store, &mut prior).await?
            }
            if prior.receipt.input_sha256 != record.receipt.input_sha256
                || prior.receipt.state() != h::ImageOperationState::Succeeded
            {
                return Err(Status::aborted(
                    "build differs from accepted input or is already in progress or terminal",
                ));
            }
            tx.try_send(Ok(result(&prior.receipt)))
                .map_err(|_| Status::internal("build replay unavailable"))?;
        } else {
            ledger::admit(
                &self.containers,
                ctx,
                &record,
                header.execution_deadline_unix_nano,
            )?;
            let (signal, cancel) = watch::channel(false);
            active.insert(record.lease.clone(), signal);
            let service = self.clone();
            let ctx = ctx.clone();
            header.image = canonical;
            tokio::spawn(async move {
                service.run(ctx, record, header, stream, tx, cancel).await;
            });
        }
        Ok(rx.into())
    }

    async fn observe_image_operation(
        &self,
        ctx: &RuntimeContext,
        req: h::ObserveImageOperationRequest,
    ) -> Result<h::ObserveImageOperationResponse, Status> {
        Ok(h::ObserveImageOperationResponse {
            receipt: Some(self.operation(ctx, req, false).await?),
        })
    }
    async fn cancel_image_operation(
        &self,
        ctx: &RuntimeContext,
        q: h::CancelImageOperationRequest,
    ) -> Result<h::CancelImageOperationResponse, Status> {
        let req = h::ObserveImageOperationRequest {
            request_id: q.request_id,
            kind: q.kind,
            input_sha256: q.input_sha256,
            execution_id: q.execution_id,
            container_id: q.container_id,
            expected_instance_id: q.expected_instance_id,
            copy_source: q.copy_source,
            target_allocation_id: q.target_allocation_id,
            target_allocation_generation: q.target_allocation_generation,
        };
        Ok(h::CancelImageOperationResponse {
            receipt: Some(self.operation(ctx, req, true).await?),
        })
    }
}
