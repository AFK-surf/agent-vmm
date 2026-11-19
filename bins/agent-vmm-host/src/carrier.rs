//! Whole-VM observations come from the same Carrier that owns the VM.
use agent_vmm_proto::host::v1 as h;
use vmm_rpc::Status;
use vmm_carrier_macos::{
    client::CarrierClient,
    protocol::{CarrierStatus, Request, State},
};
use vmm_compat::operator::VmControl;
use vmm_core::Core;

pub struct MacVmControl {
    pub client: CarrierClient,
    pub core: Core,
    pub builds: vmm_compat::image::build::Builds,
}
fn snapshot(status: CarrierStatus) -> h::Vm {
    let state = match status.state {
        State::Stopped => h::VmState::Stopped,
        State::Running => h::VmState::Running,
        State::Failed => h::VmState::Failed,
    };
    h::Vm {
        state: state as i32,
        boot_id: status.boot_id.unwrap_or_default(),
        error: status.last_error.unwrap_or_default(),
    }
}
#[async_trait::async_trait]
impl VmControl for MacVmControl {
    async fn observe(&self) -> Result<h::Vm, Status> {
        self.client
            .control(Request::Status)
            .await
            .map(snapshot)
            .map_err(|_| Status::unavailable("carrier status unavailable"))
    }
    async fn change(&self, start: bool, force: bool, operation_id: &str) -> Result<(), Status> {
        let request = if start {
            Request::EnsureStarted {
                operation_id: operation_id.into(),
            }
        } else {
            Request::Stop { force }
        };
        let status = self
            .client
            .control(request)
            .await
            .map_err(|_| Status::unavailable("carrier control unavailable"))?;
        if start && status.state == State::Running {
            self.core
                .reconcile_after_restart()
                .await
                .map_err(vmm_compat::error::core_status)?;
            self.builds.recover(&self.core.store).await?;
        } else if start || status.state != State::Stopped {
            return Err(Status::failed_precondition(
                "carrier did not reach the requested state",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl vmm_compat::guest::GuestDriver for CarrierClientAdapter {
    async fn client(
        &self,
        boot: &vmm_core::BootIdentity,
    ) -> Result<vmm_guest_proto::client::Client, Status> {
        use vmm_guest_proto::client::{Client, GuestStream};
        let handle = tokio::runtime::Handle::current();
        let boot = boot.clone();
        let carrier = self.0.clone();
        Ok(Client::bound(boot.instance_id.clone(), move |timeout| {
            let (stream, token) = handle
                .block_on(async {
                    tokio::time::timeout(timeout, async {
                        let backend = carrier.dom0();
                        let address = backend.guest_address(&boot).await?;
                        let stream = carrier
                            .connect_guest_rpc(*address.ip(), address.port())
                            .await?;
                        let endpoint = backend.open_guest_runtime(&boot).await?;
                        if endpoint.address != address {
                            return Err(vmm_core::Error::Fenced);
                        }
                        Ok((stream.into_std()?, endpoint.token))
                    })
                    .await
                })
                .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
                .map_err(std::io::Error::other)?;
            stream.set_nonblocking(false)?;
            Ok((GuestStream::Unix(stream), token))
        }))
    }

    async fn connect_port(
        &self,
        boot: &vmm_core::BootIdentity,
        port: u16,
    ) -> Result<Box<dyn vmm_compat::guest::Duplex>, Status> {
        let backend = self.0.dom0();
        let address = backend
            .guest_address(boot)
            .await
            .map_err(vmm_compat::error::core_status)?;
        if port == 0 || port == address.port() {
            return Err(Status::invalid_argument("invalid forward port"));
        }
        let stream = self
            .0
            .connect_guest_rpc(*address.ip(), port)
            .await
            .map_err(|_| Status::unavailable("guest port unavailable"))?;
        if backend
            .guest_address(boot)
            .await
            .map_err(vmm_compat::error::core_status)?
            != address
        {
            return Err(vmm_compat::error::core_status(vmm_core::Error::Fenced));
        }
        Ok(Box::new(stream))
    }
}

pub struct CarrierClientAdapter(pub CarrierClient);

/// Each published staging token owns the connection keeping its remote file alive.
pub struct CarrierImageStaging {
    pub(crate) client: CarrierClient,
    leases: std::sync::Mutex<std::collections::HashMap<String, vmm_dom0::remote::RemoteImageStage>>,
}
impl CarrierImageStaging {
    pub fn new(client: CarrierClient) -> Self {
        Self {
            client,
            leases: Default::default(),
        }
    }
    pub fn reserved_bytes(&self) -> vmm_core::Result<u64> {
        self.leases
            .lock()
            .map_err(|_| vmm_core::Error::Interrupted)?
            .values()
            .try_fold(0_u64, |total, lease| {
                total
                    .checked_add(lease.source.physical_bytes)
                    .ok_or(vmm_core::Error::Invalid("staging byte count"))
            })
    }

    pub(crate) fn disk_sample(&self) -> vmm_core::Result<vmm_compat::operator::DiskSample> {
        let status = tokio::runtime::Handle::current()
            .block_on(async {
                tokio::time::timeout(
                    std::time::Duration::from_secs(4),
                    self.client.control(Request::Status),
                )
                .await
            })
            .map_err(|_| vmm_core::Error::Interrupted)??;
        if status.state != State::Running {
            return Err(std::io::Error::other("carrier VM is not running").into());
        }
        Ok(vmm_compat::operator::DiskSample {
            used: status.pool_allocated_bytes,
            capacity: status.pool_logical_bytes,
            import_reservations: self.reserved_bytes()?,
        })
    }
}
impl vmm_compat::image::ImageStaging for CarrierImageStaging {
    fn stage_image(
        &self,
        archive: &std::path::Path,
        digest: vmm_core::Digest,
    ) -> vmm_core::Result<vmm_core::ImageSource> {
        let lease = tokio::runtime::Handle::current().block_on(
            vmm_dom0::remote::RemoteImageStage::upload(&self.client, archive, digest),
        )?;
        let source = lease.source.clone();
        self.leases
            .lock()
            .map_err(|_| vmm_core::Error::Interrupted)?
            .insert(source.staged_extent.clone(), lease);
        Ok(source)
    }
    fn discard_staged_image(&self, token: &str) -> vmm_core::Result<()> {
        self.leases
            .lock()
            .map_err(|_| vmm_core::Error::Interrupted)?
            .remove(token);
        Ok(())
    }
}
