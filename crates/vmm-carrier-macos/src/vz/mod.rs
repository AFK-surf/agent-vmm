mod config;
mod netlink;
mod quic;
use crate::{
    bundle::Bundle,
    protocol::*,
    server::{CarrierHypervisor, PendingVsock},
    sparse,
};
use block2::RcBlock;
use dispatch2::{DispatchQueue, DispatchRetained};
use objc2::{AnyThread, rc::Retained};
use objc2_foundation::NSError;
use objc2_virtualization::*;
use std::{
    fs::{File, OpenOptions},
    io,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc;

pub struct Settings {
    pub bundle: Bundle,
    pub pool: PathBuf,
    pub serial_log: PathBuf,
    pub cpus: usize,
    pub memory: u64,
}

impl Settings {
    pub fn validate(&self) -> io::Result<()> {
        let (_host, guest) = netlink::socket_pair()?;
        config::configuration(self, Some(guest)).map(drop)
    }
}

#[derive(Default)]
struct Machine {
    vm: Option<Retained<VZVirtualMachine>>,
}
// SAFETY: the retained VM is created, accessed and released only inside its serial DispatchQueue.
// Moving the slot across threads does not dereference the object. VzHypervisor::drop drains the slot.
unsafe impl Send for Machine {}
impl Machine {
    fn vm(&self) -> io::Result<&VZVirtualMachine> {
        self.vm
            .as_deref()
            .ok_or_else(|| io::Error::other("carrier stopped"))
    }
}

pub struct VzHypervisor {
    queue: DispatchRetained<DispatchQueue>,
    machine: Machine,
    settings: Settings,
    boot: Option<String>,
    target: u64,
    shares: Arc<RwLock<Shares>>,
    quic: Option<quic::CarrierQuicClient>,
    last_error: Option<String>,
}

impl VzHypervisor {
    pub fn new(settings: Settings) -> Self {
        Self {
            queue: DispatchQueue::new("com.afk-surf.agent-vmm-carrier.vm", None),
            machine: Machine::default(),
            boot: None,
            target: settings.memory,
            settings,
            shares: Arc::new(RwLock::new(Shares::new())),
            quic: None,
            last_error: None,
        }
    }
    fn on_queue<T: Send>(
        &mut self,
        operation: impl FnOnce(&mut Machine, &DispatchQueue) -> io::Result<T> + Send,
    ) -> io::Result<T> {
        let slot = &mut self.machine;
        let queue = &*self.queue;
        let mut result = None;
        queue.exec_sync(|| result = Some(operation(slot, queue)));
        result.expect("synchronous dispatch completed")
    }
    fn release(&mut self) {
        if let Some(quic) = self.quic.take() {
            quic.close();
        }
        let _ = self.on_queue(|m, _| {
            m.vm.take();
            Ok(())
        });
        self.boot = None;
    }
    fn state(&mut self) -> io::Result<State> {
        self.on_queue(|m, _| match m.vm.as_ref() {
            None => Ok(State::Stopped),
            // SAFETY: this query runs on the VM's assigned queue.
            Some(vm) => match unsafe { vm.state() } {
                VZVirtualMachineState::Stopped => Ok(State::Stopped),
                VZVirtualMachineState::Error => Ok(State::Failed),
                _ => Ok(State::Running),
            },
        })
    }
}

impl Drop for VzHypervisor {
    fn drop(&mut self) {
        self.release();
    }
}

type Completion = (
    RcBlock<dyn Fn(*mut NSError)>,
    mpsc::UnboundedReceiver<io::Result<()>>,
);
fn completion() -> Completion {
    let (tx, rx) = mpsc::unbounded_channel();
    let block = RcBlock::new(move |err: *mut NSError| {
        // SAFETY: VZ supplies nil or a live NSError for the duration of this callback.
        let result = unsafe { err.as_ref() }
            .map(|e| Err(io::Error::other(e.to_string())))
            .unwrap_or(Ok(()));
        let _ = tx.send(result);
    });
    (block, rx)
}
async fn completed<T>(mut rx: mpsc::UnboundedReceiver<io::Result<T>>) -> io::Result<T> {
    rx.recv()
        .await
        .ok_or_else(|| io::Error::other("VZ dropped its completion handler"))?
}

#[async_trait::async_trait]
impl CarrierHypervisor for VzHypervisor {
    async fn status(&mut self) -> io::Result<CarrierStatus> {
        let state = self.state()?;
        if state != State::Running {
            self.release();
        }
        let file = File::open(&self.settings.pool)?;
        let size = file.metadata()?.len();
        Ok(CarrierStatus {
            state,
            boot_id: self.boot.clone(),
            pid: std::process::id(),
            cpu_count: self.settings.cpus,
            memory_bytes: self.settings.memory,
            balloon_target_bytes: self.target,
            pool_logical_bytes: size,
            pool_allocated_bytes: sparse::allocated_bytes(&file)?,
            pool_hole_bytes: sparse::hole_bytes(&file, size)?,
            host_free_bytes: sparse::host_free_bytes(&self.settings.pool)?,
            last_error: self.last_error.clone(),
        })
    }
    async fn ensure_started(&mut self, _: String) -> io::Result<()> {
        if self.state()? == State::Running {
            return Ok(());
        }
        self.release();
        sparse::ensure_emergency_headroom(&self.settings.pool, 0, sparse::EMERGENCY_BYTES)?;
        let mut id = [0u8; 16];
        getrandom::getrandom(&mut id).map_err(|e| io::Error::other(e.to_string()))?;
        let (host_sock, guest_fd) = netlink::socket_pair()?;
        let quic = quic::CarrierQuicClient::new(host_sock, self.shares.clone())?;
        let settings = &self.settings;
        let slot = &mut self.machine;
        let queue = &*self.queue;
        let mut result = None;
        queue.exec_sync(|| {
            result = Some(config::configuration(settings, Some(guest_fd)).map(|cfg| {
                let (block, rx) = completion();
                // SAFETY: configuration was validated, the VM stays retained in the queue-confined slot;
                // VZ copies the callback for asynchronous delivery.
                unsafe {
                    let vm = VZVirtualMachine::initWithConfiguration_queue(
                        VZVirtualMachine::alloc(),
                        &cfg,
                        queue,
                    );
                    vm.startWithCompletionHandler(&block);
                    slot.store(vm);
                }
                rx
            }));
        });
        let started = match result.expect("dispatch completed") {
            Ok(rx) => completed(rx).await,
            Err(err) => Err(err),
        };
        if let Err(e) = started {
            quic.close();
            self.release();
            self.last_error = Some(e.to_string());
            return Err(e);
        }
        if let Err(e) = quic.wait_ready(std::time::Duration::from_secs(90)).await {
            quic.close();
            let _ = self.force_stop().await;
            self.last_error = Some(e.to_string());
            return Err(e);
        }
        self.quic = Some(quic);
        self.boot = Some(hex::encode(id));
        self.target = self.settings.memory;
        self.last_error = None;
        Ok(())
    }
    fn request_stop(&mut self) -> io::Result<()> {
        if self.state()? != State::Running {
            self.release();
            return Ok(());
        }
        self.on_queue(|m, _| {
            // SAFETY: requestStopWithError executes on the queue that owns this VM.
            unsafe { m.vm()?.requestStopWithError() }.map_err(config::error)
        })
    }
    async fn graceful_stop(&mut self) -> io::Result<()> {
        if self.state()? != State::Running {
            self.release();
            return Ok(());
        }
        if let Some(quic) = self.quic.clone() {
            let _ = quic.request_shutdown().await;
        }
        self.force_stop().await
    }
    async fn force_stop(&mut self) -> io::Result<()> {
        if self.state()? == State::Running {
            let rx = self.on_queue(|m, _| {
                let (block, rx) = completion();
                // SAFETY: the queue owns this VM and VZ copies the completion block.
                unsafe { m.vm()?.stopWithCompletionHandler(&block) };
                Ok(rx)
            })?;
            completed(rx).await?;
        }
        self.release();
        Ok(())
    }
    async fn set_target_virtual_machine_memory_size(&mut self, bytes: u64) -> io::Result<()> {
        // SAFETY: static VZ minimum getter has no instance or queue requirement.
        if bytes < unsafe { VZVirtualMachineConfiguration::minimumAllowedMemorySize() }
            || bytes > self.settings.memory
            || !bytes.is_multiple_of(1 << 20)
        {
            return Err(io::Error::other("invalid balloon target"));
        }
        self.on_queue(|m, _| {
            if let Some(vm) = &m.vm {
                // SAFETY: configuration installs one traditional balloon; the setter runs on the VM queue.
                unsafe {
                    let device = vm.memoryBalloonDevices().objectAtIndex(0);
                    device
                        .downcast_ref::<VZVirtioTraditionalMemoryBalloonDevice>()
                        .ok_or_else(|| io::Error::other("missing balloon device"))?
                        .setTargetVirtualMachineMemorySize(bytes);
                }
            }
            Ok(())
        })?;
        self.target = bytes;
        Ok(())
    }
    async fn set_host_shares(&mut self, shares: Shares) -> io::Result<()> {
        config::validate_shares(&shares)?;
        if let Ok(mut guard) = self.shares.write() {
            *guard = shares;
        }
        Ok(())
    }
    async fn punch(&mut self, offset: u64, len: u64) -> io::Result<()> {
        if self.state()? == State::Running {
            return Err(io::Error::other(
                "stop the carrier before offline pool maintenance",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.settings.pool)?;
        sparse::punch_hole(&file, offset, len)?;
        file.sync_all()
    }
    fn begin_open_vsock(&mut self, port: u32) -> io::Result<PendingVsock> {
        let quic = self
            .quic
            .clone()
            .ok_or_else(|| io::Error::other("carrier stopped"))?;
        Ok(Box::pin(async move { quic.open_stream(port).await }))
    }
}
impl Machine {
    fn store(&mut self, vm: Retained<VZVirtualMachine>) {
        self.vm = Some(vm);
    }
}
