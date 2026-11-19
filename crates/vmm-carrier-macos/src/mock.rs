#![forbid(unsafe_code)]
use crate::{
    protocol::*,
    server::{CarrierHypervisor, PendingVsock},
};
use std::{collections::BTreeMap, io, path::PathBuf};
use tokio::net::UnixStream;

/// In-process VM state with a Unix socket substituting for each Guest vsock listener.
pub struct MockCarrierHypervisor {
    endpoints: BTreeMap<u32, PathBuf>,
    status: CarrierStatus,
    boots: u64,
    shares: Shares,
}
impl MockCarrierHypervisor {
    pub fn set_host_free_bytes(&mut self, bytes: u64) {
        self.status.host_free_bytes = bytes;
    }
    pub fn new(endpoints: BTreeMap<u32, PathBuf>) -> Self {
        Self {
            endpoints,
            boots: 0,
            shares: Shares::new(),
            status: CarrierStatus {
                state: State::Stopped,
                boot_id: None,
                pid: std::process::id(),
                cpu_count: 2,
                memory_bytes: 1 << 30,
                balloon_target_bytes: 1 << 30,
                pool_logical_bytes: 64 << 30,
                pool_allocated_bytes: 0,
                pool_hole_bytes: 64 << 30,
                host_free_bytes: 128 << 30,
                last_error: None,
            },
        }
    }
}
#[async_trait::async_trait]
impl CarrierHypervisor for MockCarrierHypervisor {
    async fn status(&mut self) -> io::Result<CarrierStatus> {
        Ok(self.status.clone())
    }
    async fn ensure_started(&mut self, _: String) -> io::Result<()> {
        if self.status.state != State::Running {
            self.boots += 1;
            self.status.boot_id = Some(self.boots.to_string());
            self.status.state = State::Running;
        }
        Ok(())
    }
    async fn force_stop(&mut self) -> io::Result<()> {
        self.request_stop()
    }
    fn request_stop(&mut self) -> io::Result<()> {
        self.status.state = State::Stopped;
        self.status.boot_id = None;
        Ok(())
    }
    async fn set_target_virtual_machine_memory_size(&mut self, bytes: u64) -> io::Result<()> {
        self.status.balloon_target_bytes = bytes;
        Ok(())
    }
    async fn set_host_shares(&mut self, shares: Shares) -> io::Result<()> {
        self.shares = shares;
        Ok(())
    }
    async fn punch(&mut self, _: u64, _: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    fn begin_open_vsock(&mut self, port: u32) -> io::Result<PendingVsock> {
        if self.status.state != State::Running {
            return Err(io::Error::other("carrier stopped"));
        }
        let path = self
            .endpoints
            .get(&port)
            .ok_or_else(|| io::Error::other("unknown Guest port"))?
            .clone();
        Ok(Box::pin(UnixStream::connect(path)))
    }
}
