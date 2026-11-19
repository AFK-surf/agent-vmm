use std::{
    io::{Read, Write},
    net::SocketAddr,
};
use vmm_core::{Error, Result};
use vmm_guest_proto::{
    Secret,
    client::{Client, ClientError, ExecSession},
    rpc::*,
};

#[derive(Clone)]
pub struct Dom0Runtime {
    client: Client,
}

/// Extract the original guest error for host.v1 status translation.
pub fn guest_rpc_error(error: &Error) -> Option<&RpcError> {
    let Error::Io(io) = error else { return None };
    match io.get_ref()?.downcast_ref::<ClientError>()? {
        ClientError::Rpc(detail) => Some(detail),
        _ => None,
    }
}

fn guest_error(err: ClientError) -> Error {
    std::io::Error::other(err).into()
}

macro_rules! unary {
    ($name:ident,$req:ty,$reply:ty) => {
        pub async fn $name(&self, request: $req) -> Result<$reply> {
            self.call(move |c| c.$name(request)).await
        }
    };
}

impl Dom0Runtime {
    pub(crate) fn new(addr: SocketAddr, token: Secret) -> Self {
        Self {
            client: Client::new(addr, token),
        }
    }
    /// For host adapters that already own a blocking-pool thread.
    pub fn blocking_client(&self) -> &Client {
        &self.client
    }

    pub async fn call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Client) -> vmm_guest_proto::client::Result<T> + Send + 'static,
    ) -> Result<T> {
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || f(&client).map_err(guest_error))
            .await
            .map_err(|_| Error::Interrupted)?
    }
    pub async fn health(&self) -> Result<HealthReply> {
        let nonce = *crate::keys::random()?;
        self.call(move |c| c.health(nonce)).await
    }
    unary!(exec_stream, ExecRequest, ExecSession);
    unary!(process_start, ProcessStartRequest, ProcessStartReply);
    pub async fn process_list(&self) -> Result<Vec<ProcessInfo>> {
        self.call(Client::process_list).await
    }
    unary!(process_write, ProcessWriteRequest, ProcessWriteReply);
    unary!(process_tail, ProcessTailRequest, ProcessTailReply);
    unary!(process_stop, ProcessStopRequest, ProcessInfo);
    unary!(workspace_stat, WorkspaceTarget, WorkspaceEntry);
    unary!(workspace_list, WorkspaceListRequest, WorkspaceListReply);
    unary!(send_signal, Signal, ());
    unary!(wait_main_exit, u32, MainState);
    pub async fn workspace_read<W: Write + Send + 'static>(
        &self,
        req: WorkspaceReadRequest,
        mut out: W,
    ) -> Result<WorkspaceReadReply> {
        self.call(move |c| c.workspace_read(req, &mut out)).await
    }
    pub async fn workspace_write<R: Read + Send + 'static>(
        &self,
        req: WorkspaceWriteRequest,
        mut input: R,
    ) -> Result<WorkspaceWriteReply> {
        self.call(move |c| c.workspace_write(req, &mut input)).await
    }
    pub async fn workspace_import_archive<R: Read + Send + 'static>(
        &self,
        req: WorkspaceImportRequest,
        mut input: R,
    ) -> Result<WorkspaceImportReply> {
        self.call(move |c| c.workspace_import_archive(req, &mut input))
            .await
    }
    pub async fn workspace_export_archive<W: Write + Send + 'static>(
        &self,
        req: WorkspaceTarget,
        mut out: W,
    ) -> Result<WorkspaceExportReply> {
        self.call(move |c| c.workspace_export_archive(req, &mut out))
            .await
    }
    pub async fn read_logs(
        &self,
        req: ReadLogsRequest,
        on_record: impl FnMut(LogRecord) + Send + 'static,
    ) -> Result<LogsEnd> {
        self.call(move |c| c.read_logs(req, on_record)).await
    }
}
