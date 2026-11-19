//! Serves the RPC of `vmm-guest-proto`: one thread and one call for each connection.

use std::io::{self, Read};
use std::net::{IpAddr, Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use vmm_guest_proto::PROTOCOL_VERSION;
use vmm_guest_proto::frame::{ClientFrame, ServerFrame, read_frame, write_frame};
use vmm_guest_proto::rpc::{
    Call, ErrorCode, Hello, LogsEnd, MAX_WAIT_MS, ReadLogsRequest, Reply, RpcError,
};

use crate::log_ring::LogRing;
use crate::process_manager::{run_exec, signal_number};
use crate::runtime::Runtime;
use crate::{io_error, lock};

/// Time that a client gets to read the reply and close after the call ended.
pub const LINGER: Duration = Duration::from_secs(2);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest silence of a client that streams data, and longest stall of one that reads.
const STREAM_TIMEOUT: Duration = Duration::from_secs(120);
const KEEPALIVE: Duration = Duration::from_secs(15);
const MAX_CONNECTIONS: usize = 64;
const LOG_BATCH_RECORDS: usize = 128;
const MAX_ERROR_MESSAGE_BYTES: usize = 2048;

/// The sending half. Threads of one call share it frame by frame.
pub struct Outbound(Mutex<TcpStream>);

impl Outbound {
    pub fn send(&self, frame: &ServerFrame) -> io::Result<()> {
        write_frame(&mut *lock(&self.0), frame)
    }
}

pub struct Connection {
    pub inbound: TcpStream,
    pub outbound: Outbound,
}

impl Connection {
    pub fn receive_from(inbound: &mut TcpStream) -> io::Result<Option<ClientFrame>> {
        read_frame(inbound)
    }

    /// Lets the client read the reply. Closing with unread input would reset
    /// the connection and could discard the reply.
    fn linger(&mut self) {
        let _ = self.inbound.shutdown(Shutdown::Write);
        let _ = self.inbound.set_read_timeout(Some(LINGER));
        let deadline = Instant::now() + LINGER;
        let mut discarded = [0u8; 4096];
        while Instant::now() < deadline
            && matches!(self.inbound.read(&mut discarded), Ok(count) if count > 0)
        {}
    }
}

/// The client's `Data` frames as one byte stream that ends at `End`.
pub struct Upload<'a> {
    inbound: &'a mut TcpStream,
    chunk: Vec<u8>,
    position: usize,
    ended: bool,
}

impl<'a> Upload<'a> {
    fn new(connection: &'a mut Connection) -> Self {
        Self {
            inbound: &mut connection.inbound,
            chunk: Vec::new(),
            position: 0,
            ended: false,
        }
    }
}

impl Read for Upload<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        while self.position == self.chunk.len() && !self.ended {
            match Connection::receive_from(self.inbound)? {
                Some(ClientFrame::Data(data)) => (self.chunk, self.position) = (data, 0),
                Some(ClientFrame::End) => self.ended = true,
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected frame",
                    ));
                }
                None => return Err(io::ErrorKind::UnexpectedEof.into()),
            }
        }
        let count = buffer.len().min(self.chunk.len() - self.position);
        buffer[..count].copy_from_slice(&self.chunk[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

pub struct Server {
    runtime: Arc<Runtime>,
    /// When set, only this peer address can call. The workload shares the
    /// network of PID 1, so its own connections come from another address.
    allowed_peer: Option<IpAddr>,
    connections: AtomicUsize,
}

struct Slot<'a>(&'a AtomicUsize);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Server {
    pub fn new(runtime: Arc<Runtime>, allowed_peer: Option<IpAddr>) -> Arc<Self> {
        Arc::new(Self {
            runtime,
            allowed_peer,
            connections: AtomicUsize::new(0),
        })
    }

    /// Accepts connections until the listener fails, and returns that failure.
    pub fn serve(self: &Arc<Self>, listener: TcpListener) -> io::Error {
        loop {
            let (stream, peer) = match listener.accept() {
                Ok(accepted) => accepted,
                Err(error) => match error.raw_os_error() {
                    Some(libc::ECONNABORTED | libc::EINTR) => continue,
                    Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM) => {
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                    _ => return error,
                },
            };
            if self
                .allowed_peer
                .is_some_and(|allowed| peer.ip().to_canonical() != allowed)
            {
                continue;
            }
            if self.connections.fetch_add(1, Ordering::Relaxed) >= MAX_CONNECTIONS {
                self.connections.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            let server = self.clone();
            let started = std::thread::Builder::new()
                .name("rpc".into())
                .spawn(move || {
                    let _slot = Slot(&server.connections);
                    server.handle(stream);
                });
            if started.is_err() {
                self.connections.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    fn handle(&self, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let _ = stream.set_write_timeout(Some(STREAM_TIMEOUT));
        let _ = stream.set_read_timeout(Some(HELLO_TIMEOUT));
        let Ok(outbound) = stream.try_clone() else {
            return;
        };
        let mut connection = Connection {
            inbound: stream,
            outbound: Outbound(Mutex::new(outbound)),
        };
        let Ok(Some(ClientFrame::Hello(hello))) = Connection::receive_from(&mut connection.inbound)
        else {
            return;
        };
        let Hello {
            version,
            token,
            call,
        } = *hello;
        let result = if version != PROTOCOL_VERSION {
            Err(RpcError::new(
                ErrorCode::UnsupportedVersion,
                format!("version {version}"),
            ))
        } else if token != self.runtime.token {
            Err(RpcError::new(
                ErrorCode::Unauthenticated,
                "the token does not match",
            ))
        } else {
            let _ = connection.inbound.set_read_timeout(Some(STREAM_TIMEOUT));
            self.dispatch(call, &mut connection)
        };
        let power_off = matches!(result, Ok(Some(Reply::PowerOff(_))));
        if let Some(mut result) = result.transpose() {
            if let Err(error) = &mut result {
                // A message can quote a caller's argument. The reply must fit one frame.
                let mut end = error.message.len().min(MAX_ERROR_MESSAGE_BYTES);
                while !error.message.is_char_boundary(end) {
                    end -= 1;
                }
                error.message.truncate(end);
            }
            let _ = connection.outbound.send(&ServerFrame::Done(result));
            connection.linger();
        }
        if power_off {
            self.runtime.system.power_off();
        }
    }

    /// Runs one call. `None` means that the call already sent its reply.
    fn dispatch(&self, call: Call, connection: &mut Connection) -> Result<Option<Reply>, RpcError> {
        let runtime = &*self.runtime;
        let (processes, workspace) = (&runtime.processes, &runtime.workspace);
        let reply = match call {
            Call::Health(request) => Reply::Health(Box::new(runtime.health(&request))),
            Call::Exec(request) => {
                // An interactive process can stay silent for any time.
                let _ = connection.inbound.set_read_timeout(None);
                run_exec(processes, request, connection)?;
                return Ok(None);
            }
            Call::ProcessStart(request) => Reply::ProcessStart(processes.start_process(request)?),
            Call::ProcessList => Reply::ProcessList(processes.table.list()),
            Call::ProcessWrite(request) => Reply::ProcessWrite(processes.table.write(&request)?),
            Call::ProcessTail(request) => Reply::ProcessTail(processes.table.tail(&request)?),
            Call::ProcessStop(request) => {
                Reply::Process(processes.table.stop(processes.reaper(), &request)?)
            }
            Call::WorkspaceStat(target) => Reply::WorkspaceEntry(workspace.stat(&target)?),
            Call::WorkspaceList(request) => Reply::WorkspaceList(workspace.list(&request)?),
            Call::WorkspaceRead(request) => {
                Reply::WorkspaceRead(workspace.read(&request, &connection.outbound)?)
            }
            Call::WorkspaceWrite(request) => {
                Reply::WorkspaceWrite(workspace.write(&request, &mut Upload::new(connection))?)
            }
            Call::WorkspaceImportArchive(request) => Reply::WorkspaceImport(
                workspace.import_archive(&request, &mut Upload::new(connection))?,
            ),
            Call::WorkspaceExportArchive(target) => {
                Reply::WorkspaceExport(workspace.export_archive(&target, &connection.outbound)?)
            }
            Call::ReadLogs(request) => Reply::Logs(read_logs(
                &processes.main.logs,
                &request,
                &connection.outbound,
            )?),
            Call::SendSignal(signal) => {
                processes
                    .main
                    .signal(processes.reaper(), signal_number(signal))?;
                Reply::Empty
            }
            Call::WaitMainExit(request) => {
                let timeout = Duration::from_millis(request.timeout_ms.min(MAX_WAIT_MS).into());
                Reply::Main(processes.main.wait_exit(timeout))
            }
            Call::Quiesce(request) => Reply::Quiesce(processes.quiesce(&request)),
            Call::PowerOff(request) => Reply::PowerOff(runtime.prepare_power_off(&request)?),
        };
        Ok(Some(reply))
    }
}

fn read_logs(
    ring: &LogRing,
    request: &ReadLogsRequest,
    outbound: &Outbound,
) -> Result<LogsEnd, RpcError> {
    let since = request.since_unix_nanos;
    let mut position = match request.after_cursor {
        Some(cursor) => cursor,
        None if request.tail_records > 0 => ring.tail_position(request.tail_records, since),
        None => 0,
    };
    loop {
        let batch = ring.read(position, since, LOG_BATCH_RECORDS);
        for record in batch.records {
            position = record.cursor;
            outbound.send(&ServerFrame::Log(record)).map_err(io_error)?;
        }
        if batch.more {
            continue;
        }
        // Records that `since` excludes are behind the reader too.
        position = position.max(batch.last_cursor);
        if batch.ended || !request.follow {
            return Ok(LogsEnd {
                next_cursor: position,
                ended: batch.ended,
            });
        }
        if !ring.wait(position, KEEPALIVE) {
            outbound.send(&ServerFrame::Keepalive).map_err(io_error)?;
        }
    }
}
