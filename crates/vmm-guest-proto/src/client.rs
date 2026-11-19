//! Blocking client for dom0. Each call opens one TCP connection.

mod transport;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::time::Duration;
pub use transport::GuestStream;
use transport::Transport;

use crate::auth::{NONCE_LEN, verify_boot_token_proof};
use crate::frame::{ClientFrame, ServerFrame, read_frame, write_frame};
use crate::rpc::*;
use crate::{PROTOCOL_VERSION, Secret};

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    /// The Guest refused or failed the call.
    Rpc(RpcError),
    /// The peer did not follow the protocol.
    Protocol(&'static str),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "guest connection failed: {error}"),
            Self::Rpc(error) => write!(f, "guest call failed: {error}"),
            Self::Protocol(message) => write!(f, "guest protocol violation: {message}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;

const MISMATCH: ClientError = ClientError::Protocol("the reply does not match the call");

macro_rules! unary {
    ($client:expr, $call:expr, $variant:path) => {
        match $client.open($call)?.finish()? {
            $variant(value) => Ok(value),
            _ => Err(MISMATCH),
        }
    };
}

#[derive(Debug, Clone)]
pub struct Client {
    transport: Transport,
    connect_timeout: Duration,
}

struct Connection {
    stream: GuestStream,
}

impl Connection {
    fn send(&mut self, frame: &ClientFrame) -> io::Result<()> {
        write_frame(&mut self.stream, frame)
    }

    fn receive(&mut self) -> Result<ServerFrame> {
        loop {
            match read_frame(&mut self.stream)? {
                Some(ServerFrame::Keepalive) => {}
                Some(frame) => return Ok(frame),
                None => {
                    return Err(ClientError::Protocol(
                        "the connection closed before the reply",
                    ));
                }
            }
        }
    }

    /// Reads the reply of a call that streams nothing back.
    fn finish(&mut self) -> Result<Reply> {
        match self.receive()? {
            ServerFrame::Done(result) => result.map_err(ClientError::Rpc),
            _ => Err(ClientError::Protocol("unexpected stream frame")),
        }
    }

    /// Sends a byte stream, then reads the reply.
    ///
    /// A read failure of `source` ends the upload with that error, also when
    /// its kind is `Interrupted`. The Guest has ended the call when this
    /// returns, so a retry can continue from the bytes that the Guest kept.
    /// A send failure yields the Guest's own error when it rejected the upload early.
    fn upload(&mut self, source: &mut dyn Read) -> Result<Reply> {
        let mut buffer = vec![0u8; MAX_CHUNK_BYTES];
        let (error, send_failed) = loop {
            let frame = match source.read(&mut buffer) {
                Ok(0) => ClientFrame::End,
                Ok(count) => ClientFrame::Data(buffer[..count].to_vec()),
                Err(error) => break (error, false),
            };
            if let Err(error) = self.send(&frame) {
                break (error, true);
            }
            if matches!(frame, ClientFrame::End) {
                return self.finish();
            }
        };
        // The Guest waits for the next frame until its stream timeout. The end
        // of the stream makes it reply now.
        let _ = self.stream.shutdown(Shutdown::Write);
        match self.finish() {
            Err(ClientError::Rpc(rejection)) if send_failed => Err(ClientError::Rpc(rejection)),
            _ => Err(ClientError::Io(error)),
        }
    }

    /// Writes `Data` frames to `sink` until the reply arrives.
    fn download(&mut self, sink: &mut dyn Write) -> Result<Reply> {
        loop {
            match self.receive()? {
                ServerFrame::Data(data) => sink.write_all(&data)?,
                ServerFrame::Done(result) => return result.map_err(ClientError::Rpc),
                _ => return Err(ClientError::Protocol("unexpected stream frame")),
            }
        }
    }
}

impl Client {
    pub fn new(address: SocketAddr, token: Secret) -> Self {
        Self {
            transport: Transport::Direct(address, token),
            connect_timeout: Duration::from_secs(5),
        }
    }

    /// Resolve, connect and authenticate the exact boot anew for each RPC.
    /// The blocking dialer must return a fresh connected stream and its one-handshake credential.
    pub fn bound(
        instance: String,
        dial: impl Fn(Duration) -> Result<(GuestStream, Secret)> + Send + Sync + 'static,
    ) -> Self {
        Self {
            transport: Transport::Bound {
                instance,
                dial: std::sync::Arc::new(dial),
            },
            connect_timeout: Duration::from_secs(5),
        }
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    fn open(&self, call: Call) -> Result<Connection> {
        let (stream, token) = self.transport.connect(self.connect_timeout)?;
        Self::handshake(stream, token, call)
    }

    fn handshake(stream: GuestStream, token: Secret, call: Call) -> Result<Connection> {
        let mut connection = Connection { stream };
        let hello = Hello {
            version: PROTOCOL_VERSION,
            token,
            call,
        };
        connection.send(&ClientFrame::Hello(Box::new(hello)))?;
        Ok(connection)
    }

    /// Calls `Health` and verifies the Guest's proof of this client's token.
    pub fn health(&self, nonce: [u8; NONCE_LEN]) -> Result<HealthReply> {
        let (stream, token) = self.transport.connect(self.connect_timeout)?;
        let proof = match &self.transport {
            Transport::Bound { instance, .. } => {
                Some(crate::auth::boot_token_proof(&token, instance, &nonce))
            }
            Transport::Direct(..) => None,
        };
        let Reply::Health(reply) =
            Self::handshake(stream, token, Call::Health(HealthRequest { nonce }))?.finish()?
        else {
            return Err(MISMATCH);
        };
        let valid = match (&self.transport, proof) {
            (Transport::Bound { instance, .. }, Some(proof)) => {
                use subtle::ConstantTimeEq;
                instance == &reply.instance_id && bool::from(proof.ct_eq(&reply.boot_token_proof))
            }
            (Transport::Direct(_, token), _) => {
                verify_boot_token_proof(token, &reply.instance_id, &nonce, &reply.boot_token_proof)
            }
            _ => false,
        };
        if !valid {
            return Err(ClientError::Protocol(
                "the boot token proof does not verify",
            ));
        }
        Ok(*reply)
    }

    /// Runs a process and streams its input and output.
    pub fn exec_stream(&self, request: ExecRequest) -> Result<ExecSession> {
        Ok(ExecSession {
            connection: self.open(Call::Exec(request))?,
        })
    }

    /// Runs a process with `stdin` as its whole input and collects its output.
    /// `stdin` is sent only when `request.stdin` is set.
    pub fn exec(&self, request: ExecRequest, stdin: &[u8]) -> Result<ExecOutput> {
        let send_stdin = request.stdin;
        let mut session = self.exec_stream(request)?;
        let mut input = session.input()?;
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        std::thread::scope(|scope| {
            // Output is read while stdin is written, so neither side stalls the other.
            scope.spawn(move || {
                if send_stdin {
                    let _ = stdin
                        .chunks(MAX_CHUNK_BYTES)
                        .try_for_each(|chunk| input.write(chunk))
                        .and_then(|()| input.close());
                }
            });
            let result = loop {
                match session.next_event() {
                    Ok(ExecEvent::Stdout(data)) => stdout.extend_from_slice(&data),
                    Ok(ExecEvent::Stderr(data)) => stderr.extend_from_slice(&data),
                    Ok(ExecEvent::Exit(result)) => break Ok(result),
                    Err(error) => break Err(error),
                }
            };
            // Unblocks a stdin writer that the process no longer reads.
            let _ = session.connection.stream.shutdown(Shutdown::Both);
            result.map(|result| ExecOutput {
                stdout,
                stderr,
                result,
            })
        })
    }

    pub fn process_start(&self, request: ProcessStartRequest) -> Result<ProcessStartReply> {
        unary!(self, Call::ProcessStart(request), Reply::ProcessStart)
    }

    pub fn process_list(&self) -> Result<Vec<ProcessInfo>> {
        unary!(self, Call::ProcessList, Reply::ProcessList)
    }

    pub fn process_write(&self, request: ProcessWriteRequest) -> Result<ProcessWriteReply> {
        unary!(self, Call::ProcessWrite(request), Reply::ProcessWrite)
    }

    pub fn process_tail(&self, request: ProcessTailRequest) -> Result<ProcessTailReply> {
        unary!(self, Call::ProcessTail(request), Reply::ProcessTail)
    }

    pub fn process_stop(&self, request: ProcessStopRequest) -> Result<ProcessInfo> {
        unary!(self, Call::ProcessStop(request), Reply::Process)
    }

    pub fn workspace_stat(&self, target: WorkspaceTarget) -> Result<WorkspaceEntry> {
        unary!(self, Call::WorkspaceStat(target), Reply::WorkspaceEntry)
    }

    pub fn workspace_list(&self, request: WorkspaceListRequest) -> Result<WorkspaceListReply> {
        unary!(self, Call::WorkspaceList(request), Reply::WorkspaceList)
    }

    pub fn workspace_read(
        &self,
        request: WorkspaceReadRequest,
        sink: &mut dyn Write,
    ) -> Result<WorkspaceReadReply> {
        match self.open(Call::WorkspaceRead(request))?.download(sink)? {
            Reply::WorkspaceRead(reply) => Ok(reply),
            _ => Err(MISMATCH),
        }
    }

    pub fn workspace_write(
        &self,
        request: WorkspaceWriteRequest,
        source: &mut dyn Read,
    ) -> Result<WorkspaceWriteReply> {
        match self.open(Call::WorkspaceWrite(request))?.upload(source)? {
            Reply::WorkspaceWrite(reply) => Ok(reply),
            _ => Err(MISMATCH),
        }
    }

    /// Extracts a tar stream into a new directory.
    pub fn workspace_import_archive(
        &self,
        request: WorkspaceImportRequest,
        archive: &mut dyn Read,
    ) -> Result<WorkspaceImportReply> {
        match self
            .open(Call::WorkspaceImportArchive(request))?
            .upload(archive)?
        {
            Reply::WorkspaceImport(reply) => Ok(reply),
            _ => Err(MISMATCH),
        }
    }

    /// Writes a tar stream of a file or directory tree to `sink`.
    pub fn workspace_export_archive(
        &self,
        target: WorkspaceTarget,
        sink: &mut dyn Write,
    ) -> Result<WorkspaceExportReply> {
        match self
            .open(Call::WorkspaceExportArchive(target))?
            .download(sink)?
        {
            Reply::WorkspaceExport(reply) => Ok(reply),
            _ => Err(MISMATCH),
        }
    }

    pub fn read_logs(
        &self,
        request: ReadLogsRequest,
        mut on_record: impl FnMut(LogRecord),
    ) -> Result<LogsEnd> {
        let mut connection = self.open(Call::ReadLogs(request))?;
        loop {
            match connection.receive()? {
                ServerFrame::Log(record) => on_record(record),
                ServerFrame::Done(Ok(Reply::Logs(end))) => return Ok(end),
                ServerFrame::Done(Ok(_)) => return Err(MISMATCH),
                ServerFrame::Done(Err(error)) => return Err(ClientError::Rpc(error)),
                _ => return Err(ClientError::Protocol("unexpected stream frame")),
            }
        }
    }

    pub fn send_signal(&self, signal: Signal) -> Result<()> {
        match self.open(Call::SendSignal(signal))?.finish()? {
            Reply::Empty => Ok(()),
            _ => Err(MISMATCH),
        }
    }

    pub fn wait_main_exit(&self, timeout_ms: u32) -> Result<MainState> {
        unary!(
            self,
            Call::WaitMainExit(WaitMainExitRequest { timeout_ms }),
            Reply::Main
        )
    }

    pub fn quiesce(&self, request: QuiesceRequest) -> Result<QuiesceReply> {
        unary!(self, Call::Quiesce(request), Reply::Quiesce)
    }

    pub fn power_off(&self, request: PowerOffRequest) -> Result<PowerOffReply> {
        unary!(self, Call::PowerOff(request), Reply::PowerOff)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub result: ExecResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecEvent {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
    Exit(ExecResult),
}

pub struct ExecSession {
    connection: Connection,
}

impl ExecSession {
    /// A handle that writes to the process while another thread reads events.
    pub fn input(&self) -> io::Result<ExecInput> {
        Ok(ExecInput {
            connection: Connection {
                stream: self.connection.stream.try_clone()?,
            },
        })
    }

    pub fn next_event(&mut self) -> Result<ExecEvent> {
        match self.connection.receive()? {
            ServerFrame::Stdout(data) => Ok(ExecEvent::Stdout(data)),
            ServerFrame::Stderr(data) => Ok(ExecEvent::Stderr(data)),
            ServerFrame::Done(Ok(Reply::Exec(result))) => Ok(ExecEvent::Exit(result)),
            ServerFrame::Done(Ok(_)) => Err(MISMATCH),
            ServerFrame::Done(Err(error)) => Err(ClientError::Rpc(error)),
            _ => Err(ClientError::Protocol("unexpected stream frame")),
        }
    }
}

pub struct ExecInput {
    connection: Connection,
}

impl ExecInput {
    pub fn write(&mut self, data: &[u8]) -> io::Result<()> {
        data.chunks(MAX_CHUNK_BYTES)
            .try_for_each(|chunk| self.connection.send(&ClientFrame::Data(chunk.to_vec())))
    }

    /// Closes the process's stdin.
    pub fn close(&mut self) -> io::Result<()> {
        self.connection.send(&ClientFrame::End)
    }

    pub fn signal(&mut self, signal: Signal) -> io::Result<()> {
        self.connection.send(&ClientFrame::Signal(signal))
    }

    pub fn resize(&mut self, size: TtySize) -> io::Result<()> {
        self.connection.send(&ClientFrame::Resize(size))
    }
}
