//! Background processes that callers start, write to, tail, and stop.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::io::{self, Read};
use std::os::fd::AsFd;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use vmm_guest_proto::rpc::{
    ErrorCode, MAX_CHUNK_BYTES, MAX_WAIT_MS, ProcessInfo, ProcessStartReply, ProcessStartRequest,
    ProcessState, ProcessStopRequest, ProcessTailReply, ProcessTailRequest, ProcessWriteReply,
    ProcessWriteRequest, RpcError,
};

use super::{KILL_WAIT, OUTPUT_DRAIN, signal_number};
use crate::reaper::{ExitCell, Reaper};
use crate::spawn::{self, SpawnContext};
use crate::{check_request_id, io_error, lock, sys, unix_nanos};

const MAX_PROCESSES: usize = 256;
const DEFAULT_OUTPUT_LIMIT: usize = 1 << 20;
const MAX_OUTPUT_LIMIT: usize = 16 << 20;
const MAX_TAIL_BYTES: usize = 512 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Keeps a full `ProcessList` reply inside one frame.
const MAX_INFO_ARGV_BYTES: usize = 2048;

#[derive(Default)]
struct Window {
    data: VecDeque<u8>,
    /// Offset of the first retained byte.
    start: u64,
    /// The process and its descendants closed the output.
    closed: bool,
    /// The output closed, or its drain time after the exit passed.
    settled: bool,
}

/// The newest merged output of one process.
struct Output {
    limit: usize,
    window: Mutex<Window>,
    changed: Condvar,
}

impl Output {
    fn append(&self, chunk: &[u8]) {
        let mut window = lock(&self.window);
        window.data.extend(chunk);
        let excess = window.data.len().saturating_sub(self.limit);
        window.data.drain(..excess);
        window.start += excess as u64;
        drop(window);
        self.changed.notify_all();
    }

    fn update(&self, change: impl FnOnce(&mut Window)) {
        change(&mut lock(&self.window));
        self.changed.notify_all();
    }

    fn end_offset(&self) -> u64 {
        let window = lock(&self.window);
        window.start + window.data.len() as u64
    }
}

struct Entry {
    id: u64,
    request: ProcessStartRequest,
    pid: u32,
    started_unix_nanos: i64,
    exit: Arc<ExitCell>,
    stdin: Mutex<Option<File>>,
    output: Arc<Output>,
}

/// The leading arguments within the size that `ProcessInfo` reports.
fn summary(argv: &[String]) -> Vec<String> {
    let mut room = MAX_INFO_ARGV_BYTES;
    let mut summary = Vec::new();
    for argument in argv {
        let mut end = argument.len().min(room);
        while !argument.is_char_boundary(end) {
            end -= 1;
        }
        summary.push(argument[..end].to_owned());
        room -= end;
        if end < argument.len() {
            break;
        }
    }
    summary
}

impl Entry {
    fn info(&self) -> ProcessInfo {
        ProcessInfo {
            process_id: self.id.to_string(),
            request_id: self.request.request_id.clone(),
            pid: self.pid,
            argv: summary(&self.request.command.argv),
            state: match self.exit.get() {
                Some((exit, _)) => ProcessState::Exited(exit),
                None => ProcessState::Running,
            },
            started_unix_nanos: self.started_unix_nanos,
            output_end_offset: self.output.end_offset(),
        }
    }
}

#[derive(Default)]
struct Table {
    by_id: BTreeMap<u64, Arc<Entry>>,
    by_request: HashMap<String, u64>,
    last_id: u64,
}

#[derive(Default)]
pub struct ProcessTable {
    inner: Mutex<Table>,
}

impl ProcessTable {
    /// Starts a process. A repeated `request_id` returns the process of the
    /// first call and starts nothing.
    pub(super) fn start(
        &self,
        reaper: &Reaper,
        context: &SpawnContext,
        request: ProcessStartRequest,
    ) -> Result<ProcessStartReply, RpcError> {
        check_request_id(&request.request_id)?;
        let id = &request.request_id;
        // The lock spans the start, so a concurrent duplicate waits and then replays.
        let mut table = lock(&self.inner);
        if let Some(entry) = table.by_request.get(id).and_then(|id| table.by_id.get(id)) {
            return if entry.request == request {
                Ok(ProcessStartReply {
                    process: entry.info(),
                    replayed: true,
                })
            } else {
                Err(RpcError::new(
                    ErrorCode::AlreadyExists,
                    "request_id names a process with another command",
                ))
            };
        }
        if table.by_id.len() >= MAX_PROCESSES {
            let exited = table
                .by_id
                .values()
                .find(|entry| entry.exit.get().is_some());
            let Some(exited) = exited.cloned() else {
                return Err(RpcError::new(
                    ErrorCode::ResourceExhausted,
                    "too many processes",
                ));
            };
            table.by_id.remove(&exited.id);
            table.by_request.remove(&exited.request.request_id);
        }

        let limit = match request.output_limit_bytes {
            0 => DEFAULT_OUTPUT_LIMIT,
            limit => limit.min(MAX_OUTPUT_LIMIT as u64) as usize,
        };
        let output = Arc::new(Output {
            limit,
            window: Mutex::default(),
            changed: Condvar::new(),
        });
        let (output_read, output_write) = sys::pipe().map_err(io_error)?;
        let (stdin_read, stdin_write) = sys::pipe().map_err(io_error)?;
        sys::set_nonblocking(stdin_write.as_fd()).map_err(io_error)?;
        let mut command = spawn::command(context, &request.command, false)?;
        command
            .stdin(stdin_read)
            .stdout(output_write.try_clone().map_err(io_error)?)
            .stderr(output_write);
        let on_exit = output.clone();
        let spawned = reaper
            .spawn(command, Some(Box::new(move || on_exit.update(|_| {}))))
            .map_err(|error| {
                let program = &request.command.argv[0];
                RpcError::new(
                    ErrorCode::InvalidArgument,
                    format!("start {program:?}: {error}"),
                )
            })?;

        let sink = output.clone();
        let mut source = File::from(output_read);
        // The thread blocks in `read` and ends with the last writer of the pipe.
        std::thread::spawn(move || {
            let mut buffer = vec![0u8; 64 * 1024];
            loop {
                match source.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => sink.append(&buffer[..count]),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
            sink.update(|window| (window.closed, window.settled) = (true, true));
        });

        table.last_id += 1;
        let entry = Arc::new(Entry {
            id: table.last_id,
            pid: spawned.pid,
            started_unix_nanos: unix_nanos(),
            exit: spawned.exit,
            stdin: Mutex::new(Some(File::from(stdin_write))),
            output,
            request,
        });
        table
            .by_request
            .insert(entry.request.request_id.clone(), entry.id);
        table.by_id.insert(entry.id, entry.clone());
        Ok(ProcessStartReply {
            process: entry.info(),
            replayed: false,
        })
    }

    fn get(&self, process_id: &str) -> Result<Arc<Entry>, RpcError> {
        let id = process_id.parse::<u64>().ok();
        id.and_then(|id| lock(&self.inner).by_id.get(&id).cloned())
            .ok_or_else(|| RpcError::new(ErrorCode::NotFound, "no such process"))
    }

    pub fn list(&self) -> Vec<ProcessInfo> {
        lock(&self.inner)
            .by_id
            .values()
            .map(|entry| entry.info())
            .collect()
    }

    pub(super) fn running(&self) -> Vec<(u32, Arc<ExitCell>)> {
        let table = lock(&self.inner);
        let running = table
            .by_id
            .values()
            .filter(|entry| entry.exit.get().is_none());
        running
            .map(|entry| (entry.pid, entry.exit.clone()))
            .collect()
    }

    pub fn write(&self, request: &ProcessWriteRequest) -> Result<ProcessWriteReply, RpcError> {
        let entry = self.get(&request.process_id)?;
        let mut stdin = lock(&entry.stdin);
        let Some(pipe) = stdin.as_ref() else {
            return Err(RpcError::new(
                ErrorCode::FailedPrecondition,
                "stdin is closed",
            ));
        };
        let deadline = Instant::now() + WRITE_TIMEOUT;
        match sys::write_all_watched(pipe, &request.data, Some(deadline), None) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                return Err(RpcError::new(
                    ErrorCode::DeadlineExceeded,
                    "the process does not read its stdin",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                *stdin = None;
                return Err(RpcError::new(
                    ErrorCode::FailedPrecondition,
                    "the process closed its stdin",
                ));
            }
            Err(error) => return Err(io_error(error)),
        }
        if request.close_stdin {
            *stdin = None;
        }
        Ok(ProcessWriteReply {
            bytes_written: request.data.len() as u64,
        })
    }

    /// Returns output after `since_offset`. Waits for output or for the exit.
    ///
    /// The reply reports the exit only after the output closed or its drain
    /// time passed, so a caller that stops at `Exited` has the last output.
    pub fn tail(&self, request: &ProcessTailRequest) -> Result<ProcessTailReply, RpcError> {
        let entry = self.get(&request.process_id)?;
        let max_bytes = match request.max_bytes {
            0 => MAX_CHUNK_BYTES,
            max_bytes => (max_bytes as usize).min(MAX_TAIL_BYTES),
        };
        let deadline =
            Instant::now() + Duration::from_millis(request.wait_ms.min(MAX_WAIT_MS).into());
        let mut drain_deadline = None;
        let mut window = lock(&entry.output.window);
        let exit = loop {
            let now = Instant::now();
            let exit = entry.exit.get().map(|(exit, _)| exit);
            let has_output = request.since_offset < window.start + window.data.len() as u64;
            let wait_until = match exit {
                Some(_) if !window.settled => *drain_deadline.get_or_insert(now + OUTPUT_DRAIN),
                None if !has_output => deadline,
                _ => break exit,
            };
            if wait_until <= now {
                window.settled |= exit.is_some();
                break exit.filter(|_| window.settled);
            }
            window = entry
                .output
                .changed
                .wait_timeout(window, wait_until - now)
                .unwrap_or_else(|error| error.into_inner())
                .0;
        };
        let end = window.start + window.data.len() as u64;
        let offset = request.since_offset.clamp(window.start, end);
        let skip = (offset - window.start) as usize;
        let data: Vec<u8> = window
            .data
            .iter()
            .skip(skip)
            .take(max_bytes)
            .copied()
            .collect();
        Ok(ProcessTailReply {
            next_offset: offset + data.len() as u64,
            offset,
            data,
            state: exit.map_or(ProcessState::Running, ProcessState::Exited),
        })
    }

    /// Signals the process group, then kills it when it outlives the grace time.
    pub fn stop(
        &self,
        reaper: &Reaper,
        request: &ProcessStopRequest,
    ) -> Result<ProcessInfo, RpcError> {
        let entry = self.get(&request.process_id)?;
        let signaled = reaper
            .signal(entry.pid, signal_number(request.signal), true)
            .map_err(io_error)?;
        let grace = Duration::from_millis(request.grace_ms.into());
        if signaled && entry.exit.wait(Some(grace)).is_none() {
            reaper
                .signal(entry.pid, libc::SIGKILL, true)
                .map_err(io_error)?;
            entry.exit.wait(Some(KILL_WAIT));
        }
        Ok(entry.info())
    }
}
