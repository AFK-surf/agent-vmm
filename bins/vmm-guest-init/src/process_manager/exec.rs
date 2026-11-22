//! Runs one process for an `Exec` call and relays its input and output.

use std::fs::File;
use std::net::Shutdown;
use std::os::fd::AsFd;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use vmm_guest_proto::frame::{ClientFrame, ServerFrame};
use vmm_guest_proto::rpc::{ErrorCode, ExecRequest, ExecResult, ExitStatus, Reply, RpcError};

use super::cgroup::SubCgroup;
use super::{OUTPUT_DRAIN, ProcessManager, signal_number};
use crate::reaper::{ExitCell, Reaper};
use crate::rpc_server::{Connection, LINGER};
use crate::{io_error, spawn, sys};

const TERM_GRACE: Duration = Duration::from_millis(200);

fn kill_tree(reaper: &Reaper, pid: u32, exit: &ExitCell, sub_cgroup: Option<&SubCgroup>) {
    let _ = reaper.signal(pid, libc::SIGTERM, true);
    if exit.wait(Some(TERM_GRACE)).is_none() {
        let _ = reaper.signal(pid, libc::SIGKILL, true);
    }
    if let Some(cg) = sub_cgroup {
        cg.kill();
    } else {
        // Fallback when no sub-cgroup exists: kill any remaining members of the session's pgid.
        let _ = sys::kill(pid, libc::SIGKILL, true);
    }
}

/// Runs the process to its exit and sends the reply.
///
/// An error means that no process started and that the caller replies.
/// The process group is killed at the timeout and when the client disconnects,
/// because no later call can reach it.
pub fn run(
    manager: &ProcessManager,
    request: ExecRequest,
    connection: &mut Connection,
) -> Result<(), RpcError> {
    let _permit = manager.admit_exec()?;
    let sub_cgroup = SubCgroup::create(
        manager.context.cgroup_dir(),
        &format!("exec-{}", manager.next_exec_seq()),
    );
    let spawn_ctx = match &sub_cgroup {
        Some(cg) => manager.context.with_cgroup_procs(cg.procs_path()),
        None => manager.context.clone(),
    };
    let mut command = match spawn::command(&spawn_ctx, &request.command, request.tty.is_some()) {
        Ok(command) => command,
        Err(error) => {
            if let Some(cg) = &sub_cgroup {
                cg.finish(false, false);
            }
            return Err(error);
        }
    };
    let (stop_read, stop_write) = sys::pipe().map_err(io_error)?;
    let watch = connection.inbound.try_clone().map_err(io_error)?;

    let mut terminal = None;
    let mut input = None;
    let outputs = if let Some(size) = request.tty {
        let pty = sys::open_pty(size).map_err(io_error)?;
        command
            .stdin(pty.slave.try_clone().map_err(io_error)?)
            .stdout(pty.slave.try_clone().map_err(io_error)?)
            .stderr(pty.slave);
        input = Some(pty.master.try_clone().map_err(io_error)?);
        let outputs = [Some(pty.master.try_clone().map_err(io_error)?), None];
        terminal = Some(pty.master);
        outputs
    } else {
        let (stdout_read, stdout_write) = sys::pipe().map_err(io_error)?;
        let (stderr_read, stderr_write) = sys::pipe().map_err(io_error)?;
        command
            .stdin(Stdio::null())
            .stdout(stdout_write)
            .stderr(stderr_write);
        if request.stdin {
            let (stdin_read, stdin_write) = sys::pipe().map_err(io_error)?;
            sys::set_nonblocking(stdin_write.as_fd()).map_err(io_error)?;
            command.stdin(stdin_read);
            input = Some(File::from(stdin_write));
        }
        [Some(File::from(stdout_read)), Some(File::from(stderr_read))]
    };

    let spawned = manager.reaper.spawn(command, None).map_err(|error| {
        if let Some(cg) = &sub_cgroup {
            cg.finish(false, false);
        }
        let program = &request.command.argv[0];
        RpcError::new(
            ErrorCode::InvalidArgument,
            format!("start {program:?}: {error}"),
        )
    })?;
    let (reaper, pid) = (manager.reaper, spawned.pid);
    let finished = AtomicBool::new(false);
    let disconnected = AtomicBool::new(false);
    let truncated = [AtomicBool::new(false), AtomicBool::new(false)];
    let (pump_done, pumps_done) = mpsc::channel();
    let (input_done, inputs_done) = mpsc::channel();
    let Connection { inbound, outbound } = connection;
    let limit = request.max_output_bytes;

    std::thread::scope(|scope| {
        let mut pumps = 0;
        for (index, source) in outputs.into_iter().enumerate() {
            let Some(source) = source else { continue };
            pumps += 1;
            let (done, truncated, stop, outbound) = (
                pump_done.clone(),
                &truncated[index],
                stop_read.as_fd(),
                &*outbound,
            );
            scope.spawn(move || {
                let mut sent = 0u64;
                sys::pump(source, stop, |chunk| {
                    let room = match limit {
                        0 => chunk.len(),
                        limit => (limit - sent).min(chunk.len() as u64) as usize,
                    };
                    if room < chunk.len() {
                        truncated.store(true, Ordering::Relaxed);
                    }
                    if room > 0 {
                        sent += room as u64;
                        let data = chunk[..room].to_vec();
                        let frame = match index {
                            0 => ServerFrame::Stdout(data),
                            _ => ServerFrame::Stderr(data),
                        };
                        // A failed send means a closed client. The input thread handles it.
                        let _ = outbound.send(&frame);
                    }
                });
                let _ = done.send(());
            });
        }

        let (finished, disconnected, terminal, watch, exit_cell, sub_cg) = (
            &finished,
            &disconnected,
            &terminal,
            &watch,
            &spawned.exit,
            sub_cgroup.as_ref(),
        );
        scope.spawn(move || {
            loop {
                match Connection::receive_from(inbound) {
                    Ok(Some(ClientFrame::Data(data))) => {
                        let Some(sink) = &input else { continue };
                        match sys::write_all_watched(sink, &data, None, Some(watch.as_fd())) {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::ConnectionAborted => {
                                break;
                            }
                            // The process closed its stdin.
                            Err(_) => input = None,
                        }
                    }
                    Ok(Some(ClientFrame::End)) if terminal.is_none() => input = None,
                    Ok(Some(ClientFrame::End)) => {}
                    Ok(Some(ClientFrame::Signal(signal))) => {
                        let _ = reaper.signal(pid, signal_number(signal), true);
                    }
                    Ok(Some(ClientFrame::Resize(size))) => {
                        if let Some(terminal) = terminal {
                            let _ = sys::set_window_size(terminal.as_fd(), size);
                        }
                    }
                    Ok(Some(ClientFrame::Hello(_))) | Ok(None) | Err(_) => break,
                }
            }
            if !finished.load(Ordering::Relaxed) {
                disconnected.store(true, Ordering::Relaxed);
                kill_tree(reaper, pid, exit_cell, sub_cg);
            }
            let _ = input_done.send(());
        });

        let timeout = (request.timeout_ms > 0).then(|| Duration::from_millis(request.timeout_ms));
        let exit = spawned.exit.wait(timeout);
        let timed_out = exit.is_none();
        if timed_out {
            kill_tree(reaper, pid, &spawned.exit, sub_cgroup.as_ref());
        }
        let killed = ExitStatus {
            code: 128 + libc::SIGKILL,
            signal: Some(libc::SIGKILL),
        };
        let exit = exit
            .or_else(|| spawned.exit.wait(None))
            .map_or(killed, |(exit, _)| exit);

        let drain_deadline = Instant::now() + OUTPUT_DRAIN;
        let mut drained = 0;
        while drained < pumps
            && pumps_done
                .recv_timeout(drain_deadline.saturating_duration_since(Instant::now()))
                .is_ok()
        {
            drained += 1;
        }
        drop(stop_write);
        for _ in drained..pumps {
            let _ = pumps_done.recv();
        }

        let kill_remaining = timed_out || disconnected.load(Ordering::Relaxed);
        let oom = sub_cgroup
            .as_ref()
            .map(|cg| cg.finish(timed_out, kill_remaining))
            .unwrap_or_default();

        finished.store(true, Ordering::Relaxed);
        let result = ExecResult {
            exit,
            timed_out,
            stdout_truncated: truncated[0].load(Ordering::Relaxed),
            stderr_truncated: truncated[1].load(Ordering::Relaxed),
            oom_killed: oom.oom_killed,
            memory_peak_bytes: oom.memory_peak_bytes,
        };
        let _ = outbound.send(&ServerFrame::Done(Ok(Reply::Exec(result))));
        // The input thread reads until the client closes. A client that stays is cut off.
        let _ = watch.shutdown(Shutdown::Write);
        if inputs_done.recv_timeout(LINGER).is_err() {
            let _ = watch.shutdown(Shutdown::Read);
        }
    });
    Ok(())
}
