//! The production effects: the Host operator socket, child processes, HTTPS, and signals.

use crate::error::{Deadline, Error, Kind, Result, err};
use crate::rpc::SocketOperator;
use crate::operator::Operator;
use crate::{CommandOutput, Context, HttpResponse, System};
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
/// A step sequence has this long to record its receipt after a stop signal.
const STOP_GRACE: Duration = Duration::from_millis(2500);

struct Production {
    interrupted: Arc<AtomicBool>,
}

fn drain(stream: Option<impl Read + Send + 'static>) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut data = Vec::new();
        if let Some(stream) = stream {
            let _ = stream.take(16 << 20).read_to_end(&mut data);
        }
        String::from_utf8_lossy(&data).into_owned()
    })
}

fn loopback(url: &url::Url) -> bool {
    url.scheme() == "http" && matches!(url.host_str().map(str::to_ascii_lowercase).as_deref(), Some("localhost" | "127.0.0.1" | "[::1]"))
}

impl System for Production {
    fn operator(&self, socket: &Path) -> Result<Box<dyn Operator>> {
        Ok(Box::new(SocketOperator::connect(socket)?))
    }

    fn run(&self, program: &str, arguments: &[&str], deadline: Deadline) -> Result<CommandOutput> {
        let mut child = Command::new(program).args(arguments).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|error| err(format!("{program}: {error}")))?;
        let (stdout, stderr) = (drain(child.stdout.take()), drain(child.stderr.take()));
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            let stop = if self.interrupted() { Some(Kind::Interrupted) } else { deadline.expired().then_some(Kind::Timeout) };
            if let Some(kind) = stop {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::new(kind, format!("{program} did not finish")));
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        Ok(CommandOutput { code: status.code(), stdout: stdout.join().unwrap_or_default(), stderr: stderr.join().unwrap_or_default() })
    }

    fn process_running(&self, pid: u32) -> bool {
        let Some(pid) = i32::try_from(pid).ok().and_then(rustix::process::Pid::from_raw) else { return false };
        // A process of another user answers with EPERM: it exists.
        !matches!(rustix::process::test_kill_process(pid), Err(rustix::io::Errno::SRCH))
    }

    fn post(&self, url: &str, authorization: &str, body: &[u8], limit: u64) -> Result<HttpResponse> {
        let mut target = url::Url::parse(url).map_err(|error| err(error.to_string()))?;
        // A one-time secret for this machine never goes to a proxy or to a DNS answer for `localhost`.
        let local = loopback(&target);
        if local && target.host_str().is_some_and(|host| host.eq_ignore_ascii_case("localhost")) {
            target.set_host(Some("127.0.0.1")).map_err(|error| err(error.to_string()))?;
        }
        let tls = ureq::tls::TlsConfig::builder().root_certs(ureq::tls::RootCerts::PlatformVerifier).build();
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(HTTP_TIMEOUT))
            .max_redirects(0)
            .http_status_as_error(false)
            .proxy(if local { None } else { ureq::Proxy::try_from_env() })
            .tls_config(tls)
            .build()
            .new_agent();
        let mut response =
            agent.post(target.as_str()).header("Authorization", authorization).header("Content-Type", "application/json").send(body).map_err(|error| err(error.to_string()))?;
        let body = response.body_mut().with_config().limit(limit).read_to_vec().map_err(|error| err(error.to_string()))?;
        Ok(HttpResponse { status: response.status().as_u16(), body })
    }

    fn euid(&self) -> u32 {
        rustix::process::geteuid().as_raw()
    }

    fn interrupted(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed)
    }
}

/// The production context. SIGTERM and SIGINT stop the helper: the running
/// sequence fails at its next step with a retryable receipt, and the process
/// exits within the grace period in any case. The owner journal records each
/// stage before its effect, so an exit at any point is recoverable.
pub fn production() -> Context {
    use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
    let interrupted = Arc::new(AtomicBool::new(false));
    // A closed terminal must not end a maintenance that the service still needs.
    let _ = signal_hook::flag::register(SIGHUP, Arc::new(AtomicBool::new(false)));
    if let Ok(mut signals) = signal_hook::iterator::Signals::new([SIGTERM, SIGINT]) {
        let flag = interrupted.clone();
        std::thread::spawn(move || {
            if let Some(signal) = signals.forever().next() {
                flag.store(true, Ordering::Relaxed);
                std::thread::sleep(STOP_GRACE);
                std::process::exit(128 + signal);
            }
        });
    }
    Context { system: Arc::new(Production { interrupted }) }
}
