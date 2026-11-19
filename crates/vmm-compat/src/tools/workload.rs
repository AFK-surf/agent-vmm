use super::*;
use std::collections::BTreeMap;

#[derive(Deserialize)]
struct Run {
    container_id: String,
    image: String,
    entrypoint: Option<Vec<String>>,
    command: Option<Vec<String>>,
    user: Option<String>,
    workdir: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    volumes: Vec<Mount>,
    #[serde(default)]
    read_only_root: bool,
    log_limit_bytes: Option<u64>,
    startup_observation_ms: Option<u64>,
}
#[derive(Deserialize)]
struct Mount {
    owned_volume: Option<String>,
    host_share: Option<String>,
    destination: String,
    #[serde(default)]
    read_only: bool,
}
#[derive(Deserialize)]
struct Timed {
    container_id: String,
    timeout_seconds: Option<u32>,
}
#[derive(Deserialize)]
struct Signal {
    container_id: String,
    signal: String,
}
#[derive(Deserialize)]
struct Logs {
    container_id: String,
    tail_records: Option<u64>,
    since: Option<String>,
    after_cursor: Option<String>,
    max_bytes: Option<usize>,
}
#[derive(Deserialize)]
struct Exec {
    container_id: String,
    argv: Vec<String>,
    stdin: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    user: Option<String>,
    workdir: Option<String>,
    timeout_seconds: Option<u64>,
    max_output_bytes: Option<usize>,
}

fn env_list(env: BTreeMap<String, String>) -> Vec<String> {
    env.into_iter().map(|(k, v)| format!("{k}={v}")).collect()
}
fn valid_prefix(text: &str, bytes: usize) -> &str {
    &text[..text.floor_char_boundary(bytes.min(text.len()))]
}

struct Buffer {
    limit: usize,
    head: Vec<u8>,
    tail: Vec<u8>,
    truncated: bool,
}
impl Buffer {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            head: Vec::new(),
            tail: Vec::new(),
            truncated: false,
        }
    }
    fn write(&mut self, bytes: &[u8]) {
        let head = (self.limit / 2 - self.head.len()).min(bytes.len());
        self.head.extend_from_slice(&bytes[..head]);
        self.tail.extend_from_slice(&bytes[head..]);
        let excess = (self.tail.len() + self.head.len()).saturating_sub(self.limit);
        if excess > 0 {
            self.truncated = true;
            self.tail.drain(..excess);
        }
    }
    fn text(mut self) -> String {
        self.head.extend(self.tail);
        let value = String::from_utf8_lossy(&self.head);
        if self.truncated {
            valid_prefix(&value, self.limit).to_owned()
        } else {
            value.into_owned()
        }
    }
}
impl Calls<'_> {
    pub(super) async fn workload(&self, name: &str, args: Value) -> Result<Value, Status> {
        match name {
            "container_run" => self.run(parse(args)?).await,
            "container_logs" => self.logs(parse(args)?).await,
            "container_exec" => self.exec(parse(args)?).await,
            "container_wait" => {
                let input: Timed = parse(args)?;
                let seconds = input.timeout_seconds.unwrap_or(600);
                if !(1..=600).contains(&seconds) {
                    return Err(Status::invalid_argument("invalid timeout"));
                }
                let request = self.request(h::WaitContainerRequest {
                    container_id: input.container_id,
                    expected_configuration: None,
                })?;
                let result = tokio::time::timeout(
                    Duration::from_secs(seconds.into()),
                    self.runtime.wait_container(request),
                )
                .await
                .map_err(|_| Status::deadline_exceeded("request timed out"))??
                .into_inner();
                Ok(json!({"exit_code":result.exit_code}))
            }
            "container_signal" => {
                let input: Signal = parse(args)?;
                let signal = match input.signal.as_str() {
                    "INT" => h::ProcessSignal::Int,
                    "TERM" => h::ProcessSignal::Term,
                    "KILL" => h::ProcessSignal::Kill,
                    _ => return Err(Status::invalid_argument("invalid signal")),
                };
                self.runtime
                    .signal_container(self.request(h::SignalContainerRequest {
                        request_id: request_id(),
                        container_id: input.container_id,
                        signal: signal as i32,
                    })?)
                    .await?;
                Ok(json!({}))
            }
            "container_stop" => {
                let input: Timed = parse(args)?;
                if input
                    .timeout_seconds
                    .is_some_and(|v| !(1..=120).contains(&v))
                {
                    return Err(Status::invalid_argument("invalid timeout"));
                }
                let current = self.get(&input.container_id).await?;
                let result = self
                    .runtime
                    .stop_container(self.request(h::StopContainerRequest {
                        request_id: request_id(),
                        container_id: input.container_id,
                        expected_instance_id: current.instance_id,
                        timeout_seconds: input.timeout_seconds,
                        ..Default::default()
                    })?)
                    .await?;
                Ok(output::container(
                    result.into_inner().container.ok_or_else(missing)?,
                ))
            }
            "container_delete" | "container_start" => {
                let current = self.get(&parse::<ContainerId>(args)?.container_id).await?;
                if name == "container_delete" {
                    self.runtime
                        .delete_container(self.request(h::DeleteContainerRequest {
                            request_id: request_id(),
                            container_id: current.id,
                            expected_generation_id: current.generation_id,
                        })?)
                        .await?;
                    Ok(json!({}))
                } else {
                    Ok(output::container(self.start(current).await?))
                }
            }
            _ => Err(Status::invalid_argument("unknown tool")),
        }
    }
    async fn start(&self, current: h::Container) -> Result<h::Container, Status> {
        self.runtime
            .start_container(self.request(h::StartContainerRequest {
                request_id: request_id(),
                container_id: current.id,
                expected_generation_id: current.generation_id,
            })?)
            .await?
            .into_inner()
            .container
            .ok_or_else(missing)
    }
    async fn run(&self, input: Run) -> Result<Value, Status> {
        let delay = input.startup_observation_ms.unwrap_or(0);
        if delay > 5000 {
            return Err(Status::invalid_argument("invalid startup observation"));
        }
        let volumes = input
            .volumes
            .into_iter()
            .map(|m| {
                let source = match (m.owned_volume, m.host_share) {
                    (Some(id), None) => h::volume_mount::Source::OwnedVolume(id),
                    (None, Some(id)) => h::volume_mount::Source::HostShare(id),
                    _ => return Err(Status::invalid_argument("one mount source is required")),
                };
                Ok(h::VolumeMount {
                    source: Some(source),
                    destination: m.destination,
                    read_only: m.read_only,
                })
            })
            .collect::<Result<_, Status>>()?;
        let request = h::CreateContainerRequest {
            request_id: request_id(),
            container_id: input.container_id.clone(),
            image: input.image,
            entrypoint_override: input.entrypoint.map(|values| h::StringList { values }),
            command_override: input.command.map(|values| h::StringList { values }),
            user: input.user,
            workdir: input.workdir,
            env: env_list(input.env),
            volumes,
            log_limit_bytes: input.log_limit_bytes.unwrap_or(16 << 20),
            read_only_root: input.read_only_root,
            ..Default::default()
        };
        let current = self
            .runtime
            .create_container(self.request(request)?)
            .await?
            .into_inner()
            .container
            .ok_or_else(missing)?;
        self.start(current).await?;
        tokio::time::sleep(Duration::from_millis(if delay == 0 { 1000 } else { delay })).await;
        Ok(
            json!({"container":output::container(self.get(&input.container_id).await?),"launch_succeeded":true}),
        )
    }
    async fn logs(&self, input: Logs) -> Result<Value, Status> {
        let limit = input.max_bytes.unwrap_or(64 << 10);
        let tail = input.tail_records.unwrap_or(0);
        if !(1..=256 << 10).contains(&limit) || tail > 1000 {
            return Err(Status::invalid_argument("invalid log bounds"));
        }
        let since = input
            .since
            .filter(|v| !v.is_empty())
            .map(|v| {
                v.parse()
                    .map_err(|_| Status::invalid_argument("invalid since timestamp"))
            })
            .transpose()?;
        let mut stream = self
            .runtime
            .logs(self.request(h::LogsRequest {
                container_id: input.container_id,
                tail_records: tail,
                since,
                after_cursor: input.after_cursor.unwrap_or_default(),
                follow: false,
            })?)
            .await?
            .into_inner();
        let mut records = Vec::new();
        let mut used = 0;
        let mut dropped = 0u64;
        let mut truncated = false;
        let mut next = None;
        while let Some(record) = stream.next().await {
            let record = record?;
            let original = String::from_utf8_lossy(&record.data);
            let text = valid_prefix(&original, limit - used);
            truncated |= text.len() < original.len();
            used += text.len();
            dropped = dropped.saturating_add(record.dropped_records);
            records.push(json!({"cursor":record.cursor,"timestamp":record.timestamp.unwrap_or_default().to_string(),"stream":if record.stream==h::LogStream::Stderr as i32{"stderr"}else{"stdout"},
                "text":text,"truncated":record.truncated||text.len()<record.data.len()}));
            next = Some(record.cursor);
            if used >= limit {
                break;
            }
        }
        let mut value = json!({"records":records,"truncated":truncated,"dropped_records":dropped});
        if let Some(cursor) = next {
            value["next_cursor"] = cursor.into();
        }
        Ok(value)
    }
    async fn exec(&self, input: Exec) -> Result<Value, Status> {
        use h::{exec_request::Event as In, exec_response::Event as Out};
        let timeout = input.timeout_seconds.unwrap_or(600);
        let max = input.max_output_bytes.unwrap_or(128 << 10);
        if !(1..=600).contains(&timeout) || !(1..=128 << 10).contains(&max) {
            return Err(Status::invalid_argument("invalid exec bounds"));
        }
        let current = self.get(&input.container_id).await?;
        let id = request_id();
        let deadline = crate::execution::now()?
            .checked_add((timeout * 1_000_000_000) as i64)
            .ok_or_else(|| Status::internal("invalid time"))?;
        let start = In::Start(h::ExecStart {
            request_id: id.clone(),
            container_id: input.container_id,
            expected_instance_id: current.instance_id,
            execution_id: id,
            execution_deadline_unix_nano: deadline,
            execution_kind: h::ExecutionKind::Exec as i32,
            argv: input.argv,
            user: input.user,
            workdir: input.workdir,
            env: env_list(input.env),
            ..Default::default()
        });
        let mut events = vec![start];
        if let Some(stdin) = input.stdin.filter(|v| !v.is_empty()) {
            events.push(In::Stdin(stdin.into_bytes()));
        }
        events.push(In::CloseStdin(true));
        let frames = tokio_stream::iter(
            events
                .into_iter()
                .map(|event| Ok(h::ExecRequest { event: Some(event) })),
        );
        let ctx = RuntimeContext::from_request(&self.runtime.core, &self.request(())?)?;
        let run = async {
            let mut stream = self
                .runtime
                .containers
                .exec(&ctx, self.runtime.guest.as_ref(), frames)
                .await?;
            let mut stdout = Buffer::new(max.div_ceil(2));
            let mut stderr = Buffer::new(max / 2);
            while let Some(frame) = stream.next().await {
                match frame?.event {
                    Some(Out::Stdout(b)) => stdout.write(&b),
                    Some(Out::Stderr(b)) => stderr.write(&b),
                    Some(Out::ExitCode(code)) => {
                        return Ok(
                            json!({"exit_code":code,"truncated":stdout.truncated||stderr.truncated,"stdout":stdout.text(),"stderr":stderr.text()}),
                        );
                    }
                    None => return Err(missing()),
                }
            }
            Err(missing())
        };
        tokio::time::timeout(Duration::from_secs(timeout), run)
            .await
            .map_err(|_| Status::deadline_exceeded("request timed out"))?
    }
}
