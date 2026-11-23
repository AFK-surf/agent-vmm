use super::{Call, CallError, EnvironmentMemory, MemoryPool, MemorySummary, OperatorBackend};
use agent_vmm_proto::{appliance::v1 as a, host::v1 as h, trust::v1 as t};
use futures::{FutureExt, StreamExt, channel::mpsc, stream::BoxStream};
use prost_types::Timestamp;
use std::sync::Mutex;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;
/// 2026-11-22T09:14:07Z.
const NOW: i64 = 1_795_338_847;
const BOOT_ID: &str = "b7f3a2c0-5d1e-4f6a-9c2b-8e4d1a7f6035";
const LONG_ENVIRONMENT: &str = "env-monorepo-integration-suite-arm64-nightly-2026-11-22-rerun-3";

type LogRecord = Result<h::OperatorServiceLogsResponse, CallError>;

/// A Host with five environments, one appliance and a busy build, answered
/// from memory. Every call answers at once, and the same calls in the same
/// order give the same answers.
pub struct FixtureBackend {
    state: Mutex<State>,
}

struct State {
    vm: h::VmState,
    samples: u64,
    requests: u64,
    /// Operations by ID, with how often each was read.
    operations: Vec<(h::Operation, u32)>,
    disposed: Vec<String>,
    log_taps: Vec<mpsc::UnboundedSender<LogRecord>>,
}

impl Default for FixtureBackend {
    fn default() -> Self {
        Self::new()
    }
}

fn at(offset_seconds: i64) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: NOW + offset_seconds,
        nanos: 0,
    })
}

fn ready<T: Send + 'static>(value: Result<T, CallError>) -> Call<T> {
    futures::future::ready(value).boxed()
}

fn not_found(what: &str) -> CallError {
    CallError {
        code: vmm_rpc::Code::NotFound,
        message: format!("{what} not found"),
    }
}

fn resource(kind: &str, environment_id: &str, resource_id: &str) -> h::ResourceReference {
    h::ResourceReference {
        kind: kind.into(),
        environment_id: environment_id.into(),
        resource_id: resource_id.into(),
        generation: 1,
        generation_id: String::new(),
    }
}

fn usage(memory_mib: u64, cpu_seconds: u64, pids: u32, disk_mib: u64, age: i64) -> h::Usage {
    h::Usage {
        cpu_nanos: cpu_seconds * 1_000_000_000,
        memory_bytes: memory_mib * MIB,
        pids,
        read_bytes: disk_mib * MIB * 3,
        write_bytes: disk_mib * MIB,
        disk_bytes: disk_mib * MIB,
        sampled_at: at(-age),
        stale: false,
        network_rx_bytes: memory_mib * 41_000,
        network_tx_bytes: memory_mib * 9_300,
        unavailable_reason: String::new(),
    }
}

fn container(
    id: &str,
    image: &str,
    state: h::ContainerState,
    exit_code: Option<u32>,
    argv: &[&str],
    usage: h::Usage,
    started: i64,
) -> h::Container {
    h::Container {
        id: id.into(),
        image: image.into(),
        state: state as i32,
        exit_code,
        usage: Some(usage),
        argv: argv.iter().map(|arg| (*arg).into()).collect(),
        instance_id: format!("{id}-i1"),
        created_at: at(started - 4),
        started_at: at(started),
        finished_at: exit_code.map(|_| at(-610)).unwrap_or_default(),
        desired_running: Some(exit_code.is_none()),
        configuration_matches: Some(true),
        ..Default::default()
    }
}

struct Seed {
    id: &'static str,
    name: &'static str,
    remote: Option<(&'static str, &'static str)>,
    state: &'static str,
    private_disk_mib: u64,
    boot_mib: u64,
    grant_mib: u64,
    plugged_mib: u64,
    zram_mib: u64,
    ebusy: u64,
}

const SEEDS: [Seed; 5] = [
    Seed {
        id: "env-7c1e9a40",
        name: "cue-backend",
        remote: None,
        state: "observed_active",
        private_disk_mib: 6_912,
        boot_mib: 384,
        grant_mib: 4_096,
        plugged_mib: 1_272,
        zram_mib: 86,
        ebusy: 0,
    },
    Seed {
        id: "env-2f8b6d13",
        name: "docs-build",
        remote: None,
        state: "observed_active",
        private_disk_mib: 1_184,
        boot_mib: 384,
        grant_mib: 2_048,
        plugged_mib: 384,
        zram_mib: 0,
        ebusy: 0,
    },
    Seed {
        id: "env-a94f03c7",
        name: "salix-worker",
        remote: Some(("reg-salix-prod-01", "alloc-5d0c2e7a")),
        state: "observed_active",
        private_disk_mib: 3_340,
        boot_mib: 384,
        grant_mib: 4_096,
        plugged_mib: 508,
        zram_mib: 12,
        ebusy: 2,
    },
    Seed {
        id: LONG_ENVIRONMENT,
        name: "monorepo integration suite (arm64 nightly, rerun 3)",
        remote: None,
        state: "observed_active",
        private_disk_mib: 11_520,
        boot_mib: 384,
        grant_mib: 4_096,
        plugged_mib: 632,
        zram_mib: 148,
        ebusy: 0,
    },
    Seed {
        id: "env-e15d7b92",
        name: "scratch",
        remote: None,
        state: "retired",
        private_disk_mib: 212,
        boot_mib: 384,
        grant_mib: 1_024,
        plugged_mib: 384,
        zram_mib: 0,
        ebusy: 0,
    },
];

const BACKEND_IMAGE: &str = "ghcr.io/afk-surf/cue-backend@sha256:9f2c4e8b1a7d3f605c2e9b4a8d1f7e3c6b5a4d2e1f0c9b8a7d6e5f4c3b2a1908";
const POSTGRES_IMAGE: &str = "docker.io/library/postgres:17.2-alpine";
const BUILD_IMAGE: &str = "ghcr.io/afk-surf/monorepo-ci@sha256:1d4b7e0a93c65f28b1e7d40c9a6f352e8b7d1c04a95e3f6027b8d1c4e9a0f73b";

impl State {
    fn seeds(&self) -> impl Iterator<Item = &'static Seed> + '_ {
        SEEDS
            .iter()
            .filter(|seed| !self.disposed.iter().any(|id| id == seed.id))
    }

    fn vm(&self) -> h::Vm {
        h::Vm {
            state: self.vm as i32,
            boot_id: if self.vm == h::VmState::Stopped {
                String::new()
            } else {
                BOOT_ID.into()
            },
            error: String::new(),
        }
    }

    fn request_id(&mut self, prefix: &str) -> String {
        self.requests += 1;
        format!("{prefix}-{:04}", self.requests)
    }

    fn begin(
        &mut self,
        kind: &str,
        resource: h::ResourceReference,
        request: String,
    ) -> h::Operation {
        let operation = h::Operation {
            id: self.request_id("op"),
            kind: kind.into(),
            resource: Some(resource),
            state: h::OperationState::Running as i32,
            stage: "accepted".into(),
            request_id: request,
            started_at: at(0),
            updated_at: at(0),
            error: None,
            output_path: String::new(),
        };
        self.operations.push((operation.clone(), 0));
        operation
    }

    /// The build in the first environment plugs and returns memory in 128 MiB
    /// blocks as the samples go by.
    fn plugged_mib(&self, seed: &Seed) -> u64 {
        if seed.id != SEEDS[0].id {
            return seed.plugged_mib;
        }
        const STEPS: [u64; 8] = [0, 0, 128, 256, 256, 384, 256, 128];
        seed.plugged_mib + STEPS[(self.samples % STEPS.len() as u64) as usize]
    }

    fn environment(&self, seed: &Seed) -> h::AgentEnvironment {
        let stale = seed.state == "retired";
        let containers = match seed.id {
            "env-7c1e9a40" => vec![
                container(
                    "api",
                    BACKEND_IMAGE,
                    h::ContainerState::Running,
                    None,
                    &["/usr/local/bin/cue-backend", "--listen", "0.0.0.0:8080"],
                    usage(self.plugged_mib(seed) - 610, 1_284, 41, 2_304, 1),
                    -9_412,
                ),
                container(
                    "postgres",
                    POSTGRES_IMAGE,
                    h::ContainerState::Running,
                    None,
                    &["postgres", "-c", "shared_buffers=128MB"],
                    usage(318, 402, 17, 4_480, 1),
                    -9_420,
                ),
                container(
                    "migrate",
                    BACKEND_IMAGE,
                    h::ContainerState::Stopped,
                    Some(0),
                    &["/usr/local/bin/cue-backend", "migrate", "--to", "latest"],
                    usage(0, 3, 0, 128, 610),
                    -9_418,
                ),
            ],
            LONG_ENVIRONMENT => vec![
                container(
                    "integration-runner",
                    BUILD_IMAGE,
                    h::ContainerState::Running,
                    None,
                    &["cargo", "nextest", "run", "--workspace", "--profile", "ci"],
                    usage(486, 5_912, 212, 11_008, 2),
                    -3_105,
                ),
                container(
                    "buildkitd",
                    "docker.io/moby/buildkit:v0.26.1-rootless",
                    h::ContainerState::Stopped,
                    Some(137),
                    &["rootlesskit", "buildkitd"],
                    usage(0, 911, 0, 512, 610),
                    -3_120,
                ),
            ],
            "env-e15d7b92" => Vec::new(),
            _ => vec![container(
                "main",
                POSTGRES_IMAGE,
                h::ContainerState::Running,
                None,
                &["sleep", "infinity"],
                usage(seed.plugged_mib / 3, 12, 3, 96, 1),
                -1_840,
            )],
        };
        let mut total = usage(
            self.plugged_mib(seed).saturating_sub(260),
            containers
                .iter()
                .map(|c| c.usage.as_ref().map_or(0, |u| u.cpu_nanos / 1_000_000_000))
                .sum(),
            containers
                .iter()
                .map(|c| c.usage.as_ref().map_or(0, |u| u.pids))
                .sum(),
            seed.private_disk_mib,
            1,
        );
        if stale {
            total.stale = true;
            total.sampled_at = at(-1_326);
            total.unavailable_reason = "guest runtime is unavailable".into();
        }
        h::AgentEnvironment {
            id: seed.id.into(),
            display_name: seed.name.into(),
            namespace_id: format!("ns-{}", &seed.id[4..seed.id.len().min(12)]),
            limits: Some(h::ResourceLimits {
                pids: Some(4_096),
                disk_bytes: Some(32 * GIB),
            }),
            egress_mode: if seed.remote.is_some() {
                h::EgressMode::DenyAll
            } else {
                h::EgressMode::PublicInternet
            } as i32,
            health: Some(h::HealthStatus {
                state: if stale {
                    h::HealthState::Unavailable
                } else {
                    h::HealthState::Healthy
                } as i32,
                components: Vec::new(),
            }),
            usage: Some(total),
            containers,
            volumes: match seed.id {
                "env-7c1e9a40" => vec![h::Volume {
                    id: "pgdata".into(),
                    usage: Some(usage(0, 0, 0, 4_352, 1)),
                    limit_bytes: 16 * GIB,
                    mounted_by: vec!["postgres".into()],
                }],
                LONG_ENVIRONMENT => vec![h::Volume {
                    id: "cargo-target-cache".into(),
                    usage: Some(usage(0, 0, 0, 9_984, 2)),
                    limit_bytes: 24 * GIB,
                    mounted_by: vec!["integration-runner".into()],
                }],
                _ => Vec::new(),
            },
            policy_revision: 7,
            owner: Some(match seed.remote {
                Some((registration, allocation)) => h::EnvironmentOwner {
                    kind: h::EnvironmentOwnerKind::RemoteRegistration as i32,
                    registration_id: registration.into(),
                    allocation_id: allocation.into(),
                },
                None => h::EnvironmentOwner {
                    kind: h::EnvironmentOwnerKind::Local as i32,
                    ..Default::default()
                },
            }),
            origin: if seed.remote.is_some() {
                h::EnvironmentOrigin::Remote
            } else {
                h::EnvironmentOrigin::Local
            } as i32,
            inventory_state: if stale {
                h::InventoryState::Stale
            } else {
                h::InventoryState::Available
            } as i32,
            inventory_message: if stale {
                "the namespace is retained; its runtime was released 22 min ago".into()
            } else {
                String::new()
            },
            inventory_observed_at: at(if stale { -1_326 } else { -1 }),
        }
    }
}

impl FixtureBackend {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                vm: h::VmState::Running,
                samples: 0,
                requests: 0,
                operations: Vec::new(),
                disposed: Vec::new(),
                log_taps: Vec::new(),
            }),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.state.lock().expect("fixture state"))
    }

    /// Sends one more record to every open log stream.
    pub fn push_log(&self, line: &str, stream: h::LogStream, sequence: u64) {
        let record = log_record(sequence, line, stream);
        self.with(|state| {
            state
                .log_taps
                .retain(|tap| tap.unbounded_send(Ok(record.clone())).is_ok());
        });
    }

    /// The lines an open log stream starts with.
    pub fn log_lines() -> Vec<(String, h::LogStream)> {
        log_lines()
    }
}

fn log_record(sequence: u64, line: &str, stream: h::LogStream) -> h::OperatorServiceLogsResponse {
    h::OperatorServiceLogsResponse {
        cursor: format!("c{sequence:08x}"),
        timestamp: Some(Timestamp {
            seconds: NOW - 420 + (sequence as i64 * 137) / 1_000,
            nanos: ((sequence * 137) % 1_000) as i32 * 1_000_000,
        }),
        stream: stream as i32,
        data: format!("{line}\n").into_bytes(),
        truncated: false,
        dropped_records: 0,
    }
}

/// Server output with SGR colors, as `cue-backend` writes it to a terminal.
fn log_lines() -> Vec<(String, h::LogStream)> {
    const ROUTES: [&str; 6] = [
        "GET /v1/sessions",
        "POST /v1/sessions/7f2c/messages",
        "GET /v1/providers/anthropic/models",
        "PUT /v1/workspaces/4410/policy",
        "GET /healthz",
        "POST /v1/ifc/labels:batch",
    ];
    let mut lines = vec![
        (
            "\x1b[32m INFO\x1b[0m \x1b[1mcue_backend\x1b[0m starting 0.42.1 (rev 51c4426) on aarch64-unknown-linux-musl".to_owned(),
            h::LogStream::Stdout,
        ),
        (
            "\x1b[32m INFO\x1b[0m \x1b[1mcue_backend::db\x1b[0m pool ready: 16 connections to postgres:5432/cue".to_owned(),
            h::LogStream::Stdout,
        ),
        (
            "\x1b[32m INFO\x1b[0m \x1b[1mcue_backend::router\x1b[0m listening on 0.0.0.0:8080".to_owned(),
            h::LogStream::Stdout,
        ),
    ];
    let mut seed = 0x2545_f491_u64;
    for _ in 0..417 {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let pick = (seed >> 33) as usize;
        let route = ROUTES[pick % ROUTES.len()];
        let latency = 2 + (pick / 7) % 48;
        lines.push(match pick % 23 {
            0 => (
                format!(
                    "\x1b[33m WARN\x1b[0m \x1b[1mcue_backend::db\x1b[0m slow query took \x1b[1m{} ms\x1b[0m: SELECT id, label FROM ifc_labels WHERE workspace_id = $1",
                    1_040 + pick % 900
                ),
                h::LogStream::Stdout,
            ),
            1 => (
                format!(
                    "\x1b[31mERROR\x1b[0m \x1b[1mcue_backend::provider\x1b[0m upstream timeout after 30000 ms: anthropic request {:08x}",
                    pick as u32
                ),
                h::LogStream::Stderr,
            ),
            2 => (
                format!(
                    "\x1b[36mDEBUG\x1b[0m \x1b[1mcue_backend::ifc\x1b[0m \x1b[2mlabel cache: {} hits, {} misses\x1b[0m",
                    900 + pick % 4_000,
                    pick % 37
                ),
                h::LogStream::Stdout,
            ),
            3 => (
                format!(
                    "\x1b[33m WARN\x1b[0m \x1b[1mcue_backend::router\x1b[0m client closed the connection before the response: timeout budget {} ms",
                    200 + pick % 600
                ),
                h::LogStream::Stderr,
            ),
            _ => (
                format!(
                    "\x1b[32m INFO\x1b[0m \x1b[1mcue_backend::router\x1b[0m {route} \x1b[32m200\x1b[0m in {latency} ms"
                ),
                h::LogStream::Stdout,
            ),
        });
    }
    lines
}

impl OperatorBackend for FixtureBackend {
    fn endpoint(&self) -> String {
        "~/Library/Application Support/AgentVMM/operator.sock".into()
    }

    fn vm(&self) -> Call<h::Vm> {
        ready(Ok(self.with(|state| state.vm())))
    }

    fn health(&self) -> Call<h::GetSystemHealthSummaryResponse> {
        ready(Ok(self.with(|state| {
            let running = state.vm == h::VmState::Running;
            let component =
                |name: &str, health: h::HealthState, reason: &str, age: i64| h::ComponentHealth {
                    component: name.into(),
                    state: health as i32,
                    reason: reason.into(),
                    observed_at: at(-age),
                };
            let components = if running {
                vec![
                    component("carrier", h::HealthState::Healthy, "", 1),
                    component("dom0", h::HealthState::Healthy, "", 1),
                    component("l4h-memd", h::HealthState::Healthy, "", 1),
                    component("runtime", h::HealthState::Healthy, "", 2),
                    component(
                        "egress",
                        h::HealthState::Degraded,
                        "virtual DNS upstream 198.18.0.2 answered after 1840 ms",
                        14,
                    ),
                ]
            } else {
                vec![component(
                    "dom0",
                    h::HealthState::Unavailable,
                    "VM is not running",
                    0,
                )]
            };
            h::GetSystemHealthSummaryResponse {
                vm: Some(state.vm()),
                health: Some(h::HealthStatus {
                    state: if running {
                        h::HealthState::Degraded
                    } else {
                        h::HealthState::Unavailable
                    } as i32,
                    components,
                }),
                usage: None,
                environments: Vec::new(),
                collected_at: at(0),
                remote_registrations: Vec::new(),
                host_resources: None,
                guest_resources: None,
            }
        })))
    }

    fn disk(&self) -> Call<h::GetLocalDiskSummaryResponse> {
        ready(Ok(self.with(|state| {
            let private: u64 = state.seeds().map(|seed| seed.private_disk_mib).sum();
            h::GetLocalDiskSummaryResponse {
                collected_at: at(-3),
                boot_id: BOOT_ID.into(),
                vm_state: state.vm as i32,
                guest_state_fs: (state.vm == h::VmState::Running).then(|| h::DiskAccounting {
                    used_bytes: Some((private + 9_420 + 6_208 + 1_312) * MIB),
                    committed_bytes: Some((private + 9_420 + 6_208 + 1_312 + 4_096) * MIB),
                    capacity_bytes: Some(96 * GIB),
                    import_reservation_bytes: Some(4_096 * MIB),
                    system_reserve_bytes: Some(2 * GIB),
                    metadata_reserve_bytes: Some(512 * MIB),
                    image_runtime_bytes: Some(9_420 * MIB),
                    build_cache_bytes: Some(6_208 * MIB),
                    private_bytes: Some(private * MIB),
                    other_bytes: Some(1_312 * MIB),
                    breakdown_sampled_at: at(-48),
                }),
                unavailable_reason: if state.vm == h::VmState::Running {
                    String::new()
                } else {
                    "guest state disk is unavailable".into()
                },
            }
        })))
    }

    fn memory(&self) -> Call<MemorySummary> {
        ready(self.with(|state| {
            if state.vm != h::VmState::Running {
                return Err(CallError {
                    code: vmm_rpc::Code::Unavailable,
                    message: "VM is not running".into(),
                });
            }
            let environments: Vec<_> = state
                .seeds()
                .map(|seed| EnvironmentMemory {
                    environment_id: seed.id.into(),
                    boot_bytes: seed.boot_mib * MIB,
                    grant_bytes: seed.grant_mib * MIB,
                    plugged_bytes: state.plugged_mib(seed) * MIB,
                    zram_bytes: seed.zram_mib * MIB,
                    ebusy_count: seed.ebusy,
                })
                .collect();
            let dom0_limit = 256 * MIB;
            let plugged: u64 =
                dom0_limit + environments.iter().map(|e| e.plugged_bytes).sum::<u64>();
            let free = 192 * MIB;
            let inflated = 1_536 * MIB;
            let summary = MemorySummary {
                sampled_at: at(state.samples as i64),
                carrier_footprint_bytes: plugged - 1_180 * MIB + free + 38 * MIB,
                pool: MemoryPool {
                    clean_bytes: 8 * GIB - plugged - free - inflated,
                    free_bytes: free,
                    inflated_bytes: inflated,
                    guest_plugged_bytes: plugged,
                },
                dom0_limit_bytes: dom0_limit,
                dom0_used_bytes: 214 * MIB + (state.samples % 3) * MIB,
                environments,
            };
            state.samples += 1;
            Ok(summary)
        }))
    }

    fn vm_impact(&self) -> Call<h::GetVmImpactSummaryResponse> {
        ready(Ok(self.with(|state| h::GetVmImpactSummaryResponse {
            vm: Some(state.vm()),
            collected_at: at(0),
            complete: true,
            overflow: false,
            issue: String::new(),
            known_workloads: state
                .seeds()
                .filter(|seed| seed.state == "observed_active")
                .map(|seed| match seed.remote {
                    Some((registration, _)) => resource("registration", seed.id, registration),
                    None => resource("environment", seed.id, seed.id),
                })
                .collect(),
        })))
    }

    fn start_vm(&self, request_id: String) -> Call<h::Operation> {
        ready(Ok(self.with(|state| {
            state.vm = h::VmState::Running;
            state.begin("vm-start", resource("vm", "", "vm"), request_id)
        })))
    }

    fn stop_vm(&self, request_id: String, fence: h::VmMutationFence) -> Call<h::Operation> {
        ready(self.with(|state| {
            if !fence.whole_vm_impact_acknowledged || fence.boot_id != BOOT_ID {
                return Err(CallError {
                    code: vmm_rpc::Code::Aborted,
                    message: "the VM changed since its impact was read".into(),
                });
            }
            state.vm = h::VmState::Stopped;
            Ok(state.begin("vm-stop", resource("vm", "", "vm"), request_id))
        }))
    }

    fn operation(&self, operation_id: String) -> Call<h::Operation> {
        ready(self.with(|state| {
            let (operation, reads) = state
                .operations
                .iter_mut()
                .find(|(operation, _)| operation.id == operation_id)
                .ok_or_else(|| not_found("operation"))?;
            *reads += 1;
            // An operation is seen running once, then done.
            if *reads >= 2 {
                operation.state = h::OperationState::Succeeded as i32;
                operation.stage = "done".into();
                operation.updated_at = at(2);
            } else {
                operation.stage = "in progress".into();
                operation.updated_at = at(1);
            }
            Ok(operation.clone())
        }))
    }

    fn environment_page(&self, after_id: String) -> Call<h::ListLocalEnvironmentPageResponse> {
        ready(Ok(self.with(|state| {
            // Pages of three show the cursor without a long list.
            const PAGE: usize = 3;
            let all: Vec<_> = state.seeds().collect();
            let start = if after_id.is_empty() {
                0
            } else {
                all.iter()
                    .position(|seed| seed.id == after_id)
                    .map_or(all.len(), |index| index + 1)
            };
            let page = &all[start..all.len().min(start + PAGE)];
            h::ListLocalEnvironmentPageResponse {
                environments: page
                    .iter()
                    .map(|seed| h::LocalEnvironmentSummary {
                        id: seed.id.into(),
                        origin: if seed.remote.is_some() {
                            "remote"
                        } else {
                            "local"
                        }
                        .into(),
                        state: if state.vm == h::VmState::Running {
                            seed.state
                        } else {
                            "vm_stopped"
                        }
                        .into(),
                        state_observed_at: at(-1),
                        private_disk_bytes: Some(seed.private_disk_mib * MIB),
                        sampled_at: at(-48),
                        partial: false,
                        registration_id: seed.remote.map(|r| r.0.into()).unwrap_or_default(),
                        allocation_id: seed.remote.map(|r| r.1.into()).unwrap_or_default(),
                        allocation_generation: seed.remote.map_or(0, |_| 3),
                        revision: 12,
                    })
                    .collect(),
                next_cursor: if start + PAGE < all.len() {
                    page.last().map(|seed| seed.id.into()).unwrap_or_default()
                } else {
                    String::new()
                },
                collected_at: at(0),
                boot_id: BOOT_ID.into(),
            }
        })))
    }

    fn environment(&self, environment_id: String) -> Call<h::AgentEnvironment> {
        ready(self.with(|state| {
            state
                .seeds()
                .find(|seed| seed.id == environment_id)
                .map(|seed| state.environment(seed))
                .ok_or_else(|| not_found("environment"))
        }))
    }

    fn plan_cleanup(&self, kind: h::CleanupKind, environment_id: String) -> Call<h::CleanupPlan> {
        ready(self.with(|state| {
            let seed = state
                .seeds()
                .find(|seed| seed.id == environment_id)
                .ok_or_else(|| not_found("environment"))?;
            let environment = state.environment(seed);
            let discard = kind == h::CleanupKind::Discard;
            let impact = |kind: &str, id: &str, effect: &str, irreversible| h::CleanupImpact {
                resource: Some(resource(kind, seed.id, id)),
                effect: effect.into(),
                irreversible,
                blocker: false,
            };
            let mut impacts = Vec::new();
            for container in &environment.containers {
                impacts.push(impact(
                    "container",
                    &container.id,
                    "writable snapshot and logs will be deleted",
                    true,
                ));
            }
            for volume in &environment.volumes {
                impacts.push(if discard {
                    impact(
                        "volume",
                        &volume.id,
                        "named volume and all contents will be deleted",
                        true,
                    )
                } else {
                    impact("volume", &volume.id, "named volume will be retained", false)
                });
            }
            if discard {
                impacts.push(impact(
                    "environment",
                    seed.id,
                    "namespace and host policy will be deleted",
                    true,
                ));
            }
            Ok(h::CleanupPlan {
                environment_id,
                kind: kind as i32,
                revision: 12,
                impacts,
            })
        }))
    }

    fn execute_cleanup(&self, request_id: String, plan: h::CleanupPlan) -> Call<h::Operation> {
        ready(Ok(self.with(|state| {
            let kind = if plan.kind() == h::CleanupKind::Discard {
                state.disposed.push(plan.environment_id.clone());
                "environment-discard"
            } else {
                "environment-reset"
            };
            let target = resource("environment", &plan.environment_id, &plan.environment_id);
            state.begin(kind, target, request_id)
        })))
    }

    fn plan_disposal(&self, environment_id: String) -> Call<h::LocalDisposalPlan> {
        ready(self.with(|state| {
            let seed = state
                .seeds()
                .find(|seed| seed.id == environment_id)
                .ok_or_else(|| not_found("environment"))?;
            Ok(h::LocalDisposalPlan {
                target_kind: h::LocalDisposalTargetKind::Environment as i32,
                target_id: environment_id,
                revision: 12,
                namespace_id: format!("ns-{}", &seed.id[4..seed.id.len().min(12)]),
                display_name: seed.name.into(),
                environment_count: 1,
                activity_unknown: seed.state != "observed_active",
            })
        }))
    }

    fn execute_disposal(
        &self,
        request_id: String,
        plan: h::LocalDisposalPlan,
    ) -> Call<h::Operation> {
        ready(Ok(self.with(|state| {
            state.disposed.push(plan.target_id.clone());
            let target = resource("environment", &plan.target_id, &plan.target_id);
            state.begin("local-disposal", target, request_id)
        })))
    }

    fn logs(&self, request: h::OperatorServiceLogsRequest) -> BoxStream<'static, LogRecord> {
        let (tap, live) = mpsc::unbounded();
        self.with(|state| state.log_taps.push(tap));
        let follow = request.query.as_ref().is_some_and(|query| query.follow);
        let mut records: Vec<LogRecord> = log_lines()
            .into_iter()
            .enumerate()
            .map(|(index, (line, stream))| Ok(log_record(index as u64, &line, stream)))
            .collect();
        // The guest log buffer overflowed once while nobody was reading.
        if let Some(Ok(record)) = records.get_mut(96) {
            record.dropped_records = 37;
        }
        let history = futures::stream::iter(records);
        if follow {
            history.chain(live).boxed()
        } else {
            history.boxed()
        }
    }

    fn device_identity(&self) -> Call<h::GetDeviceIdentityResponse> {
        ready(Ok(h::GetDeviceIdentityResponse {
            identity: Some(t::DeviceIdentity {
                device_id: "dev-3k9x7q2m5v8t1b4n6c0h".into(),
                root_public_key: (0..65_u8)
                    .map(|index| index.wrapping_mul(73).wrapping_add(4))
                    .collect(),
                root_key_revision: 2,
                signature_suite: t::SignatureSuite::P256Sha256 as i32,
            }),
            state: h::DeviceIdentityState::Ready as i32,
        }))
    }

    fn registration_page(&self, _after_id: String) -> Call<h::ListLocalRegistrationPageResponse> {
        let row =
            |id: &str, revision, state: h::RemoteRegistrationState| h::LocalRegistrationMetadata {
                id: id.into(),
                revision,
                state: state as i32,
            };
        ready(Ok(h::ListLocalRegistrationPageResponse {
            registrations: vec![
                row("reg-salix-prod-01", 9, h::RemoteRegistrationState::Enabled),
                row(
                    "reg-salix-staging-02",
                    4,
                    h::RemoteRegistrationState::Draining,
                ),
                row("reg-bundled-mcp", 2, h::RemoteRegistrationState::Enabled),
            ],
            next_cursor: String::new(),
        }))
    }

    fn host_sockets(&self) -> Call<Vec<h::HostSocket>> {
        let socket = |environment: &str, port, path: &str, revision| h::HostSocket {
            environment_id: environment.into(),
            port,
            path: path.into(),
            revision,
        };
        ready(Ok(vec![
            socket(
                "env-7c1e9a40",
                5432,
                "/Users/dev/.agent-vmm/sockets/cue-postgres.sock",
                3,
            ),
            socket(
                "env-7c1e9a40",
                8080,
                "/Users/dev/.agent-vmm/sockets/cue-api.sock",
                3,
            ),
            socket(
                LONG_ENVIRONMENT,
                9229,
                "/Users/dev/Library/Application Support/AgentVMM/host-sockets/integration-inspector.sock",
                1,
            ),
        ]))
    }

    fn appliances(&self) -> Call<Vec<a::ApplianceStatus>> {
        ready(Ok(vec![mcp_appliance()]))
    }

    fn appliance_status(&self, installation_id: String) -> Call<a::ApplianceStatus> {
        let appliance = mcp_appliance();
        ready(if appliance.installation_id == installation_id {
            Ok(appliance)
        } else {
            Err(not_found("appliance"))
        })
    }

    fn create_mcp_token(&self) -> Call<String> {
        ready(Ok(self.with(|state| {
            state.requests += 1;
            format!(
                "avmm_mcp_1.eyJkZXYiOiJkZXYtM2s5eDdxMm01djh0MWI0bjZjMGgiLCJraWQiOjIsInNlcSI6{}.Qm9ndXNGaXh0dXJlU2lnbmF0dXJlMDEyMzQ1Njc4OWFiY2RlZg",
                state.requests + 40
            )
        })))
    }

    fn rotate_mcp_token_key(&self) -> Call<String> {
        ready(Ok(self.with(|state| {
            state
                .begin(
                    "mcp-token-key-rotation",
                    resource("mcp-token-key", "", "kid-2"),
                    String::new(),
                )
                .id
        })))
    }
}

fn mcp_appliance() -> a::ApplianceStatus {
    a::ApplianceStatus {
        installation_id: "inst-bundled-mcp".into(),
        product_id: "surf.afk.agent-vmm.mcp".into(),
        artifact_id: "sha256:c41d8e2f7a9b3065d1f8e4c7b2a6950e3d7c1b8f4a2e6d9c0b5f3a7e1d4c8b26"
            .into(),
        revision: 5,
        appliance_state: a::ApplianceState::Running as i32,
        channel_state: a::ControllerChannelState::Active as i32,
        approved_grant: None,
        registration_id: "reg-bundled-mcp".into(),
        inventory_watermark: 1_204,
        frontends: vec![
            a::ApplianceFrontendStatus {
                name: "mcp".into(),
                transport: a::FrontendTransport::Http as i32,
                exposure: a::FrontendExposure::HostLoopback as i32,
                endpoint: "http://127.0.0.1:49217/mcp".into(),
                container_instance: "mcp-i3".into(),
                available: true,
                error_reason: String::new(),
            },
            a::ApplianceFrontendStatus {
                name: "controller".into(),
                transport: a::FrontendTransport::ByteStream as i32,
                exposure: a::FrontendExposure::OutboundOnly as i32,
                endpoint: String::new(),
                container_instance: "mcp-i3".into(),
                available: false,
                error_reason: "the controller session is reconnecting".into(),
            },
        ],
        health: Some(a::ApplianceHealth {
            installation_id: "inst-bundled-mcp".into(),
            boot_id: BOOT_ID.into(),
            container_instance: "mcp-i3".into(),
            config_revision: 5,
            healthy: true,
            status: "serving".into(),
            registration_id: "reg-bundled-mcp".into(),
        }),
        enabled: true,
        boot_id: BOOT_ID.into(),
        container_instance: "mcp-i3".into(),
        runtime_stopped: Some(false),
    }
}
