//! Fast local macOS end-to-end verification harness against a live Carrier VM.
//! Runs workspace volume alignment, public DNS/HTTPS egress, BuildImage under two
//! 16 GiB volume containers, sparse pool / blk_discard accounting, and memory telemetry.

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("fast_verify requires macOS (Virtualization.framework)");
    std::process::exit(2);
}

#[cfg(target_os = "macos")]
mod macos {
    use agent_vmm_host::{
        carrier::{CarrierClientAdapter, CarrierImageStaging},
        host::HostState,
    };
    use agent_vmm_proto::host::v1::{
        self as h,
        agent_runtime_service_client::AgentRuntimeServiceClient,
        agent_runtime_service_server::AgentRuntimeServiceServer,
    };
    use clap::Parser;
    use serde_json::json;
    use std::{
        fs::{self, DirBuilder, Permissions},
        os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        sync::Arc,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use vmm_carrier_macos::{
        client::CarrierClient,
        protocol::{Request, State},
    };
    use vmm_compat::{
        image::{ImageImports, build::Builds},
        mesh::ServiceMesh,
        runtime::RuntimeService,
        session::RuntimeAuth,
        workspace::{DEFAULT_VOLUME, WorkspaceService},
    };
    use vmm_core::{Core, EgressMode, Limits, Origin, Scope, ScopeState};
    use zeroize::Zeroizing;

    #[derive(Parser)]
    struct Args {
        #[arg(long)]
        carrier_bin: PathBuf,
        #[arg(long)]
        bundle: PathBuf,
        #[arg(long)]
        work_dir: PathBuf,
        #[arg(long)]
        oci_archive: PathBuf,
        #[arg(long)]
        oci_manifest_digest: String,
        #[arg(long)]
        oci_archive_sha256: String,
        #[arg(long)]
        build_context_tar: PathBuf,
        #[arg(long, default_value_t = 8)]
        cpus: usize,
        #[arg(long, default_value_t = 128 << 30)]
        state_size: u64,
        #[arg(long)]
        test_pool_grow: bool,
        #[arg(long)]
        memory_only: bool,
    }

    struct CarrierGuard {
        child: Child,
        pid: u32,
    }
    impl CarrierGuard {
        fn spawn(
            bin: &Path,
            bundle: &Path,
            instance: &Path,
            socket: &Path,
            serial_log: &Path,
            cpus: usize,
            state_size: u64,
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let child = Command::new(bin)
                .arg("--bundle")
                .arg(bundle)
                .arg("--instance")
                .arg(instance)
                .arg("--socket")
                .arg(socket)
                .arg("--serial-log")
                .arg(serial_log)
                .arg("--cpus")
                .arg(cpus.to_string())
                .arg("--state-size")
                .arg(state_size.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()?;
            let pid = child.id();
            Ok(Self { child, pid })
        }
    }
    impl Drop for CarrierGuard {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    fn decode_hex_32(hex: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let hex = hex.strip_prefix("sha256:").unwrap_or(hex);
        if hex.len() != 64 {
            return Err("expected 64 hex chars".into());
        }
        let mut out = Vec::with_capacity(32);
        for i in (0..64).step_by(2) {
            out.push(u8::from_str_radix(&hex[i..i + 2], 16)?);
        }
        Ok(out)
    }

    fn pool_stats(path: &Path) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let meta = fs::metadata(path)?;
        let logical = meta.len();
        let physical = meta.blocks() * 512;
        Ok(json!({
            "logical_bytes": logical,
            "physical_bytes": physical,
            "logical_gib": (logical as f64) / (1024.0 * 1024.0 * 1024.0),
            "physical_mib": (physical as f64) / (1024.0 * 1024.0),
        }))
    }

    fn parse_top_mem_mib(token: &str) -> Option<f64> {
        let trimmed = token.trim().trim_end_matches(['+', '-']);
        if trimmed.is_empty() {
            return None;
        }
        let (num_str, mult) = match trimmed.as_bytes().last().copied()? {
            b'B' | b'b' => (&trimmed[..trimmed.len() - 1], 1.0 / (1024.0 * 1024.0)),
            b'K' | b'k' => (&trimmed[..trimmed.len() - 1], 1.0 / 1024.0),
            b'M' | b'm' => (&trimmed[..trimmed.len() - 1], 1.0),
            b'G' | b'g' => (&trimmed[..trimmed.len() - 1], 1024.0),
            b'T' | b't' => (&trimmed[..trimmed.len() - 1], 1024.0 * 1024.0),
            _ => (trimmed, 1.0 / (1024.0 * 1024.0)),
        };
        let value: f64 = num_str.parse().ok()?;
        Some(value * mult)
    }

    fn sample_pid_footprint_mib(pid: u32) -> Option<f64> {
        if let Ok(out) = Command::new("top")
            .args(["-l", "1", "-pid", &pid.to_string(), "-stats", "pid,mem"])
            .output()
            && out.status.success()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                let mut parts = line.split_whitespace();
                if parts.next().and_then(|s| s.parse::<u32>().ok()) == Some(pid)
                    && let Some(mem_tok) = parts.next()
                    && let Some(mib) = parse_top_mem_mib(mem_tok)
                {
                    return Some(mib);
                }
            }
        }
        if let Ok(out) = Command::new("footprint")
            .args(["-p", &pid.to_string(), "--bytes"])
            .output()
            && out.status.success()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if let Some(rest) = line.split("phys_footprint:").nth(1)
                    && let Some(num) = rest.split_whitespace().next()
                    && let Ok(bytes) = num.parse::<u64>()
                {
                    return Some((bytes as f64) / (1024.0 * 1024.0));
                }
            }
        }
        None
    }

    fn sample_memory(carrier_pid: u32, pool_path: &Path) -> serde_json::Value {
        use std::collections::{BTreeMap, BTreeSet};

        let self_pid = std::process::id();
        let mut vz_pid_set = BTreeSet::new();
        if let Ok(out) = Command::new("lsof")
            .args(["-n", "-P", "-t", "--"])
            .arg(pool_path)
            .output()
            && out.status.success()
        {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                if let Ok(pid) = line.trim().parse::<u32>()
                    && pid != carrier_pid
                    && pid != self_pid
                {
                    vz_pid_set.insert(pid);
                }
            }
        }

        let ps_out = Command::new("ps")
            .args(["-axo", "pid=,ppid=,rss="])
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        let mut rss_by_pid = BTreeMap::new();
        for line in ps_out.lines() {
            let mut parts = line.split_whitespace();
            let Some(pid) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            let Some(ppid) = parts.next().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            let Some(rss) = parts.next().and_then(|s| s.parse::<u64>().ok()) else {
                continue;
            };
            rss_by_pid.insert(pid, rss);
            if ppid == carrier_pid && pid != self_pid {
                vz_pid_set.insert(pid);
            }
        }

        let carrier_rss_kb = rss_by_pid.get(&carrier_pid).copied().unwrap_or(0);
        let host_rss_kb = rss_by_pid.get(&self_pid).copied().unwrap_or(0);
        let vz_pids: Vec<u32> = vz_pid_set.into_iter().collect();
        let vz_rss_kb: u64 = vz_pids
            .iter()
            .map(|pid| rss_by_pid.get(pid).copied().unwrap_or(0))
            .sum();

        let mut vz_footprint_sum = 0.0f64;
        let mut vz_footprint_any = false;
        for &vz_pid in &vz_pids {
            if let Some(mib) = sample_pid_footprint_mib(vz_pid) {
                vz_footprint_sum += mib;
                vz_footprint_any = true;
            }
        }
        let vz_phys_footprint_mib = vz_footprint_any.then_some(vz_footprint_sum);

        let carrier_rss_mib = (carrier_rss_kb as f64) / 1024.0;
        let host_rss_mib = (host_rss_kb as f64) / 1024.0;
        let vz_rss_mib = (vz_rss_kb as f64) / 1024.0;
        let total_rss_mib = carrier_rss_mib + host_rss_mib + vz_rss_mib;
        let total_footprint_mib =
            carrier_rss_mib + host_rss_mib + vz_phys_footprint_mib.unwrap_or(vz_rss_mib);

        json!({
            "carrier_pid": carrier_pid,
            "carrier_rss_mib": carrier_rss_mib,
            "host_rss_mib": host_rss_mib,
            "vz_pids": vz_pids,
            "vz_rss_mib": vz_rss_mib,
            "vz_phys_footprint_mib": vz_phys_footprint_mib,
            "total_rss_mib": total_rss_mib,
            "total_footprint_mib": total_footprint_mib,
        })
    }

    async fn wait_carrier_socket(socket: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            if tokio::net::UnixStream::connect(socket).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err("timed out waiting for carrier socket".into())
    }

    async fn exec_cmd(
        client: &mut AgentRuntimeServiceClient,
        container_id: &str,
        instance_id: &str,
        req_id: &str,
        argv: Vec<String>,
    ) -> Result<(i32, String, String), Box<dyn std::error::Error>> {
        use h::{exec_request::Event as In, exec_response::Event as Out};
        let deadline = (SystemTime::now() + Duration::from_secs(60))
            .duration_since(UNIX_EPOCH)?
            .as_nanos() as i64;
        let frames = [
            In::Start(h::ExecStart {
                request_id: req_id.into(),
                container_id: container_id.into(),
                argv,
                expected_instance_id: instance_id.into(),
                execution_id: format!("exec-{req_id}"),
                execution_kind: h::ExecutionKind::Exec as i32,
                execution_deadline_unix_nano: deadline,
                ..Default::default()
            }),
            In::CloseStdin(true),
        ];
        let mut stream = client
            .exec(tokio_stream::iter(
                frames.map(|event| h::ExecRequest { event: Some(event) }),
            ))
            .await?
            .into_inner();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code: i32 = -1;
        while let Some(msg) = stream.message().await? {
            match msg.event {
                Some(Out::Stdout(b)) => stdout.extend(b),
                Some(Out::Stderr(b)) => stderr.extend(b),
                Some(Out::ExitCode(c)) => exit_code = c as i32,
                None => {}
            }
        }
        Ok((
            exit_code,
            String::from_utf8_lossy(&stdout).into_owned(),
            String::from_utf8_lossy(&stderr).into_owned(),
        ))
    }

    fn ensure_private_dir(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        DirBuilder::new().recursive(true).mode(0o700).create(path)?;
        fs::set_permissions(path, Permissions::from_mode(0o700))?;
        Ok(())
    }

    pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
        let args = Args::parse();
        let total_start = Instant::now();
        ensure_private_dir(&args.work_dir)?;

        let mut grow_result = json!({"skipped": true});
        if args.test_pool_grow {
            let grow_dir = args.work_dir.join("pool-grow-test");
            let grow_inst = grow_dir.join("instance");
            let grow_sock = grow_dir.join("carrier.sock");
            let grow_serial = grow_dir.join("serial.log");
            ensure_private_dir(&grow_dir)?;
            ensure_private_dir(&grow_inst)?;

            // Phase A: boot with 64 GiB pool.raw, initialize pool.db, then stop gracefully.
            {
                let _guard = CarrierGuard::spawn(
                    &args.carrier_bin,
                    &args.bundle,
                    &grow_inst,
                    &grow_sock,
                    &grow_serial,
                    args.cpus,
                    64 << 30,
                )?;
                wait_carrier_socket(&grow_sock).await?;
                let carrier = CarrierClient::new(grow_sock.clone());
                let st = carrier
                    .control(Request::EnsureStarted {
                        operation_id: "grow-init-64g".into(),
                    })
                    .await?;
                assert_eq!(st.state, State::Running);
                let before = pool_stats(&grow_inst.join("pool.raw"))?;
                let _ = carrier.control(Request::Stop { force: false }).await?;
                grow_result = json!({"before_64g": before});
            }
            // Phase B: restart same instance with 128 GiB pool.raw and verify monotonic Pool::open grow.
            {
                let _guard = CarrierGuard::spawn(
                    &args.carrier_bin,
                    &args.bundle,
                    &grow_inst,
                    &grow_sock,
                    &grow_serial,
                    args.cpus,
                    128 << 30,
                )?;
                wait_carrier_socket(&grow_sock).await?;
                let carrier = CarrierClient::new(grow_sock.clone());
                let st = carrier
                    .control(Request::EnsureStarted {
                        operation_id: "grow-upgrade-128g".into(),
                    })
                    .await?;
                assert_eq!(st.state, State::Running);
                let after = pool_stats(&grow_inst.join("pool.raw"))?;
                assert_eq!(st.pool_logical_bytes, 128 << 30);
                let _ = carrier.control(Request::Stop { force: false }).await?;
                grow_result = json!({
                    "ok": true,
                    "before_64g": grow_result["before_64g"],
                    "after_128g": after,
                });
            }
        }

        let instance_dir = args.work_dir.join("instance");
        let carrier_sock = args.work_dir.join("carrier.sock");
        let serial_log = args.work_dir.join("serial.log");
        let host_dir = args.work_dir.join("host");
        ensure_private_dir(&instance_dir)?;
        ensure_private_dir(&host_dir)?;

        let carrier_guard = CarrierGuard::spawn(
            &args.carrier_bin,
            &args.bundle,
            &instance_dir,
            &carrier_sock,
            &serial_log,
            args.cpus,
            args.state_size,
        )?;
        wait_carrier_socket(&carrier_sock).await?;
        let carrier = CarrierClient::new(carrier_sock.clone());

        let boot_start = Instant::now();
        let status = carrier
            .control(Request::EnsureStarted {
                operation_id: "fast-verify-boot".into(),
            })
            .await?;
        assert_eq!(status.state, State::Running, "carrier VM failed to start");
        let boot_ms = boot_start.elapsed().as_millis();
        let pool_path = instance_dir.join("pool.raw");
        let pool_after_boot = pool_stats(&pool_path)?;

        // Wire up real HostState + RuntimeService over Unix socket.
        let host = HostState::open(&host_dir.join("host.sqlite"))?;
        let backend = Arc::new(carrier.dom0());
        let core = Core::new(host.store.clone(), backend.clone());
        let mut api = host.local_operator(&instance_dir, None)?;
        let adapter = Arc::new(CarrierClientAdapter(carrier.clone()));
        host.attach_guest(&mut api, adapter.clone())?;
        let staging = Arc::new(CarrierImageStaging::new(
            carrier.clone(),
            host.staging_dir().to_path_buf(),
        ));
        let containers = api.containers.clone().ok_or("missing containers")?;
        let imports = ImageImports::new(host.staging_dir().to_path_buf(), staging);
        let builds = Builds::new(adapter.clone(), containers.clone(), imports.clone())?;
        let runtime = Arc::new(RuntimeService {
            core: core.clone(),
            containers,
            workspace: WorkspaceService::new(adapter.clone()),
            guest: adapter,
            imports,
            images: Arc::new(builds),
            mesh: Arc::new(ServiceMesh {
                device_id: "fast-verify".into(),
            }),
        });

        let shell_scope_id = "scope-shell";
        let svc_scope_id = "scope-service";
        for scope_id in [shell_scope_id, svc_scope_id] {
            core.store.create_scope(
                &Scope {
                    id: scope_id.into(),
                    origin: Origin::Local,
                    generation: 1,
                    revision: 1,
                    limits: Limits {
                        pids_max: 256,
                        disk_quota_bytes: 16 << 30,
                        vcpus: 1,
                        boot_ram_mb: 512,
                        max_ram_mb: 512,
                    },
                    egress_mode: EgressMode::PublicInternet,
                    host_socket_rules: vec![],
                    state: ScopeState::Active,
                },
                None,
            )?;
        }
        let (shell_session, shell_secret) = core.store.issue_session(shell_scope_id, None)?;
        let (svc_session, svc_secret) = core.store.issue_session(svc_scope_id, None)?;
        let shell_auth = RuntimeAuth::bound(
            core.store.clone(),
            shell_session.id,
            Zeroizing::new(shell_secret),
        );
        let svc_auth = RuntimeAuth::bound(
            core.store.clone(),
            svc_session.id,
            Zeroizing::new(svc_secret),
        );
        let shell_sock = args.work_dir.join("runtime-shell.sock");
        let svc_sock = args.work_dir.join("runtime-service.sock");
        let shell_listener = tokio::net::UnixListener::bind(&shell_sock)?;
        let svc_listener = tokio::net::UnixListener::bind(&svc_sock)?;
        let shell_server = tokio::spawn(
            vmm_rpc::Router::new(vmm_rpc::Intercepted::new(
                AgentRuntimeServiceServer::from_arc(runtime.clone()),
                shell_auth,
            ))
            .serve_unix(shell_listener, 64, std::future::pending()),
        );
        let svc_server = tokio::spawn(
            vmm_rpc::Router::new(vmm_rpc::Intercepted::new(
                AgentRuntimeServiceServer::from_arc(runtime.clone()),
                svc_auth,
            ))
            .serve_unix(svc_listener, 64, std::future::pending()),
        );
        let mut client = AgentRuntimeServiceClient::new(vmm_rpc::Client::unix(&shell_sock));
        let mut svc_client = AgentRuntimeServiceClient::new(vmm_rpc::Client::unix(&svc_sock));

        // 1. Import tiny OCI image into both scopes and start 2 containers with 16 GiB volumes each.
        let archive_bytes = fs::read(&args.oci_archive)?;
        let archive_sha = decode_hex_32(&args.oci_archive_sha256)?;
        for (req_id, c) in [("import-shell", &mut client), ("import-svc", &mut svc_client)] {
            let header = h::ImportImageHeader {
                request_id: req_id.into(),
                image_reference: "example.com/tiny-busybox:latest".into(),
                manifest_digest: args.oci_manifest_digest.clone(),
                archive_size: archive_bytes.len() as u64,
                archive_sha256: archive_sha.clone(),
                platform: "linux/arm64".into(),
                ..Default::default()
            };
            let mut frames = vec![h::ImportImageRequest {
                payload: Some(h::import_image_request::Payload::Header(header)),
            }];
            for chunk in archive_bytes.chunks(1 << 20) {
                frames.push(h::ImportImageRequest {
                    payload: Some(h::import_image_request::Payload::ArchiveChunk(
                        chunk.to_vec(),
                    )),
                });
            }
            c.import_image(tokio_stream::iter(frames))
                .await?
                .into_inner();
        }

        client
            .create_volume(h::CreateVolumeRequest {
                request_id: "cv-shell".into(),
                volume_id: "vol-shell".into(),
                ..Default::default()
            })
            .await?;
        svc_client
            .create_volume(h::CreateVolumeRequest {
                request_id: "cv-svc".into(),
                volume_id: "vol-service".into(),
                ..Default::default()
            })
            .await?;

        let shell_c = client
            .create_container(h::CreateContainerRequest {
                request_id: "cc-shell".into(),
                container_id: "shell".into(),
                image: "example.com/tiny-busybox:latest".into(),
                command_override: Some(h::StringList {
                    values: vec![
                        "/bin/busybox".into(),
                        "sh".into(),
                        "-c".into(),
                        "sleep 3600".into(),
                    ],
                }),
                volumes: vec![h::VolumeMount {
                    source: Some(h::volume_mount::Source::OwnedVolume("vol-shell".into())),
                    destination: "/workspace".into(),
                    read_only: false,
                }],
                log_limit_bytes: 64 << 10,
                ..Default::default()
            })
            .await?
            .into_inner()
            .container
            .ok_or("missing shell container")?;
        let svc_c = svc_client
            .create_container(h::CreateContainerRequest {
                request_id: "cc-svc".into(),
                container_id: "service".into(),
                image: "example.com/tiny-busybox:latest".into(),
                command_override: Some(h::StringList {
                    values: vec![
                        "/bin/busybox".into(),
                        "sh".into(),
                        "-c".into(),
                        "sleep 3600".into(),
                    ],
                }),
                volumes: vec![h::VolumeMount {
                    source: Some(h::volume_mount::Source::OwnedVolume("vol-service".into())),
                    destination: "/srv".into(),
                    read_only: false,
                }],
                log_limit_bytes: 64 << 10,
                ..Default::default()
            })
            .await?
            .into_inner()
            .container
            .ok_or("missing service container")?;

        let (shell_live, svc_live) = tokio::try_join!(
            client.start_container(h::StartContainerRequest {
                request_id: "sc-shell".into(),
                container_id: "shell".into(),
                expected_generation_id: shell_c.generation_id.clone(),
            }),
            svc_client.start_container(h::StartContainerRequest {
                request_id: "sc-svc".into(),
                container_id: "service".into(),
                expected_generation_id: svc_c.generation_id.clone(),
            }),
        )?;
        let shell_instance = shell_live
            .into_inner()
            .container
            .ok_or("missing live shell")?
            .instance_id;
        let svc_instance = svc_live
            .into_inner()
            .container
            .ok_or("missing live service")?
            .instance_id;

        if args.memory_only {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let memory_two_containers = sample_memory(carrier_guard.pid, &pool_path);
            let _ = client
                .stop_container(h::StopContainerRequest {
                    request_id: "stop-shell".into(),
                    container_id: "shell".into(),
                    expected_instance_id: shell_instance,
                    timeout_seconds: Some(5),
                    ..Default::default()
                })
                .await;
            let _ = svc_client
                .stop_container(h::StopContainerRequest {
                    request_id: "stop-svc".into(),
                    container_id: "service".into(),
                    expected_instance_id: svc_instance,
                    timeout_seconds: Some(5),
                    ..Default::default()
                })
                .await;
            shell_server.abort();
            svc_server.abort();
            let _ = carrier.control(Request::Stop { force: false }).await;
            drop(carrier_guard);
            let summary = json!({
                "ok": true,
                "mode": "memory_only",
                "elapsed_ms": total_start.elapsed().as_millis(),
                "boot_ms": boot_ms,
                "memory_two_containers": memory_two_containers,
            });
            println!("{}", serde_json::to_string_pretty(&summary)?);
            return Ok(());
        }

        // 2. Workspace volume alignment (empty volume_id hits vol-shell mounted at /workspace).
        let (code, _, stderr) = exec_cmd(
            &mut client,
            "shell",
            &shell_instance,
            "write-exec",
            vec![
                "/bin/busybox".into(),
                "sh".into(),
                "-c".into(),
                "printf 'from-exec' > /workspace/exec-made.txt".into(),
            ],
        )
        .await?;
        if code != 0 {
            return Err(format!("exec write failed ({code}): {stderr}").into());
        }
        let stat = client
            .stat_workspace(h::StatWorkspaceRequest {
                path: "exec-made.txt".into(),
            })
            .await?
            .into_inner()
            .entry
            .ok_or("missing stat entry")?;
        assert_eq!(stat.size, 9);
        let listed = client
            .list_workspace(h::ListWorkspaceRequest {
                path: ".".into(),
                ..Default::default()
            })
            .await?
            .into_inner()
            .entries;
        assert!(listed.iter().any(|e| e.path == "exec-made.txt"));
        let mut read_stream = client
            .read_workspace(h::ReadWorkspaceRequest {
                path: "exec-made.txt".into(),
                ..Default::default()
            })
            .await?
            .into_inner();
        let mut read_buf = Vec::new();
        while let Some(frame) = read_stream.message().await? {
            read_buf.extend(frame.data);
        }
        assert_eq!(read_buf, b"from-exec");

        use h::write_workspace_request::Payload as W;
        let write_frames = [
            W::Header(h::WriteWorkspaceHeader {
                request_id: "host-write-1".into(),
                path: "host-made.txt".into(),
                mode: 0o644,
                ..Default::default()
            }),
            W::Data(b"from-host".to_vec()),
            W::Finish(true),
        ]
        .map(|payload| h::WriteWorkspaceRequest {
            payload: Some(payload),
        });
        client
            .write_workspace(tokio_stream::iter(write_frames))
            .await?;
        let (code, stdout, stderr) = exec_cmd(
            &mut client,
            "shell",
            &shell_instance,
            "read-host-made",
            vec![
                "/bin/busybox".into(),
                "cat".into(),
                "/workspace/host-made.txt".into(),
            ],
        )
        .await?;
        if code != 0 || stdout != "from-host" {
            return Err(format!("exec readback mismatch ({code}): stdout={stdout:?} stderr={stderr:?}").into());
        }
        let default_vol_absent = matches!(
            core.store.volume(shell_scope_id, DEFAULT_VOLUME),
            Err(vmm_core::Error::Missing(_))
        );
        assert!(default_vol_absent, "__default_workspace must not be created");

        // 3. Public DNS + HTTPS :443 egress check inside container 1.
        let (dns_code, dns_out, dns_err) = exec_cmd(
            &mut client,
            "shell",
            &shell_instance,
            "egress-dns",
            vec![
                "/bin/busybox".into(),
                "nslookup".into(),
                "example.com".into(),
            ],
        )
        .await?;
        let (https_code, https_out, https_err) = exec_cmd(
            &mut client,
            "shell",
            &shell_instance,
            "egress-https",
            vec![
                "/bin/busybox".into(),
                "sh".into(),
                "-c".into(),
                "busybox nc -w 5 -z example.com 443 && echo TCP443_OK && busybox wget -qO- -T 10 --no-check-certificate https://example.com/ | head -c 128".into(),
            ],
        )
        .await?;
        if dns_code != 0 || https_code != 0 || !https_out.contains("TCP443_OK") {
            return Err(format!(
                "egress check failed: dns=({dns_code}, {dns_out:?}, {dns_err:?}) https=({https_code}, {https_out:?}, {https_err:?})"
            )
            .into());
        }

        // 4. BuildImage while BOTH 16 GiB containers are running.
        let ctx_bytes = fs::read(&args.build_context_tar)?;
        let deadline = (SystemTime::now() + Duration::from_secs(120))
            .duration_since(UNIX_EPOCH)?
            .as_nanos() as i64;
        use h::build_image_request::Event as B;
        let build_frames = [
            B::Header(h::BuildImageHeader {
                request_id: "build-tiny-1".into(),
                dockerfile_path: "Dockerfile".into(),
                image: "example.com/built-tiny:v1".into(),
                network_mode: h::BuildNetworkMode::None as i32,
                container_id: "shell".into(),
                expected_instance_id: shell_instance.clone(),
                execution_id: "build-exec-1".into(),
                execution_deadline_unix_nano: deadline,
                ..Default::default()
            }),
            B::Context(h::BuildContextChunk {
                data: ctx_bytes,
                r#final: true,
            }),
        ]
        .map(|event| h::BuildImageRequest { event: Some(event) });
        let mut build_stream = client
            .build_image(tokio_stream::iter(build_frames))
            .await?
            .into_inner();
        let mut built_digest = None;
        let mut build_logs = Vec::new();
        loop {
            match build_stream.message().await {
                Ok(Some(m)) => match m.event {
                    Some(h::build_image_response::Event::Result(r)) => built_digest = Some(r.digest),
                    Some(h::build_image_response::Event::Progress(p)) => {
                        build_logs.push(p);
                    }
                    None => {}
                },
                Ok(None) => break,
                Err(status) => {
                    return Err(format!(
                        "BuildImage failed: {status}; logs:\n{}",
                        build_logs.join("")
                    )
                    .into());
                }
            }
        }
        let built_digest = built_digest.ok_or("BuildImage returned no result")?;

        // 5. Measure memory with both containers running, plus sparse write & blk_discard reclamation.
        let memory_two_containers = sample_memory(carrier_guard.pid, &pool_path);

        // Write 32 MiB of random data into vol-service to allocate physical blocks, then delete volume to verify blk_discard.
        let (dd_code, _, dd_err) = exec_cmd(
            &mut svc_client,
            "service",
            &svc_instance,
            "write-32m",
            vec![
                "/bin/busybox".into(),
                "sh".into(),
                "-c".into(),
                "dd if=/dev/urandom of=/srv/blob.bin bs=1M count=32 2>/dev/null && sync".into(),
            ],
        )
        .await?;
        if dd_code != 0 {
            return Err(format!("dd write failed ({dd_code}): {dd_err}").into());
        }
        let pool_after_32m_write = pool_stats(&pool_path)?;

        svc_client
            .stop_container(h::StopContainerRequest {
                request_id: "stop-svc".into(),
                container_id: "service".into(),
                expected_instance_id: svc_instance,
                timeout_seconds: Some(5),
                ..Default::default()
            })
            .await?;
        svc_client
            .delete_container(h::DeleteContainerRequest {
                request_id: "del-svc".into(),
                container_id: "service".into(),
                expected_generation_id: svc_c.generation_id,
            })
            .await?;
        svc_client
            .delete_volume(h::DeleteVolumeRequest {
                request_id: "del-vol-svc".into(),
                volume_id: "vol-service".into(),
            })
            .await?;
        let pool_after_discard = pool_stats(&pool_path)?;

        // Clean shutdown.
        let _ = client
            .stop_container(h::StopContainerRequest {
                request_id: "stop-shell".into(),
                container_id: "shell".into(),
                expected_instance_id: shell_instance,
                timeout_seconds: Some(5),
                ..Default::default()
            })
            .await;
        shell_server.abort();
        svc_server.abort();
        let _ = carrier.control(Request::Stop { force: false }).await;
        drop(carrier_guard);

        let summary = json!({
            "ok": true,
            "elapsed_ms": total_start.elapsed().as_millis(),
            "boot_ms": boot_ms,
            "workspace_alignment": {
                "ok": true,
                "default_volume_created": !default_vol_absent,
                "exec_to_host_readback": "from-exec",
                "host_to_exec_readback": stdout,
            },
            "egress": {
                "ok": true,
                "dns_output": dns_out.trim(),
                "https_head": https_out.trim(),
            },
            "build_image": {
                "ok": true,
                "digest": built_digest,
            },
            "pool_sparse": {
                "after_boot": pool_after_boot,
                "after_32m_write": pool_after_32m_write,
                "after_volume_discard": pool_after_discard,
            },
            "memory_two_containers": memory_two_containers,
            "pool_grow_64g_to_128g": grow_result,
        });
        println!("{}", serde_json::to_string_pretty(&summary)?);
        Ok(())
    }
}

#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() {
    if let Err(err) = macos::run().await {
        eprintln!("fast_verify error: {err}");
        std::process::exit(1);
    }
}
