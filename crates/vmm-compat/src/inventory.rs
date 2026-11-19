use crate::container::db_error;
use crate::{Containers, container::fingerprint, context::core_status};
use agent_vmm_proto::host::v1 as h;
use prost_types::Timestamp;
use rusqlite::{OptionalExtension, params};
use tonic::Status;
use vmm_core::{AccessMode, Sandbox, SandboxState};
use vmm_guest_proto::rpc::{MainState, ResourceUsage};

fn timestamp(nanos: i64) -> Option<Timestamp> {
    (nanos > 0).then_some(Timestamp {
        seconds: nanos / 1_000_000_000,
        nanos: (nanos % 1_000_000_000) as i32,
    })
}

fn seconds(value: i64) -> Option<Timestamp> {
    (value > 0).then_some(Timestamp {
        seconds: value,
        nanos: 0,
    })
}

fn usage(sample: &ResourceUsage) -> h::Usage {
    let missing = [
        sample.cpu_nanos,
        sample.memory_bytes,
        sample.pids,
        sample.disk_read_bytes,
        sample.disk_write_bytes,
        sample.disk_used_bytes,
        sample.network_rx_bytes,
        sample.network_tx_bytes,
    ]
    .contains(&None);
    h::Usage {
        sampled_at: timestamp(sample.sampled_unix_nanos),
        stale: sample.sampled_unix_nanos <= 0,
        unavailable_reason: if missing {
            "partial guest sample".into()
        } else {
            String::new()
        },
        cpu_nanos: sample.cpu_nanos.unwrap_or_default(),
        memory_bytes: sample.memory_bytes.unwrap_or_default(),
        pids: u32::try_from(sample.pids.unwrap_or_default()).unwrap_or(u32::MAX),
        read_bytes: sample.disk_read_bytes.unwrap_or_default(),
        write_bytes: sample.disk_write_bytes.unwrap_or_default(),
        disk_bytes: sample.disk_used_bytes.unwrap_or_default(),
        network_rx_bytes: sample.network_rx_bytes.unwrap_or_default(),
        network_tx_bytes: sample.network_tx_bytes.unwrap_or_default(),
    }
}

impl Containers {
    pub(crate) async fn observe(
        &self,
        store: &vmm_core::Store,
        sb: &Sandbox,
        expected: Option<h::CreateContainerRequest>,
    ) -> Result<h::Container, Status> {
        let def = &sb.definition;
        let facts = self.facts(&def.key)?;
        let state = match sb.state {
            SandboxState::Defined => h::ContainerState::Created,
            SandboxState::Stopped => h::ContainerState::Stopped,
            SandboxState::Deleted => return Err(Status::not_found("container deleted")),
            _ => h::ContainerState::Unknown,
        };
        let volumes = def
            .mounts
            .iter()
            .map(|m| h::VolumeMount {
                source: Some(h::volume_mount::Source::OwnedVolume(m.volume_id.clone())),
                destination: m.guest_path.clone(),
                read_only: m.access == AccessMode::ReadOnly,
            })
            .chain(def.host_shares.iter().map(|m| h::VolumeMount {
                source: Some(h::volume_mount::Source::HostShare(m.share_id.clone())),
                destination: m.guest_path.clone(),
                read_only: m.read_only,
            }))
            .collect();
        let mut out = h::Container {
            id: def.key.sandbox_id.clone(),
            image: def.image_reference.clone(),
            generation_id: def.key.generation_id.clone(),
            state: state as i32,
            volumes,
            environment_keys: def.env.keys().cloned().collect(),
            argv: def
                .entrypoint
                .iter()
                .chain(&def.command)
                .flatten()
                .cloned()
                .collect(),
            workdir: def.workdir.clone(),
            user: def.user.clone(),
            desired_running: Some(matches!(
                sb.state,
                SandboxState::Starting
                    | SandboxState::Running
                    | SandboxState::Quiesced
                    | SandboxState::Unverified
            )),
            created_at: facts.as_ref().and_then(|f| seconds(f.created)),
            started_at: facts.as_ref().and_then(|f| seconds(f.started)),
            configuration_matches: expected
                .zip(facts.as_ref())
                .map(|(r, f)| fingerprint(r).as_slice() == f.hash),
            start_request_instance_id: facts.map(|f| f.instance).unwrap_or_default(),
            ..Default::default()
        };
        if !matches!(sb.state, SandboxState::Running | SandboxState::Quiesced) {
            if sb.state == SandboxState::Stopped
                && let Some((code, ended)) = self
                    .db()?
                    .query_row(
                        "SELECT code,ended FROM terminal_exits WHERE instance=?",
                        [&out.start_request_instance_id],
                        |r| Ok((r.get::<_, u32>(0)?, r.get::<_, i64>(1)?)),
                    )
                    .optional()
                    .map_err(db_error)?
            {
                out.exit_code = Some(code);
                out.finished_at = timestamp(ended);
            }
            return Ok(out);
        }
        let boot = sb
            .boot
            .as_ref()
            .ok_or_else(|| Status::internal("missing live boot identity"))?;
        let live = self.driver.health(boot).await?;
        if live.instance_id != boot.instance_id
            || store.sandbox(&def.key).map_err(core_status)?.boot.as_ref() != Some(boot)
        {
            return Err(core_status(vmm_core::Error::Fenced));
        }
        out.instance_id = live.instance_id;
        out.state = match live.main {
            MainState::Running {
                started_unix_nanos, ..
            } => {
                out.started_at = timestamp(started_unix_nanos).or(out.started_at);
                h::ContainerState::Running
            }
            MainState::Idle => h::ContainerState::Running,
            MainState::Exited {
                exit,
                exited_unix_nanos,
            } => {
                out.exit_code = u32::try_from(exit.code).ok();
                out.finished_at = timestamp(exited_unix_nanos);
                if let Some(code) = out.exit_code {
                    self.db()?
                        .execute(
                            "INSERT OR IGNORE INTO terminal_exits VALUES(?,?,?)",
                            params![boot.instance_id, code, exited_unix_nanos],
                        )
                        .map_err(db_error)?;
                }
                h::ContainerState::Stopped
            }
            MainState::FailedToStart { .. } => h::ContainerState::Stopped,
        } as i32;
        if let Some(bytes) = live
            .usage
            .overlay_used_bytes
            .filter(|_| def.root_overlay.is_some())
        {
            store
                .record_overlay_usage(&def.key, bytes)
                .map_err(core_status)?;
        }
        for (id, bytes) in &live.usage.volume_used_bytes {
            if def.mounts.iter().any(|m| &m.volume_id == id) {
                store
                    .record_volume_usage(&def.key.scope_id, id, *bytes)
                    .map_err(core_status)?;
            }
        }
        out.usage = Some(usage(&live.usage));
        Ok(out)
    }
}
