use crate::{
    RuntimeContext,
    context::{core_status, resource_id},
    error::host_status,
};
use agent_vmm_proto::host::v1 as h;
use std::collections::{BTreeMap, HashSet};
use vmm_rpc::{Code, Status};
use uuid::Uuid;
use vmm_core::{AccessMode, HostShareMount, Mount, RootOverlay, SandboxDefinition, SandboxKey};

fn absolute(path: &str) -> bool {
    path == "/"
        || (path.starts_with('/')
            && path[1..]
                .split('/')
                .all(|p| !p.is_empty() && p != "." && p != ".." && !p.as_bytes().contains(&0)))
}

pub(crate) fn environment(values: &[String]) -> Result<BTreeMap<String, String>, Status> {
    let invalid = || {
        host_status(
            Code::InvalidArgument,
            h::ErrorReason::EnvironmentInvalid,
            "invalid environment entry",
        )
    };
    if values.len() > 256 {
        return Err(invalid());
    }
    let mut env = BTreeMap::new();
    for value in values {
        let (key, val) = value.split_once('=').ok_or_else(invalid)?;
        let mut chars = key.bytes();
        if !chars
            .next()
            .is_some_and(|c| c == b'_' || c.is_ascii_alphabetic())
            || !chars.all(|c| c == b'_' || c.is_ascii_alphanumeric())
            || val.as_bytes().contains(&0)
        {
            return Err(invalid());
        }
        env.insert(key.into(), val.into());
    }
    Ok(env)
}

pub(crate) fn definition(
    ctx: &RuntimeContext,
    r: &h::CreateContainerRequest,
) -> Result<SandboxDefinition, Status> {
    resource_id(&r.container_id)?;
    if r.image.is_empty() || r.log_limit_bytes == 0 || r.log_limit_bytes > 64 << 20 {
        return Err(Status::invalid_argument(
            "image and a bounded log limit are required",
        ));
    }
    for args in [&r.entrypoint_override, &r.command_override]
        .into_iter()
        .flatten()
    {
        if args.values.len() > 256
            || args
                .values
                .iter()
                .any(|s| s.is_empty() || s.len() > 1 << 20 || s.as_bytes().contains(&0))
        {
            return Err(Status::invalid_argument("invalid process arguments"));
        }
    }
    if r.workdir
        .as_deref()
        .is_some_and(|s| !s.is_empty() && !absolute(s))
        || r.user.as_deref().is_some_and(|s| s.as_bytes().contains(&0))
    {
        return Err(Status::invalid_argument("invalid workdir or user"));
    }
    let env = environment(&r.env)?;
    let (image_reference, image) =
        crate::image::grant::resolve(ctx, &r.image).map_err(core_status)?;
    let generation = Uuid::new_v4().to_string();
    let mut mounts = vec![];
    let mut host_shares = vec![];
    let mut destinations = HashSet::new();
    for mount in &r.volumes {
        let path = &mount.destination;
        if !absolute(path)
            || path == "/"
            || ["/proc", "/dev", "/sys"].iter().any(|p| {
                path == *p
                    || path
                        .strip_prefix(p)
                        .is_some_and(|tail| tail.starts_with('/'))
            })
            || !destinations.insert(path)
        {
            return Err(Status::invalid_argument(
                "invalid or duplicate mount destination",
            ));
        }
        match &mount.source {
            Some(h::volume_mount::Source::OwnedVolume(id)) => {
                resource_id(id)?;
                ctx.core
                    .store
                    .volume(&ctx.scope.id, id)
                    .map_err(core_status)?;
                mounts.push(Mount {
                    volume_id: id.clone(),
                    guest_path: path.clone(),
                    access: if mount.read_only {
                        AccessMode::ReadOnly
                    } else {
                        AccessMode::ReadWrite
                    },
                });
            }
            Some(h::volume_mount::Source::HostShare(id)) => {
                resource_id(id)?;
                host_shares.push(HostShareMount {
                    share_id: id.clone(),
                    guest_path: path.clone(),
                    read_only: mount.read_only,
                });
            }
            None => return Err(Status::invalid_argument("mount source required")),
        }
    }
    Ok(SandboxDefinition {
        key: SandboxKey {
            scope_id: ctx.scope.id.clone(),
            sandbox_id: r.container_id.clone(),
            generation_id: generation.clone(),
        },
        image_reference,
        manifest_digest: image.manifest_digest,
        read_only_root: r.read_only_root,
        pids_max: r.limits.as_ref().and_then(|l| l.pids).map(u64::from),
        root_overlay: (!r.read_only_root).then(|| RootOverlay {
            extent_id: format!("root-{generation}"),
            quota_bytes: ctx.scope.limits.disk_quota_bytes,
            used_bytes: 0,
        }),
        mounts,
        host_shares,
        entrypoint: r.entrypoint_override.as_ref().map(|s| s.values.clone()),
        command: r.command_override.as_ref().map(|s| s.values.clone()),
        workdir: r.workdir.clone(),
        user: r.user.clone(),
        log_limit_bytes: r.log_limit_bytes,
        env,
    })
}
