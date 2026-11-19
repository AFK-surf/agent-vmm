//! The appliance commands that start one operation and watch it: install, update, recover, stop, start, activate, restart and revoke.

use crate::appliance::{Client, Lifecycle, Outcome, bounded, lifecycle_grant, message_text, next_message, read_seed, request_id, select_installation, unix_millis, watch_operation};
use agent_vmm_proto::appliance::v1 as appliancev1;
use appliancev1::stage_content_request::Payload;
use appliancev1::{ApplianceContentReference, ApplianceContentRole, ApplianceOperation, ApplianceOperationState, ControllerApplianceManifest};
use prost::Message;
use std::io::Read;
use std::path::Path;
use std::time::SystemTime;
use tokio::time::Instant;

pub(crate) async fn run(mut client: Client, end: Instant, request: Lifecycle) -> Outcome<()> {
    let deadline = unix_millis(SystemTime::now() + request.timeout);
    let (installation, operation) = match request.command.as_str() {
        "recover" => recover(&mut client, end, &request, deadline).await?,
        "install" | "update" => deploy(&mut client, end, &request, deadline).await?,
        _ => operate(&mut client, end, &request, deadline).await?,
    };
    watch_operation(&mut client, end, &installation, &operation).await
}

/// Continues an interrupted update with a later deadline.
async fn recover(client: &mut Client, end: Instant, request: &Lifecycle, deadline: i64) -> Outcome<(String, ApplianceOperation)> {
    if request.operation.is_empty() {
        return Err("recover requires --operation".into());
    }
    let watch = appliancev1::WatchApplianceOperationRequest { installation_id: request.installation.clone(), operation_id: request.operation.clone() };
    let mut watch = bounded(end, client.watch_appliance_operation(watch)).await?;
    let operation = next_message(end, &mut watch).await?.operation.unwrap_or_default();
    drop(watch);
    if !request.installation.is_empty() && operation.installation_id != request.installation {
        return Err("operation belongs to another installation".into());
    }
    if deadline <= operation.deadline_unix_millis {
        return Err("recovery timeout must extend the operation deadline".into());
    }
    let updated = operation.updated_at.as_ref().map_or(0, |time| time.seconds * 1_000_000_000 + i64::from(time.nanos));
    let recovery = appliancev1::RecoverApplianceUpdateRequest { installation_id: operation.installation_id.clone(), operation_id: request.operation.clone(), expected_updated_unix_nanos: updated, deadline_unix_millis: deadline };
    let response = bounded(end, client.recover_appliance_update(recovery)).await?;
    Ok((operation.installation_id, response.operation.unwrap_or_default()))
}

/// Changes the run state of the one installation that the product or the installation ID selects.
async fn operate(client: &mut Client, end: Instant, request: &Lifecycle, deadline: i64) -> Outcome<(String, ApplianceOperation)> {
    let listed = bounded(end, client.list_appliances(appliancev1::ListAppliancesRequest {})).await?;
    let selected = select_installation(&listed.appliances, &request.product, &request.installation)?;
    let (installation_id, request_id, expected_revision, deadline_unix_millis) = (selected.installation_id.clone(), request_id(), selected.revision, deadline);
    let operation = match request.command.as_str() {
        "stop" => bounded(end, client.stop_appliance(appliancev1::StopApplianceRequest { installation_id, request_id, expected_revision, deadline_unix_millis })).await?.operation,
        "revoke" => bounded(end, client.revoke_appliance(appliancev1::RevokeApplianceRequest { installation_id, request_id, expected_revision, deadline_unix_millis })).await?.operation,
        "restart" => bounded(end, client.restart_appliance(appliancev1::RestartApplianceRequest { installation_id, request_id, expected_revision, deadline_unix_millis })).await?.operation,
        _ => {
            let plan = appliancev1::PlanActivationRequest { installation_id: installation_id.clone(), request_id: request_id.clone(), expected_revision, deadline_unix_millis };
            println!("{}", message_text("PlanActivationResponse", &bounded(end, client.plan_activation(plan)).await?));
            bounded(end, client.activate(appliancev1::ActivateRequest { installation_id, request_id, expected_revision, deadline_unix_millis })).await?.operation
        }
    };
    Ok((selected.installation_id.clone(), operation.unwrap_or_default()))
}

/// Stages the package and installs it, or updates the selected installation to it.
async fn deploy(client: &mut Client, end: Instant, request: &Lifecycle, deadline: i64) -> Outcome<(String, ApplianceOperation)> {
    if request.package.is_empty() {
        return Err("install/update requires --package".into());
    }
    let package = Path::new(&request.package);
    let manifest = ControllerApplianceManifest::decode(std::fs::read(package.join("manifest.pb"))?.as_slice()).map_err(|error| error.to_string())?;
    if !request.product.is_empty() && manifest.product_id != request.product {
        return Err("package product does not match --product".into());
    }
    stage_package_content(client, end, package, &manifest, deadline).await?;
    let mut grant = lifecycle_grant(&manifest, None, request.endpoints.as_deref(), true)?;
    request.resources.apply(&mut grant, &manifest)?;
    let stage = appliancev1::StageArtifactRequest { request_id: request_id(), deadline_unix_millis: deadline, manifest: Some(manifest.clone()) };
    let artifact_id = bounded(end, client.stage_artifact(stage)).await?.artifact_id;
    let request_id = request_id();
    if request.command == "install" {
        if !request.bootstrap_stdin {
            return Err("install requires --bootstrap-stdin; pass the seed through an anonymous pipe".into());
        }
        let seed = read_seed(&mut std::io::stdin().lock())?;
        let installation = if request.installation.is_empty() { format!("{}-{}", manifest.product_id, crate::appliance::request_id()) } else { request.installation.clone() };
        let plan = appliancev1::PlanInstallRequest { installation_id: installation.clone(), request_id: request_id.clone(), expected_revision: 0, artifact_id: artifact_id.clone(), deadline_unix_millis: deadline, approved_grant: Some(grant.clone()) };
        bounded(end, client.plan_install(plan)).await.map_err(|fault| fault.context("install plan rejected; reduce --system-disk-gib or --environment-disk-gib to fit VM capacity, within manifest limits"))?;
        let install = appliancev1::InstallRequest { installation_id: installation.clone(), request_id, expected_revision: 0, artifact_id, deadline_unix_millis: deadline, approved_grant: Some(grant), opaque_bootstrap_seed: seed };
        let installed = bounded(end, client.install(install)).await?.operation.unwrap_or_default();
        println!("installation={installation} operation={}", installed.operation_id);
        let operation = activate_installed(client, end, &installation, deadline).await.map_err(|fault| fault.context(&format!("installation {installation} is installed; continue with appliance activate --installation {installation}")))?;
        return Ok((installation, operation));
    }
    let listed = bounded(end, client.list_appliances(appliancev1::ListAppliancesRequest {})).await?;
    let selected = select_installation(&listed.appliances, &manifest.product_id, &request.installation)?;
    let mut grant = lifecycle_grant(&manifest, selected.approved_grant.as_ref(), request.endpoints.as_deref(), request.approve_changes)?;
    request.resources.apply(&mut grant, &manifest)?;
    let requested = lifecycle_grant(&manifest, None, None, true)?;
    let grant_text = |grant: &appliancev1::ApprovedGrant| message_text("ApprovedGrant", grant);
    println!("current_grant={}\nrequested_grant={}\nproposed_grant={}", grant_text(&selected.approved_grant.clone().unwrap_or_default()), grant_text(&requested), grant_text(&grant));
    let (installation_id, expected_revision) = (selected.installation_id.clone(), selected.revision);
    let plan = appliancev1::PlanUpdateRequest { installation_id: installation_id.clone(), request_id: request_id.clone(), expected_revision, artifact_id: artifact_id.clone(), deadline_unix_millis: deadline, approved_grant: Some(grant.clone()) };
    let plan = bounded(end, client.plan_update(plan)).await.map_err(|fault| fault.context("update plan rejected the proposed grant; inspect target permissions and adjust approval explicitly"))?;
    println!("{}", message_text("PlanUpdateResponse", &plan));
    let update = appliancev1::UpdateApplianceRequest { installation_id: installation_id.clone(), request_id, expected_revision, artifact_id, deadline_unix_millis: deadline, approved_grant: Some(grant), opaque_bootstrap_seed: None };
    let operation = bounded(end, client.update_appliance(update)).await?.operation.unwrap_or_default();
    Ok((installation_id, operation))
}

/// Activates an installation at its current revision.
async fn activate_installed(client: &mut Client, end: Instant, installation: &str, deadline: i64) -> Outcome<ApplianceOperation> {
    let status = bounded(end, client.get_appliance_status(appliancev1::GetApplianceStatusRequest { installation_id: installation.to_string() })).await?;
    let (installation_id, request_id, expected_revision) = (installation.to_string(), request_id(), status.status.map_or(0, |status| status.revision));
    let plan = appliancev1::PlanActivationRequest { installation_id: installation_id.clone(), request_id: request_id.clone(), expected_revision, deadline_unix_millis: deadline };
    bounded(end, client.plan_activation(plan)).await?;
    let activate = appliancev1::ActivateRequest { installation_id, request_id, expected_revision, deadline_unix_millis: deadline };
    Ok(bounded(end, client.activate(activate)).await?.operation.unwrap_or_default())
}

/// The archive of one content reference inside a package directory.
fn archive_path(package: &Path, content: &ApplianceContentReference) -> std::path::PathBuf {
    if content.role() == ApplianceContentRole::Controller {
        return package.join("image.oci.tar");
    }
    let digest: String = content.archive_sha256.iter().map(|byte| format!("{byte:02x}")).collect();
    package.join("content").join(format!("{digest}.oci.tar"))
}

/// Sends the Host each archive of the package that its cache does not hold.
async fn stage_package_content(client: &mut Client, end: Instant, package: &Path, manifest: &ControllerApplianceManifest, deadline: i64) -> Outcome<()> {
    let statuses = bounded(end, client.get_content_status(appliancev1::GetContentStatusRequest { content_refs: manifest.content_refs.clone() })).await?;
    for status in statuses.contents {
        let content = status.content_ref.unwrap_or_default();
        let state = if status.available { "cached" } else { "staging" };
        println!("content role={} digest={} bytes={} {state}", content.role().as_str_name(), content.image_manifest_digest, content.archive_size);
        if !status.available {
            stage_archive(client, end, &archive_path(package, &content), content, deadline).await?;
        }
    }
    Ok(())
}

async fn stage_archive(client: &mut Client, end: Instant, path: &Path, content: ApplianceContentReference, deadline: i64) -> Outcome<()> {
    let mut archive = std::fs::File::open(path)?;
    let total = content.archive_size;
    if archive.metadata()?.len() != total {
        return Err("package archive size does not match the manifest".into());
    }
    let header = appliancev1::StageContentHeader { request_id: request_id(), deadline_unix_millis: deadline, content_ref: Some(content) };
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    // The archive is read on a blocking thread. A closed channel means that the Host ended the call.
    let reader = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let send = |payload| sender.blocking_send(appliancev1::StageContentRequest { payload: Some(payload) }).is_ok();
        let (mut buffer, mut sent, mut next_report) = (vec![0u8; 1 << 20], 0u64, 256u64 << 20);
        let mut open = send(Payload::Header(header));
        while open {
            let count = archive.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            open = send(Payload::ArchiveChunk(buffer[..count].to_vec()));
            sent += count as u64;
            if sent >= next_report {
                println!("content sent_bytes={sent} total_bytes={total}");
                next_report = sent + (256 << 20);
            }
        }
        Ok(())
    });
    let receipt = bounded(end, client.stage_content(tokio_stream::wrappers::ReceiverStream::new(receiver))).await;
    if let Ok(Err(error)) = reader.await {
        return Err(error.into());
    }
    let operation = receipt?.operation.unwrap_or_default();
    println!("content receipt={} state={}", operation.operation_id, operation.state().as_str_name());
    if operation.state() != ApplianceOperationState::Succeeded {
        return Err("content stage did not succeed".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_controller_image_and_other_content_have_fixed_names_in_a_package() {
        let controller = ApplianceContentReference { role: ApplianceContentRole::Controller as i32, archive_sha256: vec![0xab; 32], ..Default::default() };
        assert_eq!(archive_path(Path::new("/package"), &controller), Path::new("/package/image.oci.tar"));
        let workload = ApplianceContentReference { role: ApplianceContentRole::Workload as i32, ..controller };
        assert_eq!(archive_path(Path::new("/package"), &workload), Path::new("/package/content").join(format!("{}.oci.tar", "ab".repeat(32))));
    }
}
