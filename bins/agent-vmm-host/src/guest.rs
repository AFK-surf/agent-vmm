use tonic::Status;
use vmm_core::BootIdentity;
pub(crate) async fn health(
    guest: &dyn vmm_compat::guest::GuestDriver,
    boot: &BootIdentity,
) -> Result<vmm_guest_proto::rpc::HealthReply, Status> {
    let client = guest.client(boot).await?;
    let mut nonce = [0; vmm_guest_proto::auth::NONCE_LEN];
    getrandom::fill(&mut nonce)
        .map_err(|_| Status::unavailable("guest health nonce unavailable"))?;
    tokio::task::spawn_blocking(move || client.health(nonce))
        .await
        .map_err(|_| Status::unavailable("guest health interrupted"))?
        .map_err(vmm_compat::error::client_status)
}
