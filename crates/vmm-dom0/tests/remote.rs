mod support;
use std::sync::Arc;
use tokio::net::UnixListener;
use vmm_core::{
    backend::{Dom0Backend, WorkspaceRequest},
    fake::FakeDom0Backend,
    *,
};
use vmm_dom0::remote::{RemoteDom0Client, serve_connection};

#[tokio::test]
async fn core_lifecycle_over_daemon_socket() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("dom0.sock");
    let listener = UnixListener::bind(&path)?;
    let daemon = tokio::spawn(async move {
        let backend = FakeDom0Backend::default();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            serve_connection(stream, &backend).await.unwrap();
        }
    });
    let client = Arc::new(RemoteDom0Client::unix(path));
    let core = Core::new(
        Arc::new(Store::open(dir.path().join("core.db"))?),
        client.clone(),
    );
    core.store.create_scope(&support::scope("a"), None)?;
    core.create_volume(&support::volume("a")).await?;
    let def = support::definition("a", "worker", AccessMode::ReadWrite);
    core.import_image(
        "a",
        "cue",
        &ImageSource {
            manifest_digest: def.manifest_digest,
            staged_extent: "staged".into(),
            physical_bytes: 4096,
        },
    )
    .await?;
    core.define_sandbox(&def).await?;
    let vm = core.start(&def.key).await?;
    assert_eq!(client.list_live_vms().await?[0].incarnation, vm);
    assert!(matches!(
        client.open_guest_runtime(&vm.boot).await,
        Err(Error::Fenced)
    ));
    client
        .workspace(
            &vm.boot,
            WorkspaceRequest::Write {
                path: "/data/file".into(),
                bytes: b"payload".to_vec(),
            },
        )
        .await?;
    assert_eq!(
        client
            .workspace(&vm.boot, WorkspaceRequest::Read("/data/file".into()))
            .await?,
        b"payload"
    );
    assert_eq!(
        core.reconcile_after_restart().await?.adopted,
        vec![vm.clone()]
    );
    core.stop(&vm.boot, false).await?;
    core.delete_sandbox(&def.key).await?;
    core.discard_volume("a", "data").await?;
    assert!(
        core.remove_image_reference("a", "cue", def.manifest_digest)
            .await?
            > 0
    );
    assert!(client.list_live_vms().await?.is_empty());
    daemon.abort();
    Ok(())
}
