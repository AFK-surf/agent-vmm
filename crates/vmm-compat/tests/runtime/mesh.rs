use super::{AgentRuntimeServiceClient, h};
use agent_vmm_proto::service::v1::ServiceProtocol;
use vmm_rpc::{Code};

pub async fn exercise(client: &mut AgentRuntimeServiceClient, instance: &str) {
    let request = h::CreateServiceExportRequest {
        request_id: "export".into(),
        container_id: "worker".into(),
        container_instance_id: instance.into(),
        port: 8080,
        protocol: ServiceProtocol::Http as i32,
    };
    let exported = client
        .create_service_export(request.clone())
        .await
        .unwrap()
        .into_inner()
        .export
        .unwrap();
    assert_eq!(
        client
            .create_service_export(request)
            .await
            .unwrap()
            .into_inner()
            .export
            .unwrap(),
        exported
    );
    let import = h::CreateServiceImportRequest {
        request_id: "import".into(),
        source_container_id: "worker".into(),
        source_container_instance_id: instance.into(),
        destination_device_id: "local".into(),
        destination_export_id: exported.export_id.clone(),
        virtual_service_name: "worker-api".into(),
        ..Default::default()
    };
    let imported = client
        .create_service_import(import.clone())
        .await
        .unwrap()
        .into_inner()
        .import
        .unwrap();
    assert_eq!(imported.destination_port, 8080);
    assert_eq!(imported.destination_device_id, "host-device");
    assert_eq!(
        client
            .create_service_import(import)
            .await
            .unwrap()
            .into_inner()
            .import
            .unwrap(),
        imported
    );
    let revoke = h::RevokeServiceExportRequest {
        request_id: "retire".into(),
        export_id: exported.export_id.clone(),
        expected_revision: exported.revision,
    };
    let mut stale = revoke.clone();
    stale.expected_revision += 1;
    assert_eq!(
        client
            .revoke_service_export(stale)
            .await
            .unwrap_err()
            .code(),
        Code::Aborted
    );
    client.revoke_service_export(revoke).await.unwrap();
    let exports = client
        .list_service_exports(h::ListServiceExportsRequest {})
        .await
        .unwrap()
        .into_inner()
        .exports
        .unwrap()
        .exports;
    let imports = client
        .list_service_imports(h::ListServiceImportsRequest {})
        .await
        .unwrap()
        .into_inner()
        .imports;
    assert!(
        exports
            .iter()
            .any(|e| e.export_id == exported.export_id && e.revoked && e.revision == 2)
    );
    assert!(
        imports
            .iter()
            .any(|i| i.import_id == imported.import_id && i.revoked && i.revision == 2)
    );
}
