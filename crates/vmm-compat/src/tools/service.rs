use super::*;
use agent_vmm_proto::{service::v1 as s, trust::v1 as t};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use prost::Message;

#[derive(Deserialize)]
struct Export {
    container_id: String,
    container_instance_id: String,
    port: u32,
    protocol: String,
}
#[derive(Deserialize)]
struct Revoke {
    kind: String,
    id: String,
    expected_revision: u64,
}
#[derive(Deserialize)]
struct Import {
    source_container_id: String,
    source_container_instance_id: String,
    destination_device_id: String,
    destination_export_id: String,
    virtual_service_name: String,
    #[serde(default)]
    destination_port: u32,
    #[serde(default)]
    mesh_id: String,
    #[serde(default)]
    capability: String,
    #[serde(default)]
    connection_limit: u32,
    #[serde(default)]
    byte_limit: u64,
    #[serde(default)]
    concurrency_limit: u32,
}
fn export(e: s::ServiceExport) -> Value {
    json!({"id":e.export_id,"container_id":e.container_id,"container_instance_id":e.container_instance_id,"port":e.port,"protocol":e.protocol().as_str_name().trim_start_matches("SERVICE_PROTOCOL_").to_ascii_lowercase(),"revision":e.revision,"revoked":e.revoked})
}
fn import(i: s::ServiceImport) -> Value {
    let mut value = json!({"id":i.import_id,"source_container_id":i.source_container_id,"source_container_instance_id":i.source_container_instance_id,
        "destination_device_id":i.destination_device_id,"destination_export_id":i.destination_export_id,"virtual_service_name":i.virtual_service_name,"revision":i.revision,"revoked":i.revoked});
    if i.destination_port != 0 {
        value["destination_port"] = i.destination_port.into();
    }
    value
}
impl Calls<'_> {
    pub(super) async fn service(&self, name: &str, args: Value) -> Result<Value, Status> {
        match name {
            "mesh_service_list" => {
                let exports = self
                    .runtime
                    .list_service_exports(self.request(h::ListServiceExportsRequest {})?)
                    .await?
                    .into_inner()
                    .exports
                    .unwrap_or_default()
                    .exports;
                let imports = self
                    .runtime
                    .list_service_imports(self.request(h::ListServiceImportsRequest {})?)
                    .await?
                    .into_inner()
                    .imports;
                Ok(
                    json!({"exports":exports.into_iter().map(export).collect::<Vec<_>>(),"imports":imports.into_iter().map(import).collect::<Vec<_>>() }),
                )
            }
            "service_export" => {
                let i: Export = parse(args)?;
                let protocol = match i.protocol.as_str() {
                    "tcp" => s::ServiceProtocol::Tcp,
                    "http" => s::ServiceProtocol::Http,
                    _ => return Err(Status::invalid_argument("invalid service protocol")),
                };
                let response = self
                    .runtime
                    .create_service_export(self.request(h::CreateServiceExportRequest {
                        request_id: request_id(),
                        container_id: i.container_id,
                        container_instance_id: i.container_instance_id,
                        port: i.port,
                        protocol: protocol as i32,
                    })?)
                    .await?;
                Ok(export(response.into_inner().export.ok_or_else(missing)?))
            }
            "service_route_revoke" => {
                let i: Revoke = parse(args)?;
                match i.kind.as_str() {
                    "export" => {
                        self.runtime
                            .revoke_service_export(self.request(h::RevokeServiceExportRequest {
                                request_id: request_id(),
                                export_id: i.id,
                                expected_revision: i.expected_revision,
                            })?)
                            .await?;
                    }
                    "import" => {
                        self.runtime
                            .revoke_service_import(self.request(h::RevokeServiceImportRequest {
                                request_id: request_id(),
                                import_id: i.id,
                                expected_revision: i.expected_revision,
                            })?)
                            .await?;
                    }
                    _ => {
                        return Err(Status::invalid_argument(
                            "route kind must be export or import",
                        ));
                    }
                }
                Ok(json!({}))
            }
            "service_import" => self.import_service(parse(args)?).await,
            _ => Err(Status::invalid_argument("unknown service tool")),
        }
    }
    async fn import_service(&self, i: Import) -> Result<Value, Status> {
        let local = i.destination_device_id == "local";
        let value = if !i.mesh_id.is_empty() {
            if local
                || !i.capability.is_empty()
                || i.connection_limit == 0
                || i.byte_limit == 0
                || i.concurrency_limit == 0
            {
                return Err(Status::invalid_argument(
                    "personal import requires a remote device and all three budgets",
                ));
            }
            let request = h::CreatePersonalServiceImportRequest {
                request_id: request_id(),
                mesh_id: i.mesh_id,
                source_container_id: i.source_container_id,
                source_container_instance_id: i.source_container_instance_id,
                destination_device_id: i.destination_device_id,
                destination_export_id: i.destination_export_id,
                virtual_service_name: i.virtual_service_name,
                budget: Some(t::RouteBudget {
                    connection_limit: i.connection_limit,
                    byte_limit: i.byte_limit,
                    concurrency_limit: i.concurrency_limit,
                }),
            };
            self.runtime
                .create_personal_service_import(self.request(request)?)
                .await?
                .into_inner()
                .import
        } else {
            if (local && i.destination_port != 0)
                || (!local
                    && (i.capability.is_empty() || !(1..=65535).contains(&i.destination_port)))
            {
                return Err(Status::invalid_argument(
                    "invalid destination port or capability",
                ));
            }
            let capability = if i.capability.is_empty() {
                None
            } else {
                let malformed = || Status::invalid_argument("route capability is malformed");
                let bytes = URL_SAFE_NO_PAD
                    .decode(i.capability)
                    .map_err(|_| malformed())?;
                Some(t::RouteCapability::decode(bytes.as_slice()).map_err(|_| malformed())?)
            };
            let request = h::CreateServiceImportRequest {
                request_id: request_id(),
                source_container_id: i.source_container_id,
                source_container_instance_id: i.source_container_instance_id,
                destination_device_id: i.destination_device_id,
                destination_export_id: i.destination_export_id,
                virtual_service_name: i.virtual_service_name,
                destination_port: i.destination_port,
                capability,
            };
            self.runtime
                .create_service_import(self.request(request)?)
                .await?
                .into_inner()
                .import
        };
        Ok(import(value.ok_or_else(missing)?))
    }
}
