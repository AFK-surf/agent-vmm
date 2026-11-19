// This suite exercises guest lifecycle and workspace RPCs. Any call into the
// registry or service mesh during those RPCs is a test failure.
use agent_vmm_proto::host::v1 as h;
use vmm_rpc::{Status, Streaming};
use vmm_compat::{
    RuntimeContext,
    guest::Output,
    runtime::{ImageRuntime, MeshRuntime},
};
pub struct Unused;

macro_rules! unused_driver {
    ($driver:ident { $($name:ident($req:ty) -> $out:ty),+ $(,)? }) => {
        #[vmm_rpc::async_trait]
        impl $driver for Unused {
            $(async fn $name(&self, _: &RuntimeContext, _: $req) -> Result<$out, Status> {
                panic!("unexpected call to {}", stringify!($name))
            })+
        }
    }
}
unused_driver! { ImageRuntime {
    build_image(Streaming<h::BuildImageRequest>) -> Output<h::BuildImageResponse>,
    observe_image_operation(h::ObserveImageOperationRequest) -> h::ObserveImageOperationResponse,
    cancel_image_operation(h::CancelImageOperationRequest) -> h::CancelImageOperationResponse,
}}
unused_driver! { MeshRuntime {
    create_service_export(h::CreateServiceExportRequest) -> h::CreateServiceExportResponse,
    list_service_exports(h::ListServiceExportsRequest) -> h::ListServiceExportsResponse,
    revoke_service_export(h::RevokeServiceExportRequest) -> h::RevokeServiceExportResponse,
    create_service_import(h::CreateServiceImportRequest) -> h::CreateServiceImportResponse,
    create_personal_service_import(h::CreatePersonalServiceImportRequest) -> h::CreatePersonalServiceImportResponse,
    list_service_imports(h::ListServiceImportsRequest) -> h::ListServiceImportsResponse,
    revoke_service_import(h::RevokeServiceImportRequest) -> h::RevokeServiceImportResponse,
}}

impl vmm_compat::operator::RegistrationControl for Unused {
    fn metadata(&self, _: &str) -> Result<(u64, String), vmm_rpc::Status> {
        unreachable!()
    }
    fn isolate(
        &self,
        _: &str,
        _: u64,
        _: &mut dyn FnMut() -> Result<(), vmm_rpc::Status>,
    ) -> Result<(), vmm_rpc::Status> {
        unreachable!()
    }
    fn delete_credential(&self, _: &str) -> Result<(), vmm_rpc::Status> {
        unreachable!()
    }
}
