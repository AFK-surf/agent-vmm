//! Host-local ownership of durable data and guest transport selection.
use crate::{socket::private_directory, storage::open_database};
use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};
use vmm_compat::{
    Containers, RuntimeDriver,
    guest::GuestDriver,
    image::{ImageImports, ImageStaging},
    runtime::{ImageRuntime, MeshRuntime, RuntimeService},
    workspace::{WorkspaceDriver, WorkspaceService},
};
use vmm_core::{Core, Store, backend::Dom0Backend};

pub struct HostState {
    pub store: Arc<Store>,
    compat_path: PathBuf,
    staging: PathBuf,
    pub state_size: u64,
}

pub struct Drivers {
    pub backend: Arc<dyn Dom0Backend>,
    pub runtime: Arc<dyn RuntimeDriver>,
    pub guest: Arc<dyn GuestDriver>,
    pub workspace: Arc<dyn WorkspaceDriver>,
    pub staging: Arc<dyn ImageStaging>,
    pub images: Arc<dyn ImageRuntime>,
    pub mesh: Arc<dyn MeshRuntime>,
}

impl HostState {
    pub fn open(database: &Path) -> Result<Self, Box<dyn Error>> {
        let directory = database
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let compat_path = directory.join(
            if database
                .file_name()
                .is_some_and(|name| name == "compat.sqlite")
            {
                "host-compat.sqlite"
            } else {
                "compat.sqlite"
            },
        );
        let store = Arc::new(open_database(database)?);
        let staging = directory.join("image-staging");
        private_directory(&staging)?;
        Ok(Self {
            store,
            compat_path,
            staging,
            state_size: 64 << 30,
        })
    }

    pub fn local_operator(
        &self,
        instance: &Path,
        backend: Option<Arc<vmm_dom0::backend::LocalDom0Backend>>,
    ) -> Result<vmm_compat::operator::LocalOperator, Box<dyn Error>> {
        self.operator_parts(instance, backend).map(|(api, _)| api)
    }

    pub(crate) fn operator_parts(
        &self,
        instance: &Path,
        backend: Option<Arc<vmm_dom0::backend::LocalDom0Backend>>,
    ) -> Result<
        (
            vmm_compat::operator::LocalOperator,
            Arc<crate::registrations::ExternalRegistrations>,
        ),
        Box<dyn Error>,
    > {
        let secrets = crate::secrets::Secrets::open(instance)?;
        let registrations = Arc::new(crate::registrations::ExternalRegistrations::open(
            &self.compat_path,
            self.store.clone(),
            secrets.credentials.clone(),
            self.state_size.saturating_sub(13 << 30),
        )?);
        let driver = backend.clone().map(|b| b as Arc<dyn Dom0Backend>);
        let mut api = vmm_compat::operator::LocalOperator::open(
            &self.compat_path,
            self.store.clone(),
            registrations.clone(),
            driver,
        )?;
        api.management = Some(registrations.clone());
        api.trust = Some(Arc::new(crate::identity::HostIdentity::open(
            &self.compat_path,
            secrets.roots,
            registrations.clone(),
        )?));
        let mcp = Arc::new(vmm_compat::operator::McpManagement::open(
            &self.compat_path,
            secrets.tokens,
        )?);
        api.appliances = Some(vmm_compat::operator::ApplianceOperations::open(
            mcp.clone(),
            &self.compat_path,
            &self.compat_path.with_file_name("appliance-artifacts"),
            self.store.clone(),
            secrets.credentials,
            self.state_size.saturating_sub(13 << 30),
        )?);
        api.mcp = Some(mcp);
        if let Some(backend) = backend {
            self.attach_guest(
                &mut api,
                Arc::new(crate::workspace::Dom0Workspaces(backend.clone())),
            )?;
            api = api.with_disk_sample(move || {
                let (used, capacity, import_reservations) = backend.disk_accounting()?;
                Ok(vmm_compat::operator::DiskSample {
                    used,
                    capacity,
                    import_reservations,
                })
            });
        }
        Ok((api, registrations))
    }

    pub fn attach_guest(
        &self,
        api: &mut vmm_compat::operator::LocalOperator,
        guest: Arc<dyn GuestDriver>,
    ) -> Result<(), Box<dyn Error>> {
        api.containers = Some(Containers::open(
            &self.compat_path,
            Arc::new(crate::workload::Dom0Workloads::open(
                guest.clone(),
                &self.compat_path,
            )?),
        )?);
        api.guest = Some(guest);
        Ok(())
    }

    pub(crate) fn online_runtime<T>(
        &self,
        core: Core,
        api: &vmm_compat::operator::LocalOperator,
        backend: Arc<T>,
        staging: Arc<dyn ImageStaging>,
    ) -> Result<(Arc<RuntimeService>, vmm_compat::image::build::Builds), Box<dyn Error>>
    where
        T: GuestDriver + WorkspaceDriver + vmm_compat::image::build::BuildBackend + 'static,
    {
        let containers = api.containers.clone().ok_or("missing container runtime")?;
        let imports = ImageImports {
            directory: self.staging.clone(),
            staging,
        };
        let builds = vmm_compat::image::build::Builds::new(
            backend.clone(),
            containers.clone(),
            imports.clone(),
        )?;
        let device_id = api
            .trust
            .as_ref()
            .ok_or("missing device trust")?
            .identity()?
            .identity
            .ok_or("missing device identity")?
            .device_id;
        let service = RuntimeService {
            core,
            containers,
            workspace: WorkspaceService::new(backend.clone()),
            guest: backend,
            imports,
            images: Arc::new(builds.clone()),
            mesh: Arc::new(vmm_compat::mesh::ServiceMesh { device_id }),
        };
        Ok((Arc::new(service), builds))
    }

    pub async fn attach(
        &self,
        backend: Arc<dyn Dom0Backend>,
    ) -> vmm_core::Result<(Core, vmm_core::Reconciliation)> {
        let core = Core::new(self.store.clone(), backend);
        let report = core.reconcile_after_restart().await?;
        Ok((core, report))
    }

    pub async fn runtime(
        self,
        drivers: Drivers,
    ) -> Result<(RuntimeService, vmm_core::Reconciliation), tonic::Status> {
        let (core, report) = self
            .attach(drivers.backend)
            .await
            .map_err(vmm_compat::error::core_status)?;
        let runtime = RuntimeService {
            core,
            containers: Containers::open(&self.compat_path, drivers.runtime)?,
            workspace: WorkspaceService::new(drivers.workspace),
            imports: ImageImports {
                directory: self.staging,
                staging: drivers.staging,
            },
            images: drivers.images,
            mesh: drivers.mesh,
            guest: drivers.guest,
        };
        Ok((runtime, report))
    }
}
