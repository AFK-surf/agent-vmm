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
        })
    }

    pub fn local_operator(
        &self,
        instance: &Path,
        backend: Option<Arc<vmm_dom0::backend::LocalDom0Backend>>,
    ) -> Result<vmm_compat::operator::LocalOperator, Box<dyn Error>> {
        let secrets = crate::secrets::Secrets::open(instance)?;
        let registrations = Arc::new(crate::registrations::ExternalRegistrations::open(
            &self.compat_path,
            self.store.clone(),
            secrets.credentials,
        )?);
        let driver = backend.clone().map(|b| b as Arc<dyn Dom0Backend>);
        let mut api = vmm_compat::operator::LocalOperator::open(
            &self.compat_path,
            self.store.clone(),
            registrations,
            driver,
        )?;
        api.mcp = Some(Arc::new(vmm_compat::operator::McpManagement::open(
            &self.compat_path,
            secrets.tokens,
        )?));
        if let Some(backend) = backend {
            api = api.with_disk_sample(move || {
                let (used, capacity, import_reservations) = backend.disk_accounting()?;
                Ok(vmm_compat::operator::DiskSample {
                    used,
                    capacity,
                    import_reservations,
                })
            });
        }
        Ok(api)
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
