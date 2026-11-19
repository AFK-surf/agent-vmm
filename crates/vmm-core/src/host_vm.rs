//! Operator admission for a native dom0 hosting multiple independent workload VMs.
use crate::{Core, Error, Result, Store};
use uuid::Uuid;

pub(crate) const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS local_vm_admission(singleton INTEGER PRIMARY KEY CHECK(singleton=1),state TEXT NOT NULL,boot_id TEXT NOT NULL)";
#[derive(Debug)]
pub struct HostVmObservation {
    pub state: String,
    pub boot_id: String,
}

impl Store {
    pub fn host_vm_observation(&self) -> Result<HostVmObservation> {
        self.read(|db| {
            Ok(db.query_row(
                "SELECT state,boot_id FROM local_vm_admission WHERE singleton=1",
                [],
                |row| {
                    Ok(HostVmObservation {
                        state: row.get(0)?,
                        boot_id: row.get(1)?,
                    })
                },
            )?)
        })
    }
}
impl Core {
    pub async fn start_local_host(&self) -> Result<()> {
        let _effects = self.store.effects.lock().await;
        self.backend.list_live_vms().await?;
        self.store.write(|db|{
            db.execute("UPDATE local_vm_admission SET state='running',boot_id=? WHERE singleton=1 AND state!='running'",[Uuid::new_v4().to_string()])?;
            Ok(())
        })
    }
    pub async fn stop_local_host(&self, force: bool) -> Result<()> {
        let _effects = self.store.effects.lock().await;
        self.store.write(|db|{
            db.execute("UPDATE local_vm_admission SET state='stopping' WHERE singleton=1 AND state!='stopped'",[])?;
            Ok(())
        })?;
        let observed = self.backend.list_live_vms().await?;
        // Verify every boot identity before the first effect. A live VM with no
        // matching durable owner cannot safely be stopped using operator authority.
        for vm in &observed {
            let boot = &vm.incarnation.boot;
            let sb = self.store.sandbox(&boot.sandbox)?;
            if sb.boot.as_ref() != Some(boot)
                || sb.vm.as_ref().is_some_and(|known| known != &vm.incarnation)
            {
                return Err(Error::Fenced);
            }
        }
        for vm in observed {
            self.stop_locked(&vm.incarnation.boot, force).await?;
        }
        let remaining = self.reconcile_locked().await?;
        if !remaining.adopted.is_empty()
            || !remaining.unowned.is_empty()
            || !remaining.mismatched.is_empty()
        {
            return Err(Error::Fenced);
        }
        self.store.write(|db| {
            db.execute(
                "UPDATE local_vm_admission SET state='stopped',boot_id='' WHERE singleton=1",
                [],
            )?;
            Ok(())
        })
    }
}
