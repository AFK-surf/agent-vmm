use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use tempfile::tempdir;
use vmm_core::{CapacityDimension, Error, Result};
use vmm_dom0::extent::{BLOCK_BYTES, BLOCK_SECTORS, Headroom, Pool};
use vmm_l4hctl::{DiskExtent, DiskInfo, DiskVolume, HypervisorCtl, MockHypervisorCtl, VmSpec};

struct Free(AtomicU64);
impl Headroom for Free {
    fn available_bytes(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

#[test]
fn discard_precedes_lowest_lba_reuse_across_restart() -> Result<()> {
    let temp = tempdir()?;
    let db = temp.path().join("state");
    let ctl = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 2,
        sectors: 20 * BLOCK_SECTORS,
        owned: false,
        failed: false,
    }])));
    let free = Arc::new(Free(AtomicU64::new(u64::MAX)));
    let open = || {
        Pool::open(
            &db,
            ctl.clone(),
            2,
            DiskExtent {
                sector: BLOCK_SECTORS,
                sectors: 16 * BLOCK_SECTORS,
            },
            free.clone(),
            BLOCK_BYTES,
        )
    };
    let pool = open()?;
    let first = pool.allocate("a", 1)?;
    pool.allocate("b", BLOCK_BYTES)?;
    let vm = ctl
        .lock()
        .unwrap()
        .create_vm(&VmSpec {
            name: "test",
            memory_bytes: 64 << 20,
            vcpus: 1,
            initrd: &[],
            cmdline: "",
            volumes: &[DiskVolume {
                disk: 2,
                read_only: true,
                volatile: false,
                extents: first.clone(),
            }],
        })
        .unwrap();
    assert!(matches!(pool.discard("a"), Err(Error::Conflict(_))));
    ctl.lock().unwrap().destroy_vm(vm).unwrap();
    assert_eq!(pool.discard("a")?, BLOCK_BYTES);
    assert_eq!(ctl.lock().unwrap().discards(), &[(2, first[0])]);
    drop(pool);
    let pool = Arc::new(open()?);
    let extents = pool.allocate("large", 2 * BLOCK_BYTES)?;
    assert_eq!(extents[0], first[0]);
    assert_eq!(extents[1].sector, 3 * BLOCK_SECTORS);
    let begin = Arc::new(Barrier::new(8));
    let runs = std::thread::scope(|s| {
        let mut handles = Vec::new();
        for i in 0..8 {
            let begin = begin.clone();
            let pool = pool.clone();
            handles.push(s.spawn(move || {
                begin.wait();
                pool.allocate(&format!("concurrent-{i}"), 1)
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap()[0].sector)
            .collect::<std::collections::BTreeSet<_>>()
    });
    assert_eq!(runs.len(), 8);
    free.0.store(BLOCK_BYTES, Ordering::SeqCst);
    assert!(matches!(
        pool.allocate("extra", 1),
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    let before = ctl.lock().unwrap().discards().len();
    assert!(
        pool.reconcile(&["unknown".into()].into_iter().collect())
            .is_err()
    );
    assert_eq!(ctl.lock().unwrap().discards().len(), before);
    drop(pool);
    std::fs::remove_file(&db)?;
    assert!(
        open().is_err(),
        "the durable pool marker must fence a lost database"
    );
    Ok(())
}

#[test]
fn pool_grows_monotonically_and_rejects_shrink() -> Result<()> {
    let temp = tempdir()?;
    let db = temp.path().join("state");
    let ctl = Arc::new(Mutex::new(MockHypervisorCtl::with_disks(vec![DiskInfo {
        id: 2,
        sectors: 40 * BLOCK_SECTORS,
        owned: false,
        failed: false,
    }])));
    let free = Arc::new(Free(AtomicU64::new(u64::MAX)));
    let pool = Pool::open(
        &db,
        ctl.clone(),
        2,
        DiskExtent {
            sector: BLOCK_SECTORS,
            sectors: 16 * BLOCK_SECTORS,
        },
        free.clone(),
        BLOCK_BYTES,
    )?;
    pool.allocate("existing", 16 * BLOCK_BYTES)?;
    assert!(matches!(
        pool.allocate("overflow", BLOCK_BYTES),
        Err(Error::Capacity(CapacityDimension::StorageHeadroom))
    ));
    drop(pool);

    // Monotonic grow succeeds and unlocks newly added sectors.
    let grown = Pool::open(
        &db,
        ctl.clone(),
        2,
        DiskExtent {
            sector: BLOCK_SECTORS,
            sectors: 32 * BLOCK_SECTORS,
        },
        free.clone(),
        BLOCK_BYTES,
    )?;
    let ext = grown.allocate("after-grow", 16 * BLOCK_BYTES)?;
    assert_eq!(ext.len(), 1);
    assert_eq!(ext[0].sector, 17 * BLOCK_SECTORS);
    drop(grown);

    // Shrinking back must be rejected as a geometry conflict.
    let shrunk = Pool::open(
        &db,
        ctl,
        2,
        DiskExtent {
            sector: BLOCK_SECTORS,
            sectors: 16 * BLOCK_SECTORS,
        },
        free,
        BLOCK_BYTES,
    );
    assert!(matches!(shrunk, Err(Error::Conflict(_))));
    Ok(())
}
