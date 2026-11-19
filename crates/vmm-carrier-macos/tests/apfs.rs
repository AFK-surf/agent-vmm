#![cfg(target_os = "macos")]
use sha2::{Digest, Sha256};
use std::{io, os::unix::fs::FileExt};
use vmm_carrier_macos::sparse::*;

#[test]
fn punch_releases_blocks_and_preserves_adjacent_data() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let file = open_pool(&dir.path().join("pool.raw"), 4 << 30)?;
    assert_eq!(allocated_bytes(&file)?, 0);
    assert_eq!(hole_bytes(&file, 4 << 30)?, 4 << 30);
    let mut chunk = vec![0; 1 << 20];
    let mut expected = Sha256::new();
    for mb in 0..64 {
        getrandom::getrandom(&mut chunk).unwrap();
        file.write_all_at(&chunk, mb << 20)?;
        if !(8..56).contains(&mb) {
            expected.update(&chunk);
        }
    }
    file.sync_all()?;
    let before = allocated_bytes(&file)?;
    assert!(before >= 64 << 20);
    punch_hole(&file, 8 << 20, 48 << 20)?;
    file.sync_all()?;
    let after = allocated_bytes(&file)?;
    assert!(after <= 20 << 20, "{after}");
    let mut actual = Sha256::new();
    for mb in 0..64 {
        file.read_exact_at(&mut chunk, mb << 20)?;
        if (8..56).contains(&mb) {
            assert!(chunk.iter().all(|b| *b == 0));
        } else {
            actual.update(&chunk);
        }
    }
    assert_eq!(actual.finalize(), expected.finalize());
    println!(
        "allocated bytes: initial=0 written={before} punched={after}; 16 MiB SHA-256 verified"
    );
    Ok(())
}
