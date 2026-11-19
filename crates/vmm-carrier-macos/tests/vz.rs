#![cfg(target_os = "macos")]
use vmm_carrier_macos::{bundle::Bundle, vz::Settings};
#[test]
#[ignore = "requires a codesigned test binary with the virtualization entitlement"]
fn validated_carrier_configuration() -> std::io::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = |name| dir.path().join(name);
    for name in ["Image", "initramfs", "rootfs", "serial.log", "pool.raw"] {
        std::fs::File::create(path(name))?.set_len(4096)?;
    }
    Settings {
        bundle: Bundle {
            kernel: path("Image"),
            initramfs: path("initramfs"),
            rootfs: Some(path("rootfs")),
        },
        pool: path("pool.raw"),
        serial_log: path("serial.log"),
        cpus: 2,
        memory: 1 << 30,
    }
    .validate()
}
