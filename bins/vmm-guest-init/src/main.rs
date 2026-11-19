#[cfg(target_os = "linux")]
fn main() {
    if std::process::id() != 1 {
        eprintln!("vmm-guest-init runs only as PID 1 of a workload VM");
        std::process::exit(1);
    }
    vmm_guest_init::pid1::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("vmm-guest-init runs only on Linux");
    std::process::exit(1);
}
