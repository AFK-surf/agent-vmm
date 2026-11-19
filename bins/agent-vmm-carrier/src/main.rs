#![forbid(unsafe_code)]

#[cfg(not(target_os = "macos"))]
fn main() -> std::process::ExitCode {
    eprintln!("agent-vmm-carrier runs on macOS only");
    std::process::ExitCode::FAILURE
}

#[cfg(target_os = "macos")]
fn main() -> std::io::Result<()> {
    vmm_carrier_macos::daemon::run()
}
