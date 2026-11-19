#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> vmm_dom0::local::Result<()> {
    linux::run().await
}
#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("l4h-dom0d requires Linux dom0");
    std::process::ExitCode::FAILURE
}
