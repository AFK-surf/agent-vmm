//! agent-vmm-service-executor is an administrator-invoked, one-shot launchd
//! adapter for the system service mode. It never runs the service programs and
//! accepts no path, template, environment variable, label or numeric identity:
//! every value comes from the protected owner file and from fixed constants.

#[cfg(target_os = "macos")]
const HOST_BUNDLE_IDENTIFIER: &str = "com.afk-surf.agent-vmm-host";

#[cfg(target_os = "macos")]
mod config;
#[cfg(target_os = "macos")]
mod executor;
#[cfg(target_os = "macos")]
#[path = "../../../crates/vmm-lifecycle/src/platform/darwin/plist.rs"]
mod plist;
#[cfg(target_os = "macos")]
mod system;

#[cfg(target_os = "macos")]
fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if let Err(message) = executor::run(&system::Production, &arguments) {
        eprintln!("service executor failed: {message}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("agent-vmm-service-executor manages launchd system jobs; it runs on macOS only");
    std::process::exit(1);
}
