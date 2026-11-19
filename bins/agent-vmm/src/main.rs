//! `agent-vmm` lets the Host owner and a managing controller observe and change the local Host.
//!
//! - `inspect`, `diagnose`, `logs`, `operation`, `repair`, `update`, `vm`, `cleanup` and
//!   `host-socket` print one result envelope. A controller reads the envelope as JSON.
//! - `appliance` installs and operates a controller appliance, and `mcp token` issues MCP client tokens.
//! - On Linux, `lifecycle` is the lifecycle helper of the release that contains this program.

mod appliance;
mod appliance_lifecycle;
mod appliance_remove;
mod cli;
mod envelope;
mod facts;
mod flags;
mod helper;
mod host;
mod logs;
mod mcp;
mod mutation;
mod observe;
mod options;
mod prepare;
mod runtime;
#[cfg(test)]
mod tests;
mod update;

use std::io::Write;

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let code = match arguments.first().map(String::as_str) {
        Some("mcp") => mcp::run(&arguments[1..]),
        #[cfg(target_os = "linux")]
        Some("lifecycle") => vmm_lifecycle::main(arguments[1..].to_vec()),
        Some("appliance") => appliance::run(&arguments[1..]).unwrap_or_else(|| managed(&[])),
        _ => managed(&arguments),
    };
    std::process::exit(code)
}

fn managed(arguments: &[String]) -> i32 {
    let (mut stdout, mut stderr) = (std::io::stdout().lock(), std::io::stderr().lock());
    match host::Production::new() {
        Ok(runtime) => cli::run(arguments, &mut stdout, &mut stderr, &runtime),
        Err(error) => {
            let _ = writeln!(stderr, "agent-vmm: {error}");
            1
        }
    }
}
