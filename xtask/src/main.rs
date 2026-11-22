mod buildinfo;
mod notices;
mod oci;

use anyhow::{Result, bail};

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("oci") => oci::run(&args.collect::<Vec<_>>()),
        Some("notices") => notices::run(args),
        _ => bail!("usage: cargo xtask {{oci|notices}} ..."),
    }
}
