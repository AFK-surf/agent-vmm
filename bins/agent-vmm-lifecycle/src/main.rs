//! The lifecycle helper of the Host bundle, at `Contents/Helpers/agent-vmm-lifecycle`.

fn main() {
    vmm_lifecycle::main(std::env::args().skip(1).collect())
}
