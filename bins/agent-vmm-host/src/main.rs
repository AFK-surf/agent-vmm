fn main() {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    std::process::exit(agent_vmm_host::main_from_args(&arguments));
}
