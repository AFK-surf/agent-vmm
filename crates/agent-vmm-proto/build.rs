mod generator;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = "../../api";
    let mut protos = Vec::new();
    for package in ["appliance", "host", "remote", "service", "trust"] {
        let directory = format!("{api}/{package}/v1");
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "proto")
            {
                protos.push(path);
            }
        }
        println!("cargo:rerun-if-changed={directory}");
    }
    protos.sort();
    let descriptor = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("agent-vmm.bin");
    prost_build::Config::new()
        .service_generator(Box::new(generator::Generator))
        .file_descriptor_set_path(descriptor)
        .extern_path(".google.rpc", "crate::google::rpc")
        .compile_protos(&protos, &[std::path::PathBuf::from(api)])?;
    prost_build::Config::new()
        .disable_comments([".google.rpc.Status"])
        .compile_protos(&["../../api/google/rpc/status.proto"], &[api])?;
    println!("cargo:rerun-if-changed=../../api/google/rpc/status.proto");
    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
