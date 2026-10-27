fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = "../../api";
    let mut protos = Vec::new();
    for package in ["appliance", "host", "remote", "service", "trust"] {
        let directory = format!("{api}/{package}/v1");
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "proto") {
                protos.push(path);
            }
        }
        println!("cargo:rerun-if-changed={directory}");
    }
    protos.sort();
    tonic_prost_build::configure()
        .extern_path(".google.rpc", "crate::google::rpc")
        // The RemoteController RPC is itself named `Connect`, so omit tonic's
        // transport convenience constructor while retaining typed RPC clients.
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .compile_protos(&protos, &[std::path::PathBuf::from(api)])?;
    tonic_prost_build::configure()
        .compile_protos(&["../../api/google/rpc/status.proto"], &[api])?;
    println!("cargo:rerun-if-changed=../../api/google/rpc/status.proto");
    for proto in protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }
    Ok(())
}
