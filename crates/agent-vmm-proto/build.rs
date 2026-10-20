fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = "../../api";
    let protos = [
        "../../api/appliance/v1/appliance.proto",
        "../../api/remote/v1/remote.proto",
        "../../api/host/v1/host.proto",
        "../../api/host/v1/host_share.proto",
        "../../api/trust/v1/trust.proto",
        "../../api/service/v1/service.proto",
        "../../api/vmmd/v1/vmmd.proto",
        "../../api/vmmd/v1/directory_sharing.proto",
    ];
    tonic_prost_build::configure()
        .extern_path(".google.rpc", "crate::google::rpc")
        // The RemoteController RPC is itself named `Connect`, so omit tonic's
        // transport convenience constructor while retaining typed RPC clients.
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .compile_protos(&protos, &[api])?;
    tonic_prost_build::configure()
        .compile_protos(&["../../api/google/rpc/status.proto"], &[api])?;
    println!("cargo:rerun-if-changed=../../api/google/rpc/status.proto");
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
