fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api = "../../api";
    let protos = [
        "../../api/remote/v1/remote.proto",
        "../../api/host/v1/host.proto",
        "../../api/trust/v1/trust.proto",
        "../../api/service/v1/service.proto",
    ];
    tonic_prost_build::configure()
        // The RemoteController RPC is itself named `Connect`, so omit tonic's
        // transport convenience constructor while retaining typed RPC clients.
        .build_client(true)
        .build_server(true)
        .build_transport(false)
        .compile_protos(&protos, &[api])?;
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    Ok(())
}
