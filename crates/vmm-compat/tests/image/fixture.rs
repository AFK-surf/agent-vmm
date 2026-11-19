use agent_vmm_proto::host::v1::ImportImageHeader;
use serde_json::json;
use sha2::{Digest, Sha256};
pub(super) fn sha(data: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(data)))
}
fn blob_path(hash: &str) -> String {
    format!("blobs/sha256/{}", hash.strip_prefix("sha256:").unwrap())
}
fn member(tar: &mut tar::Builder<Vec<u8>>, path: &str, bytes: &[u8]) {
    let mut hdr = tar::Header::new_gnu();
    hdr.set_size(bytes.len() as u64);
    hdr.set_mode(0o600);
    hdr.set_cksum();
    tar.append_data(&mut hdr, path, bytes).unwrap();
}
pub fn fixture(layer: &[u8], manifest_platform: &str) -> (Vec<u8>, ImportImageHeader) {
    let config = br#"{"os":"linux","architecture":"arm64"}"#;
    let expected_layer = b"expected";
    let manifest_type = "application/vnd.oci.image.manifest.v1+json";
    let manifest=json!({"schemaVersion":2,"mediaType":manifest_type,
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":config.len(),"digest":sha(config)},
        "layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","size":expected_layer.len(),"digest":sha(expected_layer)}]}).to_string();
    let digest = sha(manifest.as_bytes());
    let index=json!({"schemaVersion":2,"manifests":[{"mediaType":manifest_type, "size":manifest.len(),"digest":digest,
        "platform":{"os":"linux","architecture":manifest_platform}}]}).to_string();
    let mut tar = tar::Builder::new(vec![]);
    member(&mut tar, "oci-layout", br#"{"imageLayoutVersion":"1.0.0"}"#);
    member(&mut tar, "index.json", index.as_bytes());
    member(&mut tar, &blob_path(&digest), manifest.as_bytes());
    member(&mut tar, &blob_path(&sha(config)), config);
    member(&mut tar, &blob_path(&sha(expected_layer)), layer);
    let bytes = tar.into_inner().unwrap();
    let header = ImportImageHeader {
        request_id: "req".into(),
        image_reference: "example.com/test:latest".into(),
        archive_url: String::new(),
        archive_sha256: Sha256::digest(&bytes).to_vec(),
        archive_size: bytes.len() as u64,
        manifest_digest: digest,
        platform: "linux/arm64".into(),
    };
    (bytes, header)
}
