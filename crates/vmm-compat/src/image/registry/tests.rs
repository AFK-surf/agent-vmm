use super::*;
use std::collections::HashMap;

struct Registry(HashMap<String, Vec<u8>>);

impl RegistrySource for Registry {
    async fn get(&mut self, id: &str, _: bool) -> Result<reqwest::Response, Status> {
        let bytes = self.0.get(id).expect("unexpected registry request").clone();
        Ok(tonic::codegen::http::Response::new(bytes).into())
    }
}

#[tokio::test]
async fn pull_selects_arm64_v8_and_authenticates_the_index_manifest_and_layers() {
    let dir = tempfile::tempdir().unwrap();
    let layer = b"layer";
    let cfg = br#"{"os":"linux","architecture":"arm64","variant":"v8","config":{"User":"1000"}}"#;
    let manifest = json!({"schemaVersion":2,"mediaType":DOCKER_MANIFEST,
        "config":{"mediaType":"application/vnd.docker.container.image.v1+json","digest":hash(cfg),"size":cfg.len()},
        "layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar","digest":hash(layer),"size":layer.len()}]}).to_string();
    let index = json!({"schemaVersion":2,"mediaType":DOCKER_INDEX,"manifests":[
        {"platform":{"os":"linux","architecture":"amd64"}},
        {"digest":hash(manifest.as_bytes()),"size":manifest.len(),"platform":{"os":"linux","architecture":"arm64","variant":"v8"}}
    ]}).to_string();
    let root = hash(index.as_bytes());
    let reference = format!("registry.example/my/image@{root}").parse().unwrap();
    let mut registry = Registry(HashMap::from([
        (root.clone(), index.into_bytes()),
        (hash(manifest.as_bytes()), manifest.into_bytes()),
        (hash(cfg), cfg.to_vec()),
        (hash(layer), layer.to_vec()),
    ]));
    let output = export_inner(dir.path(), &reference, &mut registry)
        .await
        .unwrap();
    assert_eq!(output.header().image_reference, reference.whole());
    assert!(output.logical_bytes > layer.len() as u64);
    drop(output);
    registry.0.insert(hash(layer), b"other".to_vec());
    assert!(
        export_inner(dir.path(), &reference, &mut registry)
            .await
            .is_err()
    );
    registry.0.insert(root, b"{}".to_vec());
    assert!(
        export_inner(dir.path(), &reference, &mut registry)
            .await
            .is_err()
    );
    assert_eq!(dir.path().read_dir().unwrap().count(), 0);
}

#[tokio::test]
#[ignore = "downloads an anonymous registry image over public HTTPS"]
async fn public_registry_pull() {
    let dir = tempfile::tempdir().unwrap();
    let reference: Reference = "busybox:1.37.0".parse().unwrap();
    let archive = export(dir.path(), &reference).await.unwrap();
    assert_eq!(
        archive.header().image_reference,
        "docker.io/library/busybox:1.37.0"
    );
    assert!(archive.logical_bytes > 0);
    assert!(archive.path().is_file());
}

#[tokio::test]
async fn private_registry_is_rejected_before_connecting() {
    use prost::Message;
    let dir = tempfile::tempdir().unwrap();
    let reference: Reference = "127.0.0.1:4567/library/image:latest".parse().unwrap();
    let status = export(dir.path(), &reference).await.err().unwrap();
    let status = with_image(status, &reference.whole());
    assert_eq!(status.code(), Code::Unavailable);
    let details = agent_vmm_proto::google::rpc::Status::decode(status.details())
        .unwrap()
        .details;
    let info = tonic_types::pb::ErrorInfo::decode(details[0].value.as_slice()).unwrap();
    assert_eq!(info.reason, "IMAGE_REGISTRY_UNAVAILABLE");
    assert_eq!(info.domain, "agent-vmm");
    let resource = tonic_types::pb::ResourceInfo::decode(details[1].value.as_slice()).unwrap();
    assert_eq!(resource.resource_name, reference.whole());
    assert_eq!(dir.path().read_dir().unwrap().count(), 0);
}
