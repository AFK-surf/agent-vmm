use super::*;
use sha2::Sha256;
use std::{
    io::{Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, PermissionsExt},
};
use tar::{Builder, EntryType, Header};

fn add(tar: &mut Builder<Vec<u8>>, name: &str, data: &[u8], kind: EntryType, target: Option<&str>) {
    let mut h = Header::new_gnu();
    h.set_entry_type(kind);
    h.set_mode(0o751);
    h.set_uid(123);
    h.set_gid(456);
    h.set_mtime(9);
    h.set_size(data.len() as u64);
    if let Some(target) = target {
        tar.append_link(&mut h, name, target).unwrap();
    } else {
        tar.append_data(&mut h, name, data).unwrap();
    }
}
fn file(t: &mut Builder<Vec<u8>>, name: &str, b: &[u8]) {
    add(t, name, b, EntryType::Regular, None)
}
fn descriptor(data: &[u8]) -> serde_json::Value {
    serde_json::json!({"digest":format!("sha256:{}",hex::encode(Sha256::digest(data))),"size":data.len()})
}
fn blob(t: &mut Builder<Vec<u8>>, data: &[u8]) -> serde_json::Value {
    file(
        t,
        &format!("blobs/sha256/{}", hex::encode(Sha256::digest(data))),
        data,
    );
    descriptor(data)
}
pub(crate) fn fixture(docker: bool) -> (Vec<u8>, Digest) {
    let mut lower = Builder::new(Vec::new());
    file(&mut lower, "gone", b"lower");
    file(&mut lower, "opaque/old", b"old");
    file(&mut lower, "replaced", b"old");
    let mut upper = Builder::new(Vec::new());
    file(&mut upper, "opaque/new", b"new");
    file(&mut upper, "opaque/.wh..wh..opq", b"");
    file(&mut upper, ".wh.gone", b"");
    file(&mut upper, "replaced", b"new");
    add(&mut upper, "hard", b"", EntryType::Link, Some("replaced"));
    add(&mut upper, "sym", b"", EntryType::Symlink, Some("replaced"));
    let layers = [lower.into_inner().unwrap(), upper.into_inner().unwrap()];
    let diffs = layers
        .iter()
        .map(|l| descriptor(l)["digest"].clone())
        .collect::<Vec<_>>();
    let config=serde_json::to_vec(&serde_json::json!({"config":{"Entrypoint":["/bin/sh"],"Cmd":["run"],"Env":["A=B"],"WorkingDir":"/app","User":"123:456"},"rootfs":{"diff_ids":diffs}})).unwrap();
    let mut outer = Builder::new(Vec::new());
    let digest;
    if docker {
        digest = Sha256::digest(&config).into();
        file(&mut outer, "config.json", &config);
        for (i, layer) in layers.iter().enumerate() {
            file(&mut outer, &format!("layer{i}.tar"), layer);
        }
        file(
            &mut outer,
            "manifest.json",
            br#"[{"Config":"config.json","Layers":["layer0.tar","layer1.tar"]}]"#,
        );
    } else {
        let config = blob(&mut outer, &config);
        let layers = layers
            .iter()
            .map(|l| blob(&mut outer, l))
            .collect::<Vec<_>>();
        let manifest =
            serde_json::to_vec(&serde_json::json!({"config":config,"layers":layers})).unwrap();
        digest = Sha256::digest(&manifest).into();
        let descriptor = blob(&mut outer, &manifest);
        file(
            &mut outer,
            "index.json",
            &serde_json::to_vec(&serde_json::json!({"manifests":[descriptor]})).unwrap(),
        );
        file(
            &mut outer,
            "oci-layout",
            br#"{"imageLayoutVersion":"1.0.0"}"#,
        );
    }
    (outer.into_inner().unwrap(), digest)
}
#[test]
fn layered_archives_preserve_identity_metadata_and_links() -> Result<()> {
    for docker in [false, true] {
        let temp = tempfile::tempdir()?;
        let (archive, digest) = fixture(docker);
        let archive_file = temp.path().join("image.tar");
        std::fs::write(&archive_file, archive)?;
        for formatter in [temp.path().join("unavailable"), PathBuf::from("mkfs.erofs")] {
            let mut built = build_with(
                &archive_file,
                Some(digest),
                temp.path(),
                Some(&formatter),
                false,
            )?;
            assert_eq!(built.defaults.entrypoint.unwrap(), ["/bin/sh"]);
            assert_eq!(built.defaults.cmd.unwrap(), ["run"]);
            assert_eq!(built.defaults.env.unwrap(), ["A=B"]);
            assert_eq!(built.defaults.working_dir.as_deref(), Some("/app"));
            assert_eq!(built.defaults.user.as_deref(), Some("123:456"));
            assert_eq!(
                built.file.as_file().metadata()?.len() % crate::extent::BLOCK_BYTES,
                0
            );
            built.file.seek(SeekFrom::Start(1024))?;
            let mut magic = [0; 4];
            built.file.read_exact(&mut magic)?;
            assert_eq!(u32::from_le_bytes(magic), 0xe0f5e1e2);
            if std::env::var_os("VMM_TEST_FSCK").is_some() {
                let extracted = tempfile::tempdir()?;
                let report = Command::new("fsck.erofs")
                    .arg("--no-preserve-owner")
                    .arg("--preserve-perms")
                    .arg(format!("--extract={}", extracted.path().display()))
                    .arg(built.file.path())
                    .output()?;
                assert!(
                    report.status.success(),
                    "{}",
                    String::from_utf8_lossy(&report.stderr)
                );
                let at = |f: &str| extracted.path().join(f);
                assert!(!at("gone").exists() && !at("opaque/old").exists());
                assert_eq!(std::fs::read(at("opaque/new"))?, b"new");
                assert_eq!(std::fs::read(at("hard"))?, b"new");
                assert_eq!(
                    std::fs::metadata(at("hard"))?.ino(),
                    std::fs::metadata(at("replaced"))?.ino()
                );
                assert_eq!(
                    std::fs::metadata(at("hard"))?.permissions().mode() & 0o7777,
                    0o751
                );
                assert_eq!(std::fs::read_link(at("sym"))?, Path::new("replaced"));
            }
        }
    }
    Ok(())
}

#[test]
fn docker_config_must_match_expected_digest() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let (archive, mut digest) = fixture(true);
    digest[0] ^= 1;
    let file = tmp.path().join("docker.tar");
    std::fs::write(&file, archive)?;
    assert!(matches!(
        build(&file, digest, tmp.path()),
        Err(Error::Invalid("Docker config digest"))
    ));
    Ok(())
}

#[test]
fn trusted_builder_requires_oci_and_the_first_release_manifest() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let image = dir.path().join("builder.tar");
    let (docker, digest) = fixture(true);
    std::fs::write(&image, docker)?;
    assert!(build_trusted_builder(&image, digest, dir.path()).is_err());
    let (oci, digest) = fixture(false);
    std::fs::write(&image, &oci)?;
    build_trusted_builder(&image, digest, dir.path())?;
    let mut changed = Builder::new(Vec::new());
    for entry in tar::Archive::new(oci.as_slice()).entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let mut data = Vec::new();
        entry.read_to_end(&mut data)?;
        if path == Path::new("index.json") {
            let mut index: serde_json::Value = serde_json::from_slice(&data)?;
            index["manifests"]
                .as_array_mut()
                .unwrap()
                .insert(0, descriptor(b"other manifest"));
            data = serde_json::to_vec(&index)?;
        }
        file(&mut changed, path.to_str().unwrap(), &data);
    }
    std::fs::write(&image, changed.into_inner()?)?;
    assert!(matches!(
        build_trusted_builder(&image, digest, dir.path()),
        Err(Error::Invalid("builder manifest digest"))
    ));
    Ok(())
}

#[test]
fn security_xattr_in_layer_is_rejected() -> Result<()> {
    let mut layer = Builder::new(Vec::new());
    layer
        .append_pax_extensions([("SCHILY.xattr.security.capability", &b"\x01\x00\x00\x02"[..])])
        .unwrap();
    file(&mut layer, "bin/ping", b"elf");
    let layer = layer.into_inner().unwrap();
    let diff = descriptor(&layer)["digest"].clone();
    let config = serde_json::to_vec(&serde_json::json!({
        "rootfs": {"diff_ids": [diff]}
    }))
    .unwrap();
    let mut outer = Builder::new(Vec::new());
    let cfg_desc = blob(&mut outer, &config);
    let layer_desc = blob(&mut outer, &layer);
    let manifest =
        serde_json::to_vec(&serde_json::json!({"config": cfg_desc, "layers": [layer_desc]}))
            .unwrap();
    let digest: Digest = Sha256::digest(&manifest).into();
    let man_desc = blob(&mut outer, &manifest);
    file(
        &mut outer,
        "index.json",
        &serde_json::to_vec(&serde_json::json!({"manifests": [man_desc]})).unwrap(),
    );
    file(
        &mut outer,
        "oci-layout",
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    );
    let tmp = tempfile::tempdir()?;
    let archive_path = tmp.path().join("caps.oci.tar");
    std::fs::write(&archive_path, outer.into_inner().unwrap())?;
    assert!(matches!(
        build_plain(&archive_path, digest, tmp.path()),
        Err(Error::Invalid("unsupported security xattr"))
    ));
    Ok(())
}

#[test]
fn plain_erofs_matches_fsck_for_deep_tree_links_and_special_modes() -> Result<()> {
    let mut layer = Builder::new(Vec::new());
    let mut add_mode = |name: &str, data: &[u8], kind: EntryType, mode: u32, target: Option<&str>| {
        let mut h = Header::new_gnu();
        h.set_entry_type(kind);
        h.set_mode(mode);
        h.set_uid(0);
        h.set_gid(0);
        h.set_mtime(42);
        h.set_size(data.len() as u64);
        if let Some(t) = target {
            layer.append_link(&mut h, name, t).unwrap();
        } else {
            layer.append_data(&mut h, name, data).unwrap();
        }
    };
    add_mode("tmp", b"", EntryType::Directory, 0o1777, None);
    add_mode("a/b/c/d", b"", EntryType::Directory, 0o755, None);
    add_mode("a/b/c/d/suid", b"suid-binary", EntryType::Regular, 0o4755, None);
    add_mode("a/b/c/d/hard", b"", EntryType::Link, 0o4755, Some("a/b/c/d/suid"));
    add_mode("a/b/c/d/sym", b"", EntryType::Symlink, 0o777, Some("suid"));
    add_mode("a/b/c/d/empty", b"", EntryType::Regular, 0o644, None);
    for i in 0..20 {
        add_mode(
            &format!("a/b/file-{i:02}"),
            format!("payload-{i}").as_bytes(),
            EntryType::Regular,
            0o640,
            None,
        );
    }
    let layer = layer.into_inner().unwrap();
    let diff = descriptor(&layer)["digest"].clone();
    let config = serde_json::to_vec(&serde_json::json!({
        "config": {"Entrypoint": ["/a/b/c/d/suid"]},
        "rootfs": {"diff_ids": [diff]}
    }))
    .unwrap();
    let mut outer = Builder::new(Vec::new());
    let cfg_desc = blob(&mut outer, &config);
    let layer_desc = blob(&mut outer, &layer);
    let manifest =
        serde_json::to_vec(&serde_json::json!({"config": cfg_desc, "layers": [layer_desc]}))
            .unwrap();
    let digest: Digest = Sha256::digest(&manifest).into();
    let man_desc = blob(&mut outer, &manifest);
    file(
        &mut outer,
        "index.json",
        &serde_json::to_vec(&serde_json::json!({"manifests": [man_desc]})).unwrap(),
    );
    file(
        &mut outer,
        "oci-layout",
        br#"{"imageLayoutVersion":"1.0.0"}"#,
    );
    let tmp = tempfile::tempdir()?;
    let archive_path = tmp.path().join("deep.oci.tar");
    std::fs::write(&archive_path, outer.into_inner().unwrap())?;

    let built = build_plain(&archive_path, digest, tmp.path())?;
    assert_eq!(
        built.file.as_file().metadata()?.len() % crate::extent::BLOCK_BYTES,
        0
    );
    // Run fsck.erofs whenever available on the machine (or when VMM_TEST_FSCK is set).
    let fsck_available = Command::new("fsck.erofs")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    if fsck_available || std::env::var_os("VMM_TEST_FSCK").is_some() {
        let extracted = tempfile::tempdir()?;
        let report = Command::new("fsck.erofs")
            .arg("--no-preserve-owner")
            .arg("--preserve-perms")
            .arg(format!("--extract={}", extracted.path().display()))
            .arg(built.file.path())
            .output()?;
        assert!(
            report.status.success(),
            "fsck.erofs failed: {}",
            String::from_utf8_lossy(&report.stderr)
        );
        let at = |f: &str| extracted.path().join(f);
        assert_eq!(std::fs::metadata(at("tmp"))?.permissions().mode() & 0o7777, 0o1777);
        assert_eq!(
            std::fs::metadata(at("a/b/c/d/suid"))?.permissions().mode() & 0o7777,
            0o4755
        );
        assert_eq!(std::fs::read(at("a/b/c/d/suid"))?, b"suid-binary");
        assert_eq!(std::fs::read(at("a/b/c/d/hard"))?, b"suid-binary");
        assert_eq!(
            std::fs::metadata(at("a/b/c/d/hard"))?.ino(),
            std::fs::metadata(at("a/b/c/d/suid"))?.ino()
        );
        assert_eq!(std::fs::read_link(at("a/b/c/d/sym"))?, Path::new("suid"));
        assert_eq!(std::fs::read(at("a/b/c/d/empty"))?, b"");
        for i in 0..20 {
            assert_eq!(
                std::fs::read(at(&format!("a/b/file-{i:02}")))?,
                format!("payload-{i}").as_bytes()
            );
        }
    }
    Ok(())
}
