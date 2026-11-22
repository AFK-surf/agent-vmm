use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::Path,
    process::Command,
};
use zip::{ZipWriter, write::SimpleFileOptions};

fn xtask() -> Command {
    Command::new(env!("CARGO_BIN_EXE_xtask"))
}

fn blob<'a>(
    files: &'a BTreeMap<String, Vec<u8>>,
    descriptor: &Value,
    media_type: &str,
) -> &'a [u8] {
    assert_eq!(descriptor["mediaType"], media_type);
    let hash = descriptor["digest"]
        .as_str()
        .unwrap()
        .strip_prefix("sha256:")
        .unwrap();
    let data = &files[&format!("blobs/sha256/{hash}")];
    assert_eq!(format!("{:x}", Sha256::digest(data)), hash);
    assert_eq!(descriptor["size"], data.len());
    data
}

#[test]
fn oci_layout_and_bytes_match_the_guest_builder() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("layer.tar");
    let out = dir.path().join("image.tar");
    fs::write(&root, b"layer parity\n").unwrap();
    // Complete archive fingerprints from the Go packer, running as root as in Guest assembly.
    for (entry, expected) in [
        (
            None,
            "14ad4c5d1efad84664034c1823c4ebf37cb3c6395e7aed0019e54c74491f59a7",
        ),
        (
            Some("/bin/x<&>"),
            "4b2e41c9dc9ed64db4fab1dec266a0c0ce13a0dad8f80a778b374820d7513e5d",
        ),
    ] {
        assert!(
            xtask()
                .arg("oci")
                .arg(&root)
                .arg(&out)
                .args(entry)
                .status()
                .unwrap()
                .success()
        );
        let tar = fs::read(&out).unwrap();
        assert_eq!(format!("{:x}", Sha256::digest(&tar)), expected);
        let mut files = BTreeMap::new();
        let mut names = Vec::new();
        for item in tar::Archive::new(tar.as_slice()).entries().unwrap() {
            let mut item = item.unwrap();
            let path = item.path().unwrap().to_str().unwrap().to_owned();
            names.push(path.clone());
            let h = item.header();
            assert_eq!(
                (h.uid().unwrap(), h.gid().unwrap(), h.mtime().unwrap()),
                (0, 0, 0)
            );
            if !h.entry_type().is_dir() {
                let mut bytes = Vec::new();
                item.read_to_end(&mut bytes).unwrap();
                files.insert(path, bytes);
            }
        }
        assert!(names.is_sorted());
        assert_eq!(names.len(), 7);
        assert_eq!(files["oci-layout"], b"{\"imageLayoutVersion\":\"1.0.0\"}\n");
        let index: Value = serde_json::from_slice(&files["index.json"]).unwrap();
        assert_eq!(index["schemaVersion"], 2);
        let manifest = &index["manifests"][0];
        assert_eq!(
            manifest["platform"],
            json!({"architecture":"arm64","os":"linux"})
        );
        let m: Value = serde_json::from_slice(blob(
            &files,
            manifest,
            "application/vnd.oci.image.manifest.v1+json",
        ))
        .unwrap();
        let layer = &m["layers"][0];
        assert_eq!(
            blob(&files, layer, "application/vnd.oci.image.layer.v1.tar"),
            b"layer parity\n"
        );
        let c: Value = serde_json::from_slice(blob(
            &files,
            &m["config"],
            "application/vnd.oci.image.config.v1+json",
        ))
        .unwrap();
        assert_eq!(
            c["rootfs"],
            json!({"type":"layers","diff_ids":[layer["digest"]]})
        );
        assert_eq!(
            c.get("config").cloned(),
            entry.map(|p| json!({"Entrypoint":[p]}))
        );
    }
    let error = xtask()
        .arg("oci")
        .arg(&root)
        .arg(&out)
        .arg("relative")
        .output()
        .unwrap();
    assert!(!error.status.success());
    assert!(String::from_utf8_lossy(&error.stderr).contains("must be absolute"));
}

#[test]
fn notices_scan_a_real_go_binary_and_group_shared_licenses() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    // The fixture retains just .go.buildinfo from a stripped Go 1.24.4 linux/amd64
    // executable importing example.org/uuid, replaced by github.com/google/uuid v1.6.0.
    let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/buildinfo.elf");
    let cache = root.join("cache/download/github.com/google/uuid/@v");
    fs::create_dir_all(&cache).unwrap();
    let mut zip = ZipWriter::new(fs::File::create(cache.join("v1.6.0.zip")).unwrap());
    for (name, contents) in [
        ("LICENSE", "  module text\n"),
        ("dist/NoTiCe", "notice"),
        ("testdata/COPYING", "excluded"),
        ("README", "excluded"),
    ] {
        zip.start_file(
            format!("github.com/google/uuid@v1.6.0/{name}"),
            SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(contents.as_bytes()).unwrap();
    }
    zip.finish().unwrap();
    fs::write(root.join("MIT.txt"), "shared MIT\n").unwrap();
    fs::write(
        root.join("installed"),
        "P:busybox\nV:1.0-r0\nL:GPL-2.0-only\no:busybox\n\nP:another\nV:1\nL:MIT\n",
    )
    .unwrap();
    let output = xtask()
        .current_dir(root)
        .env("GO_MOD_CACHE_DIR", root)
        .env("PATH", "")
        .args([
            "notices",
            "-o",
            "out",
            "-extra",
            "addon=MIT",
            "-licenses",
            ".",
            "-go-binary",
        ])
        .arg(binary)
        .args(["-apk-db", "installed", "-apk-db", "installed"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("out")).unwrap(),
        concat!(
            "THIRD-PARTY NOTICES\n\nGo\n==\n\naddon@\nLicense: MIT\n",
            "\ngithub.com/google/uuid@v1.6.0\n\n--- LICENSE ---\nmodule text\n\n--- dist/NoTiCe ---\nnotice\n",
            "\nRust\n====\n\nAlpine\n======\n\nanother@1\nLicense: MIT\n",
            "\nbusybox@1.0-r0\nLicense: GPL-2.0-only\nOrigin: busybox\n",
            "\nLicense texts\n=============\n\n--- MIT ---\nshared MIT\n"
        )
    );
}
