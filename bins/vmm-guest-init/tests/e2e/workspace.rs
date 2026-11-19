use std::io::{self, Read};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use vmm_guest_proto::client::ClientError;
use vmm_guest_proto::rpc::{
    EntryKind, ErrorCode, Sha256Digest, WorkspaceImportRequest, WorkspaceListRequest,
    WorkspaceReadRequest, WorkspaceTarget, WorkspaceWriteRequest,
};

use crate::support::{Guest, refused, target};

fn sha256(data: &[u8]) -> Sha256Digest {
    Sha256::digest(data).into()
}

fn write_request(request_id: &str, path: &str) -> WorkspaceWriteRequest {
    WorkspaceWriteRequest {
        request_id: request_id.into(),
        target: target(path),
        offset: 0,
        expected_prefix_sha256: None,
        mode: 0,
    }
}

fn read(guest: &Guest, target: WorkspaceTarget, offset: u64, max_bytes: u64) -> Vec<u8> {
    let mut data = Vec::new();
    let request = WorkspaceReadRequest {
        target,
        offset,
        max_bytes,
    };
    let reply = guest.client.workspace_read(request, &mut data).unwrap();
    assert_eq!(reply.sha256, sha256(&data));
    assert_eq!(reply.end_offset, offset + data.len() as u64);
    data
}

#[test]
fn files_are_written_read_listed_and_described() {
    let guest = Guest::start(None, 4096);
    let content: Vec<u8> = (0..200_000).map(|index| (index % 253) as u8).collect();
    let mut request = write_request("upload-1", "docs/deep/data.bin");
    request.mode = 0o640;
    let written = guest
        .client
        .workspace_write(request, &mut content.as_slice())
        .unwrap();
    assert_eq!(written.sha256, sha256(&content));
    assert_eq!((written.entry.size, written.entry.mode), (200_000, 0o640));
    assert!(written.root_used_bytes.is_some());
    assert_eq!(
        std::fs::read(guest.path("workspace/docs/deep/data.bin")).unwrap(),
        content
    );

    let entry = guest
        .client
        .workspace_stat(target("docs/deep/../deep/data.bin"))
        .unwrap();
    assert_eq!(entry.path, "docs/deep/data.bin");
    assert_eq!(
        (entry.kind, entry.sha256),
        (EntryKind::File, Some(sha256(&content)))
    );
    assert_eq!(read(&guest, target("docs/deep/data.bin"), 0, 0), content);
    assert_eq!(
        read(&guest, target("docs/deep/data.bin"), 199_990, 0),
        content[199_990..]
    );
    assert_eq!(
        read(&guest, target("docs/deep/data.bin"), 10, 5),
        content[10..15]
    );

    // A second write replaces the file in one step.
    let replaced = guest.client.workspace_write(
        write_request("upload-2", "docs/deep/data.bin"),
        &mut &b"new"[..],
    );
    assert_eq!(replaced.unwrap().entry.size, 3);
    assert_eq!(read(&guest, target("docs/deep/data.bin"), 0, 0), b"new");

    std::fs::write(guest.path("workspace/docs/a.txt"), "a").unwrap();
    symlink("/etc/passwd", guest.path("workspace/docs/link")).unwrap();
    let request = WorkspaceListRequest {
        target: target("docs"),
        limit: 0,
    };
    let listed = guest.client.workspace_list(request).unwrap();
    let names: Vec<_> = listed
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry.kind))
        .collect();
    assert_eq!(
        names,
        [
            ("docs/a.txt", EntryKind::File),
            ("docs/deep", EntryKind::Directory)
        ]
    );
    let request = WorkspaceListRequest {
        target: target("docs"),
        limit: 1,
    };
    let listed = guest.client.workspace_list(request).unwrap();
    assert!(listed.truncated && listed.entries.len() == 1);

    // A named volume is a separate root.
    let volume = WorkspaceTarget {
        volume: Some("data".into()),
        path: "v.txt".into(),
    };
    let mut request = write_request("upload-3", "v.txt");
    request.target = volume.clone();
    guest
        .client
        .workspace_write(request, &mut &b"volume"[..])
        .unwrap();
    assert_eq!(
        std::fs::read(guest.path("volume/v.txt")).unwrap(),
        b"volume"
    );
    assert_eq!(
        refused(guest.client.workspace_stat(target("v.txt"))),
        ErrorCode::NotFound
    );
    let missing = WorkspaceTarget {
        volume: Some("absent".into()),
        path: ".".into(),
    };
    assert_eq!(
        refused(guest.client.workspace_stat(missing)),
        ErrorCode::NotFound
    );
}

/// Yields its bytes, then fails.
struct Failing<'a>(&'a [u8]);

impl Read for Failing<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.0.is_empty() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        self.0.read(buffer)
    }
}

#[test]
fn an_interrupted_upload_resumes_only_with_the_matching_prefix_digest() {
    let guest = Guest::start(None, 4096);
    std::fs::write(guest.path("workspace/report.txt"), "old").unwrap();

    // The source fails after the first part. The client returns that failure
    // and does not wait for the stream timeout of the Guest.
    let started = Instant::now();
    let request = write_request("resume-1", "report.txt");
    let interrupted = guest
        .client
        .workspace_write(request, &mut Failing(b"hello "));
    assert!(
        matches!(&interrupted, Err(ClientError::Io(error)) if error.kind() == io::ErrorKind::Interrupted),
        "{interrupted:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(10));

    // The Guest has ended the call: the part is staged and its lock is free.
    let staged = || {
        let names = std::fs::read_dir(guest.path("workspace")).unwrap();
        let mut names = names.map(|entry| entry.unwrap().file_name().into_string().unwrap());
        names.find(|name| name.starts_with(".report.txt.upload-"))
    };
    let staged_part = std::fs::read(guest.path("workspace").join(staged().unwrap()));
    assert_eq!(staged_part.unwrap(), b"hello ");
    // The target keeps its content until an upload completes.
    assert_eq!(
        std::fs::read(guest.path("workspace/report.txt")).unwrap(),
        b"old"
    );

    let resume = |offset: u64, prefix: Option<Sha256Digest>| {
        let mut request = write_request("resume-1", "report.txt");
        (request.offset, request.expected_prefix_sha256) = (offset, prefix);
        guest.client.workspace_write(request, &mut &b"world"[..])
    };
    assert_eq!(
        refused(resume(6, Some(sha256(b"HELLO ")))),
        ErrorCode::FailedPrecondition
    );
    assert_eq!(refused(resume(6, None)), ErrorCode::FailedPrecondition);
    assert_eq!(
        refused(resume(5, Some(sha256(b"hello")))),
        ErrorCode::FailedPrecondition
    );
    assert_eq!(
        std::fs::read(guest.path("workspace/report.txt")).unwrap(),
        b"old"
    );

    let written = resume(6, Some(sha256(b"hello "))).unwrap();
    assert_eq!(written.sha256, sha256(b"hello world"));
    assert_eq!(
        std::fs::read(guest.path("workspace/report.txt")).unwrap(),
        b"hello world"
    );
    assert_eq!(staged(), None);
}

#[test]
fn paths_cannot_leave_the_root_through_dots_or_symbolic_links() {
    let guest = Guest::start(None, 4096);
    std::fs::write(guest.path("outside/secret.txt"), "secret").unwrap();
    std::fs::write(guest.path("workspace/inside.txt"), "inside").unwrap();
    let outside = guest.path("outside");
    symlink("/etc/passwd", guest.path("workspace/absolute-link")).unwrap();
    symlink(
        "../outside/secret.txt",
        guest.path("workspace/relative-link"),
    )
    .unwrap();
    symlink(&outside, guest.path("workspace/dir-link")).unwrap();
    symlink("inside.txt", guest.path("workspace/inner-link")).unwrap();

    let escapes = [
        "../outside/secret.txt",
        "a/../../outside/secret.txt",
        "/etc/passwd",
        "absolute-link",
        "relative-link",
        "dir-link/secret.txt",
        "inner-link",
    ];
    for path in escapes {
        assert_eq!(
            refused(guest.client.workspace_stat(target(path))),
            ErrorCode::PathEscape,
            "{path}"
        );
        let request = WorkspaceReadRequest {
            target: target(path),
            offset: 0,
            max_bytes: 0,
        };
        let read = guest.client.workspace_read(request, &mut Vec::new());
        assert_eq!(refused(read), ErrorCode::PathEscape, "{path}");
    }
    for path in [
        "../outside/new.txt",
        "/tmp/new.txt",
        "dir-link/new.txt",
        "dir-link/sub/new.txt",
    ] {
        let write = guest
            .client
            .workspace_write(write_request("escape", path), &mut &b"x"[..]);
        assert_eq!(refused(write), ErrorCode::PathEscape, "{path}");
        let request = WorkspaceImportRequest {
            request_id: "escape".into(),
            target: target(path),
        };
        let import = guest
            .client
            .workspace_import_archive(request, &mut std::io::empty());
        assert_eq!(refused(import), ErrorCode::PathEscape, "{path}");
    }
    let listed = std::fs::read_dir(&outside).unwrap().count();
    assert_eq!(listed, 1, "nothing was created outside the root");
    assert_eq!(
        std::fs::read(guest.path("outside/secret.txt")).unwrap(),
        b"secret"
    );

    // Writing over a link replaces the link, never its target.
    guest
        .client
        .workspace_write(write_request("over", "relative-link"), &mut &b"mine"[..])
        .unwrap();
    assert!(
        guest
            .path("workspace/relative-link")
            .symlink_metadata()
            .unwrap()
            .is_file()
    );
    assert_eq!(
        std::fs::read(guest.path("outside/secret.txt")).unwrap(),
        b"secret"
    );
}

fn archive(entries: &[(&[u8], tar::EntryType, &[u8])]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, kind, content) in entries {
        let mut header = tar::Header::new_gnu();
        // Written raw: the builder refuses the hostile names that the Guest must reject.
        header.as_old_mut().name[..name.len()].copy_from_slice(name);
        header.set_entry_type(*kind);
        header.set_mode(0o750);
        header.set_size(content.len() as u64);
        if *kind == tar::EntryType::Symlink {
            header.set_link_name("/etc/passwd").unwrap();
        }
        header.set_cksum();
        builder.append(&header, *content).unwrap();
    }
    builder.into_inner().unwrap()
}

fn import(request_id: &str, path: &str) -> WorkspaceImportRequest {
    WorkspaceImportRequest {
        request_id: request_id.into(),
        target: target(path),
    }
}

#[test]
fn archives_round_trip_and_hostile_archives_leave_nothing_behind() {
    use tar::EntryType::{Directory, Regular, Symlink};
    let guest = Guest::start(None, 4096);
    let tree = archive(&[
        (b"src/", Directory, b""),
        (b"src/main.rs", Regular, b"fn main() {}"),
        (b"notes/deep/todo.txt", Regular, b"todo"),
    ]);
    let imported = guest
        .client
        .workspace_import_archive(import("import-1", "project"), &mut tree.as_slice());
    let imported = imported.unwrap();
    assert_eq!((imported.entries, imported.bytes), (3, tree.len() as u64));
    assert!(imported.root_used_bytes.is_some());
    assert_eq!(imported.sha256, sha256(&tree));
    assert_eq!(imported.entry.kind, EntryKind::Directory);
    assert_eq!(
        std::fs::read(guest.path("workspace/project/src/main.rs")).unwrap(),
        b"fn main() {}"
    );
    assert_eq!(
        std::fs::read(guest.path("workspace/project/notes/deep/todo.txt")).unwrap(),
        b"todo"
    );
    let mode = std::fs::metadata(guest.path("workspace/project/src/main.rs"))
        .unwrap()
        .permissions();
    assert_eq!(mode.mode() & 0o777, 0o750);

    let again = guest
        .client
        .workspace_import_archive(import("import-2", "project"), &mut tree.as_slice());
    assert_eq!(refused(again), ErrorCode::AlreadyExists);

    let mut exported = Vec::new();
    let reply = guest
        .client
        .workspace_export_archive(target("project"), &mut exported)
        .unwrap();
    assert_eq!(
        (reply.sha256, reply.bytes),
        (sha256(&exported), exported.len() as u64)
    );
    assert_eq!(reply.entries, 6);
    let mut files = Vec::new();
    for entry in tar::Archive::new(exported.as_slice()).entries().unwrap() {
        let mut entry = entry.unwrap();
        let mut content = String::new();
        entry.read_to_string(&mut content).unwrap();
        files.push((entry.path().unwrap().to_str().unwrap().to_owned(), content));
    }
    let expected = [
        ("project/", ""),
        ("project/notes/", ""),
        ("project/notes/deep/", ""),
        ("project/notes/deep/todo.txt", "todo"),
        ("project/src/", ""),
        ("project/src/main.rs", "fn main() {}"),
    ];
    let files: Vec<_> = files
        .iter()
        .map(|(path, content)| (path.as_str(), content.as_str()))
        .collect();
    assert_eq!(files, expected);

    // An exported archive imports again, here into a volume.
    let request = WorkspaceImportRequest {
        request_id: "import-3".into(),
        target: WorkspaceTarget {
            volume: Some("data".into()),
            path: "copy".into(),
        },
    };
    guest
        .client
        .workspace_import_archive(request, &mut exported.as_slice())
        .unwrap();
    assert_eq!(
        std::fs::read(guest.path("volume/copy/project/src/main.rs")).unwrap(),
        b"fn main() {}"
    );

    let hostile: [(&str, Vec<u8>, ErrorCode); 4] = [
        (
            "dots",
            archive(&[
                (b"ok.txt", Regular, b"ok"),
                (b"../evil.txt", Regular, b"evil"),
            ]),
            ErrorCode::PathEscape,
        ),
        (
            "absolute",
            archive(&[(b"/tmp/evil.txt", Regular, b"evil")]),
            ErrorCode::PathEscape,
        ),
        (
            "link",
            archive(&[
                (b"link", Symlink, b""),
                (b"link/evil.txt", Regular, b"evil"),
            ]),
            ErrorCode::InvalidArgument,
        ),
        (
            "truncated",
            tree[..700].to_vec(),
            ErrorCode::InvalidArgument,
        ),
    ];
    for (name, bytes, code) in hostile {
        let result = guest
            .client
            .workspace_import_archive(import(name, name), &mut bytes.as_slice());
        assert_eq!(refused(result), code, "{name}");
        assert!(!guest.path("workspace").join(name).exists(), "{name}");
    }
    let mut left: Vec<_> = std::fs::read_dir(guest.path("workspace"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(
        left,
        ["project"],
        "no staging directory or stray file remains"
    );
    assert!(!guest.root().join("evil.txt").exists());

    // An export refuses a tree that holds a symbolic link.
    symlink("/etc/passwd", guest.path("workspace/project/link")).unwrap();
    let export = guest
        .client
        .workspace_export_archive(target("project"), &mut Vec::new());
    assert_eq!(refused(export), ErrorCode::InvalidArgument);
}
