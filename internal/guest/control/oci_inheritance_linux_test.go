//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/plugins/content/local"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

// Uses the privileged startup test's disposable ext4 mount and the same
// containerd implementations that create production content and snapshots.
func seedOCIInheritance(t *testing.T) map[string]uint32 {
	t.Helper()
	ctx := context.Background()
	var layer bytes.Buffer
	writer := tar.NewWriter(&layer)
	for _, header := range []*tar.Header{
		{Name: "etc", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "etc/content", Typeflag: tar.TypeReg, Mode: 0o644, Size: 4},
		{Name: "etc/hardlink", Typeflag: tar.TypeLink, Linkname: "etc/content"},
		{Name: "absolute", Typeflag: tar.TypeSymlink, Linkname: "/usr/bin/oci-missing"},
		{Name: "relative", Typeflag: tar.TypeSymlink, Linkname: "etc/content"},
		{Name: "dangling", Typeflag: tar.TypeSymlink, Linkname: "missing"},
		{Name: "fifo", Typeflag: tar.TypeFifo, Mode: 0o600},
		{Name: "null", Typeflag: tar.TypeChar, Mode: 0o600, Devmajor: 1, Devminor: 3},
	} {
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if header.Size > 0 {
			if _, err := writer.Write([]byte("data")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	storeRoot := filepath.Join(containerdRoot, "io.containerd.content.v1.content")
	store, err := local.NewStore(storeRoot)
	if err != nil {
		t.Fatal(err)
	}
	desc := ocispec.Descriptor{Digest: digest.FromBytes(layer.Bytes()), Size: int64(layer.Len()), MediaType: ocispec.MediaTypeImageLayer}
	if err := content.WriteBlob(ctx, store, "inheritance", bytes.NewReader(layer.Bytes()), desc); err != nil {
		t.Fatal(err)
	}
	paths := []string{filepath.Join(storeRoot, "blobs", "sha256", desc.Digest.Encoded())}
	unfinished, err := store.Writer(ctx, content.WithRef("interrupted-ingest"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unfinished.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := unfinished.Close(); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, filepath.Join(storeRoot, "ingest", digest.FromString("interrupted-ingest").Encoded(), "data"))

	snapshotter, err := overlay.NewSnapshotter(filepath.Join(containerdRoot, "io.containerd.snapshotter.v1.overlayfs"))
	if err != nil {
		t.Fatal(err)
	}
	defer snapshotter.Close()
	interruptedMounts, err := snapshotter.Prepare(ctx, "interrupted-snapshot", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(interruptedMounts) != 1 || interruptedMounts[0].Type != "bind" {
		t.Fatalf("unexpected interrupted snapshot: %v", interruptedMounts)
	}
	partial := filepath.Join(interruptedMounts[0].Source, "partial-layer")
	if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, interruptedMounts[0].Source, partial)

	mounts, err := snapshotter.Prepare(ctx, "lower", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 1 || mounts[0].Type != "bind" {
		t.Fatalf("unexpected lower mounts: %v", mounts)
	}
	lower := mounts[0].Source
	if _, err := archive.Apply(ctx, lower, bytes.NewReader(layer.Bytes())); err != nil {
		t.Fatal(err)
	}
	paths = append(paths, lower, filepath.Join(lower, "etc"), filepath.Join(lower, "etc", "content"), filepath.Join(lower, "etc", "hardlink"))
	if err := snapshotter.Commit(ctx, "committed", "lower"); err != nil {
		t.Fatal(err)
	}
	mounts, err = snapshotter.Prepare(ctx, "writable", "committed")
	if err != nil {
		t.Fatal(err)
	}
	var upper, work string
	for _, mount := range mounts {
		for _, option := range mount.Options {
			if value, ok := strings.CutPrefix(option, "workdir="); ok {
				work = value
			}
			if value, ok := strings.CutPrefix(option, "upperdir="); ok {
				upper = value
			}
		}
	}
	if upper == "" {
		t.Fatal("missing writable upper")
	}
	if err := assignSnapshotMountProject(mounts, 10002); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(upper, "workload-data")
	if err := os.WriteFile(child, []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	if id, err := projectID(child); err != nil || id != 10002 {
		t.Fatalf("writable child project=%d err=%v", id, err)
	}
	linked, err := os.Stat(filepath.Join(lower, "etc", "hardlink"))
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.Stat(filepath.Join(lower, "etc", "content"))
	if err != nil || !os.SameFile(original, linked) {
		t.Fatalf("layer hardlink lost inode identity: %v", err)
	}
	zero := filepath.Join(stateRoot, "unowned-source")
	if err := os.WriteFile(zero, []byte("unowned"), 0o600); err != nil {
		t.Fatal(err)
	}
	for destination, source := range map[string]string{
		filepath.Join(upper, "foreign-lower"): filepath.Join(lower, "etc", "content"),
		filepath.Join(lower, "foreign-zero"):  zero,
	} {
		if err := os.Link(source, destination); !errors.Is(err, unix.EXDEV) {
			t.Fatalf("cross-project hardlink error=%v", err)
		}
		if err := os.Rename(source, destination); !errors.Is(err, unix.EXDEV) {
			t.Fatalf("cross-project rename error=%v", err)
		}
	}
	renamed := filepath.Join(lower, "etc", "renamed-content")
	if err := os.Rename(filepath.Join(lower, "etc", "hardlink"), renamed); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(renamed, filepath.Join(lower, "etc", "hardlink")); err != nil {
		t.Fatal(err)
	}
	if id, err := projectID(zero); err != nil || id != 0 {
		t.Fatalf("external source project=%d err=%v", id, err)
	}
	merged := t.TempDir()
	if err := mount.All(mounts, merged); err != nil {
		t.Fatal(err)
	}
	defer mount.UnmountAll(merged, 0)
	if err := os.WriteFile(filepath.Join(merged, "etc", "content"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(lower, "etc", "content")); err != nil || string(data) != "data" {
		t.Fatalf("copy-up changed lower: %q %v", data, err)
	}
	if err := mount.UnmountAll(merged, 0); err != nil {
		t.Fatal(err)
	}
	if err := mount.All(mounts, merged); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(merged, "etc", "after-remount"), []byte("retained"), 0o600); err != nil {
		t.Fatal(err)
	}
	projects := map[string]uint32{work: 10002, filepath.Join(upper, "etc", "after-remount"): 10002, upper: 10002, child: 10002, filepath.Join(upper, "etc"): 10002, filepath.Join(upper, "etc", "content"): 10002}
	for _, path := range paths {
		projects[path] = ociProjectID
	}
	for path, want := range projects {
		if id, err := projectID(path); err != nil || id != want {
			t.Fatalf("OCI creator project path=%s id=%d err=%v", path, id, err)
		}
	}
	return projects
}

// Exercise the build workspace owner and its actual context extractor. The
// full VM Dockerfile E2E covers builder execution and output import into CAS.
func seedBuildInheritance(t *testing.T) (map[string]uint32, func()) {
	t.Helper()
	// Namespace admission owns build-tmp and charges it to the writable budget.
	root := filepath.Join(stateRoot, "build-tmp", "inheritance")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := assignProject(root, 10003); err != nil {
		t.Fatal(err)
	}
	builder := guestbuild.NewBuilder(nil, nil, nil)
	workspace, err := builder.Begin(&vmmdv1.BuildImageHeader{RequestId: "inheritance", ActorNamespaceId: "inheritance", DockerfilePath: "Dockerfile", Image: "agent.local/inheritance:latest", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE})
	if err != nil {
		t.Fatal(err)
	}
	closeWorkspace := func() { _ = workspace.Close() }
	var contextArchive bytes.Buffer
	writer := tar.NewWriter(&contextArchive)
	data := []byte("FROM scratch\n")
	if err := writer.WriteHeader(&tar.Header{Name: "Dockerfile", Mode: 0o600, Size: int64(len(data))}); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if _, err := writer.Write(data); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	if err := workspace.AppendContext(contextArchive.Bytes(), true); err != nil {
		closeWorkspace()
		t.Fatal(err)
	}
	roots, err := filepath.Glob(filepath.Join(stateRoot, "build-tmp", "inheritance", "*"))
	if err != nil || len(roots) != 1 {
		closeWorkspace()
		t.Fatalf("build workspace roots=%v err=%v", roots, err)
	}
	projects := make(map[string]uint32)
	for _, suffix := range []string{"", "context.tar", "context", "context/Dockerfile", "output", "output/tmp", "output/home", "output/buildkit"} {
		path := filepath.Join(roots[0], suffix)
		if id, err := projectID(path); err != nil || id != 10003 {
			closeWorkspace()
			t.Fatalf("build project path=%s id=%d err=%v", path, id, err)
		}
		projects[path] = 10003
	}
	return projects, closeWorkspace
}

func TestWritableSnapshotRootSupportsNativeAndOverlaySnapshots(t *testing.T) {
	tests := []struct {
		name   string
		mounts []mount.Mount
		want   string
	}{
		{
			name:   "native",
			mounts: []mount.Mount{{Type: "bind", Source: "/control/native/active"}},
			want:   "/control/native/active",
		},
		{
			name: "overlay",
			mounts: []mount.Mount{{
				Type:    "overlay",
				Options: []string{"lowerdir=/pool/lower", "upperdir=/pool/upper", "workdir=/pool/work"},
			}},
			want: "/pool/upper",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := writableSnapshotRoot(test.mounts)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("writableSnapshotRoot()=%q, want %q", got, test.want)
			}
		})
	}
}
