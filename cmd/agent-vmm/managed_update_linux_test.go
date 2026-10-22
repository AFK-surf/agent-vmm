package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeManagedTestRelease(t *testing.T, path string, entries []*tar.Header, contents map[string]string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	compressed := gzip.NewWriter(file)
	archive := tar.NewWriter(compressed)
	for _, header := range entries {
		header.Size = int64(len(contents[header.Name]))
		if err := archive.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.Write([]byte(contents[header.Name])); err != nil {
			t.Fatal(err)
		}
	}
	for _, closer := range []interface{ Close() error }{archive, compressed, file} {
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestExtractManagedUpdateStagesOnlyTheSelectedRelease(t *testing.T) {
	inventory := `{"version":1,"release_id":"release-2","files":[` +
		`{"path":"agent-vmm","size":1,"sha256":"` + strings.Repeat("a", 64) + `","executable":true},` +
		`{"path":"agent-vmm-data","size":1,"sha256":"` + strings.Repeat("b", 64) + `","executable":true},` +
		`{"path":"guest/manifest.json","size":1,"sha256":"` + strings.Repeat("c", 64) + `"}]}`
	release := []*tar.Header{
		{Name: "agent-vmm-release-2/", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "agent-vmm-release-2/release.json", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "agent-vmm-release-2/guest/manifest.json", Typeflag: tar.TypeReg, Mode: 0o644},
	}
	contents := map[string]string{"agent-vmm-release-2/release.json": inventory, "agent-vmm-release-2/guest/manifest.json": "{}"}
	root := t.TempDir()
	archive := filepath.Join(root, "release.tar.gz")
	writeManagedTestRelease(t, archive, release, contents)
	staged := managedPreparedUpdatePath(filepath.Join(root, "current"), "update-1")
	if err := extractManagedUpdate(context.Background(), archive, staged, "release-2"); err != nil {
		t.Fatal(err)
	}
	if release, err := managedHostReleaseID(staged, true); err != nil || release != "release-2" {
		t.Fatalf("staged release = %q, %v", release, err)
	}
	if err := extractManagedUpdate(context.Background(), archive, managedPreparedUpdatePath(filepath.Join(root, "current"), "update-2"), "release-3"); err == nil {
		t.Fatal("a different release was staged")
	}

	for name, header := range map[string]*tar.Header{
		"escape":  {Name: "agent-vmm-release-2/../outside", Typeflag: tar.TypeReg, Mode: 0o644},
		"symlink": {Name: "agent-vmm-release-2/guest/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
		"second":  {Name: "other/release.json", Typeflag: tar.TypeReg, Mode: 0o644},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "release.tar.gz")
			writeManagedTestRelease(t, path, append(append([]*tar.Header{}, release...), header), contents)
			if err := extractManagedReleaseArchive(path, t.TempDir()); err == nil {
				t.Fatal("unsafe archive entry was extracted")
			}
		})
	}
}
