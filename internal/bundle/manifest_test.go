package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestVerify(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string)
		ok     bool
	}{
		{name: "valid", ok: true},
		{name: "artifact changed", mutate: func(directory string) { write(t, filepath.Join(directory, "Image"), "changed") }},
		{name: "manifest changed", mutate: func(directory string) { write(t, filepath.Join(directory, ManifestName), "invalid") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory, manifestDigest := fixture(t)
			if test.mutate != nil {
				test.mutate(directory)
			}
			_, err := Verify(directory, manifestDigest)
			if (err == nil) != test.ok {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func fixture(t *testing.T) (string, string) {
	t.Helper()
	directory := t.TempDir()
	artifacts := map[string]Artifact{}
	for name, path := range map[string]string{
		"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs",
		"mcp_appliance_digest": "mcp-appliance.digest", "mcp_appliance_manifest": "mcp-appliance/manifest.pb",
	} {
		data := []byte(name)
		write(t, filepath.Join(directory, path), string(data))
		artifacts[name] = Artifact{Path: path, Size: int64(len(data)), SHA256: digest(data)}
	}
	manifest := Manifest{SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0", Artifacts: artifacts, Components: map[string]Component{}, RootFS: RootFS{Filesystem: "squashfs", Compressor: "zstd", BlockSize: 1 << 20, Filter: "none"}}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(directory, ManifestName), string(data))
	return directory, digest(data)
}

func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func digest(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
