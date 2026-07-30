package bundle

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestVerify(t *testing.T) {
	seed, _ := hex.DecodeString("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)

	tests := []struct {
		name   string
		mutate func(string)
		ok     bool
	}{
		{name: "valid", ok: true},
		{name: "artifact changed", mutate: func(directory string) { write(t, filepath.Join(directory, "Image"), "changed") }},
		{name: "signature changed", mutate: func(directory string) { write(t, filepath.Join(directory, SignatureName), "invalid") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := fixture(t, privateKey)
			if test.mutate != nil {
				test.mutate(directory)
			}
			_, err := Verify(directory, publicKey)
			if (err == nil) != test.ok {
				t.Fatalf("Verify() error = %v", err)
			}
		})
	}
}

func fixture(t *testing.T, privateKey ed25519.PrivateKey) string {
	t.Helper()
	directory := t.TempDir()
	artifacts := map[string]Artifact{}
	for name, path := range map[string]string{
		"kernel": "Image", "initramfs": "initramfs.xz", "rootfs": "rootfs.squashfs",
		"sbom": "sbom.spdx.json", "provenance": "provenance.json", "checksums": "SHA256SUMS",
	} {
		data := []byte(name)
		write(t, filepath.Join(directory, path), string(data))
		artifacts[name] = Artifact{Path: path, Size: int64(len(data)), SHA256: digest(data)}
	}
	manifest := Manifest{SchemaVersion: 1, Architecture: "arm64", MinimumMacOS: "14.0", Signer: "development", Artifacts: artifacts, Components: map[string]Component{}, RootFS: RootFS{Filesystem: "squashfs", Compressor: "xz", BlockSize: 1 << 20, Dictionary: 1 << 20, Filter: "arm64-bcj"}}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(directory, ManifestName), string(data))
	write(t, filepath.Join(directory, SignatureName), base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, data)))
	return directory
}

func write(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}

func digest(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
