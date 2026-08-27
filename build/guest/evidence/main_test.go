package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testRevision = "0123456789abcdef0123456789abcdef01234567"

func TestEvidenceAcceptsCurrentFiles(t *testing.T) {
	bundle, rootfs, lock := evidenceFixture(t, "MIT")
	if err := generate(bundle, lock, testRevision); err != nil {
		t.Fatal(err)
	}
	if err := verify(bundle, rootfs, lock, testRevision); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceRejectsMissingAndTamperedFiles(t *testing.T) {
	t.Run("missing provenance", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "MIT")
		if err := verify(bundle, rootfs, lock, testRevision); err == nil {
			t.Fatal("verify accepted missing provenance")
		}
	})
	t.Run("manifest changed after provenance", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "MIT")
		if err := generate(bundle, lock, testRevision); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, filepath.Join(bundle, "manifest.json"), "tampered")
		if err := verify(bundle, rootfs, lock, testRevision); err == nil || !strings.Contains(err.Error(), "Guest manifest") {
			t.Fatalf("verify error = %v", err)
		}
	})
	t.Run("SBOM changed after provenance", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "MIT")
		if err := generate(bundle, lock, testRevision); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, filepath.Join(bundle, "sbom.spdx.json"), `{"spdxVersion":"SPDX-2.3","packages":[{"name":"busybox","versionInfo":"1.0-r0","licenseDeclared":"Apache-2.0"}]}`)
		if err := verify(bundle, rootfs, lock, testRevision); err == nil || !strings.Contains(err.Error(), "sbom.spdx.json") {
			t.Fatalf("verify error = %v", err)
		}
	})
	t.Run("provenance changed", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "MIT")
		if err := generate(bundle, lock, testRevision); err != nil {
			t.Fatal(err)
		}
		writeTestFile(t, filepath.Join(bundle, "provenance.json"), `{"_type":"tampered"}`)
		if err := verify(bundle, rootfs, lock, testRevision); err == nil || !strings.Contains(err.Error(), "metadata is invalid") {
			t.Fatalf("verify error = %v", err)
		}
	})
}

func TestEvidenceRejectsIncompleteLicenseInventory(t *testing.T) {
	t.Run("installed package absent", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "MIT")
		writeTestFile(t, filepath.Join(bundle, "sbom.spdx.json"), `{"spdxVersion":"SPDX-2.3","packages":[{"name":"other","versionInfo":"1","licenseDeclared":"MIT"}]}`)
		if err := generate(bundle, lock, testRevision); err != nil {
			t.Fatal(err)
		}
		if err := verify(bundle, rootfs, lock, testRevision); err == nil || !strings.Contains(err.Error(), "missing from the SBOM") {
			t.Fatalf("verify error = %v", err)
		}
	})
	t.Run("unlicensed package", func(t *testing.T) {
		bundle, rootfs, lock := evidenceFixture(t, "NOASSERTION")
		if err := generate(bundle, lock, testRevision); err != nil {
			t.Fatal(err)
		}
		if err := verify(bundle, rootfs, lock, testRevision); err == nil || !strings.Contains(err.Error(), "no declared license") {
			t.Fatalf("verify error = %v", err)
		}
	})
}

func evidenceFixture(t *testing.T, license string) (bundle, rootfs, lock string) {
	t.Helper()
	directory := t.TempDir()
	bundle = filepath.Join(directory, "bundle")
	rootfs = filepath.Join(directory, "rootfs")
	lock = filepath.Join(directory, "lock.json")
	if err := os.MkdirAll(filepath.Join(rootfs, "lib/apk/db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(rootfs, "lib/apk/db/installed"), "P:busybox\nV:1.0-r0\nL:GPL-2.0-only\n")
	writeTestFile(t, filepath.Join(bundle, "manifest.json"), `{"schema_version":1}`)
	writeTestFile(t, filepath.Join(bundle, "sbom.spdx.json"), `{"spdxVersion":"SPDX-2.3","packages":[{"name":"busybox","versionInfo":"1.0-r0","licenseDeclared":"`+license+`"}]}`)
	writeTestFile(t, lock, `{"packages":{"busybox":"1.0-r0"}}`)
	return bundle, rootfs, lock
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
