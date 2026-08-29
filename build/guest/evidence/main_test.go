package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEvidenceAcceptsCurrentFiles(t *testing.T) {
	bundle, rootfs := evidenceFixture(t, "MIT")
	if err := verifySBOM(filepath.Join(bundle, "sbom.spdx.json"), filepath.Join(rootfs, "lib/apk/db/installed")); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceRejectsIncompleteLicenseInventory(t *testing.T) {
	t.Run("installed package absent", func(t *testing.T) {
		bundle, rootfs := evidenceFixture(t, "MIT")
		writeTestFile(t, filepath.Join(bundle, "sbom.spdx.json"), `{"spdxVersion":"SPDX-2.3","packages":[{"name":"other","versionInfo":"1","licenseDeclared":"MIT"}]}`)
		if err := verifySBOM(filepath.Join(bundle, "sbom.spdx.json"), filepath.Join(rootfs, "lib/apk/db/installed")); err == nil || !strings.Contains(err.Error(), "missing from the SBOM") {
			t.Fatalf("verify error = %v", err)
		}
	})
	t.Run("unlicensed package", func(t *testing.T) {
		bundle, rootfs := evidenceFixture(t, "NOASSERTION")
		if err := verifySBOM(filepath.Join(bundle, "sbom.spdx.json"), filepath.Join(rootfs, "lib/apk/db/installed")); err == nil || !strings.Contains(err.Error(), "no declared license") {
			t.Fatalf("verify error = %v", err)
		}
	})
}

func evidenceFixture(t *testing.T, license string) (bundle, rootfs string) {
	t.Helper()
	directory := t.TempDir()
	bundle = filepath.Join(directory, "bundle")
	rootfs = filepath.Join(directory, "rootfs")
	if err := os.MkdirAll(filepath.Join(rootfs, "lib/apk/db"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(rootfs, "lib/apk/db/installed"), "P:busybox\nV:1.0-r0\nL:GPL-2.0-only\n")
	writeTestFile(t, filepath.Join(bundle, "sbom.spdx.json"), `{"spdxVersion":"SPDX-2.3","packages":[{"name":"busybox","versionInfo":"1.0-r0","licenseDeclared":"`+license+`"}]}`)
	return bundle, rootfs
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
