//go:build linux

package control

import (
	"strings"
	"testing"
)

func TestBundledSystemImageClassification(t *testing.T) {
	reference := bundledMCPImage + "sha256:" + strings.Repeat("0", 64)
	if !isBundledSystemImage(reference) {
		t.Fatalf("bundled image %q was routed to the registry", reference)
	}
	if isBundledSystemImage("registry.example/appliance@sha256:" + strings.Repeat("0", 64)) {
		t.Fatal("external image was routed to the bundled importer")
	}
}

func TestBundledImportRequestIDChangesWithArtifactInput(t *testing.T) {
	first := bundledImportRequestID(bundledMCPImage+"sha256:"+strings.Repeat("0", 64), []byte("archive-a"))
	second := bundledImportRequestID(bundledMCPImage+"sha256:"+strings.Repeat("1", 64), []byte("archive-a"))
	third := bundledImportRequestID(bundledMCPImage+"sha256:"+strings.Repeat("0", 64), []byte("archive-b"))
	if first == second || first == third {
		t.Fatalf("bundled import request IDs must be unique per artifact input: %q %q %q", first, second, third)
	}
	if len(first) > 63 {
		t.Fatalf("bundled import request ID exceeds request limit: %d", len(first))
	}
}
