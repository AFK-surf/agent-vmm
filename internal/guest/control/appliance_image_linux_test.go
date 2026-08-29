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

func TestBundledImportRequestIDChangesWithCanonicalDigest(t *testing.T) {
	first := bundledImportRequestID(bundledMCPImage + "sha256:" + strings.Repeat("0", 64))
	second := bundledImportRequestID(bundledMCPImage + "sha256:" + strings.Repeat("1", 64))
	if first == second {
		t.Fatalf("bundled import request IDs must be unique per canonical digest: %q %q", first, second)
	}
	if len(first) > 63 {
		t.Fatalf("bundled import request ID exceeds request limit: %d", len(first))
	}
}
