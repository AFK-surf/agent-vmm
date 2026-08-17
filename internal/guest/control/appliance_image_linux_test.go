//go:build linux

package control

import (
	"strings"
	"testing"
)

func TestBundledSystemImageClassification(t *testing.T) {
	for _, reference := range []string{
		bundledReferenceImage + "sha256:" + strings.Repeat("0", 64),
		bundledMCPImage + "sha256:" + strings.Repeat("0", 64),
	} {
		if !isBundledSystemImage(reference) {
			t.Fatalf("bundled image %q was routed to the registry", reference)
		}
	}
	if isBundledSystemImage("registry.example/appliance@sha256:" + strings.Repeat("0", 64)) {
		t.Fatal("external image was routed to the bundled importer")
	}
}
