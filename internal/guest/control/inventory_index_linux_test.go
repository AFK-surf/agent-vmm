//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	corecontainers "github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/errdefs"
)

func TestNamespaceExistenceRejectsAnUnindexedNamespace(t *testing.T) {
	runtime := &Runtime{inventory: newRuntimeInventoryIndex()}
	if err := runtime.namespaceExistsRaw(context.Background(), "missing"); !errors.Is(err, errdefs.ErrNotFound) {
		t.Fatalf("namespace existence error = %v", err)
	}
}

func TestBoundedRuntimeInventoryRejectsOverflowBeforeExactLoads(t *testing.T) {
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("namespace-a")
	for count := 0; count < 10_000; count++ {
		id := fmt.Sprintf("container-%05d", count)
		if err := index.addContainer("namespace-a", id); err != nil {
			t.Fatal(err)
		}
		if err := index.addImage("namespace-a", "registry.example/image-"+id, "sha256:"+id); err != nil {
			t.Fatal(err)
		}
	}
	loads := 0
	for name, identities := range map[string]func() ([]string, error){
		"containers": func() ([]string, error) { return index.containerIDs("namespace-a", 101) },
		"images":     func() ([]string, error) { return index.imageReferences("namespace-a", 101) },
	} {
		values, err := identities()
		if !errors.Is(err, errResourceExhausted) || values != nil {
			t.Fatalf("%s overflow values=%v err=%v", name, values, err)
		}
		if err == nil {
			_, _ = indexedInventoryLoad(values, func(value string) (string, error) {
				loads++
				return value, nil
			})
		}
	}
	if loads != 0 {
		t.Fatalf("overflow performed %d exact loads", loads)
	}
}

func TestBoundedRuntimeInventoryAllowsExactlyOneHundredInStableOrder(t *testing.T) {
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("namespace-a")
	for count := 99; count >= 0; count-- {
		id := fmt.Sprintf("container-%03d", count)
		if err := index.addContainer("namespace-a", id); err != nil {
			t.Fatal(err)
		}
	}
	identities, err := index.containerIDs("namespace-a", 101)
	if err != nil {
		t.Fatal(err)
	}
	loads := 0
	values, err := indexedInventoryLoad(identities, func(value string) (string, error) {
		loads++
		return value, nil
	})
	if err != nil || len(values) != 100 || loads != 100 || values[0] != "container-000" || values[99] != "container-099" {
		t.Fatalf("values=%d loads=%d first=%q last=%q err=%v", len(values), loads, values[0], values[len(values)-1], err)
	}
	if values, err := index.containerIDs("namespace-a", 1); !errors.Is(err, errResourceExhausted) || values != nil {
		t.Fatalf("limit-one overflow values=%v err=%v", values, err)
	}
}

func TestRuntimeInventoryExactLoadFailsClosed(t *testing.T) {
	want := errors.New("authoritative record is missing")
	values, err := indexedInventoryLoad([]string{"a", "b"}, func(value string) (string, error) {
		if value == "b" {
			return "", want
		}
		return value, nil
	})
	if !errors.Is(err, want) || values != nil {
		t.Fatalf("values=%v err=%v", values, err)
	}
}

func TestRuntimeInventoryContainerValidationRejectsUnknownAndPartialRecords(t *testing.T) {
	known := map[string]bool{"namespace-a": true}
	valid := corecontainers.Container{ID: runtimeContainerKey("namespace-a", "main"), Labels: map[string]string{
		runtimeOwnerLabel: "namespace-a", runtimeExternalIDLabel: "main",
	}}
	owner, externalID, internal, err := validateRuntimeInventoryContainer(valid, known)
	if err != nil || owner != "namespace-a" || externalID != "main" || internal {
		t.Fatalf("valid record owner=%q external=%q internal=%t err=%v", owner, externalID, internal, err)
	}
	tests := []corecontainers.Container{
		{ID: "unowned", Labels: map[string]string{}},
		{ID: "partial", Labels: map[string]string{runtimeOwnerLabel: "namespace-a"}},
		{ID: runtimeContainerKey("missing", "main"), Labels: map[string]string{runtimeOwnerLabel: "missing", runtimeExternalIDLabel: "main"}},
		{ID: "wrong-key", Labels: map[string]string{runtimeOwnerLabel: "namespace-a", runtimeExternalIDLabel: "main"}},
		{ID: "internal-owned", Labels: map[string]string{guestbuild.InternalContainerLabel: "true", runtimeOwnerLabel: "namespace-a", runtimeExternalIDLabel: "main"}},
	}
	for _, test := range tests {
		if _, _, _, err := validateRuntimeInventoryContainer(test, known); err == nil || !strings.Contains(err.Error(), test.ID) {
			t.Fatalf("record %q err=%v", test.ID, err)
		}
	}
	internalRecord := corecontainers.Container{ID: "builder", Labels: map[string]string{guestbuild.InternalContainerLabel: "true"}}
	if _, _, internal, err := validateRuntimeInventoryContainer(internalRecord, known); err != nil || !internal {
		t.Fatalf("internal=%t err=%v", internal, err)
	}
}

func TestRuntimeInventoryTracksPartialDeletion(t *testing.T) {
	index := newRuntimeInventoryIndex()
	index.ensureNamespace("namespace-a")
	if err := index.addContainer("namespace-a", "container-a"); err != nil {
		t.Fatal(err)
	}
	if err := index.addImage("namespace-a", "image-a", "sha256:a"); err != nil {
		t.Fatal(err)
	}
	if err := index.removeNamespace("namespace-a"); err == nil {
		t.Fatal("non-empty namespace was removed from the index")
	}
	if err := index.removeContainer("namespace-a", "container-a"); err != nil {
		t.Fatal(err)
	}
	if err := index.removeImage("namespace-a", "image-a"); err != nil {
		t.Fatal(err)
	}
	if err := index.removeNamespace("namespace-a"); err != nil {
		t.Fatal(err)
	}
	if index.hasNamespace("namespace-a") {
		t.Fatal("deleted namespace remains indexed")
	}
}

func TestRuntimeInventoryTracksDigestReferences(t *testing.T) {
	index := newRuntimeInventoryIndex()
	for _, namespaceID := range []string{"namespace-c", "namespace-a", "namespace-b"} {
		index.ensureNamespace(namespaceID)
	}
	if err := index.addImage("namespace-a", "image-a", "sha256:shared"); err != nil {
		t.Fatal(err)
	}
	if err := index.addImage("namespace-b", "image-b", "sha256:shared"); err != nil {
		t.Fatal(err)
	}
	if !index.imageDigestReferenced("sha256:shared") {
		t.Fatal("shared digest is not retained")
	}
	if err := index.removeImage("namespace-a", "image-a"); err != nil {
		t.Fatal(err)
	}
	if !index.imageDigestReferenced("sha256:shared") {
		t.Fatal("one logical reference removed the shared digest")
	}
	if err := index.removeImage("namespace-b", "image-b"); err != nil {
		t.Fatal(err)
	}
	if index.imageDigestReferenced("sha256:shared") {
		t.Fatal("unreferenced digest remains retained")
	}
}

func TestRuntimeInventoryPagesPhysicalImagesWithoutATotalGate(t *testing.T) {
	index := newRuntimeInventoryIndex()
	for item := 256; item >= 0; item-- {
		index.addRuntimeImage(fmt.Sprintf("runtime-image-%03d", item))
	}
	first := index.runtimeImagePage("", 32)
	if len(first) != 32 || first[0] != "runtime-image-000" || first[31] != "runtime-image-031" {
		t.Fatalf("first runtime image page = %v", first)
	}
	second := index.runtimeImagePage(first[len(first)-1], 32)
	if len(second) != 32 || second[0] != "runtime-image-032" || second[31] != "runtime-image-063" {
		t.Fatalf("second runtime image page = %v", second)
	}
	index.removeRuntimeImage("runtime-image-032")
	if next := index.runtimeImagePage(first[len(first)-1], 1); len(next) != 1 || next[0] != "runtime-image-033" {
		t.Fatalf("runtime image page after deletion = %v", next)
	}
}

func TestRuntimeInventoryRebuildIndexesRawLegacyRecords(t *testing.T) {
	container := corecontainers.Container{ID: runtimeContainerKey("namespace-a", "stopped"), Labels: map[string]string{
		runtimeOwnerLabel: "namespace-a", runtimeExternalIDLabel: "stopped",
	}}
	index, err := buildRuntimeInventoryIndex(
		map[string]bool{"namespace-a": true, "namespace-b": true},
		[]corecontainers.Container{container, {ID: "builder", Labels: map[string]string{guestbuild.InternalContainerLabel: "true"}}},
		map[string]map[string]string{"namespace-a": {"registry.example/materialized": "sha256:a", "registry.example/interrupted": "sha256:b"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	containers, err := index.containerIDs("namespace-a", 101)
	if err != nil || len(containers) != 1 || containers[0] != "stopped" {
		t.Fatalf("containers=%v err=%v", containers, err)
	}
	images, err := index.imageReferences("namespace-a", 101)
	if err != nil || len(images) != 2 || images[0] != "registry.example/interrupted" || images[1] != "registry.example/materialized" {
		t.Fatalf("images=%v err=%v", images, err)
	}
	if _, err := buildRuntimeInventoryIndex(map[string]bool{"namespace-a": true}, nil, map[string]map[string]string{"missing": {"image": "sha256:a"}}); err == nil {
		t.Fatal("images for an unknown namespace were indexed")
	}
	if _, err := buildRuntimeInventoryIndex(map[string]bool{"namespace-a": true}, nil, map[string]map[string]string{"namespace-a": {"": "sha256:a"}}); err == nil {
		t.Fatal("unnamed image record was indexed")
	}
}
