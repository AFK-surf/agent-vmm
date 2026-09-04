//go:build linux

package control

import (
	"strings"
	"testing"

	digest "github.com/opencontainers/go-digest"
)

func TestRuntimeResourceKeysFenceLogicalNamespaces(t *testing.T) {
	first := runtimeContainerKey("workload-a", "runtime")
	second := runtimeContainerKey("workload-b", "runtime")
	if first == second {
		t.Fatal("same external container ID collided across logical namespaces")
	}
	if first != runtimeContainerKey("workload-a", "runtime") {
		t.Fatal("runtime key is not deterministic")
	}
	if strings.Contains(first, "workload-a") {
		t.Fatal("runtime key exposes caller-owned namespace text")
	}
	if runtimeSnapshotKey("workload-a", "runtime") == first {
		t.Fatal("container and snapshot resource kinds collided")
	}
}

func TestRuntimeImageIdentityIsDigestOwned(t *testing.T) {
	value := digest.FromString("shared-content")
	first := runtimeImageReference(value)
	second := runtimeImageReference(value)
	if first != second || !strings.Contains(first, value.String()) {
		t.Fatalf("runtime image identity = %q, %q", first, second)
	}
	if first == runtimeImageReference(digest.FromString("other-content")) {
		t.Fatal("different content digests collided")
	}
}
