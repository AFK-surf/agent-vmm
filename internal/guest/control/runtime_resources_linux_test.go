//go:build linux

package control

import (
	"strings"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	digest "github.com/opencontainers/go-digest"
)

func TestNamespaceLimitsRequireOnlyPIDAndDiskIsolation(t *testing.T) {
	limits := &vmmdv1.NamespaceLimits{Pids: 512, DiskBytes: 2 << 30}
	if err := validateNamespaceLimits(limits); err != nil {
		t.Fatal(err)
	}
	if err := validateNamespaceLimits(&vmmdv1.NamespaceLimits{DiskBytes: 2 << 30}); err == nil {
		t.Fatal("namespace without a PID limit was accepted")
	}
	if err := validateNamespaceLimits(&vmmdv1.NamespaceLimits{Pids: 512}); err == nil {
		t.Fatal("system namespace without a disk limit was accepted")
	}
}

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
