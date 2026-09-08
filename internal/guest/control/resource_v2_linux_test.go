//go:build linux

package control

import (
	"os"
	"path/filepath"
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
)

func TestGuestDiskNamespaceCountRejectsUnboundedObservationScan(t *testing.T) {
	names := make([]string, maxGCNamespaces+1)
	if _, err := guestDiskNamespaceCount(names); err == nil {
		t.Fatal("Guest disk accounting accepted an unbounded namespace scan")
	}
	count, err := guestDiskNamespaceCount([]string{runtimeContainerdNamespace, "one", "two"})
	if err != nil || count != 2 {
		t.Fatalf("namespace count=(%d, %v), want (2, nil)", count, err)
	}
}

func TestReleasedTombstoneReplaysUntilExactConfirmation(t *testing.T) {
	root := t.TempDir()
	request := &vmmdv1.ReleaseWorkloadResourcesRequest{AllocationId: "allocation", Generation: 7, NamespaceId: "namespace", ContractSha256: make([]byte, 32)}
	value := &durableReleaseReceipt{AllocationID: request.GetAllocationId(), Generation: request.GetGeneration(), NamespaceID: request.GetNamespaceId(), ContractSHA256: "0000000000000000000000000000000000000000000000000000000000000000", State: "released"}
	if err := persistReleaseReceiptAt(root, request, value); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(releaseReceiptPathAt(root, request)); err != nil {
		t.Fatalf("release response loss lost the durable tombstone: %v", err)
	}
	if err := removeReleaseReceiptAt(root, request); err != nil {
		t.Fatal(err)
	}
	if err := removeReleaseReceiptAt(root, request); err != nil {
		t.Fatalf("lost confirm response was not idempotent: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "reservation-receipts"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("completed lifecycle left receipts=%d err=%v", len(entries), err)
	}
}

func TestManyCompletedReleaseLifecyclesLeaveNoReceipts(t *testing.T) {
	root := t.TempDir()
	for generation := uint64(1); generation <= 512; generation++ {
		request := &vmmdv1.ReleaseWorkloadResourcesRequest{AllocationId: "allocation", Generation: generation, NamespaceId: "namespace", ContractSha256: make([]byte, 32)}
		value := &durableReleaseReceipt{AllocationID: request.GetAllocationId(), Generation: generation, NamespaceID: request.GetNamespaceId(), ContractSHA256: "0000000000000000000000000000000000000000000000000000000000000000", State: "released"}
		if err := persistReleaseReceiptAt(root, request, value); err != nil {
			t.Fatal(err)
		}
		if err := removeReleaseReceiptAt(root, request); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "reservation-receipts"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("completed lifecycles left receipts=%d err=%v", len(entries), err)
	}
}

func TestEveryNamespaceQuotaShapeIsAccounted(t *testing.T) {
	for _, labels := range []map[string]string{
		{labelPrefix + "project-id": "10001", labelPrefix + "disk": "1024"},
		{labelPrefix + "project-id": "10002", reservationLabelPrefix + "writable-quota": "2048"},
	} {
		quota, project, err := accountableNamespaceQuota("namespace", labels)
		if err != nil || quota == 0 || project == 0 {
			t.Fatalf("quota=%d project=%d err=%v", quota, project, err)
		}
	}
	if _, _, err := accountableNamespaceQuota("unaccounted", map[string]string{labelPrefix + "project-id": "10003"}); err == nil {
		t.Fatal("namespace without a quota bypassed the global disk fact")
	}
}

func TestResourceV2ReservationProjectsToRuntimeNamespace(t *testing.T) {
	request := &vmmdv1.ReserveWorkloadResourcesRequest{
		AllocationId: "allocation", Generation: 7, EgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL,
		Resources: &vmmdv1.WorkloadResourcesV2{
			CpuGuaranteeMillis: 1000, CpuMaxMillis: 2000, CpuWeight: 5000,
			MemoryHighBytes: 1536 << 20, MemoryMaxBytes: 2 << 30,
			PidMax: 512, WritableQuotaBytes: 4 << 30,
		},
	}
	labels := workloadReservationLabels(request, 10042, "10.64.42.0/24", make([]byte, 32))
	projectID, namespace, err := namespaceFromLabels("workload", labels)
	if err != nil {
		t.Fatal(err)
	}
	want := &vmmdv1.NamespaceLimits{CpuMillis: 2000, MemoryBytes: 2 << 30, Pids: 512, DiskBytes: 4 << 30}
	if projectID != 10042 || !sameLimits(namespace.GetLimits(), want) || namespace.GetEgressMode() != vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL || namespace.GetSubnet() != "10.64.42.0/24" {
		t.Fatalf("unexpected resource-v2 namespace: %+v", namespace)
	}
}

func TestCgroupContractWritesPressureAndHardIsolationFiles(t *testing.T) {
	root := t.TempDir()
	contract := resourcev2.Contract{CPUMaxMillis: 2000, CPUWeight: 5000, MemoryHighBytes: 1536 << 20, MemoryMaxBytes: 2 << 30, PIDMax: 512}
	if err := applyResourceCgroupAt(root, "workload", contract); err != nil {
		t.Fatal(err)
	}
	for name, expected := range contract.CgroupValues() {
		data, err := os.ReadFile(filepath.Join(root, "agent-vmm", "workload", name))
		if err != nil || string(data) != expected {
			t.Fatalf("%s=%q want=%q err=%v", name, data, expected, err)
		}
	}
}
