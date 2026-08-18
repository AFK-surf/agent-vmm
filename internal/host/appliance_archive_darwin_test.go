//go:build darwin

package host

import (
	"testing"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"google.golang.org/protobuf/proto"
)

func TestApplianceImportRequestIDTracksImmutableInput(t *testing.T) {
	base := &vmmdv1.ImportImageHeader{ActorNamespaceId: "system.appliance", ImageReference: "agent-vmm.local/mcp@sha256:" + repeatedHex('a'), ArchiveSha256: []byte("archive-a"), ManifestDigest: "sha256:" + repeatedHex('b'), Platform: "linux/arm64", ArchiveSize: 1024, SystemImport: true, SystemLimits: &vmmdv1.NamespaceLimits{CpuMillis: 500, MemoryBytes: 128 << 20, Pids: 32, DiskBytes: 1 << 30}, SystemEgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL}
	first, err := applianceImportRequestID(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := applianceImportRequestID(protoCloneImportHeader(base, func(value *vmmdv1.ImportImageHeader) { value.ArchiveSha256 = []byte("archive-b") }))
	if err != nil {
		t.Fatal(err)
	}
	third, err := applianceImportRequestID(protoCloneImportHeader(base, func(value *vmmdv1.ImportImageHeader) { value.SystemLimits.MemoryBytes++ }))
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first == third || len(first) > 63 {
		t.Fatalf("request IDs do not fence input: %q %q %q", first, second, third)
	}
	again, err := applianceImportRequestID(base)
	if err != nil || first != again {
		t.Fatalf("request ID is not stable: %q %v", again, err)
	}
}

func repeatedHex(value byte) string {
	result := make([]byte, 64)
	for index := range result {
		result[index] = value
	}
	return string(result)
}

func protoCloneImportHeader(value *vmmdv1.ImportImageHeader, mutate func(*vmmdv1.ImportImageHeader)) *vmmdv1.ImportImageHeader {
	clone := proto.Clone(value).(*vmmdv1.ImportImageHeader)
	mutate(clone)
	return clone
}
