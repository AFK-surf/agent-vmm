//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"io"
	"os"
	"path/filepath"
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

// The server may finish before all archive chunks have been sent. Send's EOF
// does not carry its terminal success or rejection; CloseAndRecv does.
func TestApplianceArchiveReceivesEarlyGuestResult(t *testing.T) {
	for _, at := range []int{1, 2} {
		for _, terminal := range []error{nil, errors.New("system namespace rejected")} {
			t.Run(fmt.Sprintf("send-%d/error-%t", at, terminal != nil), func(t *testing.T) {
				controller := &Controller{supportDirectory: t.TempDir()}
				artifact := store.ApplianceArtifact{CanonicalHash: []byte("test"), Platform: "linux/arm64"}
				path := controller.applianceArchivePath(artifact)
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, make([]byte, 2*applianceArchiveChunkBytes), 0o600); err != nil {
					t.Fatal(err)
				}
				stream := &earlyApplianceResult{eofAt: at, terminal: terminal}
				err := controller.importApplianceArchive(context.Background(), &earlyApplianceClient{stream: stream}, store.ApplianceInstallation{ID: "test"}, artifact, store.ApplianceGrant{})
				if !errors.Is(err, terminal) || stream.sent != at || stream.received != 1 {
					t.Fatalf("result=%v sends=%d receives=%d", err, stream.sent, stream.received)
				}
			})
		}
	}
}

type earlyApplianceClient struct {
	vmmdv1.VMMDServiceClient
	stream *earlyApplianceResult
}

func (client *earlyApplianceClient) ImportImage(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.ImportImageRequest, vmmdv1.Image], error) {
	return client.stream, nil
}

type earlyApplianceResult struct {
	grpc.ClientStream
	eofAt, sent, received int
	terminal              error
}

func (stream *earlyApplianceResult) Send(*vmmdv1.ImportImageRequest) error {
	stream.sent++
	if stream.sent == stream.eofAt {
		return io.EOF
	}
	return nil
}
func (stream *earlyApplianceResult) CloseAndRecv() (*vmmdv1.Image, error) {
	stream.received++
	return &vmmdv1.Image{}, stream.terminal
}
