//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestApplianceImportRequestIDTracksImmutableInput(t *testing.T) {
	base := &vmmdv1.ImportImageHeader{ActorNamespaceId: "system.appliance", ImageReference: "agent-vmm.local/mcp@sha256:" + repeatedHex('a'), ArchiveSha256: []byte("archive-a"), ManifestDigest: "sha256:" + repeatedHex('b'), Platform: "linux/arm64", ArchiveSize: 1024, SystemImport: true, SystemLimits: &vmmdv1.NamespaceLimits{Pids: 32, DiskBytes: 1 << 30}, SystemEgressMode: vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL}
	first, err := applianceImportRequestID(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := applianceImportRequestID(protoCloneImportHeader(base, func(value *vmmdv1.ImportImageHeader) { value.ArchiveSha256 = []byte("archive-b") }))
	if err != nil {
		t.Fatal(err)
	}
	third, err := applianceImportRequestID(protoCloneImportHeader(base, func(value *vmmdv1.ImportImageHeader) { value.SystemLimits.Pids++ }))
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
				controller, installation, artifact := stagedUpdateArchiveFixture(t)
				stream := &earlyApplianceResult{eofAt: at, terminal: terminal}
				err := controller.importApplianceArchive(context.Background(), &earlyApplianceClient{stream: stream}, installation, artifact, store.ApplianceGrant{})
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

type availableApplianceContent struct{ grpc.ClientStream }

func (*availableApplianceContent) Send(*vmmdv1.StageApplianceContentRequest) error { return io.EOF }
func (*availableApplianceContent) CloseAndRecv() (*appliancev1.ApplianceContentStatus, error) {
	return &appliancev1.ApplianceContentStatus{Available: true}, nil
}
func (*earlyApplianceClient) StageApplianceContent(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.StageApplianceContentRequest, appliancev1.ApplianceContentStatus], error) {
	return &availableApplianceContent{}, nil
}

func stagedUpdateArchiveFixture(t *testing.T) (*Owner, store.ApplianceInstallation, store.ApplianceArtifact) {
	t.Helper()
	controller, installation := updateDrainFixture(t)
	controller.supportDirectory = t.TempDir()
	data := make([]byte, 2*applianceArchiveChunkBytes)
	hash := sha256.Sum256(data)
	digest := "sha256:" + repeatedHex('a')
	content := &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER, ArchiveSha256: hash[:], ArchiveSize: uint64(len(data)), Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + digest, ImageManifestDigest: digest}
	encoded, err := proto.Marshal(&appliancev1.ControllerApplianceManifest{ContentRefs: []*appliancev1.ApplianceContentReference{content}})
	if err != nil {
		t.Fatal(err)
	}
	artifact := store.ApplianceArtifact{ID: "target", ImageDigest: digest, ImageReference: content.GetImageReference(), Platform: "linux/arm64", Manifest: encoded}
	root := controller.applianceContentRoot()
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(root, "stage-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := appliancecontent.Publish(root, file, content); err != nil {
		t.Fatal(err)
	}
	return controller, installation, artifact
}

func TestApplianceArchiveHopsUseBoundedGuestChunks(t *testing.T) {
	controller, installation, artifact := stagedUpdateArchiveFixture(t)
	client := &recordingApplianceArchiveClient{importStream: new(recordingApplianceImport), contentStream: new(recordingApplianceStage)}
	if err := controller.importApplianceArchive(context.Background(), client, installation, artifact, store.ApplianceGrant{}); err != nil {
		t.Fatal(err)
	}
	refs, err := artifactContentReferences(artifact)
	if err != nil {
		t.Fatal(err)
	}
	for hop, chunks := range map[string][]int{"content": client.contentStream.chunks, "import": client.importStream.chunks} {
		var total int
		for _, size := range chunks {
			if size <= 0 || size > vmmdv1.MaxImportArchiveChunkBytes {
				t.Fatalf("%s chunk exceeds Guest limit: %d", hop, size)
			}
			total += size
		}
		wantChunks := (int(refs[0].GetArchiveSize()) + vmmdv1.MaxImportArchiveChunkBytes - 1) / vmmdv1.MaxImportArchiveChunkBytes
		if total != int(refs[0].GetArchiveSize()) || len(chunks) != wantChunks {
			t.Fatalf("%s archive: bytes=%d chunks=%v", hop, total, chunks)
		}
		t.Logf("%s: cached archive bytes=%d chunks=%d prior_1MiB_chunks=%d", hop, total, len(chunks), (total+applianceArchiveChunkBytes-1)/applianceArchiveChunkBytes)
	}
}

type recordingApplianceArchiveClient struct {
	vmmdv1.VMMDServiceClient
	importStream  *recordingApplianceImport
	contentStream *recordingApplianceStage
}

func (c *recordingApplianceArchiveClient) ImportImage(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.ImportImageRequest, vmmdv1.Image], error) {
	return c.importStream, nil
}
func (c *recordingApplianceArchiveClient) StageApplianceContent(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[vmmdv1.StageApplianceContentRequest, appliancev1.ApplianceContentStatus], error) {
	return c.contentStream, nil
}

type recordingApplianceImport struct {
	grpc.ClientStream
	chunks []int
}

func (s *recordingApplianceImport) Send(frame *vmmdv1.ImportImageRequest) error {
	if data := frame.GetArchiveChunk(); len(data) != 0 {
		s.chunks = append(s.chunks, len(data))
	}
	return nil
}
func (*recordingApplianceImport) CloseAndRecv() (*vmmdv1.Image, error) { return new(vmmdv1.Image), nil }

type recordingApplianceStage struct {
	grpc.ClientStream
	chunks []int
}

func (s *recordingApplianceStage) Send(frame *vmmdv1.StageApplianceContentRequest) error {
	if data := frame.GetArchiveChunk(); len(data) != 0 {
		s.chunks = append(s.chunks, len(data))
	}
	return nil
}
func (*recordingApplianceStage) CloseAndRecv() (*appliancev1.ApplianceContentStatus, error) {
	return &appliancev1.ApplianceContentStatus{Available: true}, nil
}
