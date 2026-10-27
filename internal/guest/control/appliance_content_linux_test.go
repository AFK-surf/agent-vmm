//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStageApplianceContentDuringActiveBuild(t *testing.T) {
	requireGuestRoot(t)
	runtime := &Runtime{materializationSlot: semaphore.NewWeighted(1), logger: slog.Default().With("component", "guest-control")}
	err := runtime.materializationSlot.Acquire(context.Background(), 1)
	unlock := func() { runtime.materializationSlot.Release(1) }
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	server := &Server{runtime: runtime}
	root := filepath.Join(stateRoot, "appliance-content")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"cold", "partial", "warm"} {
		t.Run(state, func(t *testing.T) {
			archive := []byte(t.Name())
			sha := sha256.Sum256(archive)
			manifest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			content := &appliancev1.ApplianceContentReference{Role: appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_WORKLOAD, ArchiveSha256: sha[:], ArchiveSize: uint64(len(archive)), Platform: "linux/arm64", ImageReference: "docker.io/library/test@" + manifest, ImageManifestDigest: manifest}
			path := filepath.Join(root, appliancecontent.ArchiveName(content))
			t.Cleanup(func() { os.Remove(path); os.Remove(path + ".pb") })
			if state != "cold" {
				file, err := os.CreateTemp(root, ".stage-test-*")
				if err != nil {
					t.Fatal(err)
				}
				defer os.Remove(file.Name())
				defer file.Close()
				if _, err := file.Write(archive); err != nil {
					t.Fatal(err)
				}
				if state == "warm" {
					if err := appliancecontent.Publish(root, file, content); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Rename(file.Name(), path); err != nil {
					t.Fatal(err)
				}
			}
			stream := &applianceContentStream{header: &appliancev1.StageContentHeader{RequestId: t.Name(), DeadlineUnixMillis: time.Now().Add(100 * time.Millisecond).UnixMilli(), ContentRef: content}}
			err := server.StageApplianceContent(stream)
			if state == "warm" {
				if err != nil || !stream.response.GetAvailable() {
					t.Fatalf("warm content: response=%v err=%v", stream.response, err)
				}
			} else if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("%s content bypassed writer admission: %v", state, err)
			}
		})
	}
}

type applianceContentStream struct {
	grpc.ServerStream
	header   *appliancev1.StageContentHeader
	response *appliancev1.ApplianceContentStatus
}

func (*applianceContentStream) Context() context.Context { return context.Background() }
func (stream *applianceContentStream) Recv() (*vmmdv1.StageApplianceContentRequest, error) {
	if stream.header == nil {
		return nil, io.EOF
	}
	header := stream.header
	stream.header = nil
	return &vmmdv1.StageApplianceContentRequest{Payload: &vmmdv1.StageApplianceContentRequest_Header{Header: header}}, nil
}
func (stream *applianceContentStream) SendAndClose(response *appliancev1.ApplianceContentStatus) error {
	stream.response = response
	return nil
}
