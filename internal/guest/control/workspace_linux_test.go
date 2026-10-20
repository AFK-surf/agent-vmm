//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"fmt"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestVolumeListingIsCompleteBoundedAndNoFollow(t *testing.T) {
	requireGuestRoot(t)
	namespace := fmt.Sprintf("capture-%d", time.Now().UnixNano())
	volume := "workspace"
	root := volumePath(namespace, volume)
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	server := &Server{runtime: &Runtime{workspaceSlots: make(chan struct{}, 1)}}
	for i := 0; i < 1001; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%04d", i)), nil, 0644); err != nil {
			t.Fatal(err)
		}
	}
	request := &vmmdv1.ListWorkspaceRequest{ActorNamespaceId: namespace, VolumeId: volume, Path: ".", Limit: 4096}
	response, err := server.ListWorkspace(context.Background(), request)
	if err != nil || len(response.GetEntries()) != 1001 {
		t.Fatalf("full listing: %d %v", len(response.GetEntries()), err)
	}
	request.Limit = 1000
	if _, err := server.ListWorkspace(context.Background(), request); err == nil {
		t.Fatal("directory overflow was silently truncated")
	}
	request.Limit = 4096
	for _, kind := range []string{"symlink", "fifo"} {
		name := filepath.Join(root, "unsupported")
		if kind == "symlink" {
			err = os.Symlink("file-0000", name)
		} else {
			err = unix.Mkfifo(name, 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := server.ListWorkspace(context.Background(), request); err == nil {
			t.Fatalf("%s was accepted", kind)
		}
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	request.ActorNamespaceId = namespace + "-other"
	if _, err := server.ListWorkspace(context.Background(), request); err == nil {
		t.Fatal("another namespace read the volume")
	}
}

func TestUploadRequestIdentityKeepsResumableStagingWithinParent(t *testing.T) {
	parent := t.TempDir()
	for _, requestID := range []string{"owner:jobs", strings.Repeat("a", 256), "../../outside/child"} {
		name := workspaceUploadStagingName("result", requestID)
		if filepath.Base(name) != name {
			t.Fatalf("request identity escaped staging parent: %q", name)
		}
		target := filepath.Join(parent, name)
		if err := os.WriteFile(target, []byte("prefix"), 0600); err != nil {
			t.Fatal(err)
		}
		replay := filepath.Join(parent, workspaceUploadStagingName("result", requestID))
		if bytes, err := os.ReadFile(replay); err != nil || string(bytes) != "prefix" {
			t.Fatalf("resume staging: %q %v", bytes, err)
		}
		if name == workspaceUploadStagingName("result", requestID+":other") {
			t.Fatal("distinct request identities shared staging")
		}
	}
}

func TestWriteWorkspaceCreatesOwnedParentsAndResumesNestedUpload(t *testing.T) {
	requireGuestRoot(t)
	namespace := fmt.Sprintf("nested-write-%d", time.Now().UnixNano())
	volume := "source"
	root := volumePath(namespace, volume)
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chown(root, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	server := &Server{runtime: &Runtime{workspaceSlots: make(chan struct{}, 1)}}
	write := func(requestID, path string, offset uint64, prefix, content []byte, finish bool) (*workspaceWriteStream, error) {
		messages := []*vmmdv1.WriteWorkspaceRequest{{Payload: &vmmdv1.WriteWorkspaceRequest_Header{Header: &vmmdv1.WriteWorkspaceHeader{RequestId: requestID, ActorNamespaceId: namespace, VolumeId: volume, Path: path, Offset: offset, ExpectedPrefixSha256: prefix, Mode: 0644}}}, {Payload: &vmmdv1.WriteWorkspaceRequest_Data{Data: content}}}
		if finish {
			messages = append(messages, &vmmdv1.WriteWorkspaceRequest{Payload: &vmmdv1.WriteWorkspaceRequest_Finish{Finish: true}})
		}
		stream := &workspaceWriteStream{messages: messages}
		return stream, server.WriteWorkspace(stream)
	}
	path := ".gitea/workflows/agentd-ci.yaml"
	if _, err := write("source:0", path, 0, nil, []byte("prefix"), false); err == nil {
		t.Fatal("unfinished upload reported success")
	}
	prefix := sha256.Sum256([]byte("prefix"))
	if _, err := write("source:0", path, 5, prefix[:], []byte("bad"), true); err == nil {
		t.Fatal("wrong resume offset accepted")
	}
	wrong := sha256.Sum256([]byte("wrong"))
	if _, err := write("source:0", path, 6, wrong[:], []byte("bad"), true); err == nil {
		t.Fatal("wrong resume prefix accepted")
	}
	stream, err := write("source:0", path, 6, prefix[:], []byte("-complete"), true)
	if err != nil || stream.response.GetEntry().GetPath() != path {
		t.Fatalf("nested resume: %+v %v", stream.response, err)
	}
	if bytes, err := os.ReadFile(filepath.Join(root, path)); err != nil || string(bytes) != "prefix-complete" {
		t.Fatalf("nested content: %q %v", bytes, err)
	}
	for _, path := range []string{".gitea", ".gitea/workflows", path} {
		var stat unix.Stat_t
		if err := unix.Stat(filepath.Join(root, path), &stat); err != nil || stat.Uid != 1000 || stat.Gid != 1000 {
			t.Fatalf("volume ownership %s: %+v %v", path, stat, err)
		}
	}
	if _, err := write("source:1", ".gitea/workflows/next.yaml", 0, nil, []byte("next"), true); err != nil {
		t.Fatalf("existing parent: %v", err)
	}
	if _, err := write("source:2", "../../escape", 0, nil, []byte("escape"), true); err == nil {
		t.Fatal("path escape accepted")
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := write("source:3", "link/subdir/escape", 0, nil, []byte("escape"), true); err == nil {
		t.Fatal("symlink parent accepted")
	}
	if _, err := os.Stat(filepath.Join(outside, "subdir")); !os.IsNotExist(err) {
		t.Fatalf("outside directory changed: %v", err)
	}
}

type workspaceWriteStream struct {
	grpc.ServerStream
	messages []*vmmdv1.WriteWorkspaceRequest
	response *vmmdv1.WriteWorkspaceResponse
}

func (stream *workspaceWriteStream) Context() context.Context { return context.Background() }
func (stream *workspaceWriteStream) Recv() (*vmmdv1.WriteWorkspaceRequest, error) {
	if len(stream.messages) == 0 {
		return nil, io.EOF
	}
	next := stream.messages[0]
	stream.messages = stream.messages[1:]
	return next, nil
}
func (stream *workspaceWriteStream) SendAndClose(response *vmmdv1.WriteWorkspaceResponse) error {
	stream.response = response
	return nil
}

type generatedWorkspaceWriteStream struct {
	workspaceWriteStream
	header    *vmmdv1.WriteWorkspaceHeader
	remaining uint64
	chunk     []byte
	finished  bool
}

func (stream *generatedWorkspaceWriteStream) Recv() (*vmmdv1.WriteWorkspaceRequest, error) {
	if stream.header != nil {
		header := stream.header
		stream.header = nil
		return &vmmdv1.WriteWorkspaceRequest{Payload: &vmmdv1.WriteWorkspaceRequest_Header{Header: header}}, nil
	}
	if stream.remaining != 0 {
		size := uint64(len(stream.chunk))
		if size > stream.remaining {
			size = stream.remaining
		}
		stream.remaining -= size
		return &vmmdv1.WriteWorkspaceRequest{Payload: &vmmdv1.WriteWorkspaceRequest_Data{Data: stream.chunk[:size]}}, nil
	}
	if !stream.finished {
		stream.finished = true
		return &vmmdv1.WriteWorkspaceRequest{Payload: &vmmdv1.WriteWorkspaceRequest_Finish{Finish: true}}, nil
	}
	return nil, io.EOF
}
func TestWriteWorkspaceStreamsBeyondOldFileCeiling(t *testing.T) {
	requireGuestRoot(t)
	namespace := fmt.Sprintf("large-write-%d", time.Now().UnixNano())
	volume := "source"
	root := volumePath(namespace, volume)
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	server := &Server{runtime: &Runtime{workspaceSlots: make(chan struct{}, 1)}}
	const total = 257 << 20
	chunk := make([]byte, maxWorkspaceChunk)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	stream := &generatedWorkspaceWriteStream{header: &vmmdv1.WriteWorkspaceHeader{RequestId: "large-pack", ActorNamespaceId: namespace, VolumeId: volume, Path: ".git/objects/pack/history.pack", Mode: 0644}, remaining: total, chunk: chunk}
	if err := server.WriteWorkspace(stream); err != nil {
		t.Fatal(err)
	}
	if stream.response.GetEntry().GetSize() != total {
		t.Fatalf("size: %d", stream.response.GetEntry().GetSize())
	}
	digest := sha256.New()
	for i := 0; i < total/maxWorkspaceChunk; i++ {
		_, _ = digest.Write(chunk)
	}
	if !equalDigest(stream.response.GetSha256(), digest.Sum(nil)) {
		t.Fatal("stream digest mismatch")
	}
	file, err := os.Open(filepath.Join(root, ".git/objects/pack/history.pack"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	verified := sha256.New()
	if _, err := io.Copy(verified, file); err != nil {
		t.Fatal(err)
	}
	if !equalDigest(stream.response.GetSha256(), verified.Sum(nil)) {
		t.Fatal("file digest mismatch")
	}
}
