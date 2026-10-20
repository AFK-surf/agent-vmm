//go:build linux

package control

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestShutdownCancelsFilesystemRPCsBeforeFirstFrame(t *testing.T) {
	for _, method := range []string{
		vmmdv1.VMMDService_BuildImage_FullMethodName,
		vmmdv1.VMMDService_BuildAppliance_FullMethodName,
		vmmdv1.VMMDService_ImportImage_FullMethodName,
		vmmdv1.VMMDService_StageApplianceContent_FullMethodName,
		vmmdv1.VMMDService_WriteWorkspace_FullMethodName,
		vmmdv1.VMMDService_ExtractWorkspace_FullMethodName,
		vmmdv1.VMMDService_Exec_FullMethodName,
	} {
		t.Run(method, func(t *testing.T) {
			server := &Server{}
			listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "vmmd.sock"))
			if err != nil {
				t.Fatal(err)
			}
			rpc := NewGRPCServer()
			vmmdv1.RegisterVMMDServiceServer(rpc, server)
			go rpc.Serve(listener)
			defer rpc.Stop()
			connection, err := grpc.NewClient("unix://"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := connection.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
			if err != nil {
				t.Fatal(err)
			}
			for {
				server.operationMu.Lock()
				accepted := server.streamContext != nil
				server.operationMu.Unlock()
				if accepted {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("stream was not admitted")
				}
				time.Sleep(time.Millisecond)
			}
			if err := server.beginShutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if err := stream.RecvMsg(new(emptypb.Empty)); err == nil {
				t.Fatal("shutdown left idle filesystem stream open")
			}
			stream, err = connection.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.RecvMsg(new(emptypb.Empty)); status.Code(err) != codes.Unavailable {
				t.Fatalf("new operation after shutdown: %v", err)
			}
		})
	}
}

func TestShutdownClosesWorkspaceStreamFileDescriptors(t *testing.T) {
	requireGuestRoot(t)
	for _, kind := range []string{"write", "read", "archive"} {
		t.Run(kind, func(t *testing.T) {
			namespace := fmt.Sprintf("shutdown-stream-%d", time.Now().UnixNano())
			root := workspacePath(namespace)
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(root)
			path := filepath.Join(root, "large")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(16 << 20); err != nil {
				t.Fatal(err)
			}
			file.Close()
			server := &Server{runtime: &Runtime{workspaceSlots: make(chan struct{}, 1)}}
			listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "vmmd.sock"))
			if err != nil {
				t.Fatal(err)
			}
			rpc := NewGRPCServer()
			vmmdv1.RegisterVMMDServiceServer(rpc, server)
			go rpc.Serve(listener)
			defer rpc.Stop()
			connection, err := grpc.NewClient("unix://"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client := vmmdv1.NewVMMDServiceClient(connection)
			switch kind {
			case "write":
				stream, err := client.WriteWorkspace(ctx)
				if err != nil {
					t.Fatal(err)
				}
				err = stream.Send(&vmmdv1.WriteWorkspaceRequest{Payload: &vmmdv1.WriteWorkspaceRequest_Header{Header: &vmmdv1.WriteWorkspaceHeader{RequestId: "upload", ActorNamespaceId: namespace, Path: "new"}}})
				if err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(root, workspaceUploadStagingName("new", "upload"))
			case "read":
				if _, err := client.ReadWorkspace(ctx, &vmmdv1.ReadWorkspaceRequest{ActorNamespaceId: namespace, Path: "large"}); err != nil {
					t.Fatal(err)
				}
			case "archive":
				if _, err := client.ArchiveWorkspace(ctx, &vmmdv1.ArchiveWorkspaceRequest{ActorNamespaceId: namespace, Path: "."}); err != nil {
					t.Fatal(err)
				}
			}
			for !shutdownHasOpenFile(t, path) {
				if ctx.Err() != nil {
					t.Fatal("stream did not open its file")
				}
				time.Sleep(time.Millisecond)
			}
			if err := server.beginShutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if shutdownHasOpenFile(t, path) {
				t.Fatal("file descriptor survived shutdown operation settlement")
			}
			if len(server.runtime.workspaceSlots) != 0 {
				t.Fatal("workspace operation survived shutdown")
			}
		})
	}
}

func shutdownHasOpenFile(t *testing.T, path string) bool {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		target, _ := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if strings.TrimSuffix(target, " (deleted)") == path {
			return true
		}
	}
	return false
}

func TestShutdownCancelsAcceptedBuildUploadAndClosesWorkspace(t *testing.T) {
	requireGuestRoot(t)
	server, metadata, _, _, _ := operationFixture(t)
	metadata.labels["owner"] = namespaceLabels(&vmmdv1.NamespaceLimits{CpuMillis: 1000, MemoryBytes: 1 << 30, Pids: 100, DiskBytes: 1 << 30}, vmmdv1.EgressMode_EGRESS_MODE_DENY_ALL, 100, "10.42.1.0/24")
	server.runtime.builder = guestbuild.NewBuilder(server.runtime.client, nil, nil)
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "vmmd.sock"))
	if err != nil {
		t.Fatal(err)
	}
	rpc := NewGRPCServer()
	vmmdv1.RegisterVMMDServiceServer(rpc, server)
	go rpc.Serve(listener)
	defer rpc.Stop()
	connection, err := grpc.NewClient("unix://"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := vmmdv1.NewVMMDServiceClient(connection).BuildImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = stream.Send(&vmmdv1.BuildImageRequest{Event: &vmmdv1.BuildImageRequest_Header{Header: &vmmdv1.BuildImageHeader{RequestId: "shutdown-build", ActorNamespaceId: "owner", DockerfilePath: "Dockerfile", Image: "image:test", NetworkMode: vmmdv1.BuildNetworkMode_BUILD_NETWORK_MODE_NONE}}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil || response.GetProgress() != "receiving context" {
		t.Fatalf("accepted upload: %v %v", response, err)
	}
	var run *imageOperationRun
	server.runtime.imageOperations.Range(func(_, value any) bool { run = value.(*imageOperationRun); return false })
	if run == nil {
		t.Fatal("build was not registered")
	}
	receipt, err := server.runtime.imageOperation(ctx, "owner", "shutdown-build", vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join("/state/build-tmp/owner", receipt.RuntimeInstanceId)
	if !shutdownHasOpenFile(t, filepath.Join(path, "context.tar")) {
		t.Fatal("build upload did not hold context FD")
	}
	if err := server.beginShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.settleImageOperations(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("build workspace retained: %v", err)
	}
	receipt, err = server.ObserveImageOperation(ctx, run.request)
	if err != nil || !receipt.WriterSettled || receipt.State != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED {
		t.Fatalf("build writer not settled: %v %v", receipt, err)
	}
	if err := server.runtime.builder.CanDeleteNamespace("owner"); err != nil {
		t.Fatal(err)
	}
}
