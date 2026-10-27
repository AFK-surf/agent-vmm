//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type buildFailureServer struct {
	*Server
	failure error
	output  string
	finish  <-chan struct{}
}

func (server *buildFailureServer) BuildImage(stream grpc.BidiStreamingServer[vmmdv1.BuildImageRequest, vmmdv1.BuildImageResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&vmmdv1.BuildImageResponse{Event: &vmmdv1.BuildImageResponse_Progress{Progress: server.output}}); err != nil {
		return err
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return rpcError(server.failure)
}

func TestBuildFailureTrailersPreserveProgressAndReceiptCause(t *testing.T) {
	const finalCause = "mv: cannot move registry/cache: Directory not empty"
	output := strings.Repeat("Vendoring registry crate 容量\n", 1024) + finalCause
	output = output[len(output)-(16<<10):]
	failure := &guestbuild.ExitError{ExitCode: 1, OutputTail: output}
	wrapped := fmt.Errorf("builder execution: %w; runsc fatal: runtime diagnostic", failure)
	server, _, _, receipt, request := operationFixture(t)
	receipt.Kind, request.Kind = vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run, _, err := server.runtime.acceptImageOperation(ctx, "owner", receipt, "", func() {})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.runtime.finishImageOperation("owner", receipt, run, wrapped, nil, false); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		failure error
		reset   bool
	}{
		{name: "old untyped output exceeds peer headers", failure: errors.New(wrapped.Error()), reset: true},
		{name: "build failure has short wire summary", failure: wrapped},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			transport := grpc.NewServer()
			finish := make(chan struct{})
			vmmdv1.RegisterVMMDServiceServer(transport, &buildFailureServer{Server: server, failure: test.failure, output: output, finish: finish})
			go transport.Serve(listener)
			defer transport.Stop()
			defer listener.Close()
			// Hyper, used by the controller's tonic Channel, advertises 16 KiB.
			connection, err := grpc.NewClient("passthrough:///build-failure", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithMaxHeaderListSize(16<<10), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			client := vmmdv1.NewVMMDServiceClient(connection)
			stream, err := client.BuildImage(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&vmmdv1.BuildImageRequest{Event: &vmmdv1.BuildImageRequest_Header{Header: &vmmdv1.BuildImageHeader{RequestId: receipt.RequestId}}}); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			progress, err := stream.Recv()
			if err != nil || progress.GetProgress() != output {
				t.Fatalf("progress lost final stderr: bytes=%d err=%v", len(progress.GetProgress()), err)
			}
			close(finish)
			_, err = stream.Recv()
			if status.Code(err) != codes.Internal {
				t.Fatalf("failure code changed: %v", err)
			}
			if test.reset {
				if !strings.Contains(err.Error(), "RST_STREAM") {
					t.Fatalf("long status did not reproduce stream reset: %v", err)
				}
				return
			}
			actual := status.Convert(err)
			if !strings.Contains(actual.Message(), "exit code 1") || !strings.Contains(actual.Message(), "image operation receipt") || strings.Contains(actual.Message(), finalCause) {
				t.Fatalf("trailer is not a short build summary: %v", err)
			}
			if len(actual.Details()) != 0 {
				t.Fatalf("uncatalogued build failure has wire details: %v", actual.Details())
			}
			observed, err := client.ObserveImageOperation(ctx, request)
			if err != nil || observed.GetRequestId() != receipt.RequestId || observed.GetState() != vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED || !observed.GetWriterSettled() || !strings.Contains(observed.GetStatus().GetMessage(), finalCause) || !strings.Contains(observed.GetStatus().GetMessage(), "runsc fatal") {
				t.Fatalf("body receipt lost settled identity/final cause: %v err=%v", observed, err)
			}
		})
	}
	if !strings.Contains(failure.Error(), output) {
		t.Fatal("wire mapping changed the original full domain error")
	}
}
