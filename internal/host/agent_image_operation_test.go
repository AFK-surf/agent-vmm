//go:build darwin || linux

package host

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type imageReceiptGuest struct {
	vmmdv1.UnimplementedVMMDServiceServer
	mu                                                      sync.Mutex
	receipt                                                 *vmmdv1.ImageOperationReceipt
	database                                                *store.Store
	identity                                                auth.Identity
	observations, copies, forgets                           int
	forgetError                                             bool
	firstCopyReturned, secondCopyEntered, releaseSecondCopy chan struct{}
	buildStarted, buildCanceled                             chan struct{}
	copyExecutions                                          int
	original                                                *vmmdv1.ImageOperationReceipt
}

func (guest *imageReceiptGuest) ObserveImageOperation(context.Context, *vmmdv1.ImageOperationRequest) (*vmmdv1.ImageOperationReceipt, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	guest.observations++
	if guest.receipt == nil {
		return nil, status.Error(codes.NotFound, "forgotten")
	}
	return proto.Clone(guest.receipt).(*vmmdv1.ImageOperationReceipt), nil
}
func (guest *imageReceiptGuest) CancelImageOperation(ctx context.Context, request *vmmdv1.ImageOperationRequest) (*vmmdv1.ImageOperationReceipt, error) {
	if guest.buildCanceled != nil {
		guest.mu.Lock()
		guest.receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED
		guest.receipt.WriterSettled = true
		result := proto.Clone(guest.receipt).(*vmmdv1.ImageOperationReceipt)
		close(guest.buildCanceled)
		guest.mu.Unlock()
		return result, nil
	}
	return guest.ObserveImageOperation(ctx, request)
}
func (guest *imageReceiptGuest) CopyImage(context.Context, *vmmdv1.CopyImageRequest) (*vmmdv1.Image, error) {
	guest.mu.Lock()
	guest.copies++
	call := guest.copies
	guest.mu.Unlock()
	if guest.releaseSecondCopy != nil {
		if call == 1 {
			<-guest.secondCopyEntered
		} else {
			close(guest.secondCopyEntered)
			<-guest.releaseSecondCopy
		}
	}
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.receipt == nil {
		guest.receipt = proto.Clone(guest.original).(*vmmdv1.ImageOperationReceipt)
		guest.copyExecutions++
	}
	if guest.firstCopyReturned != nil && call == 1 {
		close(guest.firstCopyReturned)
	}
	return &vmmdv1.Image{Reference: guest.receipt.ImageReference, Digest: guest.receipt.ManifestDigest, CopyReceipt: proto.Clone(guest.receipt).(*vmmdv1.ImageOperationReceipt)}, nil
}
func (guest *imageReceiptGuest) ForgetImageOperation(ctx context.Context, request *vmmdv1.ImageOperationRequest) (*emptypb.Empty, error) {
	guest.mu.Lock()
	defer guest.mu.Unlock()
	guest.forgets++
	key, _ := imageOperationStoreKey(guest.identity, hostv1.ImageOperationKind(request.Kind), request.RequestId)
	if _, err := guest.database.GetOperation(ctx, key); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "Host receipt missing")
	}
	if guest.forgetError {
		return nil, status.Error(codes.Unavailable, "forget unavailable")
	}
	if guest.receipt == nil {
		return &emptypb.Empty{}, nil
	}
	if request.InputSha256 != guest.receipt.InputSha256 || request.ExecutionId != guest.receipt.ExecutionId || request.ContainerId != guest.receipt.ContainerId || request.ExpectedInstanceId != guest.receipt.ExpectedInstanceId {
		return nil, status.Error(codes.FailedPrecondition, "wrong receipt identity")
	}
	guest.receipt = nil
	return &emptypb.Empty{}, nil
}

func (guest *imageReceiptGuest) BuildImage(stream vmmdv1.VMMDService_BuildImageServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	close(guest.buildStarted)
	select {
	case <-guest.buildCanceled:
		return status.Error(codes.Canceled, "build canceled")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func TestImageOperationTerminalReceiptMovesToHost(t *testing.T) {
	for _, mode := range []string{"observe", "cancel", "copy response", "concurrent copies", "forget failure", "unsettled", "failed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "host.db")
			database, err := openTestStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { database.Close() }()
			if _, err := database.CreateRemoteRegistration(ctx, store.RemoteRegistration{ID: "controller", Endpoint: "controller", DisplayName: "Controller", PerEnvPIDs: 128, PerEnvDiskBytes: 1 << 30, MaxEgressMode: 1, StopGraceSeconds: 1, State: 1, ConnectionState: 1, ConnectorPhase: 1, EnrollmentState: 2}); err != nil {
				t.Fatal(err)
			}
			environment, err := database.BeginWorkloadAdmission(ctx, store.Environment{ID: "target", NamespaceID: "namespace", OwnerKind: store.OwnerRemoteRegistration, RegistrationID: "controller", AllocationID: "allocation", AllocationGeneration: 7, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_READY), PIDs: 128, DiskBytes: 1 << 30}, resourcev2.Contract{PIDMax: 128, WritableQuotaBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			identity := auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: environment.ID, NamespaceID: environment.NamespaceID, RegistrationID: "controller", AllocationID: "allocation", AllocationGeneration: 7}
			request := &hostv1.CopyImageRequest{RequestId: "copy", Source: &hostv1.ImageSource{Source: &hostv1.ImageSource_Retained{Retained: &hostv1.RetainedImageSource{BuildRequestId: "build"}}}, TargetAllocationId: "allocation", TargetAllocationGeneration: 7, ImageReference: "example/image:copy", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
			encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			receipt := &vmmdv1.ImageOperationReceipt{RequestId: request.RequestId, InputSha256: fmt.Sprintf("%x", sha256.Sum256(encoded)), Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true, ImageReference: request.ImageReference, ManifestDigest: request.ManifestDigest}
			if mode == "unsettled" {
				receipt.WriterSettled = false
				receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_RUNNING
			}
			if mode == "failed" {
				receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_FAILED
			}
			if mode == "unknown" {
				receipt.State = vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_UNKNOWN
			}
			guest := &imageReceiptGuest{receipt: receipt, database: database, identity: identity, forgetError: mode == "forget failure"}
			if mode == "concurrent copies" {
				guest.firstCopyReturned = make(chan struct{})
				guest.secondCopyEntered = make(chan struct{})
				guest.releaseSecondCopy = make(chan struct{})
				guest.copyExecutions = 1
				guest.original = proto.Clone(receipt).(*vmmdv1.ImageOperationReceipt)
			}
			controller := newTestController(&Controller{store: database})
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			vmmdv1.RegisterVMMDServiceServer(server, guest)
			go server.Serve(listener)
			defer server.Stop()
			connection, err := grpc.NewClient("passthrough:///image-receipt", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				t.Fatal(err)
			}
			if err := controller.guestConnection.Connect(testGuestConnectionDialer{connection}, "boot"); err != nil {
				t.Fatal(err)
			}
			defer controller.guestConnection.Disconnect()
			client := executionClientWithIdentityAndServer(t, identity, &agentServer{controller: controller})
			observation := &hostv1.ObserveImageOperationRequest{RequestId: request.RequestId, Kind: hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, InputSha256: receipt.InputSha256, CopySource: request.Source, TargetAllocationId: request.TargetAllocationId, TargetAllocationGeneration: 7}
			switch mode {
			case "concurrent copies":
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(guest.releaseSecondCopy) }) })
				results := make(chan error, 2)
				otherSession := identity
				otherSession.SessionID = "second-session"
				otherClient := executionClientWithIdentityAndServer(t, otherSession, &agentServer{controller: controller})
				for _, caller := range []hostv1.AgentRuntimeServiceClient{client, otherClient} {
					go func() { _, err := caller.CopyImage(ctx, request); results <- err }()
				}
				<-guest.firstCopyReturned
				select {
				case err := <-results:
					t.Fatalf("terminal migration crossed a pending Guest mutation: %v", err)
				case <-time.After(100 * time.Millisecond):
				}
				release.Do(func() { close(guest.releaseSecondCopy) })
				for range 2 {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				guest.mu.Lock()
				executions := guest.copyExecutions
				guest.mu.Unlock()
				if executions != 1 {
					t.Fatalf("copy executed %d times", executions)
				}
			case "copy response":
				_, err = client.CopyImage(ctx, request)
			case "cancel":
				_, err = client.CancelImageOperation(ctx, &hostv1.CancelImageOperationRequest{RequestId: observation.RequestId, Kind: observation.Kind, InputSha256: observation.InputSha256, CopySource: observation.CopySource, TargetAllocationId: observation.TargetAllocationId, TargetAllocationGeneration: 7})
			default:
				_, err = client.ObserveImageOperation(ctx, observation)
			}
			if err != nil {
				t.Fatal(err)
			}
			guest.mu.Lock()
			forgotten := guest.receipt == nil
			forgets := guest.forgets
			observations := guest.observations
			copies := guest.copies
			guest.mu.Unlock()
			if mode == "unsettled" {
				if forgets != 0 || forgotten {
					t.Fatal("unsettled receipt was forgotten")
				}
				return
			}
			expectedForgets := 1
			if mode == "concurrent copies" {
				expectedForgets = 2
			}
			if forgets != expectedForgets || forgotten == (mode == "forget failure") {
				t.Fatalf("forgotten=%t calls=%d", forgotten, forgets)
			}
			// Reopen the database and disconnect the Guest. Every replay must use the durable Host row.
			controller.guestConnection.Disconnect()
			database.Close()
			database, err = store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.RecoverAfterRestart(ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
			controller = newTestController(&Controller{store: database})
			client = executionClientWithIdentityAndServer(t, identity, &agentServer{controller: controller})
			response, err := client.ObserveImageOperation(ctx, observation)
			if err != nil || response.GetReceipt().GetInputSha256() != receipt.InputSha256 {
				t.Fatal(response, err)
			}
			copied, err := client.CopyImage(ctx, request)
			if mode == "failed" || mode == "unknown" {
				if status.Code(err) != codes.Aborted {
					t.Fatal("terminal replay", err)
				}
			} else if err != nil || copied.GetImage().GetReference() != receipt.ImageReference || copied.GetImage().GetCopyReceipt().GetInputSha256() != receipt.InputSha256 {
				t.Fatal(copied, err)
			}
			changed := proto.Clone(request).(*hostv1.CopyImageRequest)
			changed.ImageReference = "other/image:copy"
			if _, err := client.CopyImage(ctx, changed); status.Code(err) != codes.Aborted {
				t.Fatal("changed replay", err)
			}
			observation.InputSha256 = strings.Repeat("0", 64)
			if _, err := client.ObserveImageOperation(ctx, observation); status.Code(err) != codes.Aborted {
				t.Fatal("changed observation", err)
			}
			guest.mu.Lock()
			defer guest.mu.Unlock()
			if guest.observations != observations || guest.copies != copies || guest.forgets != forgets {
				t.Fatal("replay contacted Guest")
			}
		})
	}
}

func TestBuildImageReplaysDurableReceiptWithoutExecution(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	identity := auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "environment", NamespaceID: "namespace"}
	header := &hostv1.BuildImageHeader{RequestId: "build", DockerfilePath: "Dockerfile", Image: "example/image:build", ContainerId: "container", ExpectedInstanceId: "old-instance", ExecutionId: "old-execution", ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano()}
	fingerprint, err := buildInputSHA256(header)
	if err != nil {
		t.Fatal(err)
	}
	guest := &imageReceiptGuest{receipt: &vmmdv1.ImageOperationReceipt{RequestId: header.RequestId, Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, InputSha256: fingerprint, ContainerId: header.ContainerId, ExpectedInstanceId: header.ExpectedInstanceId, ExecutionId: header.ExecutionId, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED, WriterSettled: true, ImageReference: header.Image, ManifestDigest: "sha256:" + strings.Repeat("b", 64)}, database: database, identity: identity}
	controller := newTestController(&Controller{store: database})
	server := &agentServer{controller: controller}
	// Observe the real Guest RPC response before testing the streamed Host replay.
	listener := bufconn.Listen(1 << 20)
	rpc := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(rpc, guest)
	go rpc.Serve(listener)
	defer rpc.Stop()
	connection, err := grpc.NewClient("passthrough:///build-receipt", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.guestConnection.Connect(testGuestConnectionDialer{connection}, "boot"); err != nil {
		t.Fatal(err)
	}
	defer controller.guestConnection.Disconnect()
	client := executionClientWithIdentityAndServer(t, identity, server)
	if _, err := client.ObserveImageOperation(t.Context(), &hostv1.ObserveImageOperationRequest{RequestId: header.RequestId, Kind: hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, InputSha256: fingerprint, ContainerId: header.ContainerId, ExpectedInstanceId: header.ExpectedInstanceId, ExecutionId: header.ExecutionId}); err != nil {
		t.Fatal(err)
	}
	controller.guestConnection.Disconnect()
	header.ExpectedInstanceId = "new-instance"
	header.ExecutionId = "new-execution"
	header.ExecutionDeadlineUnixNano++
	for _, changed := range []bool{false, true} {
		if changed {
			header.DockerfilePath = "Otherfile"
		}
		stream, err := client.BuildImage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: header}}); err != nil {
			t.Fatal(err)
		}
		_ = stream.CloseSend()
		result, err := stream.Recv()
		if changed {
			if status.Code(err) != codes.Aborted {
				t.Fatal("changed build", err)
			}
			continue
		}
		if err != nil || result.GetResult().GetImage() != header.Image {
			t.Fatal(result, err)
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatal(err)
		}
	}
}

func TestImageOperationCancelDoesNotWaitForBuildReadFence(t *testing.T) {
	controller, _ := executionTestFixture(t)
	identity := auth.Identity{Role: auth.Agent, SessionID: "host-control-a", EnvironmentID: "env-a", NamespaceID: "a", AllocationID: "allocation-a", AllocationGeneration: 1, ExecutionOwnerID: "runtime-instance-a:1"}
	header := &hostv1.BuildImageHeader{RequestId: "cancel-build", ContainerId: "main", ExpectedInstanceId: "instance-a", ExecutionId: "build:cancel", ExecutionDeadlineUnixNano: time.Now().Add(time.Minute).UnixNano(), Image: "example/cancel:build"}
	fingerprint, err := buildInputSHA256(header)
	if err != nil {
		t.Fatal(err)
	}
	guest := &imageReceiptGuest{receipt: &vmmdv1.ImageOperationReceipt{RequestId: header.RequestId, Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, InputSha256: fingerprint, ContainerId: header.ContainerId, ExpectedInstanceId: header.ExpectedInstanceId, ExecutionId: header.ExecutionId, State: vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_RUNNING}, database: controller.store, identity: identity, buildStarted: make(chan struct{}), buildCanceled: make(chan struct{})}
	listener := bufconn.Listen(1 << 20)
	rpc := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(rpc, guest)
	go rpc.Serve(listener)
	defer rpc.Stop()
	connection, err := grpc.NewClient("passthrough:///cancel-build", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.guestConnection.Connect(testGuestConnectionDialer{connection}, "boot"); err != nil {
		t.Fatal(err)
	}
	defer controller.guestConnection.Disconnect()
	client := executionClientWithIdentityAndServer(t, identity, &agentServer{controller: controller, buildLifetime: context.Background()})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	build, err := client.BuildImage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := build.Send(&hostv1.BuildImageRequest{Event: &hostv1.BuildImageRequest_Header{Header: header}}); err != nil {
		t.Fatal(err)
	}
	_ = build.CloseSend()
	select {
	case <-guest.buildStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	canceled, err := client.CancelImageOperation(ctx, &hostv1.CancelImageOperationRequest{RequestId: header.RequestId, Kind: hostv1.ImageOperationKind_IMAGE_OPERATION_KIND_BUILD, InputSha256: fingerprint, ContainerId: header.ContainerId, ExpectedInstanceId: header.ExpectedInstanceId, ExecutionId: header.ExecutionId})
	if err != nil || !canceled.GetReceipt().GetWriterSettled() || canceled.GetReceipt().GetState() != hostv1.ImageOperationState_IMAGE_OPERATION_STATE_STOPPED {
		t.Fatal(canceled, err)
	}
	if _, err := build.Recv(); status.Code(err) != codes.Canceled {
		t.Fatal("canceled build stream", err)
	}
	guest.mu.Lock()
	defer guest.mu.Unlock()
	if guest.receipt != nil || guest.forgets != 1 {
		t.Fatal("canceled receipt not migrated")
	}
}
