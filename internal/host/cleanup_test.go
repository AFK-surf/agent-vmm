//go:build darwin || linux

package host

import (
	"context"
	"errors"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"path/filepath"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestVMTransitionFenceBlocksInventoryMutation(t *testing.T) {
	controller := newTestController(&Controller{})
	releaseInventory := holdTestInventory(t, controller)
	attempted := make(chan string, 2)
	started := make(chan string, 2)
	failed := make(chan error, 2)
	for _, mutation := range []string{"image-import", "network-link-apply"} {
		mutation := mutation
		go func() {
			attempted <- mutation
			_, release, err := controller.inventory.Enter(context.Background())
			if err != nil {
				failed <- err
				return
			}
			defer release()
			started <- mutation
		}()
	}
	<-attempted
	<-attempted
	select {
	case mutation := <-started:
		t.Fatalf("%s crossed the VM transition fence", mutation)
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseInventory()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case mutation := <-started:
			seen[mutation] = true
		case err := <-failed:
			t.Fatal(err)
		case <-time.After(time.Second):
			t.Fatalf("mutations did not continue after the transition completed: %v", seen)
		}
	}
}

func TestExpiredInventoryMutationDoesNotBeginAfterCleanupUnlocks(t *testing.T) {
	controller := newTestController(&Controller{})
	releaseInventory := holdTestInventory(t, controller)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	mutated := make(chan struct{}, 1)
	go func() {
		_, release, err := controller.inventory.Enter(ctx)
		if err != nil {
			result <- err
			return
		}
		defer release()
		mutated <- struct{}{}
		result <- nil
	}()
	cancel()
	releaseInventory()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expired mutation err=%v", err)
	}
	select {
	case <-mutated:
		t.Fatal("expired mutation began after cleanup released its fence")
	default:
	}
}

func TestCleanupAdmissionDeadlineBoundsTransitionAndEnvironmentWaits(t *testing.T) {
	requested := &hostv1.CleanupPlan{
		EnvironmentId: "environment-a",
		Kind:          hostv1.CleanupKind_CLEANUP_KIND_RESET,
		Revision:      1,
	}
	for _, test := range []struct {
		name  string
		block func(*Controller) func()
	}{
		{
			name: "inventory transition",
			block: func(controller *Controller) func() {
				return holdTestInventory(t, controller)
			},
		},

		{
			name: "environment admission",
			block: func(controller *Controller) func() {
				unlock, err := controller.workloads.LockEnvironment(context.Background(), "environment-a")
				if err != nil {
					t.Fatal(err)
				}
				return unlock
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			controller := newTestController(&Controller{store: database})
			unblock := test.block(controller)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := controller.cleanup.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: "request-a", Plan: requested})
				result <- err
			}()
			select {
			case err := <-result:
				if status.Code(err) != codes.DeadlineExceeded || grpcErrorReason(err) != "CLEANUP_ADMISSION_TIMEOUT" {
					t.Fatalf("cleanup admission err=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cleanup admission did not honor its deadline")
			}
			unblock()
			releaseInventoryReader := enterTestInventory(t, controller)
			releaseInventoryReader()
		})
	}
}

func TestCleanupAdmissionTimeoutReplaysRunningRequestWithoutOrphanRepair(t *testing.T) {
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	prior, _, err := database.PutOperation(context.Background(), store.Operation{
		ID: "operation-cleanup", Kind: "environment-CLEANUP_KIND_RESET", EnvironmentID: "environment-a",
		State: int32(hostv1.OperationState_OPERATION_STATE_RUNNING), Stage: "deleting-containers", RequestID: "request-cleanup",
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database})
	unlock, err := controller.workloads.LockEnvironment(context.Background(), prior.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	replayed, err := controller.cleanup.ExecuteCleanup(ctx, &hostv1.ExecuteCleanupRequest{RequestId: prior.RequestID, Plan: &hostv1.CleanupPlan{
		EnvironmentId: prior.EnvironmentID, Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET, Revision: 1,
	}})
	if err != nil || replayed.GetOperation().GetId() != prior.ID || replayed.GetOperation().GetState() != hostv1.OperationState_OPERATION_STATE_RUNNING || replayed.GetOperation().GetStage() != prior.Stage {
		t.Fatalf("live request replay=%+v err=%v", replayed, err)
	}
	stored, err := database.OperationByRequestID(context.Background(), prior.RequestID)
	if err != nil || stored.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || stored.Stage != prior.Stage {
		t.Fatalf("live request was repaired as orphan: stored=%+v err=%v", stored, err)
	}
}

func TestRemoteCleanupAdmissionDeadlineUsesInventoryFence(t *testing.T) {
	controller := newTestController(&Controller{})
	releaseInventory := holdTestInventory(t, controller)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := (&remoteComputeServer{controller: controller}).ExecuteCleanupAllocation(ctx, &connectorv1.ExecuteCleanupAllocationRequest{AllocationId: "allocation-a", Kind: hostv1.CleanupKind_CLEANUP_KIND_RESET})
		result <- err
	}()
	select {
	case err := <-result:
		if status.Code(err) != codes.DeadlineExceeded || grpcErrorReason(err) != "CLEANUP_ADMISSION_TIMEOUT" {
			t.Fatalf("remote cleanup admission err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("remote cleanup admission did not honor its deadline")
	}
	releaseInventory()
}

func grpcErrorReason(err error) string {
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func TestVMTransitionStopsActualImageHandlerBeforeOwnerMutation(t *testing.T) {
	controller := newTestController(&Controller{})
	releaseInventory := holdTestInventory(t, controller)
	agent := &agentServer{controller: controller}
	contexts := make([]context.CancelFunc, 0, 2)
	entered := make(chan string, 2)
	results := make(chan error, 2)

	agentContext, cancelAgent := context.WithCancel(context.Background())
	contexts = append(contexts, cancelAgent)
	go func() {
		_, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Agent, SessionID: "session", EnvironmentID: "environment", NamespaceID: "namespace"})(
			agentContext,
			&hostv1.DeleteImageRequest{RequestId: "delete-image", Reference: "example.com/image", ExpectedGenerationId: "generation", ExpectedDigest: "sha256:digest"},
			nil,
			func(ctx context.Context, request any) (any, error) {
				entered <- "image"
				return agent.DeleteImage(ctx, request.(*hostv1.DeleteImageRequest))
			},
		)
		results <- err
	}()

	<-entered
	select {
	case err := <-results:
		t.Fatalf("mutation crossed cleanup fence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	for _, cancel := range contexts {
		cancel()
	}
	releaseInventory()
	for range contexts {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("fenced handler err=%v", err)
		}
	}
}
