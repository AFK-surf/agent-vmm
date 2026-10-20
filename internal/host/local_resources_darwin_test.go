//go:build darwin

package host

import (
	"context"
	"fmt"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalOperatorCapabilitiesRejectEveryNonOperatorRole(t *testing.T) {
	server := &operatorServer{}
	handlers := []grpc.UnaryHandler{
		func(ctx context.Context, _ any) (any, error) {
			return server.PlanLocalDisposal(ctx, &hostv1.PlanLocalDisposalRequest{})
		},
		func(ctx context.Context, _ any) (any, error) {
			return server.ExecuteLocalDisposal(ctx, &hostv1.ExecuteLocalDisposalRequest{})
		},
		func(ctx context.Context, _ any) (any, error) {
			return server.SignCommaRecovery(ctx, &hostv1.SignCommaRecoveryRequest{})
		},
		func(ctx context.Context, _ any) (any, error) {
			return server.GetLocalDiskSummary(ctx, &hostv1.GetLocalDiskSummaryRequest{})
		},
		func(ctx context.Context, _ any) (any, error) {
			return server.ListLocalEnvironmentPage(ctx, &hostv1.ListLocalEnvironmentPageRequest{})
		},
		func(ctx context.Context, _ any) (any, error) {
			return server.ListLocalRegistrationPage(ctx, &hostv1.ListLocalRegistrationPageRequest{})
		},
	}
	for _, role := range []auth.Role{auth.Agent, auth.RemoteConnector} {
		for _, handler := range handlers {
			_, err := auth.UnaryInterceptor(auth.Identity{Role: role})(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("non-operator role %v: %v", role, err)
			}
		}
	}
}

func TestLocalResourcePagesKeepMoreThanPreviewLimitAndUnknownMetrics(t *testing.T) {
	database, err := store.Open(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("environment-%03d", i)
		_, err = database.CreateEnvironment(context.Background(), store.Environment{ID: id, NamespaceID: id, DisplayName: "private cloud label", OwnerKind: store.OwnerLocal})
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := &Controller{store: database, manager: &vmm.Manager{}}
	server := &operatorServer{controller: controller}
	cursor := ""
	seen := map[string]bool{}
	for {
		result, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			return server.ListLocalEnvironmentPage(ctx, &hostv1.ListLocalEnvironmentPageRequest{AfterId: cursor})
		})
		if err != nil {
			t.Fatal(err)
		}
		page := result.(*hostv1.LocalEnvironmentPage)
		if len(page.GetEnvironments()) > 32 {
			t.Fatal("environment page exceeds request budget")
		}
		for _, row := range page.GetEnvironments() {
			if seen[row.GetId()] || row.PrivateDiskBytes != nil || row.GetSampledAt() != nil {
				t.Fatalf("duplicate or fabricated measurement: %v", row)
			}
			seen[row.GetId()] = true
		}
		cursor = page.GetNextCursor()
		if cursor == "" {
			break
		}
	}
	if len(seen) != 101 {
		t.Fatalf("paged environments=%d", len(seen))
	}
	database.Close()
	result, err := auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		return server.GetLocalDiskSummary(ctx, &hostv1.GetLocalDiskSummaryRequest{})
	})
	if err != nil || result.(*hostv1.LocalDiskSummary).GetGuestStateFs() != nil {
		t.Fatalf("disk summary depended on database or filled missing metric: %v,%v", result, err)
	}
}

func TestLocalDisposalRevokesStaticSharesBeforeGuestAvailability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "owner", EnvironmentID: "service", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"}); err != nil {
		t.Fatal(err)
	}
	manager, err := vmm.NewManager(vmm.Config{CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30, BundleDirectory: t.TempDir(), InstanceDirectory: t.TempDir(), SerialLog: filepath.Join(t.TempDir(), "serial")})
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{store: database, manager: manager}
	controller.vmLifecycle = newVMLifecycleOwner(controller, manager)
	var state atomic.Int32
	state.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	controller.vmLifecycle.snapshot = func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(state.Load())} }
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})))
	hostv1.RegisterOperatorServiceServer(server, &operatorServer{controller: controller})
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///operator", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "sentinel"), []byte("owned by operator"), 0600); err != nil {
		t.Fatal(err)
	}
	share, err := operator.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "share", EnvironmentId: "service", SourcePath: source})
	if err != nil {
		t.Fatal(err)
	}
	sessionListener := bufconn.Listen(1024)
	session := &sessionServer{server: grpc.NewServer(), listener: sessionListener, path: filepath.Join(t.TempDir(), "session.sock")}
	controller.sessions = map[string]*sessionServer{"session": session}
	if err := database.CreateSession(ctx, store.Session{ID: "session", IntegrationID: "owner", EnvironmentID: "service", SocketPath: session.path, CreatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	plan, err := operator.PlanLocalDisposal(ctx, &hostv1.PlanLocalDisposalRequest{TargetKind: hostv1.LocalDisposalTargetKind_LOCAL_DISPOSAL_TARGET_KIND_ENVIRONMENT, TargetId: "service"})
	if err != nil {
		t.Fatal(err)
	}
	request := &hostv1.ExecuteLocalDisposalRequest{RequestId: "dispose", Plan: plan.Plan, IrreversibleConfirmation: true}
	accepted, err := operator.ExecuteLocalDisposal(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	root, err := database.GetLocalDisposal(ctx, "environment", "service")
	if err != nil {
		t.Fatal(err)
	}
	if root.OperationID != accepted.GetOperation().GetId() {
		t.Fatal("disposal lost original operation identity")
	}
	controller.inventoryTransition.Lock()
	queued := make(chan error, 1)
	queuedCtx, cancelQueued := context.WithTimeout(ctx, time.Second)
	defer cancelQueued()
	go func() { queued <- controller.reconcileLocalDisposal(queuedCtx, root) }()
	select {
	case err := <-queued:
		controller.inventoryTransition.Unlock()
		t.Fatalf("offline share revocation crossed the VM start transition: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	state.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	controller.inventoryTransition.Unlock()
	if err := <-queued; status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "Unavailable") {
		t.Fatalf("queued disposal lost offline fence or Guest failure: %v", err)
	}
	closedSession, err := database.GetSession(ctx, "session")
	if err != nil || closedSession.ClosedAt == nil || len(controller.sessions) != 0 {
		t.Fatalf("online share failure kept session live: %+v %v", closedSession, err)
	}
	if _, err := sessionListener.Dial(); err == nil {
		t.Fatal("disposed session listener still accepts connections")
	}
	pending, err := database.GetHostShare(ctx, share.GetShare().GetId())
	if err != nil || pending.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING {
		t.Fatalf("online disposal claimed physical revoke: %+v %v", pending, err)
	}
	if _, err := database.GetEnvironment(ctx, root.TargetID); err != nil {
		t.Fatalf("online disposal removed environment before physical share revocation: %v", err)
	}
	state.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	if err := controller.reconcileLocalDisposal(ctx, root); status.Code(err) != codes.Unavailable {
		t.Fatalf("offline Guest observation: %v", err)
	}
	retired, err := database.GetHostShare(ctx, share.GetShare().GetId())
	if err != nil || retired.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		t.Fatalf("unavailable Guest prevented offline share revocation: %+v %v", retired, err)
	}
	if manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		t.Fatal("disposal started the VM")
	}
	operation, err := database.GetOperation(ctx, root.OperationID)
	if err != nil || operation.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) || operation.ErrorReason != "LOCAL_DISPOSAL_PENDING" {
		t.Fatalf("offline phase completed or lost disposal: %+v %v", operation, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	controller.store = database
	persisted, err := database.GetHostShare(ctx, retired.ID)
	if err != nil || persisted.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED || persisted.Revision != retired.Revision {
		t.Fatalf("share revocation was not durable: %+v %v", persisted, err)
	}
	if err := controller.prepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if len(controller.hostShares) != 0 {
		t.Fatal("disposed share entered the next boot map")
	}
	state.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	if err := controller.reconcileLocalDisposal(ctx, root); status.Code(err) != codes.Unavailable {
		t.Fatalf("online continuation was blocked by the already revoked share: %v", err)
	}
	replay, err := operator.ExecuteLocalDisposal(ctx, request)
	if err != nil || replay.GetOperation().GetId() != root.OperationID {
		t.Fatalf("continued disposal changed identity: %v %v", replay, err)
	}
	if data, err := os.ReadFile(filepath.Join(source, "sentinel")); err != nil || string(data) != "owned by operator" {
		t.Fatalf("disposal changed external source: %q %v", data, err)
	}
}
