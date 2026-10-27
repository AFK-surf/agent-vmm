//go:build darwin

package host

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/auth"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/session"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestHostShareOperatorReplayRebindAndRevokeSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := openTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { database.Close() }()
	for _, id := range []string{"service", "other", "controller"} {
		if _, err := database.CreateEnvironment(ctx, store.Environment{ID: id, NamespaceID: id, OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"service", "other"} {
		if _, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: id, EnvironmentID: id, AdapterID: "service", DisplayName: id, Scope: 1, ConfigTarget: id, DesiredState: 1, ConfigState: 2, PrepareRequestID: id}); err != nil {
			t.Fatal(err)
		}
	}
	server := &operatorServer{controller: newTestController(&Controller{store: database})}
	source := t.TempDir()
	replacement := t.TempDir()
	request := &hostv1.CreateHostShareRequest{RequestId: "create", EnvironmentId: "service", SourcePath: source, ReadOnly: true}
	created, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CreateHostShareResponse, error) {
		return server.CreateHostShare(ctx, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetShare().GetRevision() != 1 || created.GetShare().GetState() != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE {
		t.Fatal(created)
	}
	_, err = callAsOperator(t, func(ctx context.Context) (*hostv1.CreateHostShareResponse, error) {
		return server.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "controller", EnvironmentId: "controller", SourcePath: source})
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("controller domain authorized: %v", err)
	}
	id := created.GetShare().GetId()
	grant, err := database.GetHostShare(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	grant.State = hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE
	if err := database.UpdateHostShare(ctx, grant, grant.Revision); err != nil {
		t.Fatal(err)
	}
	mount := &vmmdv1.VolumeMount{Source: &vmmdv1.VolumeMount_HostShare{HostShare: id}, ReadOnly: true}
	for _, environment := range []string{"other", "controller"} {
		if err := server.controller.hostShares.AuthorizeHostShareMounts(ctx, environment, []*vmmdv1.VolumeMount{mount}); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("cross-environment grant accepted: %v", err)
		}
	}
	mount.ReadOnly = false
	if err := server.controller.hostShares.AuthorizeHostShareMounts(ctx, "service", []*vmmdv1.VolumeMount{mount}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("write amplification accepted: %v", err)
	}
	mount.ReadOnly = true
	if err := server.controller.hostShares.AuthorizeHostShareMounts(ctx, "service", []*vmmdv1.VolumeMount{mount}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source, source+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	sourceErr := server.controller.hostShares.AuthorizeHostShareMounts(ctx, "service", []*vmmdv1.VolumeMount{mount})
	if status.Code(sourceErr) != codes.FailedPrecondition || strings.Contains(sourceErr.Error(), source) || strings.Contains(sourceErr.Error(), grant.Source.Path) {
		t.Fatalf("replacement admitted or runtime exposed Host path: %v", sourceErr)
	}
	if err := os.Remove(source); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(source+".original", source); err != nil {
		t.Fatal(err)
	}

	rebound, err := callAsOperator(t, func(ctx context.Context) (*hostv1.RebindHostShareResponse, error) {
		return server.RebindHostShare(ctx, &hostv1.RebindHostShareRequest{RequestId: "rebind", ShareId: id, ExpectedRevision: 1, SourcePath: replacement})
	})
	if err != nil || rebound.GetShare().GetRevision() != 2 {
		t.Fatal(rebound, err)
	}
	_, err = callAsOperator(t, func(ctx context.Context) (*hostv1.RevokeHostShareResponse, error) {
		return server.RevokeHostShare(ctx, &hostv1.RevokeHostShareRequest{RequestId: "stale", ShareId: id, ExpectedRevision: 1})
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale revision accepted: %v", err)
	}
	revoked, err := callAsOperator(t, func(ctx context.Context) (*hostv1.RevokeHostShareResponse, error) {
		return server.RevokeHostShare(ctx, &hostv1.RevokeHostShareRequest{RequestId: "revoke", ShareId: id, ExpectedRevision: 2})
	})
	if err != nil || revoked.GetShare().GetState() != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		t.Fatal(revoked, err)
	}
	database.Close()
	database, err = store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	server.controller = newTestController(&Controller{store: database})
	if err := os.Remove(source); err != nil {
		t.Fatal(err)
	}
	replay, err := callAsOperator(t, func(ctx context.Context) (*hostv1.CreateHostShareResponse, error) {
		return server.CreateHostShare(ctx, request)
	})
	if err != nil || replay.GetShare().GetId() != id || replay.GetShare().GetRevision() != 1 {
		t.Fatal(replay, err)
	}
	actual, err := database.GetHostShare(ctx, id)
	if err != nil || actual.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED || actual.Revision != 3 || actual.Source.Path != rebound.GetShare().GetSourcePath() {
		t.Fatalf("create replay changed later authority: %+v %v", actual, err)
	}
	conflict := proto.Clone(request).(*hostv1.CreateHostShareRequest)
	conflict.SourcePath = replacement
	_, err = callAsOperator(t, func(ctx context.Context) (*hostv1.CreateHostShareResponse, error) {
		return server.CreateHostShare(ctx, conflict)
	})
	if reason, _ := hostv1.ErrorReasonOf(err); status.Code(err) != codes.Aborted || reason != hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT {
		t.Fatalf("changed replay accepted: %v", err)
	}
}

func TestHostShareStaticBootAndOfflineMutationsUseLifecycleFence(t *testing.T) {
	ctx := context.Background()
	database, err := openTestStore(filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.CreateEnvironment(ctx, store.Environment{ID: "service", NamespaceID: "service", OwnerKind: store.OwnerLocal, PIDs: 1, DiskBytes: 1}); err != nil {
		t.Fatal(err)
	}
	owner, err := database.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "owner", EnvironmentID: "service", AdapterID: "service", DisplayName: "Service", Scope: 1, ConfigTarget: "config", DesiredState: 1, ConfigState: 2, PrepareRequestID: "prepare"})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := vmm.NewManager(vmm.Config{CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30, BundleDirectory: t.TempDir(), InstanceDirectory: t.TempDir(), SerialLog: filepath.Join(t.TempDir(), "serial")})
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database, manager: manager})
	controller.vmLifecycle = newVMLifecycleOwner(controller, manager)
	var observed atomic.Int32
	observed.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	setTestLifecycleSnapshot(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(observed.Load())} })
	shares := setupTestHostShareOwner(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(observed.Load())} })
	guest := new(hostShareConsumersGuest)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryInterceptor(auth.Identity{Role: auth.Operator})))
	hostv1.RegisterOperatorServiceServer(server, &operatorServer{controller: controller})
	vmmdv1.RegisterVMMDServiceServer(server, guest)
	go server.Serve(listener)
	defer server.Stop()
	connection, err := grpc.NewClient("passthrough:///operator", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	source := t.TempDir()
	create := &hostv1.CreateHostShareRequest{RequestId: "create", EnvironmentId: "service", SourcePath: source, ReadOnly: true}
	created, err := operator.CreateHostShare(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	id := created.GetShare().GetId()
	observed.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	replayed, err := operator.CreateHostShare(ctx, create)
	if err != nil || !proto.Equal(created, replayed) {
		t.Fatalf("successful receipt cannot replay online: %v %v", replayed, err)
	}
	rebind := &hostv1.RebindHostShareRequest{RequestId: "rebind", ShareId: id, ExpectedRevision: 1, SourcePath: source}
	for name, call := range map[string]func() error{
		"create": func() error {
			_, err := operator.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "online-create", EnvironmentId: "service", SourcePath: source})
			return err
		},
		"rebind": func() error { _, err := operator.RebindHostShare(ctx, rebind); return err },
		"revoke": func() error {
			_, err := operator.RevokeHostShare(ctx, &hostv1.RevokeHostShareRequest{RequestId: "online-revoke", ShareId: id, ExpectedRevision: 1})
			return err
		},
	} {
		if err := call(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("online %s accepted: %v", name, err)
		}
	}
	for _, requestID := range []string{"online-create", "rebind", "online-revoke"} {
		if _, err := database.OperationByRequestID(ctx, requestID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("rejected command was accepted: %s %v", requestID, err)
		}
	}
	observed.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	rebound, err := operator.RebindHostShare(ctx, rebind)
	if err != nil || rebound.GetShare().GetReadOnly() || rebound.GetShare().GetRevision() != 2 {
		t.Fatal(rebound, err)
	}
	second, err := operator.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "second-source", EnvironmentId: "service", SourcePath: t.TempDir(), ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	missing := t.TempDir()
	bad, err := operator.CreateHostShare(ctx, &hostv1.CreateHostShareRequest{RequestId: "missing-source", EnvironmentId: "service", SourcePath: missing})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if len(shares.directories) != 2 || shares.directories[id] == nil {
		t.Fatalf("wrong boot array: %+v", shares.directories)
	}
	observed.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	guest.failProjection = true
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); err == nil {
		t.Fatalf("lost projection response was treated as settled: %v", err)
	}
	guest.failProjection = false
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); err != nil {
		t.Fatal(err)
	}
	if len(guest.projections) != 2 {
		t.Fatalf("lost Guest projection was not retried: %v", guest.projections)
	}
	for _, projection := range guest.projections {
		ids := make(map[string]bool)
		for _, share := range projection.Shares {
			ids[share.GetShareId()] = true
		}
		if len(ids) != 2 || !ids[id] || !ids[second.GetShare().GetId()] {
			t.Fatalf("incomplete Guest boot array: %v", projection)
		}
	}
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); err != nil {
		t.Fatal(err)
	}
	if len(guest.projections) != 2 {
		t.Fatal("unchanged static projection was republished")
	}
	good, err := database.GetHostShare(ctx, id)
	if err != nil || good.State != hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE {
		t.Fatal(good, err)
	}
	unavailable, err := database.GetHostShare(ctx, bad.GetShare().GetId())
	if err != nil || unavailable.State != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE {
		t.Fatal(unavailable, err)
	}

	guest.unavailableShare = second.GetShare().GetId()
	if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); status.Code(err) != codes.Unavailable {
		t.Fatalf("partial projection was not retained for retry: %v", err)
	}
	good, err = database.GetHostShare(ctx, id)
	if err != nil || good.State != hostv1.HostShareState_HOST_SHARE_STATE_AVAILABLE {
		t.Fatalf("healthy share lost authority: %+v %v", good, err)
	}
	if err := controller.hostShares.AuthorizeHostShareMounts(ctx, "service", []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: id}}}); err != nil {
		t.Fatalf("healthy share mount failed: %v", err)
	}
	unavailable, err = database.GetHostShare(ctx, second.GetShare().GetId())
	if err != nil || unavailable.State != hostv1.HostShareState_HOST_SHARE_STATE_UNAVAILABLE {
		t.Fatalf("missing Guest directory retained authority: %+v %v", unavailable, err)
	}
	guest.unavailableShare = ""
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); err != nil {
		t.Fatalf("partial projection did not recover: %v", err)
	}

	// A request queued behind Start must check the resulting state, not its entry snapshot.
	observed.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	entered, release, startDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		controller.vmLifecycle.Run(ctx, hostlifecycle.Start, nil, func(context.Context) error {
			transition, err := controller.inventory.Transition(ctx)
			if err != nil {
				return err
			}
			defer transition()
			close(entered)
			<-release
			observed.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
			return nil
		})
		close(startDone)
	}()
	<-entered
	queued := make(chan error, 1)
	go func() {
		_, err := operator.RevokeHostShare(ctx, &hostv1.RevokeHostShareRequest{RequestId: "queued-revoke", ShareId: id, ExpectedRevision: 2})
		queued <- err
	}()
	select {
	case err := <-queued:
		t.Fatalf("share mutation crossed Start: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	<-startDone
	if err := <-queued; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("queued mutation used stale stopped state: %v", err)
	}

	observed.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	entered, release, startDone = make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		controller.vmLifecycle.Run(ctx, hostlifecycle.Start, nil, func(context.Context) error {
			transition, err := controller.inventory.Transition(ctx)
			if err != nil {
				return err
			}
			defer transition()
			close(entered)
			<-release
			observed.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
			return nil
		})
		close(startDone)
	}()
	<-entered
	go func() {
		_, err := operator.BeginAgentIntegrationRemoval(ctx, &hostv1.BeginAgentIntegrationRemovalRequest{RequestId: "remove", IntegrationId: owner.ID, ExpectedRevision: owner.Revision})
		queued <- err
	}()
	select {
	case err := <-queued:
		t.Fatalf("share removal crossed Start: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	<-startDone
	if err := <-queued; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("online removal did not retain offline completion: %v", err)
	}
	good, err = database.GetHostShare(ctx, id)
	if err != nil || good.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		t.Fatalf("failed online removal revoked physical share: %+v %v", good, err)
	}
	guest.containers = []*vmmdv1.Container{{Id: "consumer", GenerationId: "generation", InstanceId: "consumer", State: vmmdv1.ContainerState_CONTAINER_STATE_RUNNING, Volumes: []*vmmdv1.VolumeMount{{Source: &vmmdv1.VolumeMount_HostShare{HostShare: id}}}}}
	if err := database.SetManagedContainerRunning(ctx, "service", "consumer", "generation", true); err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.ReconcileHostShares(ctx, vmmdv1.NewVMMDServiceClient(connection)); err != nil {
		t.Fatal(err)
	}
	good, err = database.GetHostShare(ctx, id)
	if err != nil || good.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING || len(guest.stopped) != 1 || len(guest.projections[len(guest.projections)-1].Shares) != 0 {
		t.Fatalf("online removal lost revoke intent or Guest authority: %+v %v stopped=%v", good, err, guest.stopped)
	}
	for _, id := range []string{second.GetShare().GetId(), bad.GetShare().GetId()} {
		current, err := database.GetHostShare(ctx, id)
		if err != nil || current.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKING {
			t.Fatalf("publishing another share overwrote revoke intent: %+v %v", current, err)
		}
	}
	if err := controller.hostShares.SettleEnvironmentHostShares(ctx, "no-shares"); err != nil {
		t.Fatalf("unrelated environment blocked: %v", err)
	}
	observed.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	if err := controller.hostShares.SettleEnvironmentHostShares(ctx, "service"); err != nil {
		t.Fatal(err)
	}
	if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if len(shares.directories) != 0 {
		t.Fatal("revoked shares were configured on next boot")
	}
}

func TestRemovingSharedEnvironmentIntegrationKeepsOnlineHostShare(t *testing.T) {
	controller, owner, _ := integrationAuthorityFixture(t)
	ctx := context.Background()
	if _, err := controller.store.CreateAgentIntegration(ctx, store.AgentIntegration{ID: "remaining-owner", EnvironmentID: owner.EnvironmentID, AdapterID: "other-service", DisplayName: "Other", Scope: 1, ConfigTarget: "other", DesiredState: 1, ConfigState: 2, PrepareRequestID: "other-prepare"}); err != nil {
		t.Fatal(err)
	}
	share := store.HostShare{ID: "share", EnvironmentID: owner.EnvironmentID, Source: store.HostShareSource{Path: "/fixture", FileSystemUUID: "fixture", FileID: 1}, Revision: 1}
	if _, err := controller.store.BeginHostShareMutation(ctx, store.Operation{ID: "share-create", RequestID: "share-create", Kind: "host-share-create", EnvironmentID: owner.EnvironmentID, State: 1}, share, 0); err != nil {
		t.Fatal(err)
	}
	controller.manager = &vmm.Manager{}
	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	_, err := callAsOperator(t, func(ctx context.Context) (*hostv1.BeginAgentIntegrationRemovalResponse, error) {
		return (&operatorServer{controller: controller}).BeginAgentIntegrationRemoval(ctx, &hostv1.BeginAgentIntegrationRemovalRequest{RequestId: "remove-owner", IntegrationId: owner.ID, ExpectedRevision: owner.Revision})
	})
	if err != nil {
		t.Fatal("removing one integration required VM stop", err)
	}
	current, err := controller.store.GetHostShare(ctx, share.ID)
	if err != nil || current.OperationID != "share-create" || current.State == hostv1.HostShareState_HOST_SHARE_STATE_REVOKED {
		t.Fatal("remaining integration lost share", current, err)
	}
}

func TestLocalDisposalRevokesStaticSharesBeforeGuestAvailability(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := openTestStore(path)
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
	controller := newTestController(&Controller{store: database, manager: manager})
	controller.vmLifecycle = newVMLifecycleOwner(controller, manager)
	var state atomic.Int32
	state.Store(int32(hostv1.VMState_VM_STATE_STOPPED))
	setTestLifecycleSnapshot(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(state.Load())} })
	shares := setupTestHostShareOwner(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(state.Load())} })
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
	controller.sessions = session.New(testSessionConfig(t, controller))
	t.Cleanup(controller.sessions.Close)
	controller.cleanup = assembleCleanup(controller)
	controller.remote.BindCleanup(controller.cleanup)
	integration, err := database.GetAgentIntegration(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	record := createTestSession(t, controller, "service", integration.ID, integration.Revision, auth.Identity{})
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
	releaseInventory := holdTestInventory(t, controller)
	queued := make(chan error, 1)
	queuedCtx, cancelQueued := context.WithTimeout(ctx, time.Second)
	defer cancelQueued()
	go func() { queued <- controller.cleanup.ReconcileLocalDisposal(queuedCtx, root) }()
	select {
	case err := <-queued:
		releaseInventory()
		t.Fatalf("offline share revocation crossed the VM start transition: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	state.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	releaseInventory()
	if err := <-queued; status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "Unavailable") {
		t.Fatalf("queued disposal lost offline fence or Guest failure: %v", err)
	}
	closedSession, err := database.GetSession(ctx, record.ID)
	if err != nil || closedSession.ClosedAt == nil {
		t.Fatalf("online share failure kept session live: %+v %v", closedSession, err)
	}
	if connection, err := net.Dial("unix", record.SocketPath); err == nil {
		connection.Close()
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
	if err := controller.cleanup.ReconcileLocalDisposal(ctx, root); status.Code(err) != codes.Unavailable {
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
	if err := database.RecoverAfterRestart(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	controller.store = database
	controller.services = nil
	controller.workloads = nil
	controller.remote = nil
	controller.cleanup = nil
	controller.appliances = nil
	controller.sessions = nil
	controller.hostShares = nil
	newTestController(controller)
	shares = setupTestHostShareOwner(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState(state.Load())} })
	persisted, err := database.GetHostShare(ctx, retired.ID)
	if err != nil || persisted.State != hostv1.HostShareState_HOST_SHARE_STATE_REVOKED || persisted.Revision != retired.Revision {
		t.Fatalf("share revocation was not durable: %+v %v", persisted, err)
	}
	if err := controller.hostShares.PrepareHostShares(ctx); err != nil {
		t.Fatal(err)
	}
	if len(shares.directories) != 0 {
		t.Fatal("disposed share entered the next boot map")
	}
	state.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
	if err := controller.cleanup.ReconcileLocalDisposal(ctx, root); status.Code(err) != codes.Unavailable {
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
