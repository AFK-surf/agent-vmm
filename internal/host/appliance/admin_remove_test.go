//go:build darwin || linux

package appliance

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	hostlifecycle "github.com/AFK-surf/agent-vmm/internal/host/lifecycle"
	"github.com/AFK-surf/agent-vmm/internal/host/servicenet"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestApplianceRemoveCanceledRPCPreservesAcceptedOperation(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	controller.manager = &vmm.Manager{}

	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	client := removalOperatorClient(t, controller)
	request := &appliancev1.ExecuteRemoveRequest{
		InstallationId: installation.ID, RequestId: "remove-busy", ExpectedRevision: installation.Revision,
		DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ConfirmedResourceIds: []string{installation.DataVolumeID},
	}
	unlockLifecycle := holdTestLifecycle(t, controller.vmLifecycle)
	defer unlockLifecycle()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	var callErr error
	go func() {
		_, callErr = client.ExecuteRemove(ctx, request)
		close(done)
	}()
	defer func() { cancel(); <-done }()
	var accepted store.ApplianceOperation
	for {
		operation, err := controller.store.ApplianceOperationByRequest(t.Context(), request.RequestId)
		if err == nil && operation.State == int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING) &&
			operation.Phase == int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PENDING_DESTROY) {
			accepted = operation
			break
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		select {
		case <-done:
			t.Fatalf("Remove returned before durable acceptance: %v", callErr)
		default:
		}
		select {
		case <-t.Context().Done():
			t.Fatal("Remove did not preserve its pending operation")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	<-done
	if status.Code(callErr) != codes.Canceled {
		t.Fatalf("canceled Remove RPC: %v", callErr)
	}
	waiting, err := controller.store.ApplianceOperationByRequest(t.Context(), request.RequestId)
	if err != nil || waiting.ID != accepted.ID || waiting.State != accepted.State || waiting.Phase != accepted.Phase || waiting.DeadlineUnixMillis != request.DeadlineUnixMillis || string(waiting.Receipt) != string(accepted.Receipt) {
		t.Fatalf("canceled Remove changed its accepted operation: before=%+v after=%+v err=%v", accepted, waiting, err)
	}
	committed, err := controller.store.TransitionApplianceOperation(t.Context(), waiting.ID, store.ApplianceOperationTransition{
		ExpectedState: waiting.State, ExpectedPhase: waiting.Phase,
		State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED),
		Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL), Receipt: []byte("removed:" + waiting.ID),
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := client.ExecuteRemove(t.Context(), request)
	if err != nil || replayed.GetOperation().GetOperationId() != committed.ID || replayed.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED {
		t.Fatalf("original Remove did not return committed receipt: %v %v", replayed, err)
	}
}

func TestApplianceRemoveDoesNotInvertAdmittedActivationLocks(t *testing.T) {
	if os.Getenv("AGENT_VMM_TEST_REMOVE_ACTIVATION") != "1" {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestApplianceRemoveDoesNotInvertAdmittedActivationLocks$")
		command.Env = append(os.Environ(), "AGENT_VMM_TEST_REMOVE_ACTIVATION=1", "TMPDIR="+t.TempDir())
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("activation and Remove subprocess: %v\n%s", err, output)
		}
		return
	}
	controller, installation := updateDrainFixture(t)
	controller.manager = &vmm.Manager{}

	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	server := controller
	defer hostLockWatchdog(t)()
	entered, release := make(chan struct{}), make(chan struct{})
	activationDone := make(chan error, 1)
	removeContext, cancelRemove := context.WithCancel(context.Background())
	defer cancelRemove()
	go func() {
		result := controller.vmLifecycle.Run(context.Background(), hostlifecycle.Appliance, nil, func(context.Context) error {
			close(entered)
			<-release
			// The admitted activation has reached its final installation boundary.
			// Remove has since advanced that fence, so no Guest call is allowed.
			err := controller.StartAppliance(context.Background(), nil, installation)
			cancelRemove()
			return err
		})
		activationDone <- result
	}()
	<-entered
	removeDone := make(chan error, 1)
	go func() {
		_, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ExecuteRemoveResponse, error) {
			ctx, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(removeContext, cancel)
			defer stop()
			defer cancel()
			return server.ExecuteRemove(ctx, &appliancev1.ExecuteRemoveRequest{InstallationId: installation.ID,
				RequestId: "remove-during-activation", ExpectedRevision: installation.Revision,
				DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(), ConfirmedResourceIds: []string{installation.DataVolumeID}})
		})
		removeDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		operation, err := controller.store.ApplianceOperationByRequest(context.Background(), "remove-during-activation")
		if err == nil && operation.Phase == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Remove was not durably admitted: %+v %v", operation, err)
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-activationDone; err != nil {
		t.Fatalf("stale activation crossed its installation fence: %v", err)
	}
	if err := <-removeDone; err != nil {
		t.Fatalf("accepted Remove lost its operation after cancellation: %v", err)
	}
	operation, err := controller.store.ApplianceOperationByRequest(context.Background(), "remove-during-activation")
	if err != nil || operation.State != 1 || operation.Phase != 4 || operation.ErrorReason != "APPLIANCE_REMOVE_PENDING" {
		t.Fatalf("canceled cleanup lost its original pending receipt: %+v %v", operation, err)
	}
}

func TestPendingApplianceRemovalDoesNotGateHostOrOperatorAndBoundsLockWaits(t *testing.T) {
	root := t.TempDir()
	databasePath := filepath.Join(root, "host.db")
	seed, installation := updateDrainFixtureAt(t, databasePath)
	operation, err := seed.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{
		ID: "pending-remove", InstallationID: installation.ID, RequestID: "original-remove", Kind: 7, State: 1, Phase: 4,
		DeadlineUnixMillis: time.Now().Add(150 * time.Millisecond).UnixMilli(), Receipt: []byte("original-pending-receipt")})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.store.Close(); err != nil {
		t.Fatal(err)
	}
	manager := removalRuntimeBundleManager(t, root)
	database, err := openTestStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	controller := newTestOwner(&Owner{store: database, manager: manager})
	controller.vmLifecycle = newVMLifecycleOwner(controller, manager)

	t.Cleanup(func() { closeTestOwner(controller) })
	setTestLifecycleSnapshot(controller, func() *hostv1.VM { return &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING} })
	client := removalOperatorClient(t, controller)
	for _, held := range []string{"owner", "installation", "trust-domain"} {
		t.Run(held, func(t *testing.T) {
			var unlock func()
			if held == "owner" {
				unlock = holdTestLifecycle(t, controller.vmLifecycle)
			} else if held == "installation" {
				var err error
				unlock, err = controller.applianceLocks.Lock(context.Background(), installation.ID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var err error
				unlock, err = controller.services.(*servicenet.Owner).LockServiceDomain(context.Background(), installation.RegistrationID)
				if err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- controller.recoverPendingApplianceRemovals(ctx) }()
			readContext, readCancel := context.WithTimeout(context.Background(), time.Second)
			defer readCancel()
			listed, err := client.ListAppliances(readContext, &appliancev1.ListAppliancesRequest{})
			if err != nil || len(listed.GetAppliances()) != 1 {
				unlock()
				t.Fatalf("Operator List blocked by pending cleanup: %v %v", listed, err)
			}
			watch, err := client.WatchApplianceOperation(readContext, &appliancev1.WatchApplianceOperationRequest{InstallationId: installation.ID, OperationId: operation.ID})
			if err == nil {
				_, err = watch.Recv()
			}
			if err != nil {
				unlock()
				t.Fatalf("Operator Watch blocked by pending cleanup: %v", err)
			}
			select {
			case err := <-done:
				unlock()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("cleanup lock wait did not return its deadline: %v", err)
				}
			case <-time.After(time.Second):
				unlock()
				t.Fatal("cleanup ignored its deadline while waiting for a lifecycle lock")
			}
			pending, err := controller.store.GetApplianceOperation(context.Background(), operation.ID)
			if err != nil || pending.State != 1 || pending.Phase != 4 || pending.RequestID != operation.RequestID || string(pending.Receipt) != string(operation.Receipt) || pending.ErrorReason != "APPLIANCE_REMOVE_PENDING" {
				t.Fatalf("cleanup failure lost its retry facts: %+v %v", pending, err)
			}
		})
	}
	committed, err := controller.store.TransitionApplianceOperation(context.Background(), operation.ID, store.ApplianceOperationTransition{
		ExpectedState: 1, ExpectedPhase: 4, State: 2, Phase: 5, Receipt: []byte("removed:" + operation.ID)})
	if err != nil {
		t.Fatal(err)
	}
	response, err := controller.resumeApplianceRemove(context.Background(), operation, false)
	if err != nil || response.GetOperationId() != committed.ID || int32(response.GetState()) != committed.State {
		t.Fatalf("queued stale retry did not return the committed receipt: %v %v", response, err)
	}
}

func removalRuntimeBundleManager(t *testing.T, root string) *vmm.Manager {
	t.Helper()
	directory := filepath.Join(root, "bundle")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	artifacts := make(map[string]bundle.Artifact)
	for _, name := range []string{"kernel", "initramfs", "rootfs"} {
		data := []byte(name)
		hash := sha256.Sum256(data)
		if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		artifacts[name] = bundle.Artifact{Path: name, Size: int64(len(data)), SHA256: hex.EncodeToString(hash[:])}
	}
	manifest, err := json.Marshal(bundle.Manifest{SchemaVersion: 1, Architecture: "arm64", Artifacts: artifacts,
		RootFS: bundle.RootFS{Filesystem: "squashfs", Compressor: "zstd", BlockSize: 1 << 20, Filter: "none"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, bundle.ManifestName), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(manifest)
	manager, err := vmm.NewManager(vmm.Config{BundleDirectory: directory, InstanceDirectory: filepath.Join(root, "instance"),
		SerialLog: filepath.Join(root, "serial.log"), CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30, GuestManifestDigest: hex.EncodeToString(hash[:])})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func removalOperatorClient(t *testing.T, controller *Owner) appliancev1.ApplianceAdminServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	operator := newTestOperatorServer(controller)
	go operator.Serve(listener)
	t.Cleanup(operator.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	client := appliancev1.NewApplianceAdminServiceClient(connection)
	return client
}

func TestHangingApplianceDataCleanupLeavesOperatorAndOriginalRetryAvailable(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	controller.manager = &vmm.Manager{}

	controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
	secrets, err := NewSecretStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	controller.applianceSecrets = secrets
	if err := secrets.StoreBootstrap(context.Background(), installation.ID, "bootstrap", []byte("test seed")); err != nil {
		t.Fatal(err)
	}
	operation, err := controller.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{
		ID: "pending-remove", InstallationID: installation.ID, RequestID: "original-remove", Kind: 7, State: 1, Phase: 4})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	guest := &applianceDataRemovalGuest{requests: make(chan applianceDataRemovalCall)}
	guestServer := grpc.NewServer()
	vmmdv1.RegisterVMMDServiceServer(guestServer, guest)
	go guestServer.Serve(listener)
	t.Cleanup(guestServer.Stop)
	connect := func() *grpc.ClientConn {
		connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		return connection
	}
	client := removalOperatorClient(t, controller)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	connection := connect()
	go func() {
		result := controller.vmLifecycle.Run(ctx, hostlifecycle.Appliance, nil, func(context.Context) error {
			return controller.removeApplianceData(ctx, vmmdv1.NewVMMDServiceClient(connection), installation)
		})
		done <- result
	}()
	var original applianceDataRemovalCall
	select {
	case original = <-guest.requests:
	case err := <-done:
		t.Fatalf("cleanup returned before the Guest received its request: %v", err)
	case <-t.Context().Done():
		t.Fatal("Guest did not receive cleanup and the lifecycle owner did not return")
	}
	readContext, readCancel := context.WithCancel(t.Context())
	defer readCancel()
	if _, err := client.ListAppliances(readContext, &appliancev1.ListAppliancesRequest{}); err != nil {
		t.Fatalf("hanging Guest cleanup blocked Operator: %v", err)
	}
	watch, err := client.WatchApplianceOperation(readContext, &appliancev1.WatchApplianceOperationRequest{InstallationId: installation.ID, OperationId: operation.ID})
	if err == nil {
		_, err = watch.Recv()
	}
	if err != nil {
		t.Fatalf("hanging Guest cleanup blocked receipt observation: %v", err)
	}
	readCancel()
	cancel()
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("Guest cleanup did not honor its context: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Guest cleanup did not release the lifecycle owner")
	}
	<-original.finished
	pending, err := controller.store.GetApplianceOperation(t.Context(), operation.ID)
	if err != nil || pending.State != operation.State || pending.Phase != operation.Phase || pending.RequestID != operation.RequestID || string(pending.Receipt) != string(operation.Receipt) {
		t.Fatalf("unconfirmed cleanup changed its pending receipt: %+v %v", pending, err)
	}
	if _, err := secrets.LoadBootstrap(context.Background(), installation.ID, "bootstrap"); err != nil {
		t.Fatalf("unconfirmed cleanup removed its secret: %v", err)
	}
	retryDone := make(chan error, 1)
	connection = connect()
	go func() {
		result := controller.vmLifecycle.Run(t.Context(), hostlifecycle.Appliance, nil, func(context.Context) error {
			return controller.removeApplianceData(t.Context(), vmmdv1.NewVMMDServiceClient(connection), installation)
		})
		retryDone <- result
	}()
	select {
	case retry := <-guest.requests:
		if !proto.Equal(original.request, retry.request) {
			t.Fatalf("cleanup retried a different target command: %v", retry.request)
		}
		retry.reply <- false
	case err := <-retryDone:
		t.Fatalf("cleanup retry did not reach its original target: %v", err)
	case <-t.Context().Done():
		t.Fatal("Guest did not receive cleanup retry")
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("cleanup did not retry its original target command: %v", err)
	}
	if _, err := secrets.LoadBootstrap(context.Background(), installation.ID, "bootstrap"); !errors.Is(err, errBootstrapNotFound) {
		t.Fatalf("confirmed retry retained target secret: %v", err)
	}
	pending, err = controller.store.GetApplianceOperation(context.Background(), operation.ID)
	if err != nil || pending.RequestID != operation.RequestID || pending.State != 1 || pending.Phase != 4 {
		t.Fatalf("Guest cleanup changed the durable Remove receipt: %+v %v", pending, err)
	}
}

func TestBackgroundRemovalObservesVMStateAfterOwnerAndExplicitReplayRetainsStartAuthority(t *testing.T) {
	for _, stoppedState := range []hostv1.VMState{hostv1.VMState_VM_STATE_STOPPED, hostv1.VMState_VM_STATE_FAILED} {
		t.Run(stoppedState.String(), func(t *testing.T) {
			controller, installation := updateDrainFixture(t)
			root := t.TempDir()
			manager, err := vmm.NewManager(vmm.Config{BundleDirectory: root, InstanceDirectory: root,
				SerialLog: filepath.Join(root, "serial.log"), CPUCount: 1, MemoryBytes: 512 << 20, StateBytes: 1 << 30})
			if err != nil {
				t.Fatal(err)
			}
			controller.manager = manager

			controller.vmLifecycle = newVMLifecycleOwner(controller, controller.manager)
			registration, err := controller.store.GetRemoteRegistration(context.Background(), installation.RegistrationID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := controller.store.SetRemoteEnrollment(context.Background(), registration.ID, registration.Revision, registration.EnrollmentState,
				int32(hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_PENDING_CREDENTIAL), "", ""); err != nil {
				t.Fatal(err)
			}
			operation, err := controller.store.CreateApplianceOperation(context.Background(), store.ApplianceOperation{
				ID: "pending-remove", InstallationID: installation.ID, RequestID: "original-remove", Kind: 7, State: 1, Phase: 4,
				ExpectedRevision: installation.Revision, DeadlineUnixMillis: time.Now().Add(time.Minute).UnixMilli(),
				Receipt: []byte("original-pending"), ErrorReason: "PREVIOUS_CLEANUP_FAILURE", ErrorMessage: "retained original failure"})
			if err != nil {
				t.Fatal(err)
			}
			var state atomic.Int32
			state.Store(int32(hostv1.VMState_VM_STATE_RUNNING))
			observed := make(chan hostv1.VMState, 1)
			setTestLifecycleSnapshot(controller, func() *hostv1.VM {
				current := hostv1.VMState(state.Load())
				select {
				case observed <- current:
				default:
				}
				return &hostv1.VM{State: current}
			})
			unlockLifecycle := holdTestLifecycle(t, controller.vmLifecycle)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- controller.recoverPendingApplianceRemovals(ctx) }()
			select {
			case <-observed:
				unlockLifecycle()
				t.Fatal("background cleanup observed VM state before acquiring its owner")
			case <-done:
				unlockLifecycle()
				t.Fatal("background cleanup bypassed the current lifecycle owner")
			case <-time.After(25 * time.Millisecond):
			}
			state.Store(int32(stoppedState))
			unlockLifecycle()
			if err := <-done; err != nil {
				t.Fatalf("background cleanup should retain a stopped VM's pending operation: %v", err)
			}
			if current := <-observed; current != stoppedState {
				t.Fatalf("background cleanup used stale running state: %s", current)
			}
			pending, err := controller.store.GetApplianceOperation(context.Background(), operation.ID)
			if err != nil || pending.State != operation.State || pending.Phase != operation.Phase || pending.RequestID != operation.RequestID ||
				pending.ErrorReason != operation.ErrorReason || pending.ErrorMessage != operation.ErrorMessage || string(pending.Receipt) != string(operation.Receipt) {
				t.Fatalf("background cleanup changed a stopped VM or its pending receipt: %+v %v", pending, err)
			}
			if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_STOPPED {
				t.Fatal("background cleanup attempted a VM start")
			}
			response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ExecuteRemoveResponse, error) {
				return (controller).ExecuteRemove(ctx, &appliancev1.ExecuteRemoveRequest{
					InstallationId: installation.ID, RequestId: operation.RequestID, ExpectedRevision: operation.ExpectedRevision,
					DeadlineUnixMillis: operation.DeadlineUnixMillis, ConfirmedResourceIds: []string{installation.DataVolumeID}})
			})
			// The empty Manager has no bundle, so a permitted start fails before VZ.
			// Its FAILED state proves the explicit replay reached that real boundary.
			if err != nil || controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_FAILED || response.GetOperation().GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING {
				t.Fatalf("explicit original Remove lost VM start authority or pending receipt: state=%s response=%v error=%v", controller.manager.Snapshot().GetState(), response, err)
			}
		})
	}
}

func TestApplianceRemoveApprovesWholeCurrentDomain(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	for index := range 3 {
		id := fmt.Sprintf("environment-%d", index)
		if _, err := controller.store.CreateEnvironment(ctx, store.Environment{ID: id, NamespaceID: id, DisplayName: id, OwnerKind: store.OwnerRemoteRegistration, RegistrationID: installation.RegistrationID, AllocationID: id, AllocationGeneration: 1, AllocationState: int32(hostv1.AllocationState_ALLOCATION_STATE_RETAINED), PIDs: 1, DiskBytes: 1, EgressMode: 1}); err != nil {
			t.Fatal(err)
		}
	}
	server := controller
	deadline := time.Now().Add(time.Minute).UnixMilli()
	plan, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.PlanRemoveResponse, error) {
		return server.PlanRemove(ctx, &appliancev1.PlanRemoveRequest{InstallationId: installation.ID, RequestId: "remove-domain", ExpectedRevision: installation.Revision, DeadlineUnixMillis: deadline})
	})
	if err != nil {
		t.Fatal(err)
	}
	allocations, domain := 0, false
	for _, impact := range plan.GetPlan().GetImpacts() {
		if impact.GetBlocker() || !impact.GetIrreversible() {
			t.Fatalf("impact is not part of domain removal: %v", impact)
		}
		if impact.GetResourceType() == "allocation" {
			allocations++
		}
		domain = domain || impact.GetResourceType() == "private_domain"
	}
	if allocations != 3 || !domain {
		t.Fatalf("domain plan = %v", plan)
	}
	operation, err := callAsOperator(t, func(ctx context.Context) (store.ApplianceOperation, error) {
		return server.acceptApplianceRemove(ctx, &appliancev1.ExecuteRemoveRequest{InstallationId: installation.ID, RequestId: "remove-domain", ExpectedRevision: installation.Revision, DeadlineUnixMillis: deadline, ConfirmedResourceIds: []string{installation.DataVolumeID}})
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.store.CommitApplianceRemove(ctx, store.ApplianceRemove{InstallationID: installation.ID, RegistrationID: installation.RegistrationID, OperationID: operation.ID, CommitRevision: current.Revision}); !errors.Is(err, store.ErrApplianceDomainNotEmpty) {
		t.Fatalf("nonempty domain committed: %v", err)
	}
	replay, err := server.acceptApplianceRemove(ctx, &appliancev1.ExecuteRemoveRequest{InstallationId: installation.ID, RequestId: operation.RequestID, ExpectedRevision: operation.ExpectedRevision, DeadlineUnixMillis: deadline, ConfirmedResourceIds: []string{installation.DataVolumeID}})
	if err != nil || replay.ID != operation.ID {
		t.Fatalf("Remove replay identity = %v, %v", replay, err)
	}
}

func TestRevokeRemoveReinstallDoesNotRetainOldRevocation(t *testing.T) {
	controller, installation := updateDrainFixture(t)
	ctx := context.Background()
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	registration, err := controller.store.GetRemoteRegistration(ctx, installation.RegistrationID)
	if err != nil {
		t.Fatal(err)
	}
	server := controller
	deadline := time.Now().Add(time.Minute).UnixMilli()
	_, err = callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperation, error) {
		return server.setDesiredState(ctx, applianceMutation{installation.ID, "revoke-before-remove", installation.Revision, deadline}, installation.Revision, false, appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_REVOKE, "revoke-pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		t.Fatal(err)
	}
	remove := store.ApplianceRemove{InstallationID: current.ID, RegistrationID: current.RegistrationID, OperationID: "remove-revoked", ExpectedRevision: current.Revision, ExpectedState: current.State, PendingState: 4, ExpectedPhase: 1, Operation: store.ApplianceOperation{ID: "remove-revoked", InstallationID: current.ID, RequestID: "remove-revoked", Kind: 7, State: 1, Phase: 1}}
	pending, err := controller.store.BeginApplianceRemove(ctx, remove)
	if err != nil {
		t.Fatal(err)
	}
	remove.CommitRevision = pending.Revision
	if _, err := controller.store.CommitApplianceRemove(ctx, remove); err != nil {
		t.Fatal(err)
	}
	installation.Revision = 1
	installation, _, _, err = controller.store.InstallApplianceDomain(ctx, installation, grant, registration, nil, store.ApplianceOperation{ID: "reinstall", InstallationID: installation.ID, RequestID: "reinstall", Kind: 2, State: 2, Phase: 5})
	if err != nil {
		t.Fatal(err)
	}
	response, err := callAsOperator(t, func(ctx context.Context) (*appliancev1.ApplianceOperation, error) {
		return server.setDesiredState(ctx, applianceMutation{installation.ID, "activate-reinstalled", installation.Revision, deadline}, installation.Revision, true, appliancev1.ApplianceState_APPLIANCE_STATE_STARTING, appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_ACTIVATE, "activation-pending")
	})
	if err != nil || response.GetPhase() != appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_PLANNED {
		t.Fatalf("replacement activation = %v, %v", response, err)
	}
}
