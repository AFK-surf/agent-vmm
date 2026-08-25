//go:build darwin && e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

type e2eCredentialStore struct {
	mu    sync.Mutex
	value []byte
}

func (store *e2eCredentialStore) Load(context.Context, string) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]byte(nil), store.value...), nil
}
func (store *e2eCredentialStore) Replace(_ context.Context, _ string, value []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.value = append(store.value[:0], value...)
	return nil
}
func (store *e2eCredentialStore) Delete(context.Context, string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.value = nil
	return nil
}

type remoteE2EController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	registrationID  string
	sessions        chan remoteE2ESession
	finish          chan struct{}
	connections     chan int
	failures        chan string
	connectionEpoch atomic.Uint64
}
type remoteE2ESession struct {
	allocationID string
	connection   net.Conn
}

func (server *remoteE2EController) Enroll(context.Context, *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (server *remoteE2EController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	if values := metadata.ValueFromIncomingContext(stream.Context(), "authorization"); len(values) != 1 || values[0] != controllerremote.EncodeBearer([]byte("remote-e2e")) {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	session, err := controllerremote.AcceptControl(stream, server.registrationID)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "invalid initial inventory")
	}
	hello := session.Hello()
	server.connectionEpoch.Store(hello.GetConnectionEpoch())
	leases := make([]*remotev1.Lease, 0, 2)
	initialAllocations := make(map[string]*remotev1.Allocation, len(hello.GetInventory()))
	for _, allocation := range hello.GetInventory() {
		initialAllocations[allocation.GetAllocationId()] = allocation
		if allocation.GetLease() != nil {
			leases = append(leases, allocation.GetLease())
		}
	}
	if err := session.Ready(); err != nil {
		return err
	}
	expectedAllocations := map[string]struct{}{"remote-a": {}, "remote-b": {}}
	for _, allocation := range hello.GetInventory() {
		if _, ok := expectedAllocations[allocation.GetAllocationId()]; !ok {
			return status.Errorf(codes.FailedPrecondition, "unexpected initial allocation %q", allocation.GetAllocationId())
		}
	}
	for index, allocationID := range []string{"remote-a", "remote-b"} {
		egress := hostv1.EgressMode_EGRESS_MODE_DENY_ALL
		if index == 0 {
			egress = hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET
		}
		if existing := initialAllocations[allocationID]; existing != nil && existing.GetLease() != nil {
			// A reconnect may already carry a live lease. Preserve it instead of
			// issuing EnsureAllocation against a partially settled inventory.
			continue
		}
		limits := &hostv1.ResourceLimits{CpuMillis: optional(uint32(500)), MemoryBytes: optional(uint64(1 << 30)), Pids: optional(uint32(256)), DiskBytes: optional(uint64(2 << 30))}
		result, err := server.command(session, &remotev1.ControllerCommand{CommandId: "ensure-" + allocationID, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: allocationID, DisplayLabel: allocationID, Limits: limits, EgressMode: egress, PolicyRevision: hello.GetPoolRevision()}}})
		if err != nil {
			return err
		}
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			server.reportFailure("ensure " + result.GetReason().String())
			return status.Error(codes.Internal, "ensure failed")
		}
		if lease := result.GetAllocation().GetLease(); lease != nil {
			leases = appendLeaseOnce(leases, lease)
			continue
		}
		result, err = server.command(session, &remotev1.ControllerCommand{CommandId: "acquire-" + allocationID, TargetRevision: result.GetAllocation().GetRevision(), Command: &remotev1.ControllerCommand_AcquireLease{AcquireLease: &remotev1.AcquireLease{AllocationId: allocationID, DurationSeconds: 300, WorkloadRef: "e2e-" + allocationID}}})
		if err != nil {
			return err
		}
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			server.reportFailure("acquire " + result.GetReason().String())
			return status.Error(codes.Internal, "acquire failed")
		}
		leases = appendLeaseOnce(leases, result.GetLease())
	}
	server.connections <- len(leases)
	for _, lease := range leases {
		result, err := server.command(session, &remotev1.ControllerCommand{CommandId: "open-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: lease.GetAllocationId(), LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
		if err != nil {
			return err
		}
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			server.reportFailure("open " + lease.GetAllocationId() + " lease=" + lease.GetLeaseId() + " generation=" + strconv.FormatUint(lease.GetGeneration(), 10) + " " + result.GetReason().String() + " " + result.GetMessage())
			return status.Error(codes.Internal, "open failed")
		}
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, lease := range leases {
		releasedLease, err := server.command(session, &remotev1.ControllerCommand{CommandId: "release-lease-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ReleaseLease{ReleaseLease: &remotev1.ReleaseLease{LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
		if err != nil || releasedLease.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "release lease %s failed: %v %s", lease.GetAllocationId(), err, releasedLease.GetMessage())
		}
		revision := releasedLease.GetLease().GetAllocationRevision()
		releasedAllocation, err := server.command(session, &remotev1.ControllerCommand{CommandId: "release-allocation-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: lease.GetAllocationId(), ExpectedRevision: revision}}})
		if err != nil || releasedAllocation.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "release allocation %s failed: %v %s", lease.GetAllocationId(), err, releasedAllocation.GetMessage())
		}
		plan, err := server.command(session, &remotev1.ControllerCommand{CommandId: "plan-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: lease.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD}}})
		if err != nil || plan.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "plan discard %s failed: %v %s", lease.GetAllocationId(), err, plan.GetMessage())
		}
		discarded, err := server.command(session, &remotev1.ControllerCommand{CommandId: "discard-" + lease.GetAllocationId(), Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: lease.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: plan.GetCleanupPlan().GetRevision(), ConfirmedVolumeIds: plan.GetCleanupPlan().GetVolumeIds()}}})
		if err != nil || discarded.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "discard %s failed: %v %s", lease.GetAllocationId(), err, discarded.GetMessage())
		}
	}
	return nil
}

func (server *remoteE2EController) reportFailure(message string) {
	select {
	case server.failures <- message:
	default:
	}
}

func appendLeaseOnce(leases []*remotev1.Lease, lease *remotev1.Lease) []*remotev1.Lease {
	if lease == nil {
		return leases
	}
	for _, current := range leases {
		if current.GetAllocationId() == lease.GetAllocationId() {
			return leases
		}
	}
	return append(leases, lease)
}

func (server *remoteE2EController) command(session *controllerremote.ControlSession, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	return dispatchRemoteCommand(session, command)
}

func dispatchRemoteCommand(session *controllerremote.ControlSession, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	if ensure := command.GetEnsureAllocation(); ensure != nil {
		command.TargetRevision = ensure.GetPolicyRevision()
	}
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_ReleaseAllocation:
		command.TargetRevision = value.ReleaseAllocation.GetExpectedRevision()
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		command.TargetRevision = value.ExecuteCleanupAllocation.GetPlanRevision()
	case *remotev1.ControllerCommand_RenewLease:
		command.LeaseGeneration = value.RenewLease.GetGeneration()
	case *remotev1.ControllerCommand_ReleaseLease:
		command.LeaseGeneration = value.ReleaseLease.GetGeneration()
	case *remotev1.ControllerCommand_OpenSession:
		command.LeaseGeneration = value.OpenSession.GetGeneration()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	transcript, err := session.Dispatch(ctx, command)
	return transcript.Result, err
}

type isolatedDomainController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	registrationID string
	session        chan net.Conn
	finish         chan struct{}
	cleaned        chan struct{}
	epoch          atomic.Uint64
	cleanedOnce    sync.Once
	finished       atomic.Bool
}

func (server *isolatedDomainController) Enroll(context.Context, *remotev1.EnrollRequest) (*remotev1.EnrollResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (server *isolatedDomainController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	if values := metadata.ValueFromIncomingContext(stream.Context(), "authorization"); len(values) != 1 || values[0] != controllerremote.EncodeBearer([]byte("remote-e2e")) {
		return status.Error(codes.Unauthenticated, "rejected")
	}
	if server.finished.Load() {
		return status.Error(codes.FailedPrecondition, "domain test completed")
	}
	session, err := controllerremote.AcceptControl(stream, server.registrationID)
	if err != nil {
		return err
	}
	hello := session.Hello()
	server.epoch.Store(hello.GetConnectionEpoch())
	if err := session.Ready(); err != nil {
		return err
	}
	if len(hello.GetInventory()) != 0 {
		return status.Error(codes.FailedPrecondition, "isolated domain started with unexpected inventory")
	}
	limits := remoteLimits(100, 128<<20, 64, 256<<20)
	ensured, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "ensure-shared", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "shared", DisplayLabel: "shared", Limits: limits, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: hello.GetPoolRevision()}}})
	if err != nil || ensured.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "ensure failed: %v %s", err, ensured.GetMessage())
	}
	acquired, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "acquire-shared", TargetRevision: ensured.GetAllocation().GetRevision(), Command: &remotev1.ControllerCommand_AcquireLease{AcquireLease: &remotev1.AcquireLease{AllocationId: "shared", DurationSeconds: 300, WorkloadRef: "three-domain-e2e"}}})
	if err != nil || acquired.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "acquire failed: %v %s", err, acquired.GetMessage())
	}
	lease := acquired.GetLease()
	opened, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "open-shared", Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: "shared", LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
	if err != nil || opened.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "open failed: %v %s", err, opened.GetMessage())
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	releasedLease, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "release-lease-shared", Command: &remotev1.ControllerCommand_ReleaseLease{ReleaseLease: &remotev1.ReleaseLease{LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
	if err != nil || releasedLease.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "release lease failed: %v %s", err, releasedLease.GetMessage())
	}
	revision := releasedLease.GetLease().GetAllocationRevision()
	released, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "release-allocation-shared", Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: "shared", ExpectedRevision: revision}}})
	if err != nil || released.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "release allocation failed: %v %s", err, released.GetMessage())
	}
	plan, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "plan-shared", Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: "shared", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD}}})
	if err != nil || plan.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "plan discard failed: %v %s", err, plan.GetMessage())
	}
	discarded, err := dispatchRemoteCommand(session, &remotev1.ControllerCommand{CommandId: "discard-shared", Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "shared", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: plan.GetCleanupPlan().GetRevision(), ConfirmedVolumeIds: plan.GetCleanupPlan().GetVolumeIds()}}})
	if err != nil || discarded.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "discard failed: %v %s", err, discarded.GetMessage())
	}
	server.finished.Store(true)
	server.cleanedOnce.Do(func() { close(server.cleaned) })
	return nil
}

func (server *isolatedDomainController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	_, connection, err := controllerremote.AcceptSession(stream, server.registrationID, server.epoch.Load())
	if err != nil {
		return err
	}
	select {
	case server.session <- connection:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case <-connection.Done():
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (server *remoteE2EController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	header, connection, err := controllerremote.AcceptSession(stream, server.registrationID, server.connectionEpoch.Load())
	if err != nil {
		return status.Error(codes.Unauthenticated, "bad session header")
	}
	select {
	case server.sessions <- remoteE2ESession{allocationID: header.GetAllocationId(), connection: connection}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case <-connection.Done():
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func testRemoteWorkflow(t *testing.T, harness *hostHarness) {
	trustConnection, err := grpc.NewClient("passthrough:///trust", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", harness.trustSocket)
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer trustConnection.Close()
	if _, err := trustv1.NewTrustServiceClient(trustConnection).GetDeviceIdentity(context.Background(), &trustv1.GetDeviceIdentityRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("same-UID trust client without connector capability code=%s err=%v", status.Code(err), err)
	}
	controller := &remoteE2EController{sessions: make(chan remoteE2ESession, 4), finish: make(chan struct{}), connections: make(chan int, 8), failures: make(chan string, 8)}
	endpoint, roots, stopController := startRemoteController(t, controller)
	defer stopController()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	pool := &hostv1.RemotePoolPolicy{MaxEnvironments: 2, AggregateLimits: remoteLimits(1000, 2<<30, 512, 4<<30), PerEnvironmentLimits: remoteLimits(500, 1<<30, 256, 2<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, MaxLeaseDuration: durationpb.New(5 * time.Minute), DisconnectGrace: durationpb.New(30 * time.Second), StopGrace: durationpb.New(2 * time.Second)}
	registered, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: "remote-e2e", RegistrationId: "registration-remote-e2e", Endpoint: endpoint, DisplayName: "E2E Controller", ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool})
	if err != nil {
		t.Fatal(err)
	}
	registered, err = harness.client.CompleteRemoteEnrollment(ctx, &hostv1.CompleteRemoteEnrollmentRequest{
		RequestId: "remote-e2e-complete", RegistrationId: registered.GetRegistration().GetId(),
		ExpectedRevision: registered.GetRegistration().GetRevision(),
	})
	if err != nil {
		t.Fatal(err)
	}
	controller.registrationID = registered.GetRegistration().GetId()
	store := &e2eCredentialStore{value: []byte("remote-e2e")}
	startConnector := func() (context.CancelFunc, <-chan error) {
		connectorContext, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- remoteconnector.Run(connectorContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost",
				RegistrationID: controller.registrationID, RemoteComputeSocket: harness.remoteComputeSocket, TrustSocket: harness.trustSocket,
				TrustCapabilityFile: harness.trustCapabilityFile, CredentialStore: store, TLSRoots: roots})
		}()
		return stop, done
	}
	collectSessions := func() map[string]remoteE2ESession {
		sessions := make(map[string]remoteE2ESession)
		for len(sessions) != 2 {
			select {
			case session := <-controller.sessions:
				sessions[session.allocationID] = session
			case failure := <-controller.failures:
				current, _ := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: controller.registrationID})
				t.Fatalf("controller command failed: %s state=%+v", failure, current.GetRegistration().GetAllocations())
			case <-ctx.Done():
				t.Fatal("remote sessions did not open")
			}
		}
		return sessions
	}
	awaitConnection := func(timeout time.Duration) int {
		select {
		case count := <-controller.connections:
			return count
		case failure := <-controller.failures:
			current, _ := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: controller.registrationID})
			t.Fatalf("controller connection failed: %s state=%+v", failure, current.GetRegistration().GetAllocations())
			return 0
		case <-time.After(timeout):
			t.Fatal("controller did not report connection inventory")
			return 0
		}
	}
	stopConnector, connectorDone := startConnector()
	if count := awaitConnection(30 * time.Second); count != 2 {
		t.Fatalf("first connection lease inventory=%d", count)
	}
	firstSessions := collectSessions()
	for _, session := range firstSessions {
		session.connection.Close()
	}
	stopConnector()
	select {
	case <-connectorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first connector did not stop")
	}
	stopConnector, connectorDone = startConnector()
	select {
	case count := <-controller.connections:
		if count != 2 {
			t.Fatalf("reconnect lease inventory=%d", count)
		}
	case failure := <-controller.failures:
		current, _ := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: controller.registrationID})
		t.Fatalf("controller reconnect failed: %s state=%+v", failure, current.GetRegistration().GetAllocations())
	case <-time.After(10 * time.Second):
		t.Fatal("controller did not reconnect")
	}
	sessions := collectSessions()
	clients := make(map[string]*grpc.ClientConn)
	for allocationID, session := range sessions {
		connection, err := grpc.NewClient("passthrough:///remote-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return session.connection, nil }))
		if err != nil {
			t.Fatal(err)
		}
		clients[allocationID] = connection
		runtime := hostv1.NewAgentRuntimeServiceClient(connection)
		environment, err := runtime.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if environment.GetEnvironment().GetOrigin() != hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_REMOTE || environment.GetEnvironment().GetId() != "" || environment.GetEnvironment().GetNamespaceId() != "" {
			t.Fatalf("unsafe remote projection: %+v", environment)
		}
	}
	remoteClient := &productClient{sessions: map[string]*productSession{"remote-a": {connection: clients["remote-a"], client: hostv1.NewAgentRuntimeServiceClient(clients["remote-a"])}}}
	pullImage(t, remoteClient, "remote-a")
	contextArchive := dockerfileContext(t, buildShell(t))
	result := buildImage(t, remoteClient, "remote-a", "agent.local/remote-e2e:latest", contextArchive, []byte("remote-secret"))
	if result.digest == "" {
		t.Fatal("remote build returned no digest")
	}
	createVolume(t, remoteClient, "remote-a", "data")
	serverContainer := createContainerWithImageAndVolumes(t, remoteClient, "remote-a", "server", "agent.local/remote-e2e:latest", []string{"/bin/result", "--serve", "8080"}, []*hostv1.VolumeMount{{VolumeId: "data", Destination: "/data"}})
	startContainer(t, remoteClient, "remote-a", serverContainer.GetId())
	time.Sleep(500 * time.Millisecond)
	if output, code := execContainer(t, remoteClient, "remote-a", serverContainer.GetId(), []string{"/bin/result", "--write", "/data/value", "remote-exec"}); code != 0 || string(output) != "remote-exec\n" {
		t.Fatalf("remote exec=%q code=%d", output, code)
	}
	if output, code := execContainer(t, remoteClient, "remote-a", serverContainer.GetId(), []string{"/bin/result", "--cpu"}); code != 0 || string(output) != "cpu-ok\n" {
		t.Fatalf("remote cpu exec=%q code=%d", output, code)
	}
	testRemoteForward(t, remoteClient, serverContainer.GetId())
	logged := createContainerWithImage(t, remoteClient, "remote-a", "logged", "agent.local/remote-e2e:latest", []string{"/bin/result"})
	startContainer(t, remoteClient, "remote-a", logged.GetId())
	waitContainer(t, remoteClient, "remote-a", logged.GetId(), 0)
	if output := readLogs(t, remoteClient, "remote-a", logged.GetId()); !bytes.Contains(output, []byte("build-ok")) {
		t.Fatalf("remote logs=%q", output)
	}
	for _, connection := range clients {
		connection.Close()
	}
	close(controller.finish)
	deadline := time.Now().Add(30 * time.Second)
	drained := false
	for time.Now().Before(deadline) {
		current, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: controller.registrationID})
		if err == nil && len(current.GetRegistration().GetAllocations()) == 0 {
			drained = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !drained {
		current, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: controller.registrationID})
		t.Fatalf("remote allocations did not drain: state=%+v err=%v", current.GetRegistration().GetAllocations(), err)
	}
	stopConnector()
	select {
	case <-connectorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("connector did not stop")
	}
}

func testThreeRemoteDomains(t *testing.T, harness *hostHarness) {
	type domain struct {
		controller   *isolatedDomainController
		registration string
		stop         context.CancelFunc
		done         <-chan error
		stopServer   func()
		connection   *grpc.ClientConn
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool := &hostv1.RemotePoolPolicy{MaxEnvironments: 1, AggregateLimits: remoteLimits(100, 128<<20, 64, 256<<20), PerEnvironmentLimits: remoteLimits(100, 128<<20, 64, 256<<20), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, MaxLeaseDuration: durationpb.New(5 * time.Minute), DisconnectGrace: durationpb.New(5 * time.Second), StopGrace: durationpb.New(time.Second)}
	domains := make([]domain, 0, 3)
	defer func() {
		for _, current := range domains {
			if current.connection != nil {
				current.connection.Close()
			}
			current.stop()
			current.stopServer()
		}
	}()
	for index := range 3 {
		controller := &isolatedDomainController{session: make(chan net.Conn, 1), finish: make(chan struct{}), cleaned: make(chan struct{})}
		endpoint, roots, stopServer := startRemoteController(t, controller)
		requestID := "three-domain-" + strconv.Itoa(index)
		registered, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: requestID, RegistrationId: "registration-" + requestID, Endpoint: endpoint, DisplayName: requestID, ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool})
		if err != nil {
			t.Fatal(err)
		}
		registered, err = harness.client.CompleteRemoteEnrollment(ctx, &hostv1.CompleteRemoteEnrollmentRequest{RequestId: requestID + "-complete", RegistrationId: registered.GetRegistration().GetId(), ExpectedRevision: registered.GetRegistration().GetRevision()})
		if err != nil {
			t.Fatal(err)
		}
		controller.registrationID = registered.GetRegistration().GetId()
		connectorContext, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- remoteconnector.Run(connectorContext, remoteconnector.Config{Endpoint: endpoint, EndpointIdentity: "localhost", RegistrationID: controller.registrationID, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: &e2eCredentialStore{value: []byte("remote-e2e")}, TLSRoots: roots})
		}()
		domains = append(domains, domain{controller: controller, registration: controller.registrationID, stop: stop, done: done, stopServer: stopServer})
	}

	environmentIDs := make(map[string]struct{}, 3)
	for index := range domains {
		var raw net.Conn
		select {
		case raw = <-domains[index].controller.session:
		case <-ctx.Done():
			t.Fatal("three-domain runtime session did not open")
		}
		connection, err := grpc.NewClient("passthrough:///isolated-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return raw, nil }))
		if err != nil {
			t.Fatal(err)
		}
		domains[index].connection = connection
		runtime := hostv1.NewAgentRuntimeServiceClient(connection)
		if _, err := runtime.CreateVolume(ctx, &hostv1.CreateVolumeRequest{RequestId: "create-shared-volume-domain-" + strconv.Itoa(index), VolumeId: "shared-volume"}); err != nil {
			t.Fatal(err)
		}
		volumes, err := runtime.ListVolumes(ctx, &hostv1.ListVolumesRequest{})
		if err != nil || len(volumes.GetVolumes()) != 1 || volumes.GetVolumes()[0].GetId() != "shared-volume" {
			t.Fatalf("domain %d volumes=%+v err=%v", index, volumes, err)
		}
		registration, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: domains[index].registration})
		if err != nil || len(registration.GetRegistration().GetAllocations()) != 1 || registration.GetRegistration().GetAllocations()[0].GetAllocationId() != "shared" || registration.GetRegistration().GetDomainMetrics().GetActiveLeases() != 1 || registration.GetRegistration().GetDomainMetrics().GetActiveSessions() != 1 {
			t.Fatalf("domain %d projection=%+v err=%v", index, registration, err)
		}
		environmentID := registration.GetRegistration().GetAllocations()[0].GetEnvironmentId()
		if _, duplicate := environmentIDs[environmentID]; duplicate || environmentID == "" {
			t.Fatalf("domain %d reused environment identity %q", index, environmentID)
		}
		environmentIDs[environmentID] = struct{}{}
	}

	metricsDeadline := time.Now().Add(8 * time.Second)
	for {
		complete := true
		for _, current := range domains {
			registration, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: current.registration})
			if err != nil || registration.GetRegistration().GetDomainMetrics().GetBytesToController() == 0 || registration.GetRegistration().GetDomainMetrics().GetBytesToRuntime() == 0 || registration.GetRegistration().GetDomainMetrics().GetReconciliations() == 0 {
				complete = false
				break
			}
		}
		if complete {
			break
		}
		if time.Now().After(metricsDeadline) {
			t.Fatal("three-domain transport metrics did not settle")
		}
		time.Sleep(100 * time.Millisecond)
	}
	revokedBefore, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: domains[0].registration})
	if err != nil {
		t.Fatal(err)
	}
	revoking, err := harness.client.BeginRemoteRevocation(ctx, &hostv1.BeginRemoteRevocationRequest{RequestId: "revoke-three-domain-zero", RegistrationId: domains[0].registration, ExpectedRevision: revokedBefore.GetRegistration().GetRevision()})
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := harness.client.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RequestId: "complete-revoke-three-domain-zero", RegistrationId: domains[0].registration, ExpectedRevision: revoking.GetRegistration().GetRevision()})
	if err != nil || revoked.GetRegistration().GetState() != hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED || len(revoked.GetRegistration().GetAllocations()) != 1 || revoked.GetRegistration().GetAllocations()[0].GetState() != hostv1.AllocationState_ALLOCATION_STATE_RETAINED {
		t.Fatalf("revoked domain projection=%+v err=%v", revoked, err)
	}
	revokedRuntime := hostv1.NewAgentRuntimeServiceClient(domains[0].connection)
	revokedContext, cancelRevoked := context.WithTimeout(ctx, 3*time.Second)
	_, revokedErr := revokedRuntime.ListVolumes(revokedContext, &hostv1.ListVolumesRequest{})
	cancelRevoked()
	if revokedErr == nil {
		t.Fatal("revoked domain runtime session remained usable")
	}
	for index := 1; index < len(domains); index++ {
		volumes, err := hostv1.NewAgentRuntimeServiceClient(domains[index].connection).ListVolumes(ctx, &hostv1.ListVolumesRequest{})
		if err != nil || len(volumes.GetVolumes()) != 1 {
			t.Fatalf("revoke changed domain %d runtime: volumes=%+v err=%v", index, volumes, err)
		}
	}
	domains[0].connection.Close()
	domains[0].connection = nil
	domains[0].stop()
	select {
	case <-domains[0].done:
	case <-time.After(5 * time.Second):
		t.Fatal("revoked domain connector did not stop")
	}

	for index := 1; index < len(domains); index++ {
		domains[index].connection.Close()
		domains[index].connection = nil
		close(domains[index].controller.finish)
	}
	for _, current := range domains[1:] {
		select {
		case <-current.controller.cleaned:
		case <-ctx.Done():
			t.Fatal("three-domain cleanup did not finish")
		}
		current.stop()
		select {
		case <-current.done:
		case <-time.After(5 * time.Second):
			t.Fatal("three-domain connector did not stop")
		}
	}
}

func testRemoteForward(t *testing.T, client *productClient, containerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.ForwardTCP(ctx, "remote-a")
	if err != nil {
		t.Fatal(err)
	}
	container, err := client.GetContainer(ctx, "remote-a", &hostv1.ContainerRequest{ContainerId: containerID})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Header{Header: &hostv1.ForwardTCPHeader{ContainerId: containerID, Port: 8080, ContainerInstanceId: container.GetContainer().GetInstanceId()}}}); err != nil {
		t.Fatal(err)
	}
	payload := []byte("remote-forward")
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_Data{Data: payload}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hostv1.ForwardTCPRequest{Event: &hostv1.ForwardTCPRequest_CloseWrite{CloseWrite: true}}); err != nil {
		t.Fatal(err)
	}
	var received []byte
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) || response.GetCloseRead() {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, response.GetData()...)
	}
	if !bytes.Equal(received, payload) {
		t.Fatalf("remote forward=%q", received)
	}
}

func remoteLimits(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
	return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
}

func startRemoteController(t *testing.T, service remotev1.RemoteControllerServiceServer) (string, *x509.CertPool, func()) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IsCA: true, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewServerTLSFromCert(&certificate)), grpc.InitialWindowSize(remoteconnector.StreamWindowBytes), grpc.InitialConnWindowSize(remoteconnector.ConnectionWindowBytes))
	remotev1.RegisterRemoteControllerServiceServer(server, service)
	go server.Serve(listener)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	return net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), roots, func() { server.Stop(); listener.Close() }
}
