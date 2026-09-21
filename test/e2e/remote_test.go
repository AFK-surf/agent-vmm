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
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
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
	allocations := make([]*remotev1.Allocation, 0, 2)
	initialAllocations := make(map[string]*remotev1.Allocation, len(hello.GetInventory()))
	for _, allocation := range hello.GetInventory() {
		initialAllocations[allocation.GetAllocationId()] = allocation
		allocations = appendAllocationOnce(allocations, allocation)
	}
	if err := session.Ready(); err != nil {
		return err
	}
	cursor := &remoteCommandCursor{session: session}
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
		if existing := initialAllocations[allocationID]; existing != nil {
			// A reconnect can carry the durable allocation. Preserve it instead of
			// issuing EnsureAllocation against a partially settled inventory.
			continue
		}
		resources := remoteResources(500, 1<<30, 256, 2<<30)
		result, err := server.command(cursor, &remotev1.ControllerCommand{CommandId: "ensure-" + allocationID, Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: allocationID, Generation: 1, DisplayLabel: allocationID, ResourcesV2: resources, EgressMode: egress, PolicyRevision: hello.GetPoolRevision()}}})
		if err != nil {
			return err
		}
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			server.reportFailure("ensure " + result.GetReason().String())
			return status.Error(codes.Internal, "ensure failed")
		}
		allocations = appendAllocationOnce(allocations, result.GetAllocation())
	}
	server.connections <- len(allocations)
	for _, allocation := range allocations {
		result, err := server.command(cursor, &remotev1.ControllerCommand{CommandId: "open-" + allocation.GetAllocationId(), Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: allocation.GetAllocationId(), AllocationGeneration: allocation.GetGeneration(), ExecutionOwnerId: "e2e:" + allocation.GetAllocationId()}}})
		if err != nil {
			return err
		}
		if result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			server.reportFailure("open " + allocation.GetAllocationId() + " " + result.GetReason().String() + " " + result.GetMessage())
			return status.Error(codes.Internal, "open failed")
		}
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	for _, allocation := range allocations {
		releasedAllocation, err := server.command(cursor, &remotev1.ControllerCommand{CommandId: "release-allocation-" + allocation.GetAllocationId(), Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: allocation.GetAllocationId(), ExpectedRevision: allocation.GetRevision(), ExpectedGeneration: allocation.GetGeneration()}}})
		if err != nil || releasedAllocation.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "release allocation %s failed: %v %s", allocation.GetAllocationId(), err, releasedAllocation.GetMessage())
		}
		plan, err := server.command(cursor, &remotev1.ControllerCommand{CommandId: "plan-" + allocation.GetAllocationId(), Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: allocation.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD}}})
		if err != nil || plan.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "plan discard %s failed: %v %s", allocation.GetAllocationId(), err, plan.GetMessage())
		}
		discarded, err := server.command(cursor, &remotev1.ControllerCommand{CommandId: "discard-" + allocation.GetAllocationId(), Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: allocation.GetAllocationId(), Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: plan.GetCleanupPlan().GetRevision(), ConfirmedVolumeIds: plan.GetCleanupPlan().GetVolumeIds()}}})
		if err != nil || discarded.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return status.Errorf(codes.Internal, "discard %s failed: %v %s", allocation.GetAllocationId(), err, discarded.GetMessage())
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

func appendAllocationOnce(allocations []*remotev1.Allocation, allocation *remotev1.Allocation) []*remotev1.Allocation {
	if allocation == nil {
		return allocations
	}
	for _, current := range allocations {
		if current.GetAllocationId() == allocation.GetAllocationId() {
			return allocations
		}
	}
	return append(allocations, allocation)
}

type remoteCommandCursor struct {
	session  *controllerremote.ControlSession
	sequence uint64
}

func (server *remoteE2EController) command(cursor *remoteCommandCursor, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	return dispatchRemoteCommand(cursor, command)
}

func dispatchRemoteCommand(cursor *remoteCommandCursor, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	if ensure := command.GetEnsureAllocation(); ensure != nil {
		command.TargetRevision = ensure.GetPolicyRevision()
	}
	switch value := command.GetCommand().(type) {
	case *remotev1.ControllerCommand_ReleaseAllocation:
		command.TargetRevision = value.ReleaseAllocation.GetExpectedRevision()
	case *remotev1.ControllerCommand_ExecuteCleanupAllocation:
		command.TargetRevision = value.ExecuteCleanupAllocation.GetPlanRevision()
	}
	cursor.sequence++
	command.Sequence = cursor.sequence
	command.ConnectionEpoch = cursor.session.Hello().GetConnectionEpoch()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	transcript, err := cursor.session.Dispatch(ctx, command)
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
	cursor := &remoteCommandCursor{session: session}
	resources := remoteResources(100, 128<<20, 64, 256<<20)
	ensured, err := dispatchRemoteCommand(cursor, &remotev1.ControllerCommand{CommandId: "ensure-shared", Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: "shared", Generation: 1, DisplayLabel: "shared", ResourcesV2: resources, EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, PolicyRevision: hello.GetPoolRevision()}}})
	if err != nil || ensured.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "ensure failed: %v %s", err, ensured.GetMessage())
	}
	allocation := ensured.GetAllocation()
	opened, err := dispatchRemoteCommand(cursor, &remotev1.ControllerCommand{CommandId: "open-shared", Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: "shared", AllocationGeneration: allocation.GetGeneration(), ExecutionOwnerId: "e2e:shared"}}})
	if err != nil || opened.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "open failed: %v %s", err, opened.GetMessage())
	}
	select {
	case <-server.finish:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	released, err := dispatchRemoteCommand(cursor, &remotev1.ControllerCommand{CommandId: "release-allocation-shared", Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: "shared", ExpectedRevision: allocation.GetRevision(), ExpectedGeneration: allocation.GetGeneration()}}})
	if err != nil || released.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "release allocation failed: %v %s", err, released.GetMessage())
	}
	plan, err := dispatchRemoteCommand(cursor, &remotev1.ControllerCommand{CommandId: "plan-shared", Command: &remotev1.ControllerCommand_PlanCleanupAllocation{PlanCleanupAllocation: &remotev1.PlanCleanupAllocation{AllocationId: "shared", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD}}})
	if err != nil || plan.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
		return status.Errorf(codes.Internal, "plan discard failed: %v %s", err, plan.GetMessage())
	}
	discarded, err := dispatchRemoteCommand(cursor, &remotev1.ControllerCommand{CommandId: "discard-shared", Command: &remotev1.ControllerCommand_ExecuteCleanupAllocation{ExecuteCleanupAllocation: &remotev1.ExecuteCleanupAllocation{AllocationId: "shared", Kind: hostv1.CleanupKind_CLEANUP_KIND_DISCARD, PlanRevision: plan.GetCleanupPlan().GetRevision(), ConfirmedVolumeIds: plan.GetCleanupPlan().GetVolumeIds()}}})
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
	pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(500, 1<<30, 256, 2<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(2 * time.Second)}
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
	remoteClient := &productClient{archiveDirectory: harness.runtimeClient.archiveDirectory, archiveURL: harness.runtimeClient.archiveURL, sessions: map[string]*productSession{"remote-a": {connection: clients["remote-a"], client: hostv1.NewAgentRuntimeServiceClient(clients["remote-a"])}}}
	importRegistryImage(t, remoteClient, "remote-a")
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
	startAndWaitContainer(t, remoteClient, "remote-a", logged.GetId(), 0)
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

func testSixRemoteDomains(t *testing.T, harness *hostHarness) {
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
	pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(100, 128<<20, 64, 256<<20), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL, StopGrace: durationpb.New(time.Second)}
	domains := make([]domain, 0, 6)
	defer func() {
		for _, current := range domains {
			if current.connection != nil {
				current.connection.Close()
			}
			current.stop()
			current.stopServer()
		}
	}()
	for index := range 6 {
		controller := &isolatedDomainController{session: make(chan net.Conn, 1), finish: make(chan struct{}), cleaned: make(chan struct{})}
		endpoint, roots, stopServer := startRemoteController(t, controller)
		requestID := "six-domain-" + strconv.Itoa(index)
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

	environmentIDs := make(map[string]struct{}, 6)
	for index := range domains {
		var raw net.Conn
		select {
		case raw = <-domains[index].controller.session:
		case <-ctx.Done():
			t.Fatal("six-domain runtime session did not open")
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
		if err != nil || len(registration.GetRegistration().GetAllocations()) != 1 || registration.GetRegistration().GetAllocations()[0].GetAllocationId() != "shared" || registration.GetRegistration().GetDomainMetrics().GetActiveSessions() != 1 {
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
			t.Fatal("six-domain transport metrics did not settle")
		}
		time.Sleep(100 * time.Millisecond)
	}
	revokedBefore, err := harness.client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: domains[0].registration})
	if err != nil {
		t.Fatal(err)
	}
	revoking, err := harness.client.BeginRemoteRevocation(ctx, &hostv1.BeginRemoteRevocationRequest{RequestId: "revoke-six-domain-zero", RegistrationId: domains[0].registration, ExpectedRevision: revokedBefore.GetRegistration().GetRevision()})
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := harness.client.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RequestId: "complete-revoke-six-domain-zero", RegistrationId: domains[0].registration, ExpectedRevision: revoking.GetRegistration().GetRevision()})
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
			t.Fatal("six-domain cleanup did not finish")
		}
		current.stop()
		select {
		case <-current.done:
		case <-time.After(5 * time.Second):
			t.Fatal("six-domain connector did not stop")
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

func remoteResources(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.WorkloadResourcesV2 {
	return &hostv1.WorkloadResourcesV2{CpuMaxMillis: cpu,
		MemoryMaxBytes: memory, PidMax: pids, WritableQuotaBytes: disk}
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

// TestCuePressureBridgeE2E is driven by Cue's opt-in Salix integration test.
// All registrations and VM state belong to that fixture, never a saved host.
func TestCuePressureBridgeE2E(t *testing.T) {
	configPath := os.Getenv("CUE_VMM_E2E_CONFIG")
	if configPath == "" {
		t.Skip("set CUE_VMM_E2E_CONFIG from the Cue pressure integration test")
	}
	var fixture struct {
		Endpoint      string `json:"remote_endpoint"`
		CA            string `json:"ca_file"`
		Archive       string `json:"archive_path"`
		Registrations []struct {
			ID         string `json:"id"`
			Credential []byte `json:"credential"`
		} `json:"registrations"`
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Registrations) != 6 {
		t.Fatal("pressure fixture requires exactly six isolated registrations")
	}
	roots := x509.NewCertPool()
	raw, err = os.ReadFile(fixture.CA)
	if err != nil || !roots.AppendCertsFromPEM(raw) {
		t.Fatal("invalid fixture CA")
	}
	harness := startHostWithState(t, 32<<30)
	harness.start(t)
	t.Cleanup(func() { harness.stop(t, true) })
	t.Cleanup(func() {
		for suffix, name := range map[string]string{"": "guest.log", ".host.log": "host-runtime.log"} {
			if err := os.WriteFile(filepath.Join(filepath.Dir(configPath), name), readFileTail(harness.serial+suffix, 256<<10), 0600); err != nil {
				t.Errorf("preserve pressure fixture diagnostics: %v", err)
			}
		}
	})
	archiveName := "import-cue-pressure.tar"
	source, err := os.Open(fixture.Archive)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := os.Create(filepath.Join(harness.runtimeClient.archiveDirectory, archiveName))
	if err != nil {
		source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	if err := errors.Join(copyErr, source.Close(), destination.Close()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	failures := make(chan error, 6)
	for _, registration := range fixture.Registrations {
		pool := &hostv1.RemotePoolPolicy{PerEnvironmentLimits: remoteLimits(100, 64<<20, 512, 3<<30), MaxEgressMode: hostv1.EgressMode_EGRESS_MODE_PUBLIC_INTERNET, StopGrace: durationpb.New(time.Second)}
		started, err := harness.client.BeginRemoteEnrollment(ctx, &hostv1.BeginRemoteEnrollmentRequest{RequestId: registration.ID + "-begin", RegistrationId: registration.ID, Endpoint: fixture.Endpoint, DisplayName: "Cue pressure fixture", ProtocolVersion: remoteconnector.ProtocolVersion, Pool: pool})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := harness.client.CompleteRemoteEnrollment(ctx, &hostv1.CompleteRemoteEnrollmentRequest{RequestId: registration.ID + "-complete", RegistrationId: registration.ID, ExpectedRevision: started.GetRegistration().GetRevision()}); err != nil {
			t.Fatal(err)
		}
		go func() {
			failures <- remoteconnector.Run(ctx, remoteconnector.Config{Endpoint: fixture.Endpoint, EndpointIdentity: "localhost", RegistrationID: registration.ID, RemoteComputeSocket: harness.remoteComputeSocket, CredentialStore: &e2eCredentialStore{value: registration.Credential}, TLSRoots: roots})
		}()
	}
	ready, _ := json.Marshal(map[string]string{"archive_url": harness.runtimeClient.archiveURL + "/" + archiveName, "serial_log": harness.serial})
	if err := os.WriteFile(configPath+".host-ready", ready, 0600); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-failures:
			t.Fatalf("fixture remote connector exited: %v", err)
		case <-ctx.Done():
			t.Fatal("Cue pressure fixture exceeded 15 minutes")
		case <-ticker.C:
			if _, err := os.Stat(configPath + ".stop"); err == nil {
				return
			}
		}
	}
}
