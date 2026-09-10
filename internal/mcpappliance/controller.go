package mcpappliance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"github.com/AFK-surf/agent-vmm/internal/mcpserver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	commandTimeout = 30 * time.Second
	sessionTimeout = 30 * time.Second
)

type Controller struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	identity *Identity
	store    stateStore
	logger   *slog.Logger

	mu       sync.Mutex
	live     *controlConnection
	waiters  map[sessionKey]chan controllerremote.SessionConnection
	runtimes map[string]*clientRuntime
}

type controlConnection struct {
	session  *controllerremote.ControlSession
	requests chan commandRequest
	done     chan struct{}
	hello    *remotev1.HostHello
	close    context.CancelFunc
	sequence uint64
}

type commandRequest struct {
	ctx      context.Context
	command  *remotev1.ControllerCommand
	response chan commandResponse
}

type commandResponse struct {
	result *remotev1.CommandResult
	err    error
}

type sessionKey struct {
	allocationID string
	leaseID      string
	generation   uint64
}

func NewController(identity *Identity, stateDirectory string, logger *slog.Logger) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{identity: identity, store: stateStore{directory: stateDirectory}, logger: logger, waiters: make(map[sessionKey]chan controllerremote.SessionConnection), runtimes: make(map[string]*clientRuntime)}
}

func (controller *Controller) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
	registrationID := controller.identity.RegistrationID()
	if registrationID == "" {
		return errors.New("controller identity is not initialized")
	}
	session, err := controllerremote.AcceptControl(stream, registrationID)
	if err != nil {
		return err
	}
	if err := controller.store.saveInventory(session.Hello()); err != nil {
		return fmt.Errorf("persist authoritative inventory: %w", err)
	}
	if err := session.Ready(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	live := &controlConnection{session: session, requests: make(chan commandRequest), done: make(chan struct{}), hello: proto.Clone(session.Hello()).(*remotev1.HostHello), close: cancel}
	controller.install(live)
	defer controller.uninstall(live)
	return controller.runControl(ctx, live)
}

func (controller *Controller) runControl(ctx context.Context, live *controlConnection) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-live.requests:
			live.sequence++
			request.command.Sequence = live.sequence
			request.command.ConnectionEpoch = live.hello.GetConnectionEpoch()
			transcript, err := live.session.Dispatch(request.ctx, request.command)
			result := transcript.Result
			if err == nil && (result == nil || result.GetOutcome() != remotev1.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED) {
				err = commandFailure(result)
			}
			select {
			case request.response <- commandResponse{result: result, err: err}:
			case <-request.ctx.Done():
			}
		}
	}
}

func (controller *Controller) install(live *controlConnection) {
	controller.mu.Lock()
	previous := controller.live
	controller.live = live
	controller.mu.Unlock()
	if previous != nil {
		previous.close()
	}
}

func (controller *Controller) uninstall(live *controlConnection) {
	controller.mu.Lock()
	if controller.live == live {
		controller.live = nil
	}
	controller.mu.Unlock()
	live.close()
	close(live.done)
}

func (controller *Controller) dispatch(ctx context.Context, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	controller.mu.Lock()
	live := controller.live
	controller.mu.Unlock()
	if live == nil {
		return nil, errors.New("controller transport is unavailable")
	}
	return controller.dispatchOn(ctx, live, command)
}

func (controller *Controller) dispatchOn(ctx context.Context, live *controlConnection, command *remotev1.ControllerCommand) (*remotev1.CommandResult, error) {
	request := commandRequest{ctx: ctx, command: command, response: make(chan commandResponse, 1)}
	select {
	case live.requests <- request:
	case <-live.done:
		return nil, errors.New("controller transport disconnected")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case response := <-request.response:
		return response.result, response.err
	case <-live.done:
		return nil, errors.New("controller transport disconnected")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func commandFailure(result *remotev1.CommandResult) error {
	if result == nil {
		return errors.New("controller command returned no result")
	}
	return fmt.Errorf("controller command failed: %s: %s", result.GetReason(), result.GetMessage())
}

func (controller *Controller) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
	controller.mu.Lock()
	live := controller.live
	controller.mu.Unlock()
	if live == nil {
		return errors.New("controller transport is unavailable")
	}
	header, connection, err := controllerremote.AcceptSession(stream, live.hello.GetRegistrationId(), live.hello.GetConnectionEpoch())
	if err != nil {
		return err
	}
	key := sessionKey{allocationID: header.GetAllocationId(), leaseID: header.GetLeaseId(), generation: header.GetGeneration()}
	controller.mu.Lock()
	waiter := controller.waiters[key]
	if waiter != nil {
		delete(controller.waiters, key)
	}
	controller.mu.Unlock()
	if waiter == nil {
		connection.Close()
		return errors.New("runtime session was not requested")
	}
	select {
	case waiter <- connection:
	case <-stream.Context().Done():
		connection.Close()
		return stream.Context().Err()
	}
	select {
	case <-connection.Done():
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

type clientRuntime struct {
	controller        *Controller
	clientID          string
	allocationID      string
	lease             *remotev1.Lease
	context           context.Context
	cancel            context.CancelFunc
	runtimeConnection controllerremote.SessionConnection
	grpcConnection    *grpc.ClientConn
	adapter           *mcpserver.Adapter
	cleanup           *clientCleanupOwner
	live              *controlConnection
	closeOnce         sync.Once
	done              chan struct{}
}

func (controller *Controller) openClientRuntime(parent context.Context, allocationID string) (*clientRuntime, error) {
	controller.mu.Lock()
	previous := controller.runtimes[allocationID]
	controller.mu.Unlock()
	if previous != nil {
		// A binding identifies one current MCP capability.  Close the previous
		// runtime synchronously before acquiring its replacement lease so a
		// crashed/reconnected proxy cannot consume the per-allocation lease cap.
		previous.Close()
		<-previous.done
	}
	clientID, err := randomID("mcp-client-")
	if err != nil {
		return nil, err
	}
	lease, live, err := controller.allocate(parent, clientID, allocationID)
	if err != nil {
		return nil, err
	}
	clientContext, cancelClient := context.WithCancel(parent)
	failed := true
	defer func() {
		if failed {
			cancelClient()
			controller.release(clientID, allocationID, lease)
		}
	}()
	go controller.renewAllocationLease(clientContext, live, clientID, lease, cancelClient)
	key := sessionKey{allocationID: allocationID, leaseID: lease.GetLeaseId(), generation: lease.GetGeneration()}
	waiter := make(chan controllerremote.SessionConnection, 1)
	controller.mu.Lock()
	controller.waiters[key] = waiter
	controller.mu.Unlock()
	defer func() {
		controller.mu.Lock()
		delete(controller.waiters, key)
		controller.mu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(clientContext, commandTimeout)
	_, err = controller.dispatchOn(ctx, live, &remotev1.ControllerCommand{CommandId: clientID + "-open", LeaseGeneration: lease.GetGeneration(), Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: allocationID, LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
	cancel()
	if err != nil {
		return nil, err
	}
	var runtimeConnection controllerremote.SessionConnection
	select {
	case runtimeConnection = <-waiter:
	case <-time.After(sessionTimeout):
		return nil, errors.New("runtime session did not connect")
	case <-live.done:
		return nil, errors.New("controller transport disconnected")
	case <-clientContext.Done():
		return nil, clientContext.Err()
	}
	grpcConnection, err := runtimeClientConnection(runtimeConnection)
	if err != nil {
		runtimeConnection.Close()
		return nil, err
	}
	cleanup := &clientCleanupOwner{controller: controller, live: live, clientID: clientID, allocationID: allocationID, lease: lease}
	adapter := mcpserver.New(hostv1.NewAgentRuntimeServiceClient(grpcConnection), cleanup, controller.logger)
	client := &clientRuntime{controller: controller, clientID: clientID, allocationID: allocationID, lease: lease, context: clientContext, cancel: cancelClient, runtimeConnection: runtimeConnection, grpcConnection: grpcConnection, adapter: adapter, cleanup: cleanup, live: live, done: make(chan struct{})}
	controller.mu.Lock()
	if controller.runtimes == nil {
		controller.runtimes = make(map[string]*clientRuntime)
	}
	controller.runtimes[allocationID] = client
	controller.mu.Unlock()
	go func() {
		select {
		case <-clientContext.Done():
		case <-runtimeConnection.Done():
			if cleanup.waitForExecution() {
				timer := time.NewTimer(500 * time.Millisecond)
				select {
				case <-timer.C:
				case <-clientContext.Done():
					timer.Stop()
				}
			}
		}
		client.Close()
	}()
	failed = false
	return client, nil
}

func (client *clientRuntime) Close() {
	client.closeOnce.Do(func() {
		client.adapter.Close()
		client.cancel()
		client.grpcConnection.Close()
		client.runtimeConnection.Close()
		if !client.cleanup.completedSuccessfully() {
			client.controller.release(client.clientID, client.allocationID, client.lease)
		}
		client.controller.mu.Lock()
		if client.controller.runtimes[client.allocationID] == client {
			delete(client.controller.runtimes, client.allocationID)
		}
		client.controller.mu.Unlock()
		close(client.done)
	})
}

func (controller *Controller) allocate(parent context.Context, clientID, allocationID string) (*remotev1.Lease, *controlConnection, error) {
	controller.mu.Lock()
	live := controller.live
	controller.mu.Unlock()
	if live == nil {
		return nil, nil, errors.New("controller transport is unavailable")
	}
	hello := live.hello
	if hello.GetCapacity() == nil || hello.GetCapacity().GetPerEnvironmentLimits() == nil {
		return nil, nil, errors.New("controller capacity is incomplete")
	}
	limits := proto.Clone(hello.GetCapacity().GetPerEnvironmentLimits()).(*hostv1.ResourceLimits)
	resources := &hostv1.WorkloadResourcesV2{CpuMaxMillis: limits.GetCpuMillis(),
		MemoryMaxBytes: limits.GetMemoryBytes(), PidMax: limits.GetPids(), WritableQuotaBytes: limits.GetDiskBytes()}
	ctx, cancel := context.WithTimeout(parent, commandTimeout)
	ensured, err := controller.dispatchOn(ctx, live, &remotev1.ControllerCommand{CommandId: clientID + "-ensure", TargetRevision: hello.GetPoolRevision(), Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: allocationID, Generation: 1, DisplayLabel: "MCP client", ResourcesV2: resources, EgressMode: hello.GetCapacity().GetMaxEgressMode(), PolicyRevision: hello.GetPoolRevision()}}})
	cancel()
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel = context.WithTimeout(parent, commandTimeout)
	acquired, err := controller.dispatchOn(ctx, live, &remotev1.ControllerCommand{CommandId: clientID + "-acquire", TargetRevision: ensured.GetAllocation().GetRevision(), Command: &remotev1.ControllerCommand_AcquireLease{AcquireLease: &remotev1.AcquireLease{AllocationId: allocationID, DurationSeconds: boundedLeaseSeconds(hello.GetCapacity().GetMaxLeaseSeconds()), WorkloadRef: "bundled-mcp"}}})
	cancel()
	if err != nil {
		return nil, nil, err
	}
	lease := acquired.GetLease()
	if lease == nil || lease.GetAllocationId() != allocationID || lease.GetLeaseId() == "" || lease.GetGeneration() == 0 || lease.GetExpiresUnixMillis() <= time.Now().UnixMilli() {
		return nil, nil, errors.New("lease result is incomplete")
	}
	return lease, live, nil
}

func (controller *Controller) renewAllocationLease(ctx context.Context, live *controlConnection, clientID string, lease *remotev1.Lease, stop context.CancelFunc) {
	expiresAt := time.UnixMilli(lease.GetExpiresUnixMillis())
	duration := boundedLeaseSeconds(live.hello.GetCapacity().GetMaxLeaseSeconds())
	sequence := uint64(0)
	for {
		wait := max(time.Until(expiresAt)/3, time.Second)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-live.done:
			timer.Stop()
			stop()
			return
		case <-timer.C:
		}
		sequence++
		commandContext, cancel := context.WithTimeout(ctx, commandTimeout)
		result, err := controller.dispatchOn(commandContext, live, &remotev1.ControllerCommand{CommandId: fmt.Sprintf("%s-renew-%d", clientID, sequence), LeaseGeneration: lease.GetGeneration(), Command: &remotev1.ControllerCommand_RenewLease{RenewLease: &remotev1.RenewLease{LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration(), DurationSeconds: duration}}})
		cancel()
		renewed := result.GetLease()
		if err != nil || renewed.GetLeaseId() != lease.GetLeaseId() || renewed.GetGeneration() != lease.GetGeneration() || renewed.GetExpiresUnixMillis() <= time.Now().UnixMilli() {
			if err == nil {
				err = errors.New("renewed lease result is incomplete")
			}
			controller.logger.Warn("renew MCP lease", "allocation", lease.GetAllocationId(), "error", err)
			stop()
			return
		}
		expiresAt = time.UnixMilli(renewed.GetExpiresUnixMillis())
	}
}

func (controller *Controller) release(clientID, allocationID string, lease *remotev1.Lease) {
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	released, err := controller.dispatch(ctx, &remotev1.ControllerCommand{CommandId: clientID + "-release-lease", LeaseGeneration: lease.GetGeneration(), Command: &remotev1.ControllerCommand_ReleaseLease{ReleaseLease: &remotev1.ReleaseLease{LeaseId: lease.GetLeaseId(), Generation: lease.GetGeneration()}}})
	if err != nil {
		controller.logger.Warn("release MCP lease", "allocation", allocationID, "error", err)
		return
	}
	revision := released.GetLease().GetAllocationRevision()
	_, err = controller.dispatch(ctx, &remotev1.ControllerCommand{CommandId: clientID + "-retain", TargetRevision: revision, Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{AllocationId: allocationID, ExpectedRevision: revision, ExpectedGeneration: 1}}})
	if err != nil {
		controller.logger.Warn("retain MCP allocation", "allocation", allocationID, "error", err)
	}
}

func runtimeClientConnection(connection net.Conn) (*grpc.ClientConn, error) {
	var once sync.Once
	return grpc.NewClient("passthrough:///agent-vmm-runtime", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		var result net.Conn
		once.Do(func() { result = connection })
		if result == nil {
			return nil, errors.New("runtime session was already consumed")
		}
		return result, nil
	}))
}

func boundedLeaseSeconds(maximum uint32) uint32 {
	if maximum == 0 || maximum > 300 {
		return 300
	}
	return maximum
}
