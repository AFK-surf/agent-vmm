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
	allocationID         string
	allocationGeneration uint64
}

func NewController(identity *Identity, logger *slog.Logger) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	return &Controller{identity: identity, logger: logger, waiters: make(map[sessionKey]chan controllerremote.SessionConnection), runtimes: make(map[string]*clientRuntime)}
}

func (controller *Controller) Connect(stream grpc.BidiStreamingServer[remotev1.ConnectRequest, remotev1.ConnectResponse]) error {
	registrationID := controller.identity.RegistrationID()
	if registrationID == "" {
		return errors.New("controller identity is not initialized")
	}
	session, err := controllerremote.AcceptControl(stream, registrationID)
	if err != nil {
		return err
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
		case <-live.session.Done():
			return live.session.Err()
		case request := <-live.requests:
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

func (controller *Controller) ServeSession(stream grpc.BidiStreamingServer[remotev1.ServeSessionRequest, remotev1.ServeSessionResponse]) error {
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
	key := sessionKey{allocationID: header.GetAllocationId(), allocationGeneration: header.GetAllocationGeneration()}
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
	allocation        *remotev1.Allocation
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
		// A binding identifies one current MCP capability. Close the previous
		// runtime session before opening its replacement.
		previous.Close()
		<-previous.done
	}
	clientID, err := randomID("mcp-client-")
	if err != nil {
		return nil, err
	}
	allocation, live, err := controller.allocate(parent, clientID, allocationID)
	if err != nil {
		return nil, err
	}
	clientContext, cancelClient := context.WithCancel(parent)
	failed := true
	defer func() {
		if failed {
			cancelClient()
		}
	}()
	key := sessionKey{allocationID: allocationID, allocationGeneration: allocation.GetGeneration()}
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
	_, err = controller.dispatchOn(ctx, live, &remotev1.ControllerCommand{CommandId: clientID + "-open", Command: &remotev1.ControllerCommand_OpenSession{OpenSession: &remotev1.OpenSession{AllocationId: allocationID, AllocationGeneration: allocation.GetGeneration(), ExecutionOwnerId: "appliance:" + allocationID}}})
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
	cleanup := &clientCleanupOwner{controller: controller, live: live, clientID: clientID, allocationID: allocationID, allocationGeneration: allocation.GetGeneration()}
	adapter := mcpserver.New(hostv1.NewAgentRuntimeServiceClient(grpcConnection), cleanup, controller.logger)
	client := &clientRuntime{controller: controller, clientID: clientID, allocationID: allocationID, allocation: allocation, context: clientContext, cancel: cancelClient, runtimeConnection: runtimeConnection, grpcConnection: grpcConnection, adapter: adapter, cleanup: cleanup, live: live, done: make(chan struct{})}
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
		if !client.cleanup.completedSuccessfully() {
			ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
			_, err := client.controller.dispatchOn(ctx, client.live, &remotev1.ControllerCommand{
				CommandId:      client.clientID + "-release",
				TargetRevision: client.allocation.GetRevision(),
				Command: &remotev1.ControllerCommand_ReleaseAllocation{ReleaseAllocation: &remotev1.ReleaseAllocation{
					AllocationId: client.allocationID, ExpectedRevision: client.allocation.GetRevision(),
					ExpectedGeneration: client.allocation.GetGeneration(),
				}},
			})
			cancel()
			if err != nil {
				client.controller.logger.Warn("release MCP allocation", "allocation", client.allocationID, "error", err)
			}
		}
		client.cancel()
		client.grpcConnection.Close()
		client.runtimeConnection.Close()
		client.controller.mu.Lock()
		if client.controller.runtimes[client.allocationID] == client {
			delete(client.controller.runtimes, client.allocationID)
		}
		client.controller.mu.Unlock()
		close(client.done)
	})
}

func (controller *Controller) allocate(parent context.Context, clientID, allocationID string) (*remotev1.Allocation, *controlConnection, error) {
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
	limits := proto.Clone(hello.GetCapacity().GetPerEnvironmentLimits()).(*remotev1.ResourceLimits)
	resources := &remotev1.ResourceLimits{
		Pids: proto.Uint32(limits.GetPids()), DiskBytes: proto.Uint64(limits.GetDiskBytes())}
	ctx, cancel := context.WithTimeout(parent, commandTimeout)
	ensured, err := controller.dispatchOn(ctx, live, &remotev1.ControllerCommand{CommandId: clientID + "-ensure", TargetRevision: hello.GetPoolRevision(), Command: &remotev1.ControllerCommand_EnsureAllocation{EnsureAllocation: &remotev1.EnsureAllocation{AllocationId: allocationID, Generation: 1, DisplayLabel: "MCP client", Limits: resources, EgressMode: hello.GetCapacity().GetMaxEgressMode(), PolicyRevision: hello.GetPoolRevision()}}})
	cancel()
	if err != nil {
		return nil, nil, err
	}
	allocation := ensured.GetAllocation()
	if allocation == nil || allocation.GetAllocationId() != allocationID || allocation.GetGeneration() == 0 || allocation.GetRevision() == 0 {
		return nil, nil, errors.New("allocation result is incomplete")
	}
	return allocation, live, nil
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
