//go:build darwin || linux

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type operatorFacts struct {
	hostReadable       bool
	hostHealthy        bool
	connectorConnected bool
	registrationRead   string
	registrationState  string
	registrationReason string
	remoteProtocol     *remoteProtocolStatus
	snapshot           *hostv1.GetSystemHealthSummaryResponse
}

func parseStatusArgs(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) == 2 && args[0] == "--registration-id" && operationIDPattern.MatchString(args[1]) {
		return args[1], nil
	}
	return "", errors.New("status accepts --registration-id ID")
}

func parseLifecycleOperationArgs(args []string) (string, error) {
	if len(args) == 2 && (args[0] == "--operation-id" || args[0] == "--request-id") && operationIDPattern.MatchString(args[1]) {
		return args[1], nil
	}
	return "", errors.New("operation requires --operation-id ID")
}

func readOperatorFactsContext(ctx context.Context, socket string, registrationIDs ...string) (operatorFacts, error) {
	connection, err := operatorConnection(ctx, socket)
	if err != nil {
		return operatorFacts{registrationRead: "unreadable", registrationReason: "operator_unavailable"}, err
	}
	defer connection.Close()

	snapshot, err := hostv1.NewOperatorServiceClient(connection).GetSystemHealthSummary(ctx, &hostv1.GetSystemHealthSummaryRequest{})
	if err != nil {
		return operatorFacts{registrationRead: "unreadable", registrationReason: "operator_unavailable"}, err
	}
	facts := operatorFacts{
		hostReadable: true,
		snapshot:     snapshot,
		hostHealthy: snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
			snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY,
	}
	if len(registrationIDs) != 0 && registrationIDs[0] != "" {
		remote, err := hostv1.NewOperatorServiceClient(connection).GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationIDs[0]})
		facts.registrationRead, facts.registrationReason = classifyRegistrationRead(err)
		if err != nil {
			return facts, err
		}
		switch remote.GetRegistration().GetState() {
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED:
			facts.registrationState = "enabled"
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING:
			facts.registrationState = "draining"
		case hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED:
			facts.registrationState = "revoked"
		}
		if protocol := remote.GetRegistration().GetConnectorStatus().GetRemoteProtocol(); protocol != nil {
			facts.remoteProtocol = &remoteProtocolStatus{HostProtocol: protocol.GetHostProtocol(), MinProtocol: protocol.GetMinProtocol(), MaxProtocol: protocol.GetMaxProtocol(), HostSide: protocol.GetHostSide()}
		}
		facts.connectorConnected = remote.GetRegistration().GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED
	}
	return facts, nil
}

func ensureVMStarted(value paths, requestID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCoordinatorInitTimeout)
	defer cancel()
	return ensureVMStartedContext(ctx, value, requestID)
}

func ensureVMStartedContext(ctx context.Context, value paths, requestID string) error {
	startedAt := time.Now()
	reportLifecycleProgress(lifecycleProgress{Event: "operator_connect_started", RequestID: requestID})
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable")
	}
	defer connection.Close()
	operator := hostv1.NewOperatorServiceClient(connection)
	connected := false
	for {
		vm, err := operator.GetVM(ctx, &hostv1.GetVMRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM status could not be read")
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if !connected {
			reportLifecycleProgress(lifecycleProgress{Event: "operator_connected", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
			connected = true
		}
		// A Host that resumes its installed VM at startup owns that transition; wait for it instead of racing it.
		if state := vm.GetVm().GetState(); state == hostv1.VMState_VM_STATE_STOPPED || state == hostv1.VMState_VM_STATE_FAILED {
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_requested", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds(), VMState: vm.GetVm().GetState().String()})
			if err := startVMWithProgress(ctx, operator, requestID, startedAt); err != nil {
				return fmt.Errorf("Agent VMM VM could not be started: %w", err)
			}
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_accepted", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
		}
		lastProgress := time.Time{}
		for {
			snapshot, snapshotErr := operator.GetSystemHealthSummary(ctx, &hostv1.GetSystemHealthSummaryRequest{})
			if snapshotErr == nil && snapshot.GetVm().GetState() == hostv1.VMState_VM_STATE_RUNNING &&
				snapshot.GetHealth().GetState() == hostv1.HealthState_HEALTH_STATE_HEALTHY {
				reportLifecycleProgress(lifecycleProgress{Event: "vm_healthy", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds(), VMState: snapshot.GetVm().GetState().String(), HealthState: snapshot.GetHealth().GetState().String()})
				return nil
			}
			if lastProgress.IsZero() || time.Since(lastProgress) >= 15*time.Second {
				progress := lifecycleProgress{Event: "health_wait", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()}
				if snapshotErr == nil {
					progress.VMState = snapshot.GetVm().GetState().String()
					progress.HealthState = snapshot.GetHealth().GetState().String()
				}
				reportLifecycleProgress(progress)
				lastProgress = time.Now()
			}
			if ctx.Err() != nil {
				return errors.New("Agent VMM VM did not become healthy")
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func startVMWithProgress(ctx context.Context, operator hostv1.OperatorServiceClient, requestID string, startedAt time.Time) error {
	summary, err := operator.GetVMImpactSummary(ctx, &hostv1.GetVMImpactSummaryRequest{})
	if err != nil || summary.GetVm() == nil {
		return errors.New("Agent VMM whole-VM impact could not be acknowledged before VM start")
	}
	fence := vmMutationFence(summary.GetVm())
	result := make(chan error, 1)
	go func() {
		_, err := operator.StartVM(ctx, &hostv1.StartVMRequest{RequestId: requestID, Fence: fence})
		result <- err
	}()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-ticker.C:
			reportLifecycleProgress(lifecycleProgress{Event: "vm_start_wait", RequestID: requestID, ElapsedMS: time.Since(startedAt).Milliseconds()})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func operatorConnection(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
}

func parseRegistrationStateArgs(args []string) (string, hostv1.RemoteRegistrationState, string) {
	registrationID := ""
	requestID := ""
	state := hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_UNSPECIFIED
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--registration-id":
			if index+1 >= len(args) {
				fatal("--registration-id requires a value")
			}
			index++
			registrationID = args[index]
		case "--state":
			if index+1 >= len(args) {
				fatal("--state requires a value")
			}
			index++
			switch args[index] {
			case "enabled":
				state = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED
			case "draining":
				state = hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING
			default:
				fatal("--state must be enabled or draining")
			}
		case "--request-id":
			if index+1 >= len(args) {
				fatal("--request-id requires a value")
			}
			index++
			requestID = args[index]
		default:
			fatal("registration-state accepts --registration-id, --state, and --request-id")
		}
	}
	if !operationIDPattern.MatchString(registrationID) || state == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_UNSPECIFIED {
		fatal("registration-state requires a valid --registration-id and --state")
	}
	return registrationID, state, requestID
}

func setRemoteRegistrationState(ctx context.Context, value paths, registrationID string, state hostv1.RemoteRegistrationState, requestID string) error {
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable for registration state change")
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return setRemoteRegistrationStateWith(ctx, registrationID, state, requestID, client.GetRemoteRegistration, client.SetRemoteRegistrationState)
}

func waitRemoteRegistrationConnected(value paths, registrationID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("Agent VMM operator is not readable for registration connection observation")
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return waitRemoteRegistrationConnectedWith(ctx, registrationID, client.GetRemoteRegistration, func(ctx context.Context) error {
		timer := time.NewTimer(250 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	})
}

func waitRemoteRegistrationConnectedWith(ctx context.Context, registrationID string, get func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error), sleep func(context.Context) error) error {
	for {
		response, readErr := get(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
		if readErr == nil && response.GetRegistration().GetId() == registrationID && response.GetRegistration().GetConnectionState() == hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("managed registration did not reconnect before the repair deadline: %w", context.DeadlineExceeded)
		}
		if sleep(ctx) != nil {
			return fmt.Errorf("managed registration did not reconnect before the repair deadline: %w", context.DeadlineExceeded)
		}
	}
}

func setRemoteRegistrationStateWith(
	ctx context.Context,
	registrationID string,
	state hostv1.RemoteRegistrationState,
	requestID string,
	get func(context.Context, *hostv1.GetRemoteRegistrationRequest, ...grpc.CallOption) (*hostv1.GetRemoteRegistrationResponse, error),
	set func(context.Context, *hostv1.SetRemoteRegistrationStateRequest, ...grpc.CallOption) (*hostv1.SetRemoteRegistrationStateResponse, error),
) error {
	current, err := get(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
	if err != nil {
		return fmt.Errorf("read managed registration: %w", err)
	}
	if current.GetRegistration().GetId() != registrationID {
		return errors.New("managed registration identity changed")
	}
	if current.GetRegistration().GetState() == state && !(state == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED && current.GetRegistration().GetConnectionState() != hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED) {
		return nil
	}
	_, err = set(ctx, &hostv1.SetRemoteRegistrationStateRequest{
		RegistrationId:   registrationID,
		ExpectedRevision: current.GetRegistration().GetRevision(),
		RequestId:        requestID,
		State:            state,
	})
	if err != nil {
		return fmt.Errorf("set managed registration state: %w", err)
	}
	return nil
}

func stopVMForUpdate(ctx context.Context, value paths, requestID string) error {
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return errors.New("running Agent VMM Host is not readable; refusing to replace its application bundle")
	}
	defer connection.Close()
	return stopVMForUpdateWith(ctx, hostv1.NewOperatorServiceClient(connection), requestID)
}

type updateVMClient interface {
	GetVM(context.Context, *hostv1.GetVMRequest, ...grpc.CallOption) (*hostv1.GetVMResponse, error)
	StopVM(context.Context, *hostv1.StopVMRequest, ...grpc.CallOption) (*hostv1.StopVMResponse, error)
}

func stopVMForUpdateWith(ctx context.Context, client updateVMClient, requestID string) error {
	vm, err := client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil {
		return errors.New("Agent VMM VM status could not be read before update")
	}
	if vm.GetVm().GetState() == hostv1.VMState_VM_STATE_STOPPED {
		return nil
	}
	fence := vmMutationFence(vm.GetVm())
	gracefulCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	_, gracefulErr := client.StopVM(gracefulCtx, &hostv1.StopVMRequest{RequestId: requestID + "-graceful", Fence: fence})
	cancel()
	if gracefulErr != nil {
		forceCtx, forceCancel := context.WithTimeout(ctx, 20*time.Second)
		latest, latestErr := client.GetVM(forceCtx, &hostv1.GetVMRequest{})
		if latestErr != nil || latest.GetVm() == nil {
			forceCancel()
			return errors.New("VM state could not be read before forced stop")
		}
		if latest.GetVm().GetBootId() != fence.GetBootId() {
			forceCancel()
			return errors.New("VM boot identity changed during update stop")
		}
		_, forceErr := client.StopVM(forceCtx, &hostv1.StopVMRequest{RequestId: requestID + "-force", Force: true, Fence: vmMutationFence(latest.GetVm())})
		forceCancel()
		if forceErr != nil {
			return fmt.Errorf("VM did not reach terminal state before update: graceful stop: %v; force stop: %w", gracefulErr, forceErr)
		}
	}
	vm, err = client.GetVM(ctx, &hostv1.GetVMRequest{})
	if err != nil || vm.GetVm().GetState() != hostv1.VMState_VM_STATE_STOPPED {
		return errors.New("VM stop returned without a terminal stopped state; refusing to bootout the Host")
	}
	return nil
}

func vmMutationFence(vm *hostv1.VM) *hostv1.VMMutationFence {
	return &hostv1.VMMutationFence{State: vm.GetState(), BootId: vm.GetBootId(), WholeVmImpactAcknowledged: true}
}

func classifyRegistrationRead(err error) (string, string) {
	if err == nil {
		return "present", ""
	}
	if status.Code(err) == codes.NotFound {
		return "absent", "registration_not_found"
	}
	return "unreadable", "registration_unreadable"
}

func revokeRemoteRegistration(ctx context.Context, value paths, registrationID, requestID string) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	connection, err := operatorConnection(ctx, filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	return completeRemoteRevocation(ctx, client, registrationID, requestID)
}

func registrationRevisionConflict(err error) bool {
	reason, ok := hostv1.ErrorReasonOf(err)
	return ok && reason == hostv1.ErrorReason_ERROR_REASON_REGISTRATION_REVISION_CONFLICT
}

func completeRemoteRevocation(ctx context.Context, client hostv1.OperatorServiceClient, registrationID, requestID string) error {
	var last error
	for range 3 {
		current, err := client.GetRemoteRegistration(ctx, &hostv1.GetRemoteRegistrationRequest{RegistrationId: registrationID})
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if current.GetRegistration().GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
			return nil
		}
		begun, err := client.BeginRemoteRevocation(ctx, &hostv1.BeginRemoteRevocationRequest{RegistrationId: registrationID, ExpectedRevision: current.GetRegistration().GetRevision(), RequestId: requestID})
		if err == nil {
			_, err = client.CompleteRemoteRevocation(ctx, &hostv1.CompleteRemoteRevocationRequest{RegistrationId: registrationID, ExpectedRevision: begun.GetRegistration().GetRevision(), RequestId: requestID})
		}
		if !registrationRevisionConflict(err) {
			return err
		}
		last = err
	}
	return last
}
