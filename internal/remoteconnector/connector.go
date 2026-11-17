package remoteconnector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errConnectorConfiguration = errors.New("connector configuration is invalid")
	// The Host restarts its data worker after an unexpected exit, so an
	// unreachable worker socket is a retryable local condition.
	errDataWorkerUnavailable = errors.New("data worker is unavailable")
)

func Run(ctx context.Context, config Config) error {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	config.Logger = config.Logger.With("component", "remoteconnector", "registration_id", config.RegistrationID)
	observations := newEndpointObservationState(nil)
	backoff := reconnectBackoff{}
	var attempt uint32
	for ctx.Err() == nil {
		attempt++
		started := time.Now()
		connected, err := runAttempt(ctx, config, observations)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && config.OnAttemptError != nil {
			config.OnAttemptError(err)
		}
		protocol := remoteProtocolFailure(err)
		delay := backoff.next(connected, time.Since(started))
		if protocol != nil {
			delay, backoff.delay = MaxReconnectDelay, MaxReconnectDelay
		}
		wait := jitter(delay)
		phase, code, message := connectorFailure(err)
		reportConnectorStatus(ctx, config, phase, "", attempt, time.Now().Add(wait), code, message, protocol)
		config.Logger.Warn("controller connection ended", "error", err)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
	return nil
}

func runAttempt(ctx context.Context, config Config, observations *endpointObservationState) (bool, error) {
	if err := config.validate(); err != nil {
		return false, fmt.Errorf("%w: %w", errConnectorConfiguration, err)
	}
	var worker *dataworker.Supervisor
	if socket := config.trustSocket(); socket != "" {
		if err := probeTrust(ctx, socket); err != nil {
			return false, fmt.Errorf("%w: local trust: %w", errConnectorConfiguration, err)
		}
	}
	if external, ok := config.Transport.(ExternalTransport); ok && external.Worker != nil {
		var err error
		worker, err = dataworker.Attach(external.Worker.Socket)
		if err != nil {
			return false, fmt.Errorf("%w: attach data worker: %w", errConnectorConfiguration, err)
		}
		state, err := worker.Status(ctx)
		if err != nil {
			return false, fmt.Errorf("%w: %w", errDataWorkerUnavailable, err)
		}
		current := observations.Load()
		if current == nil || string(current.GetEndpointNodeId()) != state.EndpointID || current.GetEndpointGeneration() != state.EndpointGeneration ||
			current.GetExpiresAt() == nil || time.Until(current.GetExpiresAt().AsTime()) <= endpointRefreshWindow {
			observation, err := signEndpointObservation(ctx, config.trustSocket(), state.EndpointID, state.EndpointGeneration)
			if err != nil {
				return false, fmt.Errorf("%w: endpoint observation: %w", errConnectorConfiguration, err)
			}
			observations.Store(observation)
		}
	}
	reportConnectorStatus(ctx, config, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTING, "", 0, time.Time{}, "", "", nil)
	return connectAttempt(ctx, config, worker, observations)
}

// reconnectBackoff doubles the retry delay up to MaxReconnectDelay. Only a
// connection that stayed established for MaxReconnectDelay resets it, so a
// controller that fails right after each connect cannot drive a reconnect storm.
type reconnectBackoff struct{ delay time.Duration }

func (backoff *reconnectBackoff) next(connected bool, established time.Duration) time.Duration {
	if backoff.delay == 0 || connected && established >= MaxReconnectDelay {
		backoff.delay = time.Second
	}
	current := backoff.delay
	backoff.delay = min(2*backoff.delay, MaxReconnectDelay)
	return current
}

func connectAttempt(ctx context.Context, config Config, worker *dataworker.Supervisor, observations *endpointObservationState) (bool, error) {
	var controller *controllerClient
	var err error
	if transport, private := config.Transport.(PrivateTransport); private {
		connection, dialErr := transport.Dial(ctx)
		if dialErr != nil {
			return false, dialErr
		}
		controller = &controllerClient{connection: connection, client: remotev1.NewRemoteControllerServiceClient(connection)}
	} else {
		credential, loadErr := config.external().Credentials.Load(ctx, config.RegistrationID)
		if loadErr != nil {
			return false, loadErr
		}
		wipe(credential)
		controller, err = newStoredControllerClient(config)
		if err != nil {
			return false, fmt.Errorf("%w: controller TLS configuration: %w", errConnectorConfiguration, err)
		}
	}
	defer controller.connection.Close()
	local, err := newLocalClient(config.RemoteComputeSocket, config.RegistrationID)
	if err != nil {
		return false, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &connectorv1.GetRemoteStateRequest{RegistrationId: config.RegistrationID})
	if err != nil {
		return false, fmt.Errorf("remote compute state unavailable: %w", err)
	}
	registration := state.GetRegistration()
	if registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		return false, status.Error(codes.PermissionDenied, "local registration is revoked")
	}
	if registration.GetId() != config.RegistrationID {
		return false, fmt.Errorf("%w: local registration identity", errConnectorConfiguration)
	}
	instance, err := randomInstance()
	if err != nil {
		return false, err
	}
	epoch, err := randomEpoch()
	if err != nil {
		return false, err
	}
	initialObservation := observations.Load()
	if initialObservation != nil {
		if err := validateEndpointObservation(initialObservation, time.Now()); err != nil {
			return false, fmt.Errorf("%w: initial endpoint observation: %w", errConnectorConfiguration, err)
		}
	}
	remoteObservation := collectObservation(ctx, local.client, registration, 1)
	helloMessage := hello(registration, instance, epoch, initialObservation, remoteObservation)
	connectionContext, stopConnection := context.WithCancelCause(ctx)
	var heartbeatDone <-chan struct{}
	defer func() {
		stopConnection(context.Canceled)
		if heartbeatDone != nil {
			<-heartbeatDone
		}
	}()
	stream, err := controller.client.Connect(connectionContext)
	if err != nil {
		return false, fmt.Errorf("controller control stream failed: %w", err)
	}
	writer := newControlWriter(connectionContext, stream, stopConnection)
	if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Hello{Hello: helloMessage}}); err != nil {
		if errors.Is(err, io.EOF) {
			if _, receiveErr := stream.Recv(); receiveErr != nil {
				err = receiveErr
			}
		}
		return false, fmt.Errorf("controller handshake failed: %w", err)
	}
	readyMessage, err := stream.Recv()
	if err != nil {
		return false, fmt.Errorf("controller ready handshake failed: %w", err)
	}
	controllerReady := readyMessage.GetControllerReady()
	if err := validateControllerReady(controllerReady, registration.GetId(), epoch, helloMessage.GetInventoryWatermark()); err != nil {
		return false, err
	}
	if config.trustSocket() != "" && observations.Load() != nil {
		go personalMeshSyncLoop(connectionContext, config, observations, trustv1.NewPersonalMeshRegistryServiceClient(controller.connection))
	}
	if worker != nil && observations.Load() != nil {
		go func() {
			err := renewEndpointObservations(connectionContext, epoch, observations, writer, func(renewContext context.Context) (*trustv1.EndpointObservation, error) {
				status, err := worker.Status(renewContext)
				if err != nil {
					return nil, fmt.Errorf("data worker status unavailable: %w", err)
				}
				return signEndpointObservation(renewContext, config.trustSocket(), status.EndpointID, status.EndpointGeneration)
			}, endpointRefreshWindow)
			if err != nil && connectionContext.Err() == nil {
				stopConnection(fmt.Errorf("endpoint observation renewal failed: %w", err))
			}
		}()
		go func() {
			if err := watchEndpointIdentity(connectionContext, observations, worker, endpointIdentityInterval); err != nil {
				stopConnection(err)
			}
		}()
	}
	if remoteObservationEnabled(controllerReady) {
		go func() {
			if err := renewRemoteObservations(connectionContext, local.client, registration, epoch, remoteObservation.GetSequence(), writer); err != nil && connectionContext.Err() == nil {
				stopConnection(fmt.Errorf("remote observation renewal failed: %w", err))
			}
		}()
	}
	connectedState, err := local.client.SetConnectionState(ctx, &connectorv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED})
	if err != nil {
		return false, fmt.Errorf("local connection fence failed: %w", err)
	}
	abortConnectedState := func() {
		disconnectContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = local.client.SetConnectionState(disconnectContext, &connectorv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
	}
	// Do not admit commands or frontends if the local connection fence changed
	// after the bounded snapshot acknowledged by ControllerReady.
	connectedRegistration := connectedState.GetRegistration()
	if connectedRegistration == nil {
		abortConnectedState()
		return false, errors.New("local connection fence returned no registration")
	}
	if connectedRegistration.GetInventoryRevision() != helloMessage.GetInventoryWatermark() || connectedRegistration.GetPoolPolicyRevision() != registration.GetPoolPolicyRevision() {
		abortConnectedState()
		return false, fmt.Errorf("local connection fence changed registration snapshot: expected inventory %d/pool %d got inventory %d/pool %d", helloMessage.GetInventoryWatermark(), registration.GetPoolPolicyRevision(), connectedRegistration.GetInventoryRevision(), connectedRegistration.GetPoolPolicyRevision())
	}
	if _, err := local.client.SetRemoteAdmissionState(ctx, &connectorv1.SetRemoteAdmissionStateRequest{
		RegistrationId: config.RegistrationID, ConnectionInstance: instance,
		State:                      connectedRegistration.GetState(),
		ExpectedInventoryRevision:  helloMessage.GetInventoryWatermark(),
		ExpectedPoolPolicyRevision: registration.GetPoolPolicyRevision(),
	}); err != nil {
		abortConnectedState()
		return false, fmt.Errorf("local admission fence failed: %w", err)
	}
	tunnels := newTunnelManager(connectionContext, controller.client, local.client, registration.GetId(), instance, epoch, config.Logger)
	defer tunnels.Close()

	if _, err := reportConnectorStatusWithClient(ctx, local.client, config.RegistrationID, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", "", tunnels.DomainMetrics(), nil); err != nil {
		return false, fmt.Errorf("connector status fence failed: %w", err)
	}
	if config.OnReady != nil {
		config.OnReady()
	}
	if config.CommandAdmission != nil {
		select {
		case <-config.CommandAdmission:
		case <-connectionContext.Done():
			return true, context.Cause(connectionContext)
		}
	}
	done := make(chan struct{})
	heartbeatDone = done
	go connectorHeartbeat(connectionContext, local.client, config.RegistrationID, instance, 5*time.Second, stopConnection, tunnels.DomainMetrics, done)
	defer func() {
		disconnectContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = local.client.SetConnectionState(disconnectContext, &connectorv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
	}()
	handler := commandHandler{logger: config.Logger, client: local.client, registrationID: config.RegistrationID, connectionInstance: instance, connectionEpoch: epoch, tunnels: tunnels}
	tracker := newCommandTracker(epoch, helloMessage.GetInventoryWatermark())
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return true, nil
		}
		if err != nil {
			if cause := context.Cause(connectionContext); cause != nil && !errors.Is(cause, context.Canceled) {
				return true, cause
			}
			return true, fmt.Errorf("controller receive failed: %w", err)
		}
		switch payload := message.GetPayload().(type) {
		case *remotev1.ConnectResponse_Command:
			receivedAt := time.Now()
			ack, cached := tracker.admit(payload.Command)
			if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Ack{Ack: ack}}); err != nil {
				return true, fmt.Errorf("controller acknowledgement send failed: %w", err)
			}
			if ack.GetDisposition() == remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED {
				continue
			}
			if cached != nil {
				if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Result{Result: cached}}); err != nil {
					return true, fmt.Errorf("controller cached result send failed: %w", err)
				}
				continue
			}
			started := &remotev1.CommandEvidence{CommandId: payload.Command.GetCommandId(), Sequence: payload.Command.GetSequence(), ConnectionEpoch: epoch, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_STARTED, ObservedUnixMillis: time.Now().UnixMilli()}
			if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Evidence{Evidence: started}}); err != nil {
				return true, fmt.Errorf("controller execution evidence send failed: %w", err)
			}
			commandContext, cancel := context.WithDeadline(connectionContext, receivedAt.Add(time.Duration(payload.Command.GetTimeoutMillis())*time.Millisecond))
			result := handler.handle(commandContext, payload.Command)
			cancel()
			state, stateErr := local.client.GetRemoteState(connectionContext, &connectorv1.GetRemoteStateRequest{RegistrationId: config.RegistrationID})
			if stateErr != nil || state.GetRegistration().GetId() != config.RegistrationID || state.GetRegistration().GetInventoryRevision() == 0 {
				return true, fmt.Errorf("authoritative inventory watermark unavailable after command")
			}
			result.InventoryWatermark = state.GetRegistration().GetInventoryRevision()
			if err := tracker.record(payload.Command, result); err != nil {
				return true, err
			}
			finished := &remotev1.CommandEvidence{CommandId: payload.Command.GetCommandId(), Sequence: payload.Command.GetSequence(), ConnectionEpoch: epoch, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_FINISHED, ObservedUnixMillis: time.Now().UnixMilli()}
			if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Evidence{Evidence: finished}}); err != nil {
				return true, fmt.Errorf("controller completion evidence send failed: %w", err)
			}
			if err := writer.Send(connectionContext, &remotev1.ConnectRequest{Payload: &remotev1.ConnectRequest_Result{Result: result}}); err != nil {
				return true, fmt.Errorf("controller result send failed: %w", err)
			}
		default:
			return true, fmt.Errorf("controller sent an unknown control message")
		}
	}
}

func reportConnectorStatus(ctx context.Context, config Config, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string, protocol *hostv1.RemoteProtocolStatus) {
	local, err := newLocalClient(config.RemoteComputeSocket, config.RegistrationID)
	if err != nil {
		return
	}
	defer local.connection.Close()
	_, _ = reportConnectorStatusWithClient(ctx, local.client, config.RegistrationID, phase, instance, attempt, nextRetry, code, message, nil, protocol)
}

func reportConnectorStatusWithClient(ctx context.Context, client connectorv1.RemoteComputeServiceClient, registrationID string, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string, metrics *hostv1.ControllerDomainMetrics, protocol *hostv1.RemoteProtocolStatus) (*hostv1.RemoteRegistration, error) {
	statusValue := &hostv1.ConnectorStatus{Phase: phase, ConnectorRelease: ConnectorRelease, ObservedAt: timestamppb.Now(), ReconnectAttempt: attempt, FailureCode: code, FailureMessage: message, DomainMetrics: metrics, RemoteProtocol: protocol}
	if !nextRetry.IsZero() {
		statusValue.NextRetryAt = timestamppb.New(nextRetry)
	}
	response, err := client.ReportConnectorStatus(ctx, &connectorv1.ReportConnectorStatusRequest{RegistrationId: registrationID, ConnectionInstance: instance, Status: statusValue})
	if err != nil {
		return nil, err
	}
	return response.GetRegistration(), nil
}

func connectorHeartbeat(ctx context.Context, client connectorv1.RemoteComputeServiceClient, registrationID, instance string, interval time.Duration, cancel context.CancelCauseFunc, metrics func() *hostv1.ControllerDomainMetrics, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatContext, heartbeatCancel := context.WithTimeout(ctx, 3*time.Second)
			registration, err := reportConnectorStatusWithClient(heartbeatContext, client, registrationID, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", "", metrics(), nil)
			heartbeatCancel()
			if err != nil {
				cancel(fmt.Errorf("local connector heartbeat failed: %w", err))
				return
			}
			if registration.GetEnrollmentState() != hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY || registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
				cancel(errors.New("local connector heartbeat rejected"))
				return
			}
		}
	}
}

func connectorFailure(err error) (hostv1.ConnectorPhase, string, string) {
	switch {
	case remoteProtocolFailure(err) != nil:
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, "REMOTE_PROTOCOL_UNSUPPORTED", "The controller does not support this Host protocol."
	case errors.Is(err, errDataWorkerUnavailable):
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, "DATA_WORKER_UNAVAILABLE", "The local data worker is unavailable; retrying."
	case errors.Is(err, errConnectorConfiguration):
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, "CONNECTOR_CONFIG_INVALID", "Connector configuration must be repaired."
	case errors.Is(err, ErrCredentialUnavailable):
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR, "CREDENTIAL_UNAVAILABLE", "The controller credential is unavailable. Retry enrollment."
	case status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.PermissionDenied:
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, "CONTROLLER_AUTH_FAILED", "Controller authentication failed."
	case err == nil:
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, "CONNECTION_ENDED", "Controller connection ended."
	default:
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_RETRY_WAIT, "CONNECTION_FAILED", "Controller connection failed; retrying."
	}
}

func randomInstance() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("connection identity unavailable")
	}
	return hex.EncodeToString(value), nil
}

func jitter(delay time.Duration) time.Duration {
	value := []byte{0}
	if _, err := rand.Read(value); err != nil {
		return delay
	}
	return delay/2 + time.Duration(value[0])*delay/512
}

func remoteProtocolFailure(err error) *hostv1.RemoteProtocolStatus {
	if status.Code(err) != codes.FailedPrecondition {
		return nil
	}
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != hostv1.ErrorDomain || info.GetReason() != "REMOTE_PROTOCOL_UNSUPPORTED" {
			continue
		}
		metadata := info.GetMetadata()
		minimum, _ := strconv.ParseUint(metadata["min_protocol"], 10, 32)
		maximum, _ := strconv.ParseUint(metadata["max_protocol"], 10, 32)
		return &hostv1.RemoteProtocolStatus{HostProtocol: metadata["host_protocol"], MinProtocol: uint32(minimum), MaxProtocol: uint32(maximum), HostSide: metadata["host_side"]}
	}
	return nil
}
