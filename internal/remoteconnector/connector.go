package remoteconnector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrCredentialRevoked = errors.New("controller credential revoked")
var errConnectorConfiguration = errors.New("connector configuration is invalid")

func Run(ctx context.Context, config Config) error {
	if err := config.validate(); err != nil {
		return err
	}
	if config.TrustSocket != "" {
		if err := probeTrust(ctx, config.TrustSocket, config.TrustCapabilityFile); err != nil {
			return fmt.Errorf("local trust boundary unavailable: %w", err)
		}
	}
	var worker *dataworker.Supervisor
	ownedWorker := false
	if config.DataWorkerPath != "" {
		startedWorker, err := dataworker.Start(ctx, config.DataWorkerPath, config.DataWorkerSocket, config.ServiceTunnelSocket, config.MeshControlSocket, config.TrustCapabilityFile)
		if err != nil {
			return fmt.Errorf("data worker unavailable: %w", err)
		}
		worker = startedWorker
		ownedWorker = true
	} else if config.UseExistingDataWorker {
		attachedWorker, err := dataworker.Attach(config.DataWorkerSocket)
		if err != nil {
			return err
		}
		worker = attachedWorker
	}
	if worker != nil {
		status, err := worker.Status(ctx)
		if err != nil {
			return fmt.Errorf("data worker status unavailable: %w", err)
		}
		observation, err := signEndpointObservation(ctx, config.TrustSocket, config.TrustCapabilityFile, status.EndpointID, status.EndpointGeneration)
		if err != nil {
			return fmt.Errorf("data endpoint observation unavailable: %w", err)
		}
		config.endpointObservation = observation
	}
	if ownedWorker {
		defer func() {
			drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := worker.Close(drainCtx); err != nil && config.Logger != nil {
				config.Logger.Printf("data worker drain failed")
			}
		}()
	}
	observations := newEndpointObservationState(config.endpointObservation)
	backoff := reconnectBackoff{}
	var attempt uint32
	for ctx.Err() == nil {
		attempt++
		currentObservation := observations.Load()
		if worker != nil && (currentObservation == nil || currentObservation.GetExpiresAt() == nil ||
			time.Until(currentObservation.GetExpiresAt().AsTime()) <= endpointRefreshWindow) {
			status, statusErr := worker.Status(ctx)
			if statusErr != nil {
				return fmt.Errorf("data worker status unavailable: %w", statusErr)
			}
			observation, observationErr := signEndpointObservation(ctx, config.TrustSocket, config.TrustCapabilityFile, status.EndpointID, status.EndpointGeneration)
			if observationErr != nil {
				return fmt.Errorf("data endpoint observation renewal unavailable: %w", observationErr)
			}
			observations.Store(observation)
		}
		reportConnectorStatus(ctx, config, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTING, "", attempt, time.Time{}, "", "")
		started := time.Now()
		connected, err := connectAttempt(ctx, config, worker, observations)
		delay := backoff.next(connected, time.Since(started))
		// Only explicit revocation is terminal. Authentication and
		// configuration failures keep the reported phase and retry, because
		// controller or local repair can clear them without a Host restart.
		if errors.Is(err, ErrCredentialRevoked) {
			reportConnectorStatus(ctx, config, hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, "", attempt, time.Time{}, "CREDENTIAL_REVOKED", "Controller access was revoked.")
			return err
		}
		if err != nil && config.OnAttemptError != nil {
			config.OnAttemptError(err)
		}
		if ctx.Err() != nil {
			return nil
		}
		if config.Logger != nil {
			config.Logger.Printf("controller connection ended; retrying")
		}
		wait := jitter(delay)
		nextRetry := time.Now().Add(wait)
		phase, code, message := connectorFailure(err)
		reportConnectorStatus(ctx, config, phase, "", attempt, nextRetry, code, message)
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

func connectOnce(ctx context.Context, config Config) error {
	_, err := connectAttempt(ctx, config, nil, newEndpointObservationState(config.endpointObservation))
	return err
}

func connectAttempt(ctx context.Context, config Config, worker *dataworker.Supervisor, observations *endpointObservationState) (bool, error) {
	var controller *controllerClient
	var err error
	if config.PrivateControllerDialer != nil {
		connection, dialErr := config.PrivateControllerDialer(ctx)
		if dialErr != nil {
			return false, dialErr
		}
		controller = &controllerClient{connection: connection, client: remotev1.NewRemoteControllerServiceClient(connection)}
	} else {
		credential, loadErr := config.CredentialStore.Load(ctx, config.RegistrationID)
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
	local, err := newLocalClient(ctx, config.RemoteComputeSocket, config.RegistrationID)
	if err != nil {
		return false, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: config.RegistrationID})
	if err != nil {
		return false, fmt.Errorf("remote compute state unavailable: %w", err)
	}
	registration := state.GetRegistration()
	if registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		return false, ErrCredentialRevoked
	}
	if registration.GetId() != config.RegistrationID || registration.GetProtocolVersion() != ProtocolVersion {
		return false, fmt.Errorf("%w: local registration identity or version", errConnectorConfiguration)
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
	if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: helloMessage}}); err != nil {
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
	if config.TrustSocket != "" && observations.Load() != nil {
		go personalMeshSyncLoop(connectionContext, config, observations, trustv1.NewPersonalMeshRegistryServiceClient(controller.connection))
	}
	if worker != nil && observations.Load() != nil {
		go func() {
			err := renewEndpointObservations(connectionContext, epoch, observations, writer, func(renewContext context.Context) (*trustv1.EndpointObservation, error) {
				status, err := worker.Status(renewContext)
				if err != nil {
					return nil, fmt.Errorf("data worker status unavailable: %w", err)
				}
				return signEndpointObservation(renewContext, config.TrustSocket, config.TrustCapabilityFile, status.EndpointID, status.EndpointGeneration)
			}, endpointRefreshWindow)
			if err != nil && connectionContext.Err() == nil {
				stopConnection(fmt.Errorf("endpoint observation renewal failed: %w", err))
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
	connectedState, err := local.client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED})
	if err != nil {
		return false, fmt.Errorf("local connection fence failed: %w", err)
	}
	abortConnectedState := func() {
		disconnectContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = local.client.SetConnectionState(disconnectContext, &hostv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
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
	if _, err := local.client.SetRemoteAdmissionState(ctx, &hostv1.SetRemoteAdmissionStateRequest{
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
	frontends := newFrontendManager(connectionContext, controller.client, local.client, registration.GetId(), instance, epoch, func(event *hostv1.HostLoopbackFrontendEvent) error {
		return writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_FrontendEvent{FrontendEvent: event}})
	})
	defer frontends.Close()
	if _, err := reportConnectorStatusWithClient(ctx, local.client, config.RegistrationID, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", "", tunnels.DomainMetrics()); err != nil {
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
		_, _ = local.client.SetConnectionState(disconnectContext, &hostv1.SetConnectionStateRequest{RegistrationId: config.RegistrationID, ConnectionInstance: instance, ConnectionEpoch: epoch, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
	}()
	handler := commandHandler{client: local.client, registrationID: config.RegistrationID, connectionInstance: instance, connectionEpoch: epoch, tunnels: tunnels}
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
		case *remotev1.ControllerControlMessage_Command:
			ack, cached := tracker.admit(payload.Command, time.Now())
			if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Ack{Ack: ack}}); err != nil {
				return true, fmt.Errorf("controller acknowledgement send failed: %w", err)
			}
			if ack.GetDisposition() == remotev1.CommandAckDisposition_COMMAND_ACK_DISPOSITION_REJECTED {
				continue
			}
			if cached != nil {
				if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Result{Result: cached}}); err != nil {
					return true, fmt.Errorf("controller cached result send failed: %w", err)
				}
				continue
			}
			started := &remotev1.CommandEvidence{CommandId: payload.Command.GetCommandId(), Sequence: payload.Command.GetSequence(), ConnectionEpoch: epoch, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_STARTED, ObservedUnixMillis: time.Now().UnixMilli()}
			if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Evidence{Evidence: started}}); err != nil {
				return true, fmt.Errorf("controller execution evidence send failed: %w", err)
			}
			commandContext, cancel := context.WithDeadline(connectionContext, time.UnixMilli(payload.Command.GetDeadlineUnixMillis()))
			result := handler.handle(commandContext, payload.Command)
			cancel()
			state, stateErr := local.client.GetRemoteState(connectionContext, &hostv1.GetRemoteStateRequest{RegistrationId: config.RegistrationID})
			if stateErr != nil || state.GetRegistration().GetId() != config.RegistrationID || state.GetRegistration().GetInventoryRevision() == 0 {
				return true, fmt.Errorf("authoritative inventory watermark unavailable after command")
			}
			result.InventoryWatermark = state.GetRegistration().GetInventoryRevision()
			if err := tracker.record(payload.Command, result); err != nil {
				return true, err
			}
			finished := &remotev1.CommandEvidence{CommandId: payload.Command.GetCommandId(), Sequence: payload.Command.GetSequence(), ConnectionEpoch: epoch, Stage: remotev1.ExecutionStage_EXECUTION_STAGE_FINISHED, ObservedUnixMillis: time.Now().UnixMilli()}
			if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Evidence{Evidence: finished}}); err != nil {
				return true, fmt.Errorf("controller completion evidence send failed: %w", err)
			}
			if err := writer.Send(connectionContext, &remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Result{Result: result}}); err != nil {
				return true, fmt.Errorf("controller result send failed: %w", err)
			}
		case *remotev1.ControllerControlMessage_RotateCredential:
			if config.CredentialStore == nil {
				return true, fmt.Errorf("private controller cannot rotate a connector credential")
			}
			value := payload.RotateCredential.GetCredential()
			if len(value) == 0 || len(value) > MaxCredentialBytes {
				return true, fmt.Errorf("controller rotation credential is invalid")
			}
			if err := config.CredentialStore.Replace(ctx, config.RegistrationID, value); err != nil {
				return true, err
			}
			wipe(value)
		case *remotev1.ControllerControlMessage_RevokeCredential:
			if config.CredentialStore == nil {
				return true, fmt.Errorf("private controller cannot revoke a connector credential")
			}
			if err := config.CredentialStore.Delete(ctx, config.RegistrationID); err != nil {
				return true, err
			}
			return true, ErrCredentialRevoked
		default:
			return true, fmt.Errorf("controller sent an unknown control message")
		}
	}
}

func reportConnectorStatus(ctx context.Context, config Config, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string) {
	local, err := newLocalClient(ctx, config.RemoteComputeSocket, config.RegistrationID)
	if err != nil {
		return
	}
	defer local.connection.Close()
	_, _ = reportConnectorStatusWithClient(ctx, local.client, config.RegistrationID, phase, instance, attempt, nextRetry, code, message, nil)
}

func reportConnectorStatusWithClient(ctx context.Context, client hostv1.RemoteComputeServiceClient, registrationID string, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string, metrics *hostv1.ControllerDomainMetrics) (*hostv1.RemoteRegistration, error) {
	statusValue := &hostv1.ConnectorStatus{Phase: phase, ConnectorRelease: ConnectorRelease, ObservedAt: timestamppb.Now(), ReconnectAttempt: attempt, FailureCode: code, FailureMessage: message, DomainMetrics: metrics}
	if !nextRetry.IsZero() {
		statusValue.NextRetryAt = timestamppb.New(nextRetry)
	}
	response, err := client.ReportConnectorStatus(ctx, &hostv1.ReportConnectorStatusRequest{RegistrationId: registrationID, ConnectionInstance: instance, Status: statusValue})
	if err != nil {
		return nil, err
	}
	return response.GetRegistration(), nil
}

func connectorHeartbeat(ctx context.Context, client hostv1.RemoteComputeServiceClient, registrationID, instance string, interval time.Duration, cancel context.CancelCauseFunc, metrics func() *hostv1.ControllerDomainMetrics, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatContext, heartbeatCancel := context.WithTimeout(ctx, 3*time.Second)
			registration, err := reportConnectorStatusWithClient(heartbeatContext, client, registrationID, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", "", metrics())
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
	case errors.Is(err, ErrCredentialRevoked):
		return hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, "CREDENTIAL_REVOKED", "Controller access was revoked."
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
