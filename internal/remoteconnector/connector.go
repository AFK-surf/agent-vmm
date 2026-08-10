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
	delay := time.Second
	var attempt uint32
	for ctx.Err() == nil {
		attempt++
		reportConnectorStatus(ctx, config, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTING, "", attempt, time.Time{}, "", "")
		err := connectOnce(ctx, config)
		if errors.Is(err, ErrCredentialRevoked) {
			reportConnectorStatus(ctx, config, hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED, "", attempt, time.Time{}, "CREDENTIAL_REVOKED", "Controller access was revoked.")
			return err
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
		if phase == hostv1.ConnectorPhase_CONNECTOR_PHASE_AUTH_FAILED || phase == hostv1.ConnectorPhase_CONNECTOR_PHASE_CONFIG_ERROR {
			return err
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if delay < MaxReconnectDelay {
			delay *= 2
			if delay > MaxReconnectDelay {
				delay = MaxReconnectDelay
			}
		}
	}
	return nil
}

func connectOnce(ctx context.Context, config Config) error {
	credential, err := config.CredentialStore.Load(ctx, config.RegistrationID)
	if err != nil {
		return err
	}
	defer wipe(credential)
	controller, err := newStoredControllerClient(config)
	if err != nil {
		return fmt.Errorf("%w: controller TLS configuration", errConnectorConfiguration)
	}
	defer controller.connection.Close()
	local, err := newLocalClient(ctx, config.RemoteComputeSocket)
	if err != nil {
		return err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{})
	if err != nil {
		return fmt.Errorf("remote compute state unavailable")
	}
	registration := state.GetRegistration()
	if registration.GetId() != config.RegistrationID || registration.GetProtocolVersion() != ProtocolVersion || registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		return fmt.Errorf("%w: local registration identity or version", errConnectorConfiguration)
	}
	if len(registration.GetAllocations()) > MaxInventoryItems {
		return fmt.Errorf("%w: remote inventory limit", errConnectorConfiguration)
	}
	instance, err := randomInstance()
	if err != nil {
		return err
	}
	connectionContext, stopConnection := context.WithCancel(ctx)
	var heartbeatDone <-chan struct{}
	defer func() {
		stopConnection()
		if heartbeatDone != nil {
			<-heartbeatDone
		}
	}()
	stream, err := controller.client.Connect(connectionContext)
	if err != nil {
		return fmt.Errorf("controller control stream failed: %w", err)
	}
	if err := stream.Send(&remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello(registration, instance)}}); err != nil {
		return fmt.Errorf("controller handshake failed: %w", err)
	}
	if _, err := local.client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{ConnectionInstance: instance, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}); err != nil {
		return fmt.Errorf("local connection fence failed")
	}
	if _, err := reportConnectorStatusWithClient(ctx, local.client, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", ""); err != nil {
		return fmt.Errorf("connector status fence failed")
	}
	done := make(chan struct{})
	heartbeatDone = done
	go connectorHeartbeat(connectionContext, local.client, instance, 5*time.Second, stopConnection, done)
	defer func() {
		disconnectContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = local.client.SetConnectionState(disconnectContext, &hostv1.SetConnectionStateRequest{ConnectionInstance: instance, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
	}()
	tunnels := newTunnelManager(connectionContext, controller.client, local.client, registration.GetId(), config.Logger)
	defer tunnels.Close()
	handler := commandHandler{client: local.client, connectionInstance: instance, tunnels: tunnels}
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("controller receive failed")
		}
		switch payload := message.GetPayload().(type) {
		case *remotev1.ControllerControlMessage_Command:
			commandContext, cancel := context.WithTimeout(connectionContext, 30*time.Second)
			result := handler.handle(commandContext, payload.Command)
			cancel()
			if err := stream.Send(&remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Result{Result: result}}); err != nil {
				return fmt.Errorf("controller result send failed")
			}
		case *remotev1.ControllerControlMessage_RotateCredential:
			value := payload.RotateCredential.GetCredential()
			if len(value) == 0 || len(value) > MaxCredentialBytes {
				return fmt.Errorf("controller rotation credential is invalid")
			}
			if err := config.CredentialStore.Replace(ctx, config.RegistrationID, value); err != nil {
				return err
			}
			wipe(value)
		case *remotev1.ControllerControlMessage_RevokeCredential:
			if err := config.CredentialStore.Delete(ctx, config.RegistrationID); err != nil {
				return err
			}
			return ErrCredentialRevoked
		default:
			return fmt.Errorf("controller sent an unknown control message")
		}
	}
}

func reportConnectorStatus(ctx context.Context, config Config, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string) {
	local, err := newLocalClient(ctx, config.RemoteComputeSocket)
	if err != nil {
		return
	}
	defer local.connection.Close()
	_, _ = reportConnectorStatusWithClient(ctx, local.client, phase, instance, attempt, nextRetry, code, message)
}

func reportConnectorStatusWithClient(ctx context.Context, client hostv1.RemoteComputeServiceClient, phase hostv1.ConnectorPhase, instance string, attempt uint32, nextRetry time.Time, code, message string) (*hostv1.RemoteRegistration, error) {
	statusValue := &hostv1.ConnectorStatus{Phase: phase, ConnectorRelease: ConnectorRelease, ObservedAt: timestamppb.Now(), ReconnectAttempt: attempt, FailureCode: code, FailureMessage: message}
	if !nextRetry.IsZero() {
		statusValue.NextRetryAt = timestamppb.New(nextRetry)
	}
	response, err := client.ReportConnectorStatus(ctx, &hostv1.ReportConnectorStatusRequest{ConnectionInstance: instance, Status: statusValue})
	if err != nil {
		return nil, err
	}
	return response.GetRegistration(), nil
}

func connectorHeartbeat(ctx context.Context, client hostv1.RemoteComputeServiceClient, instance string, interval time.Duration, cancel context.CancelFunc, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatContext, heartbeatCancel := context.WithTimeout(ctx, 3*time.Second)
			registration, err := reportConnectorStatusWithClient(heartbeatContext, client, hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, instance, 0, time.Time{}, "", "")
			heartbeatCancel()
			if err != nil || registration.GetEnrollmentState() != hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY || registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
				cancel()
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
