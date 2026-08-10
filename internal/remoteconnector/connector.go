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
)

var ErrCredentialRevoked = errors.New("controller credential revoked")

func Run(ctx context.Context, config Config) error {
	if err := config.validate(); err != nil {
		return err
	}
	delay := time.Second
	for ctx.Err() == nil {
		err := connectOnce(ctx, config)
		if errors.Is(err, ErrCredentialRevoked) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		if config.Logger != nil {
			config.Logger.Printf("controller connection ended; retrying")
		}
		wait := jitter(delay)
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
		return fmt.Errorf("controller TLS connection failed")
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
		return fmt.Errorf("local registration identity or version mismatch")
	}
	if len(registration.GetAllocations()) > MaxInventoryItems {
		return fmt.Errorf("remote inventory exceeds connector limit")
	}
	instance, err := randomInstance()
	if err != nil {
		return err
	}
	stream, err := controller.client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("controller control stream failed")
	}
	if err := stream.Send(&remotev1.HostControlMessage{Payload: &remotev1.HostControlMessage_Hello{Hello: hello(registration, instance)}}); err != nil {
		return fmt.Errorf("controller handshake failed")
	}
	if _, err := local.client.SetConnectionState(ctx, &hostv1.SetConnectionStateRequest{ConnectionInstance: instance, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED}); err != nil {
		return fmt.Errorf("local connection fence failed")
	}
	defer func() {
		disconnectContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = local.client.SetConnectionState(disconnectContext, &hostv1.SetConnectionStateRequest{ConnectionInstance: instance, State: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_DISCONNECTED})
	}()
	tunnels := newTunnelManager(ctx, controller.client, local.client, registration.GetId(), config.Logger)
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
			commandContext, cancel := context.WithTimeout(ctx, 30*time.Second)
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
