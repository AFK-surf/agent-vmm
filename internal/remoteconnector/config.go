package remoteconnector

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"time"
)

import hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"

const (
	ProtocolVersion         = "1"
	HostAPIVersion          = "host.v1"
	ConnectorRelease        = "0.1.0"
	MaxControlBytes         = 1 << 20
	MaxInventoryItems       = 128
	MaxLabelBytes           = 256
	MaxCredentialBytes      = 4096
	FrameBytes              = 32 << 10
	StreamWindowBytes       = 8 << 20
	ConnectionWindowBytes   = 16 << 20
	MaxInFlightBytes        = 2 << 20
	MaxReconnectDelay       = 30 * time.Second
	MaxActiveTunnels        = 32
	MaxTunnelBytesPerSecond = 64 << 20
)

type Config struct {
	Endpoint            string
	EndpointIdentity    string
	RegistrationID      string
	RemoteComputeSocket string
	CredentialStore     CredentialStore
	Logger              Logger
	TLSRoots            *x509.CertPool
}

type Logger interface {
	Printf(format string, arguments ...any)
}

// LoadConfig reads non-secret controller identity from the restricted local API.
// The credential remains in Keychain and is loaded only when an RPC is sent.
func LoadConfig(ctx context.Context, socket string, store CredentialStore) (Config, error) {
	local, err := newLocalClient(ctx, socket)
	if err != nil {
		return Config{}, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{})
	if err != nil {
		return Config{}, fmt.Errorf("remote registration unavailable")
	}
	registration := state.GetRegistration()
	parsed, err := url.Parse("https://" + registration.GetEndpoint())
	if err != nil || parsed.Hostname() == "" {
		return Config{}, fmt.Errorf("remote registration endpoint is invalid")
	}
	config := Config{Endpoint: registration.GetEndpoint(), EndpointIdentity: parsed.Hostname(), RegistrationID: registration.GetId(), RemoteComputeSocket: socket, CredentialStore: store}
	if registration.GetState() == hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_REVOKED {
		return Config{}, ErrCredentialRevoked
	}
	if registration.GetEnrollmentState() != hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY {
		return Config{}, fmt.Errorf("remote enrollment is not ready")
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) validate() error {
	if config.RegistrationID == "" || len(config.RegistrationID) > 128 {
		return fmt.Errorf("registration ID is required and bounded")
	}
	if config.EndpointIdentity == "" || len(config.EndpointIdentity) > 253 {
		return fmt.Errorf("endpoint identity is required and bounded")
	}
	if !filepath.IsAbs(config.RemoteComputeSocket) {
		return fmt.Errorf("remote compute socket must be absolute")
	}
	parsed, err := url.Parse("https://" + config.Endpoint)
	if err != nil || parsed.Host != config.Endpoint || parsed.Port() == "" || net.ParseIP(parsed.Hostname()) != nil {
		return fmt.Errorf("endpoint must be a TLS host:port with DNS identity")
	}
	if parsed.Hostname() != config.EndpointIdentity {
		return fmt.Errorf("endpoint identity must match endpoint hostname")
	}
	if config.CredentialStore == nil {
		return fmt.Errorf("credential store is required")
	}
	return nil
}
