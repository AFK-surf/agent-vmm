package remoteconnector

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"google.golang.org/grpc"
)

func LoadTLSRoots(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("controller TLS CA file is unavailable")
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("system TLS roots are unavailable")
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("controller TLS CA file is invalid")
	}
	return roots, nil
}

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
	Endpoint                string
	EndpointIdentity        string
	RegistrationID          string
	RemoteComputeSocket     string
	TrustSocket             string
	TrustCapabilityFile     string
	ServiceTunnelSocket     string
	MeshControlSocket       string
	DataWorkerPath          string
	DataWorkerSocket        string
	UseExistingDataWorker   bool
	CredentialStore         CredentialStore
	PrivateControllerDialer func(context.Context) (*grpc.ClientConn, error)
	OnReady                 func()
	CommandAdmission        <-chan struct{}
	OnAttemptError          func(error)
	Logger                  Logger
	TLSRoots                *x509.CertPool
	endpointObservation     *trustv1.EndpointObservation
}

type Logger interface {
	Printf(format string, arguments ...any)
}

// LoadConfig reads non-secret controller identity from the restricted local API.
// The credential remains in Keychain and is loaded only when an RPC is sent.
func LoadConfig(ctx context.Context, socket, registrationID string, store CredentialStore) (Config, error) {
	if registrationID == "" {
		return Config{}, fmt.Errorf("registration ID is required")
	}
	local, err := newLocalClient(ctx, socket, registrationID)
	if err != nil {
		return Config{}, err
	}
	defer local.connection.Close()
	state, err := local.client.GetRemoteState(ctx, &hostv1.GetRemoteStateRequest{RegistrationId: registrationID})
	if err != nil {
		return Config{}, fmt.Errorf("remote registration unavailable")
	}
	registration := state.GetRegistration()
	parsed, err := url.Parse("https://" + registration.GetEndpoint())
	if err != nil || parsed.Hostname() == "" {
		return Config{}, fmt.Errorf("remote registration endpoint is invalid")
	}
	config := Config{Endpoint: registration.GetEndpoint(), EndpointIdentity: parsed.Hostname(), RegistrationID: registration.GetId(), RemoteComputeSocket: socket,
		TrustSocket: filepath.Join(filepath.Dir(socket), "trust.sock"), TrustCapabilityFile: filepath.Join(filepath.Dir(socket), "connector-trust.cap"), CredentialStore: store}
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
	if !filepath.IsAbs(config.RemoteComputeSocket) {
		return fmt.Errorf("remote compute socket must be absolute")
	}
	if (config.TrustSocket == "") != (config.TrustCapabilityFile == "") {
		return fmt.Errorf("trust socket and connector capability file must be configured together")
	}
	if config.TrustSocket != "" && (!filepath.IsAbs(config.TrustSocket) || !filepath.IsAbs(config.TrustCapabilityFile)) {
		return fmt.Errorf("trust socket and connector capability file must be absolute")
	}
	if config.UseExistingDataWorker && config.DataWorkerPath != "" {
		return fmt.Errorf("an existing data worker cannot also be started by the connector")
	}
	if !config.UseExistingDataWorker && (config.DataWorkerPath == "") != (config.DataWorkerSocket == "") {
		return fmt.Errorf("data worker path and socket must be configured together")
	}
	if config.UseExistingDataWorker && !filepath.IsAbs(config.DataWorkerSocket) {
		return fmt.Errorf("existing data worker socket must be absolute")
	}
	if config.DataWorkerPath != "" && (!filepath.IsAbs(config.DataWorkerPath) || !filepath.IsAbs(config.DataWorkerSocket)) {
		return fmt.Errorf("data worker path and socket must be absolute")
	}
	if (config.DataWorkerPath != "" || config.UseExistingDataWorker) && (!filepath.IsAbs(config.ServiceTunnelSocket) || !filepath.IsAbs(config.MeshControlSocket)) {
		return fmt.Errorf("service and mesh-control sockets must be absolute when the data worker is configured")
	}
	if config.PrivateControllerDialer != nil {
		if config.Endpoint != "" || config.EndpointIdentity != "" || config.CredentialStore != nil || config.TrustSocket != "" || config.DataWorkerPath != "" || config.UseExistingDataWorker {
			return fmt.Errorf("private controller transport cannot include external connector configuration")
		}
		return nil
	}
	if config.EndpointIdentity == "" || len(config.EndpointIdentity) > 253 {
		return fmt.Errorf("endpoint identity is required and bounded")
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
