package remoteconnector

import (
	"context"
	"crypto/x509"
	"fmt"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"log/slog"
	"net"
	"net/url"
	"path/filepath"

	"google.golang.org/grpc"
	"time"
)

const (
	ProtocolVersion  = controllerremote.ProtocolVersion
	ConnectorRelease = "0.1.0"
	MaxControlBytes  = 1 << 20
	// MaxActiveInventoryItems bounds one connection snapshot. It is not a
	// registration-wide allocation limit; exact command results carry facts for
	// allocations outside this snapshot.
	MaxActiveInventoryItems = controllerremote.MaxActiveInventoryItems
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
	RegistrationID      string
	RemoteComputeSocket string
	Transport           Transport
	OnReady             func()
	CommandAdmission    <-chan struct{}
	OnAttemptError      func(error)
	Logger              *slog.Logger
}

type Transport interface{ connectorTransport() }

type ExternalTransport struct {
	Endpoint    string
	Roots       *x509.CertPool
	Credentials CredentialStore
	Trust       *TrustBoundary
	Worker      *WorkerSockets
}

func (ExternalTransport) connectorTransport() {}

type PrivateTransport struct {
	Dial func(context.Context) (*grpc.ClientConn, error)
}

func (PrivateTransport) connectorTransport() {}

type TrustBoundary struct{ Socket string }
type WorkerSockets struct{ Socket, ServiceTunnelSocket, MeshControlSocket string }

func ParseEndpoint(endpoint string) (string, error) {
	parsed, err := url.Parse("https://" + endpoint)
	if err != nil || parsed.Host != endpoint || parsed.Port() == "" || parsed.Hostname() == "" || len(parsed.Hostname()) > 253 || net.ParseIP(parsed.Hostname()) != nil {
		return "", fmt.Errorf("endpoint must be a TLS host:port with DNS identity")
	}
	return parsed.Hostname(), nil
}

func (config Config) external() ExternalTransport { return config.Transport.(ExternalTransport) }
func (config Config) trustSocket() string {
	transport, external := config.Transport.(ExternalTransport)
	if !external || transport.Trust == nil {
		return ""
	}
	return transport.Trust.Socket
}

func (config Config) validate() error {
	if config.RegistrationID == "" || len(config.RegistrationID) > 128 {
		return fmt.Errorf("registration ID is required and bounded")
	}
	if !filepath.IsAbs(config.RemoteComputeSocket) {
		return fmt.Errorf("remote compute socket must be absolute")
	}
	switch transport := config.Transport.(type) {
	case PrivateTransport:
		if transport.Dial == nil {
			return fmt.Errorf("private controller dialer is required")
		}
	case ExternalTransport:
		if transport.Credentials == nil {
			return fmt.Errorf("credential store is required")
		}
		if transport.Trust != nil && !filepath.IsAbs(transport.Trust.Socket) {
			return fmt.Errorf("trust socket must be absolute")
		}
		if transport.Worker != nil && (!filepath.IsAbs(transport.Worker.Socket) || !filepath.IsAbs(transport.Worker.ServiceTunnelSocket) || !filepath.IsAbs(transport.Worker.MeshControlSocket)) {
			return fmt.Errorf("worker sockets must be absolute")
		}
	default:
		return fmt.Errorf("controller transport is required")
	}
	return nil
}

func (config Config) endpointIdentity() string {
	hostname, _ := ParseEndpoint(config.external().Endpoint)
	return hostname
}
