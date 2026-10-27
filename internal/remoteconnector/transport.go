package remoteconnector

import (
	"context"
	"crypto/tls"
	"fmt"
	connectorv1 "github.com/AFK-surf/agent-vmm/internal/api/connector/v1"
	"net"
	"net/url"
	"os"
	"time"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"golang.org/x/net/http/httpproxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

var controllerConnectParams = grpc.ConnectParams{
	Backoff:           backoff.Config{BaseDelay: time.Second, Multiplier: 1.6, Jitter: 0.2, MaxDelay: MaxReconnectDelay},
	MinConnectTimeout: gatewayDialBudget,
}

var controllerKeepalive = keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}

func dialController(config Config, credential []byte) (*grpc.ClientConn, error) {
	target, transportOptions := gatewayDialOptions(config)
	options := append(transportOptions, []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)),
		grpc.WithInitialWindowSize(StreamWindowBytes), grpc.WithInitialConnWindowSize(ConnectionWindowBytes),
		grpc.WithConnectParams(controllerConnectParams), grpc.WithKeepaliveParams(controllerKeepalive),
	}...)
	if len(credential) != 0 {
		options = append(options, grpc.WithPerRPCCredentials(bearerCredential{value: credential, registration: config.RegistrationID}))
	}
	return grpc.NewClient(target, options...)
}

func dialStoredController(config Config) (*grpc.ClientConn, error) {
	target, transportOptions := gatewayDialOptions(config)
	options := append(transportOptions, []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)),
		grpc.WithInitialWindowSize(StreamWindowBytes), grpc.WithInitialConnWindowSize(ConnectionWindowBytes),
		grpc.WithConnectParams(controllerConnectParams), grpc.WithKeepaliveParams(controllerKeepalive),
		grpc.WithPerRPCCredentials(storedCredential{store: config.external().Credentials, account: config.RegistrationID}),
	}...)
	return grpc.NewClient(target, options...)
}

// Preserve explicit HTTP CONNECT proxy policy. Interface recovery applies only to
// direct sockets (including system-managed TUN routing).
func gatewayDialOptions(config Config) (string, []grpc.DialOption) {
	tlsCredentials := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: config.endpointIdentity(), RootCAs: config.external().Roots})
	proxy, err := httpproxy.FromEnvironment().ProxyFunc()(&url.URL{Scheme: "https", Host: config.external().Endpoint})
	if err != nil || proxy != nil {
		return config.external().Endpoint, []grpc.DialOption{grpc.WithTransportCredentials(tlsCredentials)}
	}
	transport := newGatewayTransport(tlsCredentials)
	return "passthrough:///" + config.external().Endpoint, []grpc.DialOption{grpc.WithTransportCredentials(transport), grpc.WithContextDialer(transport.Dial)}
}

type bearerCredential struct {
	value        []byte
	registration string
}

func (credential bearerCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	if len(credential.value) == 0 || len(credential.value) > MaxCredentialBytes {
		return nil, fmt.Errorf("controller credential is invalid")
	}
	if credential.registration == "" || len(credential.registration) > 128 {
		return nil, fmt.Errorf("controller registration is invalid")
	}
	return map[string]string{"authorization": controllerremote.EncodeBearer(credential.value), "x-agent-vmm-registration": credential.registration}, nil
}
func (bearerCredential) RequireTransportSecurity() bool { return true }

type storedCredential struct {
	store   CredentialStore
	account string
}

func (credential storedCredential) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	value, err := credential.store.Load(ctx, credential.account)
	if err != nil {
		return nil, err
	}
	defer wipe(value)
	if len(value) == 0 || len(value) > MaxCredentialBytes {
		return nil, fmt.Errorf("controller credential is invalid")
	}
	if credential.account == "" || len(credential.account) > 128 {
		return nil, fmt.Errorf("controller registration is invalid")
	}
	return map[string]string{"authorization": controllerremote.EncodeBearer(value), "x-agent-vmm-registration": credential.account}, nil
}
func (storedCredential) RequireTransportSecurity() bool { return true }

func dialRemoteCompute(socket, registrationID string) (*grpc.ClientConn, error) {
	if err := validateSocket(socket); err != nil {
		return nil, err
	}
	if registrationID == "" || len(registrationID) > 128 {
		return nil, fmt.Errorf("remote registration identity is required")
	}
	return grpc.NewClient("passthrough:///agent-vmm-remote", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}), grpc.WithPerRPCCredentials(localRegistrationCredential(registrationID)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)))
}

type localRegistrationCredential string

func (credential localRegistrationCredential) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"x-agent-vmm-registration": string(credential)}, nil
}
func (localRegistrationCredential) RequireTransportSecurity() bool { return false }

func validateSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("remote compute socket unavailable")
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("remote compute path is not a socket")
	}
	return checkSocketOwner(info, "remote compute socket")
}

type controllerClient struct {
	connection *grpc.ClientConn
	client     remotev1.RemoteControllerServiceClient
}

func newControllerClient(config Config, credential []byte) (*controllerClient, error) {
	connection, err := dialController(config, credential)
	if err != nil {
		return nil, err
	}
	return &controllerClient{connection: connection, client: remotev1.NewRemoteControllerServiceClient(connection)}, nil
}

func newStoredControllerClient(config Config) (*controllerClient, error) {
	connection, err := dialStoredController(config)
	if err != nil {
		return nil, err
	}
	return &controllerClient{connection: connection, client: remotev1.NewRemoteControllerServiceClient(connection)}, nil
}

type localClient struct {
	connection *grpc.ClientConn
	client     connectorv1.RemoteComputeServiceClient
}

func newLocalClient(socket, registrationID string) (*localClient, error) {
	connection, err := dialRemoteCompute(socket, registrationID)
	if err != nil {
		return nil, err
	}
	return &localClient{connection: connection, client: connectorv1.NewRemoteComputeServiceClient(connection)}, nil
}
