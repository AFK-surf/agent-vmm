package remoteconnector

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func dialController(config Config, credential []byte) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: config.EndpointIdentity, RootCAs: config.TLSRoots})),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)),
		grpc.WithInitialWindowSize(StreamWindowBytes), grpc.WithInitialConnWindowSize(ConnectionWindowBytes),
	}
	if len(credential) != 0 {
		options = append(options, grpc.WithPerRPCCredentials(bearerCredential{value: credential, registration: config.RegistrationID}))
	}
	return grpc.NewClient(config.Endpoint, options...)
}

func dialStoredController(config Config) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: config.EndpointIdentity, RootCAs: config.TLSRoots})),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)),
		grpc.WithInitialWindowSize(StreamWindowBytes), grpc.WithInitialConnWindowSize(ConnectionWindowBytes),
		grpc.WithPerRPCCredentials(storedCredential{store: config.CredentialStore, account: config.RegistrationID}),
	}
	return grpc.NewClient(config.Endpoint, options...)
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

func dialRemoteCompute(ctx context.Context, socket string) (*grpc.ClientConn, error) {
	if err := validateSocket(socket); err != nil {
		return nil, err
	}
	return grpc.NewClient("passthrough:///agent-vmm-remote", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxControlBytes), grpc.MaxCallSendMsgSize(MaxControlBytes)))
}

func validateSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("remote compute socket unavailable")
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("remote compute socket has unsafe permissions")
	}
	if runtime.GOOS != "windows" {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != os.Getuid() {
			return fmt.Errorf("remote compute socket has unsafe owner")
		}
	}
	return nil
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
	client     hostv1.RemoteComputeServiceClient
}

func newLocalClient(ctx context.Context, socket string) (*localClient, error) {
	connection, err := dialRemoteCompute(ctx, socket)
	if err != nil {
		return nil, err
	}
	return &localClient{connection: connection, client: hostv1.NewRemoteComputeServiceClient(connection)}, nil
}
