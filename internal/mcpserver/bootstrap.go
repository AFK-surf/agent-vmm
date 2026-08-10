package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Session struct {
	Runtime    hostv1.AgentRuntimeServiceClient
	connection *grpc.ClientConn
	sessionID  string
}

func DefaultOperatorSocket() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "Application Support", "AgentVMM", "operator.sock"), nil
}

func Bootstrap(ctx context.Context, operatorSocket, environmentID, displayName string) (*Session, error) {
	if environmentID == "" {
		return nil, fmt.Errorf("environment is required")
	}
	if !filepath.IsAbs(operatorSocket) {
		return nil, fmt.Errorf("operator socket must be absolute")
	}
	operatorConnection, err := dialUnix(ctx, operatorSocket)
	if err != nil {
		return nil, fmt.Errorf("operator unavailable")
	}
	operator := hostv1.NewOperatorServiceClient(operatorConnection)
	requestID, err := newRequestID()
	if err != nil {
		operatorConnection.Close()
		return nil, fmt.Errorf("request identity unavailable")
	}
	ensured, err := operator.EnsureLocalEnvironment(ctx, &hostv1.EnsureLocalEnvironmentRequest{RequestId: requestID, EnvironmentId: environmentID, DisplayName: displayName})
	if err != nil {
		operatorConnection.Close()
		return nil, fmt.Errorf("local environment bootstrap failed")
	}
	if ensured.GetEnvironment().GetOwner().GetKind() != hostv1.EnvironmentOwnerKind_ENVIRONMENT_OWNER_KIND_LOCAL {
		operatorConnection.Close()
		return nil, fmt.Errorf("environment is not locally owned")
	}
	requestID, err = newRequestID()
	if err != nil {
		operatorConnection.Close()
		return nil, fmt.Errorf("request identity unavailable")
	}
	created, err := operator.CreateAgentSession(ctx, &hostv1.CreateAgentSessionRequest{RequestId: requestID, EnvironmentId: environmentID})
	operatorConnection.Close()
	if err != nil {
		return nil, fmt.Errorf("agent session creation failed")
	}
	if created.GetEnvironment().GetOrigin() != hostv1.EnvironmentOrigin_ENVIRONMENT_ORIGIN_LOCAL {
		return nil, fmt.Errorf("agent session is not local")
	}
	if err := validateSessionSocket(created.GetSocketPath()); err != nil {
		return nil, err
	}
	connection, err := dialUnix(ctx, created.GetSocketPath())
	if err != nil {
		return nil, fmt.Errorf("agent session unavailable")
	}
	return &Session{Runtime: hostv1.NewAgentRuntimeServiceClient(connection), connection: connection, sessionID: created.GetSessionId()}, nil
}

func (session *Session) Close(ctx context.Context) error {
	requestID, err := newRequestID()
	if err == nil {
		_, err = session.Runtime.ReleaseSession(ctx, &hostv1.ReleaseSessionRequest{RequestId: requestID})
	}
	closeErr := session.connection.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func dialUnix(ctx context.Context, path string) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}))
}

func validateSessionSocket(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("agent session socket must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("agent session socket unavailable")
	}
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		return fmt.Errorf("agent session socket has unsafe permissions")
	}
	if runtime.GOOS != "windows" {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != os.Getuid() {
			return fmt.Errorf("agent session socket has unsafe owner")
		}
	}
	return nil
}

func newRequestID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "mcp-" + hex.EncodeToString(bytes), nil
}
