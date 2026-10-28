package lifecycle

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/launchdservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type protocolStatusOperator struct{ managedInstallOperator }

func (*protocolStatusOperator) GetRemoteRegistration(_ context.Context, request *hostv1.GetRemoteRegistrationRequest) (*hostv1.GetRemoteRegistrationResponse, error) {
	if request.GetRegistrationId() != "registration-a" {
		return nil, status.Error(codes.NotFound, "registration not found")
	}
	return &hostv1.GetRemoteRegistrationResponse{Registration: &hostv1.RemoteRegistration{Id: "registration-a", State: hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_ENABLED, ConnectorStatus: &hostv1.ConnectorStatus{FailureCode: "REMOTE_PROTOCOL_UNSUPPORTED", RemoteProtocol: &hostv1.RemoteProtocolStatus{HostProtocol: "4", MinProtocol: 5, MaxProtocol: 5, HostSide: "too_old"}}}}, nil
}

func TestProtocolStatusIsRegistrationScoped(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "vmm-protocol-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	value := testPaths(root)
	value.owner = &serviceOwner{system: launchdservice.DefaultSystem(), observe: func(context.Context, string) (hostServiceFacts, error) {
		return hostServiceFacts{Loaded: true, Running: true, PID: 42}, nil
	}}
	if err := os.MkdirAll(value.applicationData, 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", filepath.Join(value.applicationData, "operator.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	hostv1.RegisterOperatorServiceServer(server, &protocolStatusOperator{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	for _, id := range []string{"", "registration-a", "other-registration"} {
		got := readStatusContext(context.Background(), value, id)
		if !got.HostReadable || !got.HostHealthy || (got.RemoteProtocol != nil) != (id == "registration-a") {
			t.Fatalf("id=%q status=%+v", id, got)
		}
		if id == "registration-a" {
			if got.RemoteProtocol.HostSide != "too_old" || got.RemoteProtocol.MinProtocol != 5 || got.RegistrationRead != "present" || got.ConnectorLoaded {
				t.Fatalf("protocol status=%+v", got)
			}
			writeContractFixture(t, "status.json", got)
		}
	}
}

func writeContractFixture(t *testing.T, name string, value any) {
	t.Helper()
	directory := os.Getenv("AGENT_VMM_CONTRACT_OUTPUT_DIR")
	if directory == "" {
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name), data, 0o600); err != nil {
		t.Fatal(err)
	}
}
