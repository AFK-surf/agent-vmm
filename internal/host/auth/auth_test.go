package auth

import (
	"context"
	"net"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type agentContractServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
}

func (agentContractServer) GetEnvironment(ctx context.Context, _ *hostv1.GetEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	identity, err := AgentIdentity(ctx)
	if err != nil {
		return nil, err
	}
	return &hostv1.GetEnvironmentResponse{Environment: &hostv1.AgentEnvironment{
		Id: identity.EnvironmentID, NamespaceId: identity.NamespaceID,
	}}, nil
}

type operatorContractServer struct {
	hostv1.UnimplementedOperatorServiceServer
}

func (operatorContractServer) GetVM(ctx context.Context, _ *hostv1.GetVMRequest) (*hostv1.GetVMResponse, error) {
	return &hostv1.GetVMResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_STOPPED}}, nil
}

func TestEndpointAndMetadataContract(t *testing.T) {
	agentIdentity := Identity{Role: Agent, SessionID: "session-a", EnvironmentID: "environment-a", NamespaceID: "namespace-a"}
	agent := grpc.NewServer(grpc.UnaryInterceptor(UnaryInterceptor(agentIdentity)), grpc.StreamInterceptor(StreamInterceptor(agentIdentity)))
	hostv1.RegisterAgentRuntimeServiceServer(agent, agentContractServer{})
	agentConnection := serve(t, agent)

	operator := grpc.NewServer(grpc.UnaryInterceptor(UnaryInterceptor(Identity{Role: Operator})), grpc.StreamInterceptor(StreamInterceptor(Identity{Role: Operator})))
	hostv1.RegisterOperatorServiceServer(operator, operatorContractServer{})
	operatorConnection := serve(t, operator)

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"actor_namespace_id", "forged-namespace",
		"role", "operator",
	))
	response, err := hostv1.NewAgentRuntimeServiceClient(agentConnection).GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetEnvironment().GetNamespaceId() != agentIdentity.NamespaceID {
		t.Fatalf("metadata replaced namespace: %q", response.GetEnvironment().GetNamespaceId())
	}
	if _, err := hostv1.NewOperatorServiceClient(agentConnection).GetVM(ctx, &hostv1.GetVMRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("operator method on agent endpoint = %v", err)
	}
	if _, err := hostv1.NewAgentRuntimeServiceClient(operatorConnection).GetEnvironment(ctx, &hostv1.GetEnvironmentRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("agent method on operator endpoint = %v", err)
	}
}

func serve(t *testing.T, server *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///contract", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connection.Close() })
	return connection
}
