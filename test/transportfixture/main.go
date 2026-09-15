package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"sync"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type operatorServer struct {
	hostv1.UnimplementedOperatorServiceServer
}

func (operatorServer) GetSystemSnapshot(context.Context, *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	now := time.Now()
	return &hostv1.GetSystemSnapshotResponse{
		Vm:     &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "transport-fixture"},
		Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY},
		RemoteRegistrations: []*hostv1.RemoteRegistration{{
			Id: "remote-fixture", Endpoint: "controller.example:443", DisplayName: "Fixture Controller",
			ProtocolVersion: "1", Revision: 7,
			State:           hostv1.RemoteRegistrationState_REMOTE_REGISTRATION_STATE_DRAINING,
			ConnectionState: hostv1.RemoteConnectionState_REMOTE_CONNECTION_STATE_CONNECTED,
			EnrollmentState: hostv1.RemoteEnrollmentState_REMOTE_ENROLLMENT_STATE_READY,
			Pool: &hostv1.RemotePoolPolicy{
				MaxEnvironments:      3,
				PerEnvironmentLimits: fixtureLimits(1000, 1<<30, 256, 4<<30),
				MaxEgressMode:        hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
				MaxLeaseDuration:     durationpb.New(45 * time.Minute), DisconnectGrace: durationpb.New(45 * time.Second), StopGrace: durationpb.New(12 * time.Second),
			},
			ConnectorStatus: &hostv1.ConnectorStatus{Phase: hostv1.ConnectorPhase_CONNECTOR_PHASE_CONNECTED, ConnectorRelease: "fixture-1", ObservedAt: timestamppb.New(now), LastConnectedAt: timestamppb.New(now)},
			Allocations: []*hostv1.RemoteAllocation{{
				AllocationId: "allocation-a", EnvironmentId: "environment-a", DisplayLabel: "Build",
				Limits: fixtureLimits(500, 512<<20, 64, 2<<30), EgressMode: hostv1.EgressMode_EGRESS_MODE_DENY_ALL,
				Revision: 4, State: hostv1.AllocationState_ALLOCATION_STATE_LEASED,
				Lease: &hostv1.RemoteLease{LeaseId: "lease-a", AllocationId: "allocation-a", WorkloadRef: "job-a", Generation: 2, ExpiresAt: timestamppb.New(now.Add(time.Minute)), State: hostv1.LeaseState_LEASE_STATE_ACTIVE},
			}},
		}},
	}, nil
}

func (operatorServer) GetSystemHealthSummary(context.Context, *hostv1.GetSystemHealthSummaryRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	return &hostv1.GetSystemSnapshotResponse{
		Vm:     &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "transport-fixture"},
		Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY},
	}, nil
}

func (operatorServer) ListAgentIntegrations(context.Context, *hostv1.ListAgentIntegrationsRequest) (*hostv1.ListAgentIntegrationsResponse, error) {
	return &hostv1.ListAgentIntegrationsResponse{Integrations: []*hostv1.AgentIntegration{{
		Id: "integration-a", AdapterId: "codex", DisplayName: "Codex", Scope: hostv1.IntegrationScope_INTEGRATION_SCOPE_USER,
		ConfigTarget: "/fixture/config.toml", EnvironmentId: "environment-a",
		DesiredState:      hostv1.IntegrationDesiredState_INTEGRATION_DESIRED_STATE_ALLOWED,
		ConfigState:       hostv1.IntegrationConfigState_INTEGRATION_CONFIG_STATE_READY,
		ConfigFingerprint: "fixture", Revision: 3, CreatedAt: timestamppb.New(time.Now()),
	}}}, nil
}

func (operatorServer) ListAgentSessions(context.Context, *hostv1.ListAgentSessionsRequest) (*hostv1.ListAgentSessionsResponse, error) {
	return &hostv1.ListAgentSessionsResponse{Sessions: []*hostv1.AgentSession{{
		Id: "session-a", IntegrationId: "integration-a", EnvironmentId: "environment-a",
		Origin:    hostv1.AgentSessionOrigin_AGENT_SESSION_ORIGIN_LOCAL_INTEGRATION,
		State:     hostv1.AgentSessionState_AGENT_SESSION_STATE_CONNECTED,
		CreatedAt: timestamppb.New(time.Now().Add(-time.Minute)), ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
		LastActivityAt: timestamppb.New(time.Now()), ActiveConnectionCount: 2,
	}}}, nil
}

func fixtureLimits(cpu uint32, memory uint64, pids uint32, disk uint64) *hostv1.ResourceLimits {
	return &hostv1.ResourceLimits{CpuMillis: &cpu, MemoryBytes: &memory, Pids: &pids, DiskBytes: &disk}
}

func (operatorServer) WatchEvents(_ *hostv1.WatchEventsRequest, stream hostv1.OperatorService_WatchEventsServer) error {
	if err := stream.Send(&hostv1.RuntimeEvent{Sequence: 1, Type: "fixture", GuestBootId: "transport-fixture"}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type agentServer struct {
	hostv1.UnimplementedAgentRuntimeServiceServer
}

func (agentServer) GetEnvironment(context.Context, *hostv1.GetOwnEnvironmentRequest) (*hostv1.GetEnvironmentResponse, error) {
	return &hostv1.GetEnvironmentResponse{Environment: &hostv1.AgentEnvironment{Id: "fixture"}}, nil
}

func (agentServer) Exec(stream hostv1.AgentRuntimeService_ExecServer) error {
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.Send(&hostv1.ExecResponse{Event: &hostv1.ExecResponse_ExitCode{ExitCode: 0}})
		}
		if err != nil {
			return err
		}
		if data := request.GetStdin(); len(data) != 0 {
			if err := stream.Send(&hostv1.ExecResponse{Event: &hostv1.ExecResponse_Stdout{Stdout: data}}); err != nil {
				return err
			}
		}
	}
}

func main() {
	var operatorPath, agentPath string
	flag.StringVar(&operatorPath, "operator-socket", "", "operator socket")
	flag.StringVar(&agentPath, "agent-socket", "", "agent socket")
	flag.Parse()
	var group sync.WaitGroup
	serve := func(path string, register func(*grpc.Server)) {
		defer group.Done()
		_ = os.Remove(path)
		listener, err := net.Listen("unix", path)
		if err != nil {
			panic(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			panic(err)
		}
		server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
		register(server)
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}
	group.Add(2)
	go serve(operatorPath, func(server *grpc.Server) { hostv1.RegisterOperatorServiceServer(server, operatorServer{}) })
	go serve(agentPath, func(server *grpc.Server) { hostv1.RegisterAgentRuntimeServiceServer(server, agentServer{}) })
	group.Wait()
}
