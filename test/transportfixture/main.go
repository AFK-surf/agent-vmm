package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"sync"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
)

type operatorServer struct {
	hostv1.UnimplementedOperatorServiceServer
}

func (operatorServer) GetSystemSnapshot(context.Context, *hostv1.GetSystemSnapshotRequest) (*hostv1.GetSystemSnapshotResponse, error) {
	return &hostv1.GetSystemSnapshotResponse{Vm: &hostv1.VM{State: hostv1.VMState_VM_STATE_RUNNING, BootId: "transport-fixture"}, Health: &hostv1.HealthStatus{State: hostv1.HealthState_HEALTH_STATE_HEALTHY}}, nil
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
