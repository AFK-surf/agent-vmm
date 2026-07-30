//go:build darwin

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type hostServer struct {
	hostv1.UnimplementedHostServiceServer
	manager *vmm.Manager
}

func (server *hostServer) StartVM(ctx context.Context, _ *hostv1.StartVMRequest) (*hostv1.StartVMResponse, error) {
	vm, err := server.manager.Start(ctx)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &hostv1.StartVMResponse{Vm: vm}, nil
}

func (server *hostServer) GetVM(context.Context, *hostv1.GetVMRequest) (*hostv1.GetVMResponse, error) {
	return &hostv1.GetVMResponse{Vm: server.manager.Snapshot()}, nil
}

func (server *hostServer) StopVM(ctx context.Context, request *hostv1.StopVMRequest) (*hostv1.StopVMResponse, error) {
	if request.GetMode() != hostv1.StopMode_STOP_MODE_GRACEFUL && request.GetMode() != hostv1.StopMode_STOP_MODE_FORCE {
		return nil, status.Error(codes.InvalidArgument, "stop mode is required")
	}
	vm, err := server.manager.Stop(ctx, request.GetMode() == hostv1.StopMode_STOP_MODE_FORCE)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &hostv1.StopVMResponse{Vm: vm}, nil
}

func main() {
	var config vmm.Config
	var controlSocket, relaySocket string
	flag.StringVar(&config.BundleDirectory, "bundle", "", "verified guest bundle directory")
	flag.StringVar(&config.InstanceDirectory, "instance", "", "VM instance directory")
	flag.UintVar(&config.CPUCount, "cpus", 2, "fixed VM CPU count")
	flag.Uint64Var(&config.MemoryBytes, "memory", 1<<30, "fixed VM memory bytes")
	flag.Int64Var(&config.StateBytes, "state-size", 8<<30, "state disk logical bytes")
	flag.StringVar(&config.SerialLog, "serial-log", "", "serial log path")
	flag.StringVar(&controlSocket, "control-socket", "", "host control Unix socket")
	flag.StringVar(&relaySocket, "relay-socket", "", "vmmd relay Unix socket")
	flag.Parse()
	config.TrustRoot = trustRoot
	if controlSocket == "" || relaySocket == "" {
		log.Fatal("control-socket and relay-socket are required")
	}
	for _, socket := range []string{controlSocket, relaySocket} {
		if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
			log.Fatal(err)
		}
	}
	manager, err := vmm.NewManager(config)
	if err != nil {
		log.Fatal(err)
	}
	listener, err := listenUnix(controlSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	grpcServer := grpc.NewServer()
	hostv1.RegisterHostServiceServer(grpcServer, &hostServer{manager: manager})
	relayErrors := make(chan error, 1)
	go func() { relayErrors <- vmm.ServeRelay(ctx, relaySocket, manager) }()
	go func() {
		<-ctx.Done()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopCancel()
		if manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
			_, _ = manager.Stop(stopCtx, false)
		}
		grpcServer.GracefulStop()
	}()
	if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}
	select {
	case err := <-relayErrors:
		if err != nil {
			log.Print(fmt.Errorf("relay: %w", err))
		}
	default:
	}
}

func listenUnix(path string) (net.Listener, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}
