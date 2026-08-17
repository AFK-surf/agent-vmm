package mcpappliance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	BootstrapSocket    = "bootstrap.sock"
	ControllerSocket   = "controller.sock"
	FrontendHTTPSocket = "frontend-mcp-http.sock"
)

type Config struct {
	RuntimeDirectory string
	StateDirectory   string
	Logger           *slog.Logger
}

func Run(ctx context.Context, config Config) error {
	if !filepath.IsAbs(config.RuntimeDirectory) || !filepath.IsAbs(config.StateDirectory) {
		return errors.New("appliance runtime and state directories must be absolute")
	}
	if err := os.MkdirAll(config.RuntimeDirectory, 0o700); err != nil {
		return err
	}
	identity := &Identity{}
	controller := NewController(identity, config.StateDirectory, config.Logger)
	bootstrap := NewBootstrapServer(identity, config.StateDirectory)
	bootstrapGRPC := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(2<<20), grpc.MaxConcurrentStreams(1))
	appliancev1.RegisterApplianceBootstrapServiceServer(bootstrapGRPC, bootstrap)
	controllerGRPC := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(2<<20), grpc.MaxConcurrentStreams(64),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second}))
	remotev1.RegisterRemoteControllerServiceServer(controllerGRPC, controller)

	bootstrapListener, err := privateListener(config.RuntimeDirectory, BootstrapSocket)
	if err != nil {
		return err
	}
	defer bootstrapListener.Close()
	controllerListener, err := privateListener(config.RuntimeDirectory, ControllerSocket)
	if err != nil {
		return err
	}
	defer controllerListener.Close()
	httpFrontendListener, err := privateListener(config.RuntimeDirectory, FrontendHTTPSocket)
	if err != nil {
		return err
	}
	defer httpFrontendListener.Close()

	go func() {
		<-ctx.Done()
		bootstrapGRPC.GracefulStop()
		controllerGRPC.GracefulStop()
		bootstrapListener.Close()
		controllerListener.Close()
		httpFrontendListener.Close()
	}()
	errorsChannel := make(chan error, 3)
	go func() { errorsChannel <- bootstrapGRPC.Serve(bootstrapListener) }()
	go func() { errorsChannel <- controllerGRPC.Serve(controllerListener) }()
	go func() { errorsChannel <- controller.ServeHTTPFrontend(ctx, httpFrontendListener) }()
	err = <-errorsChannel
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func privateListener(directory, name string) (net.Listener, error) {
	path := filepath.Join(directory, name)
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
