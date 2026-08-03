//go:build darwin

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/host"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

func main() {
	var config vmm.Config
	var operatorSocket, databasePath, sessionDirectory string
	flag.StringVar(&config.BundleDirectory, "bundle", "", "verified guest bundle directory")
	flag.StringVar(&config.InstanceDirectory, "instance", "", "VM instance directory")
	flag.UintVar(&config.CPUCount, "cpus", 2, "fixed VM CPU count")
	flag.Uint64Var(&config.MemoryBytes, "memory", 1<<30, "fixed VM memory bytes")
	flag.Int64Var(&config.StateBytes, "state-size", 8<<30, "state disk logical bytes")
	flag.StringVar(&config.SerialLog, "serial-log", "", "serial log path")
	flag.StringVar(&operatorSocket, "operator-socket", "", "operator Unix socket")
	flag.StringVar(&databasePath, "database", "", "host control-plane SQLite database")
	flag.StringVar(&sessionDirectory, "session-directory", "", "agent session socket directory")
	flag.Parse()
	config.TrustRoot = trustRoot
	if operatorSocket == "" || databasePath == "" || sessionDirectory == "" {
		log.Fatal("operator-socket, database, and session-directory are required")
	}
	for _, directory := range []string{filepath.Dir(operatorSocket), filepath.Dir(databasePath), filepath.Dir(config.SerialLog), sessionDirectory} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			log.Fatal(err)
		}
	}
	manager, err := vmm.NewManager(config)
	if err != nil {
		log.Fatal(err)
	}
	controller, err := host.New(host.Config{
		Manager: manager, DatabasePath: databasePath, SessionDirectory: sessionDirectory,
		InstanceID: filepath.Base(config.InstanceDirectory), BundleDirectory: config.BundleDirectory,
		InstanceDirectory: config.InstanceDirectory, StateDiskIdentity: filepath.Join(config.InstanceDirectory, "state.raw"),
		CPUCount: config.CPUCount, MemoryBytes: config.MemoryBytes, StateBytes: config.StateBytes,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer controller.Close()
	listener, err := listenOperator(operatorSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	server := controller.NewOperatorServer()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go shutdown(ctx, server, manager)
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}
}

func shutdown(ctx context.Context, server *grpc.Server, manager *vmm.Manager) {
	<-ctx.Done()
	stopContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if _, err := manager.Stop(stopContext, false); err != nil {
			_, _ = manager.Stop(stopContext, true)
		}
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-stopContext.Done():
		server.Stop()
	}
}

func listenOperator(path string) (net.Listener, error) {
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
