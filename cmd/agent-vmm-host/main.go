//go:build darwin

package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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
	"github.com/AFK-surf/agent-vmm/internal/host/servicepreface"
	"github.com/AFK-surf/agent-vmm/internal/host/trustkeys"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc"
)

func main() {
	var config vmm.Config
	var operatorSocket, remoteComputeSocket, trustSocket, serviceTunnelSocket, meshControlSocket, dataWorkerSocket, trustCapabilityFile, databasePath, sessionDirectory string
	var disablePersonalMeshPairing bool
	flag.StringVar(&config.BundleDirectory, "bundle", "", "verified guest bundle directory")
	flag.StringVar(&config.InstanceDirectory, "instance", "", "VM instance directory")
	flag.UintVar(&config.CPUCount, "cpus", 2, "fixed VM CPU count")
	flag.Uint64Var(&config.MemoryBytes, "memory", 1<<30, "fixed VM memory bytes")
	flag.Int64Var(&config.StateBytes, "state-size", 8<<30, "state disk logical bytes")
	flag.StringVar(&config.SerialLog, "serial-log", "", "serial log path")
	flag.StringVar(&operatorSocket, "operator-socket", "", "operator Unix socket")
	flag.StringVar(&remoteComputeSocket, "remote-compute-socket", "", "restricted remote connector Unix socket")
	flag.StringVar(&trustSocket, "trust-socket", "", "connector-only trust Unix socket")
	flag.StringVar(&serviceTunnelSocket, "service-tunnel-socket", "", "data-worker-only destination service Unix socket")
	flag.StringVar(&meshControlSocket, "mesh-control-socket", "", "data-worker-only personal mesh control Unix socket")
	flag.StringVar(&dataWorkerSocket, "data-worker-socket", "", "connector-supervised data worker control Unix socket")
	flag.StringVar(&trustCapabilityFile, "trust-capability-file", "", "install-scoped connector capability file")
	flag.StringVar(&databasePath, "database", "", "host control-plane SQLite database")
	flag.StringVar(&sessionDirectory, "session-directory", "", "agent session socket directory")
	flag.BoolVar(&disablePersonalMeshPairing, "disable-personal-mesh-pairing", false, "deny personal mesh genesis and new member admission while preserving revoke/repair")
	flag.Parse()
	config.TrustRoot = trustRoot
	if operatorSocket == "" || remoteComputeSocket == "" || databasePath == "" || sessionDirectory == "" {
		log.Fatal("operator-socket, remote-compute-socket, database, and session-directory are required")
	}
	if trustSocket == "" {
		trustSocket = filepath.Join(filepath.Dir(operatorSocket), "trust.sock")
	}
	if trustCapabilityFile == "" {
		trustCapabilityFile = filepath.Join(filepath.Dir(operatorSocket), "connector-trust.cap")
	}
	if serviceTunnelSocket == "" {
		serviceTunnelSocket = filepath.Join(filepath.Dir(operatorSocket), "service-tunnel.sock")
	}
	if dataWorkerSocket == "" {
		dataWorkerSocket = filepath.Join(filepath.Dir(operatorSocket), "data-worker.sock")
	}
	if meshControlSocket == "" {
		meshControlSocket = filepath.Join(filepath.Dir(operatorSocket), "mesh-control.sock")
	}
	for _, directory := range []string{filepath.Dir(operatorSocket), filepath.Dir(remoteComputeSocket), filepath.Dir(trustSocket), filepath.Dir(serviceTunnelSocket), filepath.Dir(meshControlSocket), filepath.Dir(trustCapabilityFile), filepath.Dir(databasePath), filepath.Dir(config.SerialLog), sessionDirectory} {
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
		TrustKeyProvider:           trustkeys.NewPlatformProvider(),
		DisablePersonalMeshPairing: disablePersonalMeshPairing,
		DataWorkerSocket:           dataWorkerSocket,
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
	remoteListener, err := listenOperator(remoteComputeSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer remoteListener.Close()
	trustListener, err := listenOperator(trustSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer trustListener.Close()
	serviceBaseListener, err := listenOperator(serviceTunnelSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer serviceBaseListener.Close()
	meshBaseListener, err := listenOperator(meshControlSocket)
	if err != nil {
		log.Fatal(err)
	}
	defer meshBaseListener.Close()
	connectorCapability, err := ensureConnectorCapability(trustCapabilityFile)
	if err != nil {
		log.Fatal(err)
	}
	server := controller.NewOperatorServer()
	remoteServer := controller.NewRemoteComputeServer()
	trustServer, err := controller.NewTrustServer(connectorCapability)
	if err != nil {
		log.Fatal(err)
	}
	serviceListener, err := servicepreface.New(serviceBaseListener, connectorCapability)
	if err != nil {
		log.Fatal(err)
	}
	serviceServer, err := controller.NewServiceTunnelServer()
	if err != nil {
		log.Fatal(err)
	}
	meshListener, err := servicepreface.New(meshBaseListener, connectorCapability)
	if err != nil {
		log.Fatal(err)
	}
	meshServer, err := controller.NewMeshControlServer()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go shutdown(ctx, []*grpc.Server{server, remoteServer, trustServer, serviceServer, meshServer}, manager)
	go func() {
		if err := meshServer.Serve(meshListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("mesh control stopped: %v", err)
			server.Stop()
		}
	}()
	go func() {
		if err := serviceServer.Serve(serviceListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("service tunnel stopped: %v", err)
			server.Stop()
		}
	}()
	go func() {
		if err := trustServer.Serve(trustListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("trust service stopped: %v", err)
			server.Stop()
		}
	}()
	go func() {
		if err := remoteServer.Serve(remoteListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("remote compute service stopped: %v", err)
			server.Stop()
		}
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}
}

func ensureConnectorCapability(path string) ([]byte, error) {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, errors.New("connector trust capability must be a regular file")
		}
		value, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if len(value) < 32 {
			return nil, errors.New("connector trust capability is invalid")
		}
		if err := os.Chmod(path, 0o600); err != nil {
			return nil, err
		}
		return value, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	randomValue := make([]byte, 32)
	if _, err := rand.Read(randomValue); err != nil {
		return nil, err
	}
	value := []byte(base64.RawURLEncoding.EncodeToString(randomValue))
	temporary, err := os.CreateTemp(filepath.Dir(path), ".connector-trust-*")
	if err != nil {
		return nil, err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return nil, err
	}
	if _, err := temporary.Write(value); err != nil {
		temporary.Close()
		return nil, err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return nil, err
	}
	if err := temporary.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return nil, err
	}
	return value, nil
}

func shutdown(ctx context.Context, servers []*grpc.Server, manager *vmm.Manager) {
	<-ctx.Done()
	stopContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if manager.Snapshot().GetState() == hostv1.VMState_VM_STATE_RUNNING {
		if _, err := manager.Stop(stopContext, false); err != nil {
			_, _ = manager.Stop(stopContext, true)
		}
	}
	for _, server := range servers {
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
