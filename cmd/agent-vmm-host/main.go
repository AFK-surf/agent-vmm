//go:build darwin

package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
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
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"github.com/AFK-surf/agent-vmm/internal/host"
	"github.com/AFK-surf/agent-vmm/internal/host/servicepreface"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

const (
	installedVMResumeTimeout = 10 * time.Minute
	defaultNewStateBytes     = int64(64 << 30)
	minimumNewStateBytes     = int64(16 << 30)
)

func main() {
	if err := validateRuntimeUID(os.Geteuid()); err != nil {
		log.Fatal(err)
	}
	if err := disableCoreDumps(); err != nil {
		log.Fatal(err)
	}
	var config vmm.Config
	var newStateBytes int64
	var migrateStateFrom string
	var operatorSocket, remoteComputeSocket, trustSocket, serviceTunnelSocket, meshControlSocket, dataWorkerSocket, dataWorkerPath, trustCapabilityFile, databasePath, sessionDirectory string
	var disablePersonalMeshPairing bool
	flag.StringVar(&config.BundleDirectory, "bundle", "", "verified guest bundle directory")
	flag.StringVar(&config.InstanceDirectory, "instance", "", "VM instance directory")
	flag.UintVar(&config.CPUCount, "cpus", 8, "fixed VM CPU count")
	flag.Uint64Var(&config.MemoryBytes, "memory", 12<<30, "fixed VM memory bytes")
	flag.Int64Var(&newStateBytes, "state-size", defaultNewStateBytes, "fully physically backed state disk bytes for a new installation")
	flag.StringVar(&migrateStateFrom, "migrate-state-from", "", "offline legacy state disk to copy into a new state.raw in --instance")
	flag.StringVar(&config.SerialLog, "serial-log", "", "serial log path")
	flag.StringVar(&operatorSocket, "operator-socket", "", "operator Unix socket")
	flag.StringVar(&remoteComputeSocket, "remote-compute-socket", "", "restricted remote connector Unix socket")
	flag.StringVar(&trustSocket, "trust-socket", "", "connector-only trust Unix socket")
	flag.StringVar(&serviceTunnelSocket, "service-tunnel-socket", "", "data-worker-only destination service Unix socket")
	flag.StringVar(&meshControlSocket, "mesh-control-socket", "", "data-worker-only personal mesh control Unix socket")
	flag.StringVar(&dataWorkerSocket, "data-worker-socket", "", "data worker control Unix socket")
	flag.StringVar(&dataWorkerPath, "data-worker", "", "host-supervised Agent VMM data worker")
	flag.StringVar(&trustCapabilityFile, "trust-capability-file", "", "install-scoped data worker capability file")
	flag.StringVar(&databasePath, "database", "", "host control-plane SQLite database")
	flag.StringVar(&sessionDirectory, "session-directory", "", "agent session socket directory")
	flag.BoolVar(&disablePersonalMeshPairing, "disable-personal-mesh-pairing", false, "deny personal mesh genesis and new member admission while preserving revoke/repair")
	flag.Parse()
	config.GuestManifestDigest = guestManifestDigest
	if migrateStateFrom != "" {
		if err := runStateMigration(config, migrateStateFrom); err != nil {
			log.Fatal(err)
		}
		return
	}
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
	if dataWorkerPath == "" {
		dataWorkerPath = filepath.Join(filepath.Dir(os.Args[0]), "agent-vmm-data")
	}
	for _, directory := range []string{config.InstanceDirectory, filepath.Dir(operatorSocket), filepath.Dir(remoteComputeSocket), filepath.Dir(trustSocket), filepath.Dir(serviceTunnelSocket), filepath.Dir(meshControlSocket), filepath.Dir(trustCapabilityFile), filepath.Dir(databasePath), filepath.Dir(config.SerialLog), sessionDirectory} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			log.Fatal(err)
		}
	}
	ownerLock, err := acquireHostOwnerLock(filepath.Join(config.InstanceDirectory, "host-owner.lock"))
	if err != nil {
		log.Fatal(err)
	}
	defer ownerLock.Close()
	stateDisk := filepath.Join(config.InstanceDirectory, "state.raw")
	config.StateBytes, err = resolveStateDiskBytes(stateDisk, newStateBytes)
	if err != nil {
		log.Fatal(err)
	}
	manager, err := vmm.NewManager(config)
	if err != nil {
		log.Fatal(err)
	}
	controller, err := host.New(host.Config{
		Manager: manager, DatabasePath: databasePath, SessionDirectory: sessionDirectory,
		InstanceID: filepath.Base(config.InstanceDirectory), BundleDirectory: config.BundleDirectory,
		InstanceDirectory: config.InstanceDirectory, StateDiskIdentity: stateDisk,
		CPUCount: config.CPUCount, MemoryBytes: config.MemoryBytes, StateBytes: config.StateBytes,
		TrustKeyProvider:           newTrustKeyProvider(config.InstanceDirectory),
		DisablePersonalMeshPairing: disablePersonalMeshPairing,
		DataWorkerSocket:           dataWorkerSocket,
		RemoteComputeSocket:        remoteComputeSocket,
		TrustSocket:                trustSocket,
		TrustCapabilityFile:        trustCapabilityFile,
		ServiceTunnelSocket:        serviceTunnelSocket,
		MeshControlSocket:          meshControlSocket,
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
	worker, err := dataworker.Start(ctx, dataWorkerPath, dataWorkerSocket, serviceTunnelSocket, meshControlSocket, trustCapabilityFile)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := worker.Close(closeContext); err != nil {
			log.Printf("stop data worker: %v", err)
		}
	}()
	go shutdown(ctx, []*grpc.Server{server, remoteServer, trustServer, serviceServer, meshServer}, controller)
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
	if err := controller.StartExternalConnectors(ctx); err != nil {
		log.Fatal(err)
	}
	// Keep the operator endpoint observable while a slow Guest resumes. The
	// lifecycle helper must be able to distinguish STARTING from an unreadable
	// Host instead of timing out and creating a competing update owner.
	go func() {
		resumeContext, resumeCancel := context.WithTimeout(ctx, installedVMResumeTimeout)
		defer resumeCancel()
		resumeRequestID := fmt.Sprintf("host-start-resume-%d", time.Now().UnixNano())
		if err := resumeInstalledVM(resumeContext, stateDisk, resumeRequestID, controller.StartVM); err != nil {
			log.Printf("installed VM was not resumed: %v", err)
		}
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}
}

func runStateMigration(config vmm.Config, source string) error {
	if config.BundleDirectory == "" || config.InstanceDirectory == "" || config.SerialLog == "" {
		return errors.New("state migration requires bundle, instance, and serial-log")
	}
	sourcePath, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	targetPath := filepath.Join(config.InstanceDirectory, "state.raw")
	targetAbsolute, err := filepath.Abs(targetPath)
	if err != nil {
		return err
	}
	if sourcePath == targetAbsolute {
		return errors.New("state migration source and target must be distinct")
	}
	if filepath.Dir(sourcePath) == filepath.Dir(targetAbsolute) {
		return errors.New("state migration target must use a separate instance directory")
	}
	info, err := os.Lstat(sourcePath)
	if err != nil || !info.Mode().IsRegular() || info.Size() < minimumNewStateBytes {
		return errors.New("state migration source must be a regular disk of at least 16 GiB")
	}
	if _, err := os.Lstat(targetAbsolute); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("state migration target already exists")
		}
		return err
	}
	if err := os.MkdirAll(config.InstanceDirectory, 0o700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(config.SerialLog), 0o700); err != nil {
		return err
	}
	sourceLock, err := acquireHostOwnerLock(filepath.Join(filepath.Dir(sourcePath), "host-owner.lock"))
	if err != nil {
		return fmt.Errorf("lock legacy state owner: %w", err)
	}
	defer sourceLock.Close()
	targetLock, err := acquireHostOwnerLock(filepath.Join(config.InstanceDirectory, "host-owner.lock"))
	if err != nil {
		return fmt.Errorf("lock migration target: %w", err)
	}
	defer targetLock.Close()
	config.StateBytes = info.Size()
	config.LegacyStatePath = sourcePath
	manager, err := vmm.NewManager(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if _, err := manager.Start(ctx); err != nil {
		return fmt.Errorf("migrate state: %w", err)
	}
	stopContext, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	if _, err := manager.Stop(stopContext, false); err != nil {
		return fmt.Errorf("stop migration VM: %w", err)
	}
	fmt.Printf("state migration complete: source=%s target=%s bytes=%d\n", sourcePath, targetAbsolute, info.Size())
	return nil
}

func acquireHostOwnerLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Host owner lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errors.New("another Agent VMM Host owns the VM state disk and control-plane database")
		}
		return nil, fmt.Errorf("acquire Host owner lock: %w", err)
	}
	return file, nil
}

// HostVMRestartRecovery.tla owns this one-shot post-install transition. It is
// deliberately separate from installation retries and ordinary connector
// reconnect.
func resumeInstalledVM(ctx context.Context, stateDisk, requestID string, start func(context.Context, string) (*hostv1.OperationResponse, error)) error {
	initializingMarker := filepath.Join(filepath.Dir(stateDisk), "state.initializing")
	if marker, markerErr := os.Lstat(initializingMarker); markerErr == nil {
		if !marker.Mode().IsRegular() {
			return errors.New("installed VM initialization marker is invalid; run Agent VMM diagnostics")
		}
		return errors.New("installed VM state initialization is incomplete; rerun the bounded install operation")
	} else if !errors.Is(markerErr, os.ErrNotExist) {
		return fmt.Errorf("read installed VM initialization marker: %w", markerErr)
	}
	_, exists, err := existingStateDiskBytes(stateDisk)
	if !exists && err == nil {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = start(ctx, requestID)
	return err
}

func resolveStateDiskBytes(path string, newStateBytes int64) (int64, error) {
	bytes, exists, err := existingStateDiskBytes(path)
	if err != nil {
		return 0, err
	}
	if exists {
		return bytes, nil
	}
	if newStateBytes < minimumNewStateBytes {
		return 0, fmt.Errorf("new state disk must be at least %d bytes", minimumNewStateBytes)
	}
	return newStateBytes, nil
}

func existingStateDiskBytes(path string) (int64, bool, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ENOENT) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errors.New("installed VM state disk is unavailable; run Agent VMM diagnostics")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1<<30 {
		return 0, false, errors.New("installed VM state disk is invalid; run Agent VMM diagnostics")
	}
	return info.Size(), true, nil
}

func validateRuntimeUID(uid int) error {
	if uid == 0 {
		return errors.New("agent-vmm-host must run as a non-root user")
	}
	return nil
}

func disableCoreDumps() error {
	return syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{Cur: 0, Max: 0})
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

func shutdown(ctx context.Context, servers []*grpc.Server, controller *host.Controller) {
	<-ctx.Done()
	stopContext, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	if err := controller.Shutdown(stopContext); err != nil {
		log.Printf("VM shutdown did not reach a terminal state: %v", err)
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
