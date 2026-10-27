//go:build darwin || linux

package hostapp

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/dataworker"
	"github.com/AFK-surf/agent-vmm/internal/host"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"github.com/AFK-surf/agent-vmm/internal/hostdisk"
	"github.com/AFK-surf/agent-vmm/internal/privatefs"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

const (
	installedVMResumeTimeout = 10 * time.Minute
	defaultNewStateBytes     = hostdisk.DefaultStateBytes
	minimumNewStateBytes     = int64(16 << 30)
)

var ErrSchemaRejected = errors.New("host database schema rejected")

func Run(ctx context.Context, manifestDigest string) error {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := configureArchiveTransport(); err != nil {
		return err
	}
	if err := validateRuntimeUID(os.Geteuid()); err != nil {
		return err
	}
	if err := disableCoreDumps(); err != nil {
		return err
	}
	var config vmm.Config
	var newStateBytes int64
	var operatorSocket, remoteComputeSocket, trustSocket, serviceTunnelSocket, meshControlSocket, dataWorkerSocket, dataWorkerPath, databasePath, sessionDirectory string
	var disablePersonalMeshPairing bool
	flag.StringVar(&config.BundleDirectory, "bundle", "", "verified guest bundle directory")
	flag.StringVar(&config.InstanceDirectory, "instance", "", "VM instance directory")
	flag.UintVar(&config.CPUCount, "cpus", 0, "VM CPU count; 0 derives it from the host")
	flag.Uint64Var(&config.MemoryBytes, "memory", 0, "VM memory bytes; 0 derives it from the host")
	flag.Int64Var(&newStateBytes, "state-size", defaultNewStateBytes, "fully physically backed state disk bytes for a new installation")
	flag.StringVar(&config.SerialLog, "serial-log", "", "serial log path")
	flag.StringVar(&operatorSocket, "operator-socket", "", "operator Unix socket")
	flag.StringVar(&remoteComputeSocket, "remote-compute-socket", "", "restricted remote connector Unix socket")
	flag.StringVar(&trustSocket, "trust-socket", "", "connector-only trust Unix socket")
	flag.StringVar(&serviceTunnelSocket, "service-tunnel-socket", "", "data-worker-only destination service Unix socket")
	flag.StringVar(&meshControlSocket, "mesh-control-socket", "", "data-worker-only personal mesh control Unix socket")
	flag.StringVar(&dataWorkerSocket, "data-worker-socket", "", "data worker control Unix socket")
	flag.StringVar(&dataWorkerPath, "data-worker", "", "host-supervised Agent VMM data worker")
	flag.StringVar(&databasePath, "database", "", "host control-plane SQLite database")
	flag.StringVar(&sessionDirectory, "session-directory", "", "agent session socket directory")
	flag.BoolVar(&disablePersonalMeshPairing, "disable-personal-mesh-pairing", false, "deny personal mesh genesis and new member admission while preserving revoke/repair")
	flag.Parse()
	hostCPUs, hostMemoryBytes, err := hostResources()
	if err != nil {
		return err
	}
	config.CPUCount, config.MemoryBytes = vmResources(config.CPUCount, config.MemoryBytes, hostCPUs, hostMemoryBytes)
	config.GuestManifestDigest = manifestDigest
	if operatorSocket == "" || remoteComputeSocket == "" || databasePath == "" || sessionDirectory == "" {
		return errors.New("operator-socket, remote-compute-socket, database, and session-directory are required")
	}
	if trustSocket == "" {
		trustSocket = filepath.Join(filepath.Dir(operatorSocket), "trust.sock")
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
	serviceDirectory := filepath.Join(filepath.Dir(operatorSocket), "services")
	for _, directory := range []string{serviceDirectory, config.InstanceDirectory, filepath.Dir(operatorSocket), filepath.Dir(remoteComputeSocket), filepath.Dir(trustSocket), filepath.Dir(serviceTunnelSocket), filepath.Dir(meshControlSocket), filepath.Dir(databasePath), filepath.Dir(config.SerialLog), sessionDirectory} {
		if err := privatefs.EnsureDirectory(directory); err != nil {
			return err
		}
	}
	ownerLock, err := acquireHostOwnerLock(filepath.Join(config.InstanceDirectory, "host-owner.lock"))
	if err != nil {
		return err
	}
	defer ownerLock.Close()
	stateDisk := filepath.Join(config.InstanceDirectory, "state.raw")
	config.StateBytes, err = resolveStateDiskBytes(stateDisk, newStateBytes)
	if err != nil {
		return err
	}
	manager, err := vmm.NewManager(config)
	if err != nil {
		return err
	}
	stores, err := newSecretStores(ctx, config.InstanceDirectory, databasePath)
	var controller *host.Controller
	if err == nil {
		controller, err = host.New(host.Config{
			ServiceDirectory: serviceDirectory, Manager: manager, DatabasePath: databasePath, SessionDirectory: sessionDirectory,
			InstanceID: filepath.Base(config.InstanceDirectory), BundleDirectory: config.BundleDirectory,
			InstanceDirectory: config.InstanceDirectory, StateDiskIdentity: stateDisk,
			CPUCount: config.CPUCount, MemoryBytes: config.MemoryBytes, StateBytes: config.StateBytes,
			TrustKeyProvider: stores.roots, Secrets: stores.appliance, Credentials: stores.credentials,
			DisablePersonalMeshPairing: disablePersonalMeshPairing,
			DataWorkerSocket:           dataWorkerSocket,
			RemoteComputeSocket:        remoteComputeSocket,
			TrustSocket:                trustSocket,
			ServiceTunnelSocket:        serviceTunnelSocket,
			MeshControlSocket:          meshControlSocket,
		})
	}
	if refused, recordErr := recordStartResult(filepath.Dir(databasePath), err); refused {
		return errors.Join(ErrSchemaRejected, err, recordErr)
	} else if recordErr != nil {
		return recordErr
	}
	if err != nil {
		return err
	}
	defer controller.Close()
	if err := controller.Start(ctx); err != nil {
		return err
	}
	listener, err := privatefs.ListenUnix(operatorSocket)
	if err != nil {
		return err
	}
	defer listener.Close()
	remoteListener, err := privatefs.ListenUnix(remoteComputeSocket)
	if err != nil {
		return err
	}
	defer remoteListener.Close()
	trustListener, err := privatefs.ListenUnix(trustSocket)
	if err != nil {
		return err
	}
	defer trustListener.Close()
	serviceBaseListener, err := privatefs.ListenUnix(serviceTunnelSocket)
	if err != nil {
		return err
	}
	defer serviceBaseListener.Close()
	meshBaseListener, err := privatefs.ListenUnix(meshControlSocket)
	if err != nil {
		return err
	}
	defer meshBaseListener.Close()
	if err := os.Remove(filepath.Join(filepath.Dir(operatorSocket), "connector-trust.cap")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	server := controller.NewOperatorServer()
	remoteServer := controller.NewRemoteComputeServer()
	trustServer, err := controller.NewTrustServer()
	if err != nil {
		return err
	}
	serviceServer, err := controller.NewServiceTunnelServer()
	if err != nil {
		return err
	}
	meshServer, err := controller.NewMeshControlServer()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker, err := dataworker.Start(ctx, dataWorkerPath, dataWorkerSocket, serviceTunnelSocket, meshControlSocket)
	if err != nil {
		return err
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := worker.Close(closeContext); err != nil {
			slog.Warn("stop data worker", "error", err)
		}
	}()
	if err := controller.StartExternalConnectors(ctx); err != nil {
		return err
	}
	// Keep the operator endpoint observable while a slow Guest resumes. The
	// lifecycle helper must be able to distinguish STARTING from an unreadable
	// Host instead of timing out and creating a competing update owner.
	go func() {
		resumeContext, resumeCancel := context.WithTimeout(ctx, installedVMResumeTimeout)
		defer resumeCancel()
		resumeRequestID := fmt.Sprintf("host-start-resume-%d", time.Now().UnixNano())
		if err := resumeInstalledVM(resumeContext, stateDisk, resumeRequestID, controller.StartVM); err != nil {
			slog.Warn("installed VM was not resumed", "error", err)
		}
	}()
	return serveServers(ctx, []serverListener{
		{server, listener}, {remoteServer, remoteListener}, {trustServer, trustListener},
		{serviceServer, serviceBaseListener}, {meshServer, meshBaseListener},
	}, controller.Shutdown)
}

// recordStartResult writes the start failure record when the store refuses the
// database and removes it after a successful start. It reports a refusal.
func recordStartResult(directory string, startErr error) (bool, error) {
	path := filepath.Join(directory, store.StartFailureFile)
	var schemaErr *store.SchemaError
	if errors.As(startErr, &schemaErr) {
		data, err := json.Marshal(store.StartFailure{Code: schemaErr.Code, DataSchema: schemaErr.DataSchema, CurrentSchema: store.CurrentSchema()})
		if err != nil {
			return true, err
		}
		if err := os.WriteFile(path+".tmp", data, 0o600); err != nil {
			return true, err
		}
		return true, os.Rename(path+".tmp", path)
	}
	if startErr == nil {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
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
func resumeInstalledVM(ctx context.Context, stateDisk, requestID string, start func(context.Context, string) (*hostv1.StartVMResponse, error)) error {
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

type serverListener struct {
	server   *grpc.Server
	listener net.Listener
}

func serveServers(ctx context.Context, endpoints []serverListener, shutdown func(context.Context) error) error {
	group, serving := errgroup.WithContext(ctx)
	for _, endpoint := range endpoints {
		group.Go(func() error {
			err := endpoint.server.Serve(endpoint.listener)
			if errors.Is(err, grpc.ErrServerStopped) {
				return nil
			}
			return err
		})
	}
	group.Go(func() error {
		<-serving.Done()
		stopContext, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		shutdownErr := shutdown(stopContext)
		for _, endpoint := range endpoints {
			done := make(chan struct{})
			go func() { endpoint.server.GracefulStop(); close(done) }()
			select {
			case <-done:
			case <-stopContext.Done():
				endpoint.server.Stop()
			}
		}
		return shutdownErr
	})
	return group.Wait()
}
