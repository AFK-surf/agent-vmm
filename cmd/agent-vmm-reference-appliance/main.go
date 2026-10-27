// Command agent-vmm-reference-appliance exercises controller and bootstrap contracts in E2E tests.
// It runs inside a test appliance and has no production controller authority.
package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/appliancebootstrap"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"google.golang.org/grpc"
)

const (
	runtimeDirectory = "/run/agent-vmm"
)

var stateDirectory = "/var/lib/agent-vmm"

type bootstrapServer struct {
	appliancev1.UnimplementedApplianceBootstrapServiceServer
	controller *referenceControllerIdentity
}

func (server bootstrapServer) Bootstrap(stream appliancebootstrap.Stream) error {
	store := appliancebootstrap.SeedStore{Directory: stateDirectory}
	return appliancebootstrap.Serve(stream, appliancebootstrap.Hooks{
		OnClaim: func(claim *appliancev1.ApplianceLaunchClaim) error {
			if err := store.Initialize(claim); err != nil {
				return err
			}
			server.controller.set(claim)
			return nil
		},
		OnEnvelope: func(claim *appliancev1.ApplianceLaunchClaim, envelope *appliancev1.OpaqueBootstrapEnvelope) error {
			if err := store.Accept(claim, envelope); err != nil {
				return err
			}
			if envelope != nil {
				return attestSecretSurfaces(envelope.Ciphertext)
			}
			return nil
		},
		BeforeReceipt: func(stream appliancebootstrap.Stream, claim *appliancev1.ApplianceLaunchClaim, _ *appliancev1.BootstrapReceipt) error {
			bundle, err := loadOrCreateManagedTrust(claim)
			if err != nil {
				return err
			}
			return stream.Send(&appliancev1.BootstrapResponse{Payload: &appliancev1.BootstrapResponse_ManagedTrust{ManagedTrust: bundle}})
		},
	})
}

func attestSecretSurfaces(secret []byte) error {
	if len(secret) == 0 {
		return errors.New("bootstrap secret is empty")
	}
	if err := scanSecretLocations(secret,
		[]string{"/proc/self/cmdline", "/proc/self/environ", "/proc/1/cmdline"},
		[]string{runtimeDirectory, stateDirectory}); err != nil {
		return err
	}
	return requireCoreDumpsDisabled([]string{"/proc/self/limits", "/proc/1/limits"})
}

func scanSecretLocations(secret []byte, processFiles, roots []string) error {
	for _, path := range processFiles {
		value, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(value, secret) {
			return errors.New("bootstrap secret leaked into process metadata")
		}
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, os.ErrPermission) {
					if entry != nil && entry.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
				return walkErr
			}
			if !entry.Type().IsRegular() {
				return nil
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Size() > 4<<20 {
				return errors.New("secret scan file exceeds fixed bound")
			}
			value, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(value, secret) {
				return errors.New("bootstrap secret leaked into appliance filesystem")
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func requireCoreDumpsDisabled(paths []string) error {
	for _, path := range paths {
		value, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		found := false
		for _, line := range strings.Split(string(value), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 6 && strings.Join(fields[:4], " ") == "Max core file size" {
				found = fields[4] == "0" && fields[5] == "0"
				break
			}
		}
		if !found {
			return errors.New("core dumps are not disabled")
		}
	}
	return nil
}

type referenceControllerIdentity struct {
	mu             sync.RWMutex
	registrationID string
	configRevision uint64
}

func (identity *referenceControllerIdentity) set(claim *appliancev1.ApplianceLaunchClaim) {
	identity.mu.Lock()
	identity.registrationID = claim.GetRegistrationId()
	identity.configRevision = claim.GetConfigRevision()
	identity.mu.Unlock()
}

func (identity *referenceControllerIdentity) get() string {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return identity.registrationID
}

func (identity *referenceControllerIdentity) routeGeneration() uint64 {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return identity.configRevision
}

type referenceController struct {
	remotev1.UnimplementedRemoteControllerServiceServer
	identity *referenceControllerIdentity
	sessions *referenceSessionBroker
}

func (controller referenceController) Connect(stream grpc.BidiStreamingServer[remotev1.ConnectRequest, remotev1.ConnectResponse]) error {
	registrationID := controller.identity.get()
	if registrationID == "" {
		return errors.New("controller identity is not initialized")
	}
	session, err := controllerremote.AcceptControl(stream, registrationID)
	if err != nil {
		return err
	}
	controller.sessions.setEpoch(session.Hello().GetConnectionEpoch())
	if err := session.Ready(); err != nil {
		return err
	}
	cursor := newReferenceCommandCursor(session)
	allocations, err := reconcileReferenceAllocations(stream.Context(), cursor)
	if err != nil {
		return err
	}
	if err := controller.runReferenceContract(stream.Context(), cursor, allocations); err != nil {
		return err
	}
	return maintainReferenceAllocations(stream.Context())
}

func (controller referenceController) ServeSession(stream grpc.BidiStreamingServer[remotev1.ServeSessionRequest, remotev1.ServeSessionResponse]) error {
	header, connection, err := controllerremote.AcceptSession(stream, controller.identity.get(), controller.sessions.epochValue())
	if err != nil {
		return err
	}
	if err := controller.sessions.deliver(stream.Context(), header.GetAllocationId(), connection); err != nil {
		_ = connection.Close()
		return err
	}
	select {
	case <-connection.Done():
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func main() {
	if len(os.Args) == 2 {
		if err := runReferenceProcess(os.Args[1]); err != nil {
			panic(err)
		}
		return
	}
	if err := os.MkdirAll(runtimeDirectory, 0o700); err != nil {
		panic(err)
	}
	bootstrapPath := filepath.Join(runtimeDirectory, "bootstrap.sock")
	controllerPath := filepath.Join(runtimeDirectory, "controller.sock")
	_ = os.Remove(bootstrapPath)
	_ = os.Remove(controllerPath)
	bootstrapListener, err := net.Listen("unix", bootstrapPath)
	if err != nil {
		panic(err)
	}
	defer bootstrapListener.Close()
	controllerListener, err := net.Listen("unix", controllerPath)
	if err != nil {
		panic(err)
	}
	defer controllerListener.Close()
	identity := &referenceControllerIdentity{}
	sessions := newReferenceSessionBroker()
	bootstrap := bootstrapServer{controller: identity}
	bootstrapGRPC := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(2<<20), grpc.MaxConcurrentStreams(1))
	appliancev1.RegisterApplianceBootstrapServiceServer(bootstrapGRPC, bootstrap)
	controllerGRPC := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(2<<20), grpc.MaxConcurrentStreams(4))
	remotev1.RegisterRemoteControllerServiceServer(controllerGRPC, referenceController{identity: identity, sessions: sessions})
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go func() {
		<-ctx.Done()
		bootstrapGRPC.GracefulStop()
		controllerGRPC.GracefulStop()
	}()
	serveErrors := make(chan error, 2)
	go func() { serveErrors <- bootstrapGRPC.Serve(bootstrapListener) }()
	go func() { serveErrors <- controllerGRPC.Serve(controllerListener) }()
	if err := <-serveErrors; err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, io.EOF) {
		panic(err)
	}
}
