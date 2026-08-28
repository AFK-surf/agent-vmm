package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	controllerremote "github.com/AFK-surf/agent-vmm/controller/remote"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	runtimeDirectory = "/run/agent-vmm"
)

var stateDirectory = "/var/lib/agent-vmm"

type bootstrapServer struct {
	appliancev1.UnimplementedApplianceBootstrapServiceServer
	controller *referenceControllerIdentity
}

func (server bootstrapServer) Bootstrap(stream grpc.BidiStreamingServer[appliancev1.HostBootstrapFrame, appliancev1.ApplianceBootstrapFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	claim := first.GetLaunchClaim()
	if err := verifyClaim(claim); err != nil {
		return err
	}
	server.controller.set(claim)
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	var receipt *appliancev1.BootstrapReceipt
	if envelope := second.GetEnvelope(); envelope != nil {
		if err := consumeEnvelope(claim, envelope); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		if err := attestSecretSurfaces(envelope.Ciphertext); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		receipt = &appliancev1.BootstrapReceipt{InstallationId: claim.GetInstallationId(), BootstrapId: envelope.GetBootstrapId(),
			Nonce: append([]byte(nil), envelope.GetNonce()...)}
		clear(envelope.Ciphertext)
		third, err := stream.Recv()
		if err != nil || !third.GetCloseWrite() {
			return errors.New("bootstrap stream was not closed after the envelope")
		}
	} else if !second.GetCloseWrite() {
		return errors.New("bootstrap stream has an unexpected frame")
	} else if _, err := os.Stat(filepath.Join(stateDirectory, "bootstrap.id")); err != nil {
		return errors.New("durable appliance state is unavailable")
	}
	bundle, err := loadOrCreateManagedTrust(claim)
	if err != nil {
		return err
	}
	if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_ManagedTrust{ManagedTrust: bundle}}); err != nil {
		return err
	}
	if receipt != nil {
		if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Receipt{Receipt: receipt}}); err != nil {
			return err
		}
	}
	readiness := make([]byte, 32)
	if _, err := rand.Read(readiness); err != nil {
		return err
	}
	status := "ready"
	if receipt != nil {
		status = "ready;bootstrap-secret-surfaces-clean"
	}
	health := &appliancev1.ApplianceHealth{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), ConfigRevision: claim.GetConfigRevision(), Healthy: true, Status: status, ReadinessNonce: readiness}
	if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Health{Health: health}}); err != nil {
		return err
	}
	return stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_CloseWrite{CloseWrite: true}})
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

func (controller referenceController) Connect(stream grpc.BidiStreamingServer[remotev1.HostControlMessage, remotev1.ControllerControlMessage]) error {
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
	leases, err := reconcileReferenceAllocations(stream.Context(), cursor)
	if err != nil {
		return err
	}
	if err := controller.runReferenceContract(stream.Context(), cursor, leases); err != nil {
		return err
	}
	return maintainReferenceLeases(stream.Context(), cursor, leases)
}

func (controller referenceController) ServeSession(stream grpc.BidiStreamingServer[remotev1.HostSessionFrame, remotev1.ControllerSessionFrame]) error {
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

func verifyClaim(claim *appliancev1.ApplianceLaunchClaim) error {
	device := claim.GetDeviceIdentity()
	if claim == nil || claim.GetInstallationId() == "" || claim.GetArtifactId() == "" || claim.GetRegistrationId() == "" || claim.GetBootId() == "" || claim.GetSystemNamespace() == "" || claim.GetContainerInstance() == "" || len(claim.GetBootNonce()) != 32 || claim.GetConfigRevision() == 0 || claim.GetApprovedGrant() == nil || claim.GetDeviceRootRevision() == 0 ||
		device.GetDeviceId() == "" || device.GetRootKeyRevision() != claim.GetDeviceRootRevision() || !bytes.Equal(device.GetRootPublicKey(), claim.GetDeviceRootPublicKey()) || device.GetSignatureSuite() != trustv1.SignatureSuite_SIGNATURE_SUITE_P256_SHA256 {
		return errors.New("launch claim is incomplete")
	}
	now := time.Now()
	if claim.GetExpiresUnixMillis() <= now.UnixMilli() {
		return errors.New("launch claim is expired")
	}
	if claim.GetExpiresUnixMillis() > now.Add(5*time.Minute).UnixMilli() {
		return errors.New("launch claim lifetime exceeds the fixed bound")
	}
	publicKey, err := publicKeyFromCompressed(claim.GetDeviceRootPublicKey())
	if err != nil || len(claim.GetSignature()) != 64 {
		return errors.New("launch claim signer is invalid")
	}
	signature := append([]byte(nil), claim.Signature...)
	claim.Signature = nil
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(claim)
	claim.Signature = signature
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	if !ecdsa.Verify(publicKey, digest[:], newBigInt(signature[:32]), newBigInt(signature[32:])) {
		return errors.New("launch claim signature is invalid")
	}
	return nil
}

func consumeEnvelope(claim *appliancev1.ApplianceLaunchClaim, envelope *appliancev1.OpaqueBootstrapEnvelope) error {
	if envelope.GetInstallationId() != claim.GetInstallationId() || envelope.GetBootstrapId() == "" || envelope.GetBootstrapId() != claim.GetBootstrapId() || len(envelope.GetNonce()) != 32 || len(envelope.GetCiphertext()) == 0 || len(envelope.GetCiphertext()) > 1<<20 || envelope.GetExpiresUnixMillis() != claim.GetExpiresUnixMillis() {
		return errors.New("bootstrap envelope identity is invalid")
	}
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(stateDirectory, "bootstrap.id"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("bootstrap envelope was already consumed")
	}
	if _, err := file.WriteString(envelope.GetBootstrapId()); err != nil {
		file.Close()
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func publicKeyFromCompressed(value []byte) (*ecdsa.PublicKey, error) {
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), value)
	if x == nil || y == nil {
		return nil, errors.New("invalid compressed P-256 public key")
	}
	return &ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, nil
}

func newBigInt(value []byte) *big.Int { return new(big.Int).SetBytes(value) }

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
