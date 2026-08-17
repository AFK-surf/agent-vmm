package mcpappliance

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"math/big"
	"sync"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Identity struct {
	mu             sync.RWMutex
	registrationID string
}

func (identity *Identity) set(registrationID string) {
	identity.mu.Lock()
	identity.registrationID = registrationID
	identity.mu.Unlock()
}

func (identity *Identity) RegistrationID() string {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return identity.registrationID
}

type BootstrapServer struct {
	appliancev1.UnimplementedApplianceBootstrapServiceServer
	identity *Identity
	store    stateStore
	now      func() time.Time
}

func NewBootstrapServer(identity *Identity, stateDirectory string) *BootstrapServer {
	return &BootstrapServer{identity: identity, store: stateStore{directory: stateDirectory}, now: time.Now}
}

func (server *BootstrapServer) Bootstrap(stream grpc.BidiStreamingServer[appliancev1.HostBootstrapFrame, appliancev1.ApplianceBootstrapFrame]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	claim := first.GetLaunchClaim()
	if err := verifyClaim(claim, server.now()); err != nil {
		return err
	}
	if err := server.store.initializeIdentity(durableIdentity{InstallationID: claim.GetInstallationId(), RegistrationID: claim.GetRegistrationId()}); err != nil {
		return err
	}
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	var receipt *appliancev1.BootstrapReceipt
	if envelope := second.GetEnvelope(); envelope != nil {
		if err := validateEnvelope(claim, envelope); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		if err := server.store.consumeBootstrap(claim.GetBootstrapId()); err != nil {
			clear(envelope.Ciphertext)
			return err
		}
		receipt = &appliancev1.BootstrapReceipt{InstallationId: claim.GetInstallationId(), BootstrapId: claim.GetBootstrapId(), Nonce: append([]byte(nil), envelope.GetNonce()...)}
		clear(envelope.Ciphertext)
		third, err := stream.Recv()
		if err != nil || !third.GetCloseWrite() {
			return errors.New("bootstrap stream was not closed after the seed")
		}
	} else if !second.GetCloseWrite() {
		return errors.New("bootstrap stream has an unexpected frame")
	} else if !server.store.hasSeed() {
		return errors.New("durable bootstrap state is unavailable")
	}
	server.identity.set(claim.GetRegistrationId())
	if receipt != nil {
		if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Receipt{Receipt: receipt}}); err != nil {
			return err
		}
	}
	readiness := make([]byte, 32)
	if _, err := rand.Read(readiness); err != nil {
		return err
	}
	health := &appliancev1.ApplianceHealth{InstallationId: claim.GetInstallationId(), RegistrationId: claim.GetRegistrationId(), BootId: claim.GetBootId(), ContainerInstance: claim.GetContainerInstance(), ConfigRevision: claim.GetConfigRevision(), Healthy: true, Status: "ready", ReadinessNonce: readiness}
	if err := stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_Health{Health: health}}); err != nil {
		return err
	}
	return stream.Send(&appliancev1.ApplianceBootstrapFrame{Payload: &appliancev1.ApplianceBootstrapFrame_CloseWrite{CloseWrite: true}})
}

func verifyClaim(claim *appliancev1.ApplianceLaunchClaim, now time.Time) error {
	if claim == nil || claim.GetInstallationId() == "" || claim.GetArtifactId() == "" || claim.GetRegistrationId() == "" || claim.GetBootId() == "" || claim.GetSystemNamespace() == "" || claim.GetContainerInstance() == "" || len(claim.GetBootNonce()) != 32 || claim.GetConfigRevision() == 0 || claim.GetApprovedGrant() == nil || claim.GetDeviceRootRevision() == 0 {
		return errors.New("launch claim is incomplete")
	}
	if claim.GetExpiresUnixMillis() <= now.UnixMilli() || claim.GetExpiresUnixMillis() > now.Add(5*time.Minute).UnixMilli() {
		return errors.New("launch claim lifetime is invalid")
	}
	x, y := elliptic.UnmarshalCompressed(elliptic.P256(), claim.GetDeviceRootPublicKey())
	if x == nil || y == nil || len(claim.GetSignature()) != 64 {
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
	if !ecdsa.Verify(&ecdsa.PublicKey{Curve: elliptic.P256(), X: x, Y: y}, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
		return errors.New("launch claim signature is invalid")
	}
	return nil
}

func validateEnvelope(claim *appliancev1.ApplianceLaunchClaim, envelope *appliancev1.OpaqueBootstrapEnvelope) error {
	if envelope.GetInstallationId() != claim.GetInstallationId() || envelope.GetBootstrapId() != claim.GetBootstrapId() || len(envelope.GetNonce()) != 32 || len(envelope.GetCiphertext()) == 0 || len(envelope.GetCiphertext()) > 1<<20 || envelope.GetExpiresUnixMillis() != claim.GetExpiresUnixMillis() {
		return errors.New("bootstrap envelope identity is invalid")
	}
	return nil
}
