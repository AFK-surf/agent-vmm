//go:build darwin || linux

package host

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) bootstrapAppliance(ctx context.Context, vmmd vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, grant store.ApplianceGrant, identity *vmmdv1.SystemApplianceIdentity, configRevision uint64) (result *appliancev1.ApplianceHealth, resultErr error) {
	started := time.Now()
	log.Printf("appliance bootstrap begin: installation=%s boot=%s instance=%s", installation.ID, identity.GetBootId(), identity.GetContainerInstanceId())
	defer func() {
		log.Printf("appliance bootstrap end: installation=%s elapsed=%s succeeded=%t", installation.ID, time.Since(started).Round(time.Millisecond), resultErr == nil)
	}()
	var seed []byte
	if installation.BootstrapID != "" {
		loaded, err := controller.applianceSecrets.LoadBootstrap(ctx, installation.ID, installation.BootstrapID)
		if err == nil {
			seed = loaded
		} else if errors.Is(err, errBootstrapNotFound) {
			// A host crash may follow successful secret deletion but precede the
			// installation CAS that clears BootstrapID. Retry as an ordinary
			// restart; the guest's durable receipt decides whether that is valid.
			installation.BootstrapID = ""
		} else {
			return nil, fmt.Errorf("%w: %v", errBootstrapStoreIntegrity, err)
		}
	}
	defer clearSecret(seed)
	claim := &appliancev1.ApplianceLaunchClaim{InstallationId: installation.ID, ArtifactId: installation.ArtifactID, BootId: identity.GetBootId(),
		SystemNamespace: identity.GetNamespaceId(), ContainerInstance: identity.GetContainerInstanceId(),
		BootNonce: append([]byte(nil), identity.GetBootNonce()...), BootstrapId: installation.BootstrapID, ConfigRevision: configRevision,
		ApprovedGrant: grantToProto(grant), ExpiresUnixMillis: time.Now().Add(4 * time.Minute).UnixMilli(), RegistrationId: installation.RegistrationID}
	artifact, err := controller.store.GetApplianceArtifact(ctx, installation.ArtifactID)
	if err != nil {
		return nil, err
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(artifact.Manifest, manifest); err != nil {
		return nil, err
	}
	for _, content := range manifest.GetContentRefs() {
		if content.GetRole() == appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_CONTROLLER {
			continue
		}
		claim.ContentMounts = append(claim.ContentMounts, &appliancev1.ApplianceContentMount{Content: content, ArchivePath: filepath.Join(appliancecontent.MountRoot, appliancecontent.ArchiveName(content))})
	}
	if controller.trustKeyProvider == nil {
		return nil, errors.New("device root signer is unavailable")
	}
	device, err := controller.trust.Identity(ctx)
	if err != nil {
		return nil, err
	}
	claim.DeviceIdentity = proto.Clone(device.Identity).(*trustv1.DeviceIdentity)
	claim.DeviceRootPublicKey = append([]byte(nil), device.Identity.GetRootPublicKey()...)
	claim.DeviceRootRevision = device.Identity.GetRootKeyRevision()
	unsigned, err := proto.MarshalOptions{Deterministic: true}.Marshal(claim)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(unsigned)
	claim.Signature, err = controller.trustKeyProvider.Sign(ctx, device.Identity.GetRootKeyRevision(), digest[:])
	if err != nil {
		return nil, err
	}
	streamNonce := make([]byte, 32)
	if _, err := rand.Read(streamNonce); err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///system-appliance", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return openSystemStreamConn(streamCtx, vmmd, &vmmdv1.SystemStreamHeader{Identity: identity, Kind: vmmdv1.SystemStreamKind_SYSTEM_STREAM_KIND_BOOTSTRAP,
			Nonce: streamNonce, MaxFrameBytes: 64 << 10, MaxTotalBytes: (1 << 20) + (64 << 10), IdleSeconds: 15, MaxConcurrentStreams: 1})
	}))
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	stream, err := appliancev1.NewApplianceBootstrapServiceClient(connection).Bootstrap(streamCtx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&appliancev1.HostBootstrapFrame{Payload: &appliancev1.HostBootstrapFrame_LaunchClaim{LaunchClaim: claim}}); err != nil {
		return nil, err
	}
	envelopeNonce := make([]byte, 32)
	if len(seed) != 0 {
		if _, err := rand.Read(envelopeNonce); err != nil {
			return nil, err
		}
		envelope := &appliancev1.OpaqueBootstrapEnvelope{InstallationId: installation.ID, BootstrapId: installation.BootstrapID,
			Nonce: envelopeNonce, Ciphertext: append([]byte(nil), seed...), ExpiresUnixMillis: claim.GetExpiresUnixMillis()}
		if err := stream.Send(&appliancev1.HostBootstrapFrame{Payload: &appliancev1.HostBootstrapFrame_Envelope{Envelope: envelope}}); err != nil {
			return nil, err
		}
	}
	if err := stream.Send(&appliancev1.HostBootstrapFrame{Payload: &appliancev1.HostBootstrapFrame_CloseWrite{CloseWrite: true}}); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	receiptValid, healthValid, trustSeen := len(seed) == 0, false, false
	var applianceHealth *appliancev1.ApplianceHealth
	var managedTrust *appliancev1.ApplianceManagedTrustBundle
	for {
		frame, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			return nil, receiveErr
		}
		if receipt := frame.GetReceipt(); receipt != nil {
			receiptValid = validateBootstrapReceipt(receipt, installation.ID, installation.BootstrapID, envelopeNonce)
		}
		if health := frame.GetHealth(); health != nil {
			healthValid = health.GetInstallationId() == installation.ID && health.GetRegistrationId() == installation.RegistrationID &&
				health.GetBootId() == identity.GetBootId() && health.GetContainerInstance() == identity.GetContainerInstanceId() &&
				health.GetConfigRevision() == configRevision && health.GetHealthy() && len(health.GetStatus()) <= 1024 && len(health.GetReadinessNonce()) == 32
			if healthValid {
				applianceHealth = proto.Clone(health).(*appliancev1.ApplianceHealth)
			}
		}
		if bundle := frame.GetManagedTrust(); bundle != nil {
			if trustSeen || bundle.GetInstallationId() != installation.ID || bundle.GetRegistrationId() != installation.RegistrationID ||
				bundle.GetConfigRevision() != configRevision || bundle.GetBootId() != identity.GetBootId() ||
				bundle.GetContainerInstance() != identity.GetContainerInstanceId() ||
				subtle.ConstantTimeCompare(bundle.GetBootNonce(), identity.GetBootNonce()) != 1 {
				return nil, errors.New("appliance managed trust fence did not match")
			}
			trustSeen = true
			managedTrust = proto.Clone(bundle).(*appliancev1.ApplianceManagedTrustBundle)
		}
		if frame.GetCloseWrite() {
			break
		}
	}
	if !receiptValid || !healthValid {
		return nil, errors.New("appliance bootstrap receipt or health fence did not match")
	}
	// Managed trust exists only for appliances that issue managed route
	// capabilities. Without a bundle the installation has no anchor, so its
	// managed route authority stays closed.
	if !trustSeen {
		return applianceHealth, nil
	}
	if err := controller.trust.InstallApplianceManagedTrust(ctx, store.ApplianceTrustAuthority{
		AuthorityID: managedTrust.GetTrustAnchor().GetAuthority().GetAuthorityId(), InstallationID: installation.ID,
		RegistrationID: installation.RegistrationID, BootID: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(), ConfigRevision: configRevision,
	}, managedTrust.GetTrustAnchor(), managedTrust.GetMembershipCredential()); err != nil {
		return nil, errors.Join(errors.New("appliance managed trust install failed"), err)
	}
	return applianceHealth, nil
}
