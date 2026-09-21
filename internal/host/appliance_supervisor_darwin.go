//go:build darwin

package host

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) systemApplianceProjection(ctx context.Context) map[string]*vmmdv1.SystemAppliance {
	result := make(map[string]*vmmdv1.SystemAppliance)
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return result
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return result
	}
	defer connection.Close()
	projection, err := listSystemAppliances(ctx, client)
	if err != nil {
		return result
	}
	return projection
}

func listSystemAppliances(ctx context.Context, client vmmdv1.VMMDServiceClient) (map[string]*vmmdv1.SystemAppliance, error) {
	result := make(map[string]*vmmdv1.SystemAppliance)
	after := ""
	for {
		response, err := client.ListSystemAppliances(ctx, &vmmdv1.ListSystemAppliancesRequest{AfterInstallationId: after, Limit: 32})
		if err != nil {
			return nil, err
		}
		for _, appliance := range response.GetAppliances() {
			if installationID := appliance.GetIdentity().GetInstallationId(); installationID != "" {
				result[installationID] = appliance
			}
		}
		after = response.GetNextCursor()
		if after == "" {
			return result, nil
		}
	}
}

func getSystemAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installationID string) (*vmmdv1.SystemAppliance, error) {
	response, err := client.GetSystemAppliance(ctx, &vmmdv1.GetSystemApplianceRequest{InstallationId: installationID})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return response.GetAppliance(), nil
}

func (controller *Controller) systemAppliance(ctx context.Context, installationID string) *vmmdv1.SystemAppliance {
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return nil
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return nil
	}
	defer connection.Close()
	appliance, err := getSystemAppliance(ctx, client, installationID)
	if err != nil {
		return nil
	}
	return appliance
}

func (controller *Controller) activateAppliance(ctx context.Context, installationID string) error {
	return controller.vmLifecycle.ActivateAppliance(ctx, installationID)
}

func (controller *Controller) stopAppliance(ctx context.Context, installationID string) error {
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	controller.stopApplianceFrontends(installationID)
	controller.clearApplianceHealth(installationID)
	controller.stopPrivateControllerRoute(installationID)
	if installation.RegistrationID != "" {
		if err := controller.invalidateRemoteController(ctx, installation.RegistrationID); err != nil {
			return err
		}
	}
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return nil
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	appliance, err := getSystemAppliance(ctx, client, installationID)
	if err != nil {
		return err
	}
	if appliance == nil {
		return nil
	}
	_, err = client.StopSystemAppliance(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: derivedRequestID("stop-system-" + installationID + "-" + appliance.GetIdentity().GetContainerInstanceId()), Identity: appliance.GetIdentity(), TimeoutSeconds: 10})
	return err
}

func (controller *Controller) startAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation) error {
	unlock := controller.lockIntegration("appliance-lifecycle:" + installation.ID)
	defer unlock()
	return controller.startApplianceLocked(ctx, client, installation)
}

func (controller *Controller) startApplianceLocked(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation) error {
	current, err := controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		return err
	}
	// A reconciliation snapshot may have been read immediately before an
	// explicit lifecycle mutation. Do not consume the bootstrap for that stale
	// snapshot; the mutation that advanced the installation fence owns the next
	// launch.
	if current.Revision != installation.Revision || current.State != installation.State || current.DesiredEnabled != installation.DesiredEnabled {
		return nil
	}
	installation = current
	artifact, err := controller.store.GetApplianceArtifact(ctx, installation.ArtifactID)
	if err != nil {
		return controller.sealAppliance(ctx, installation, "ARTIFACT_UNAVAILABLE")
	}
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return controller.sealAppliance(ctx, installation, "APPROVED_GRANT_UNAVAILABLE")
	}
	volume, err := controller.store.GetApplianceVolume(ctx, installation.ID)
	if err != nil {
		return controller.sealAppliance(ctx, installation, "DATA_VOLUME_FACT_UNAVAILABLE")
	}
	if err := controller.importApplianceArchive(ctx, client, installation, artifact, grant); err != nil {
		return controller.failAppliance(ctx, installation, "ARTIFACT_IMAGE_IMPORT_FAILED", err)
	}
	identity, err := controller.startApplianceInstance(ctx, client, installation, artifact, grant, volume, installation.Revision+1)
	if err != nil {
		if strings.Contains(err.Error(), "appliance data volume is sealed") {
			return controller.sealAppliance(ctx, installation, "DATA_VOLUME_SEALED")
		}
		return controller.failAppliance(ctx, installation, bootstrapFailureReason(err), err)
	}
	previous := installation
	previousRevision := installation.Revision
	bootstrapID := installation.BootstrapID
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_STARTING)
	installation.CurrentBootID = identity.GetBootId()
	installation.CurrentContainerInstance = identity.GetContainerInstanceId()
	persisted, err := controller.store.UpdateApplianceInstallation(ctx, installation, previousRevision)
	if err != nil {
		return controller.failStartedAppliance(ctx, client, previous, identity, "INSTANCE_RECORD_FAILED", err)
	}
	installation = persisted
	configRevision := previousRevision + 1
	health, err := controller.bootstrapAppliance(ctx, client, installation, grant, identity, configRevision)
	if err != nil {
		if errors.Is(err, errBootstrapStoreIntegrity) || (installation.BootstrapID != "" && bootstrapFailureReason(err) == "BOOTSTRAP_STATE_UNAVAILABLE") {
			return controller.sealAppliance(ctx, installation, "BOOTSTRAP_SECRET_INTEGRITY_FAILED")
		}
		return controller.failStartedAppliance(ctx, client, installation, identity, bootstrapFailureReason(err), err)
	}
	if bootstrapID != "" {
		if err := controller.applianceSecrets.DeleteBootstrap(ctx, installation.ID, bootstrapID); err != nil {
			controller.setPendingApplianceHealth(health)
			controller.revokeFailedAppliancePublication(ctx, previous.ID)
			return fmt.Errorf("delete consumed bootstrap envelope %s/%s: %w", installation.ID, bootstrapID, err)
		}
	}
	installation, err = controller.store.GetApplianceInstallation(ctx, installation.ID)
	if err != nil {
		controller.revokeFailedAppliancePublication(ctx, previous.ID)
		return controller.failStartedAppliance(ctx, client, previous, identity, "INSTANCE_RECORD_FAILED", err)
	}
	if err := controller.publishApplianceInstance(ctx, installation, identity, health); err != nil {
		return controller.failStartedAppliance(ctx, client, previous, identity, "INSTANCE_PUBLICATION_FAILED", err)
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING)
	installation.BootstrapID = ""
	if _, err := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision); err != nil {
		controller.revokeFailedAppliancePublication(ctx, previous.ID)
		return controller.failStartedAppliance(ctx, client, previous, identity, "INSTANCE_RECORD_FAILED", err)
	}
	controller.clearPendingApplianceHealth(installation.ID)
	return nil
}

func (controller *Controller) startApplianceInstance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, artifact store.ApplianceArtifact, grant store.ApplianceGrant, volume store.ApplianceVolume, configRevision uint64) (*vmmdv1.SystemApplianceIdentity, error) {
	key, err := controller.applianceSecrets.EnsureVolumeKey(ctx, installation.ID)
	if err != nil {
		return nil, fmt.Errorf("load appliance volume key: %w", err)
	}
	defer clearSecret(key)
	bootNonce := make([]byte, 32)
	if _, err := rand.Read(bootNonce); err != nil {
		return nil, err
	}
	legacyDeviceIdentifier := ""
	switch volume.Layout {
	case store.ApplianceVolumeLayoutHostBlock:
		if !controller.manager.HasApplianceBlock(installation.ID) {
			return nil, errors.New("legacy appliance data image is not attached for conversion")
		}
		legacyDeviceIdentifier = applianceDeviceIdentifier(volume.VolumeID)
	case store.ApplianceVolumeLayoutGuestImage:
	default:
		return nil, errors.New("appliance data volume layout is invalid")
	}
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: installation.ID,
		BootId: controller.manager.Snapshot().GetBootId(), NamespaceId: systemNamespaceID(installation.ID),
		ContainerId: systemContainerID(), ImageManifestDigest: artifact.ImageDigest, BootNonce: bootNonce}
	response, err := client.EnsureSystemAppliance(ctx, &vmmdv1.EnsureSystemApplianceRequest{RequestId: derivedRequestID(fmt.Sprintf("system-%s-%s-%d", installation.ID, identity.GetBootId(), configRevision)), Identity: identity,
		ImageReference: artifact.ImageReference, LegacyDataDeviceIdentifier: legacyDeviceIdentifier, DataVolumeId: volume.VolumeID,
		DataVolumeIdentity: volume.Identity, DataVolumeKey: append([]byte(nil), key...),
		DataVolumeBytes: installation.DataVolumeBytes,
		Limits:          &vmmdv1.NamespaceLimits{Pids: uint32(grant.System.PIDs), DiskBytes: grant.System.DiskBytes},
		EgressMode:      vmmdv1.EgressMode(grant.EgressMode), ConfigRevision: configRevision})
	if err != nil {
		return nil, err
	}
	identity = response.GetAppliance().GetIdentity()
	if response.GetAppliance().GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED {
		return nil, errors.New("appliance data volume is sealed: " + response.GetAppliance().GetStatus())
	}
	if response.GetAppliance().GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || identity.GetContainerInstanceId() == "" {
		return nil, errors.New("guest did not start the exact system appliance")
	}
	if err := controller.finishApplianceBlockConversion(ctx, volume); err != nil {
		return nil, errors.Join(err, controller.stopStartedAppliance(ctx, client, identity))
	}
	return identity, nil
}

func (controller *Controller) publishApplianceInstance(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, health *appliancev1.ApplianceHealth) error {
	if !controller.privateControllerRouteCurrent(installation.ID, identity) {
		if err := controller.startPrivateControllerRoute(ctx, installation, identity); err != nil {
			return err
		}
	}
	return controller.admitApplianceInstance(ctx, installation, identity, health)
}

func (controller *Controller) admitApplianceInstance(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, health *appliancev1.ApplianceHealth) error {
	if err := controller.activatePrivateRegistration(ctx, installation); err != nil {
		return err
	}
	if err := controller.admitPrivateControllerRoute(installation.ID); err != nil {
		return err
	}
	if err := controller.startApplianceFrontends(ctx, installation, identity); err != nil {
		return err
	}
	controller.setApplianceHealth(health)
	return nil
}

func (controller *Controller) bootstrapAppliance(ctx context.Context, vmmd vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, grant store.ApplianceGrant, identity *vmmdv1.SystemApplianceIdentity, configRevision uint64) (*appliancev1.ApplianceHealth, error) {
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
			Nonce: streamNonce, MaxFrameBytes: 64 << 10, MaxTotalBytes: (1 << 20) + (64 << 10), IdleSeconds: 15, MaxConcurrentStreams: 1, MaxUseCount: 1})
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
			if err := controller.trust.InstallApplianceManagedTrust(ctx, store.ApplianceTrustAuthority{AuthorityID: bundle.GetTrustAnchor().GetAuthority().GetAuthorityId(),
				InstallationID: installation.ID, RegistrationID: installation.RegistrationID,
				BootID: identity.GetBootId(), ContainerInstance: identity.GetContainerInstanceId(), ConfigRevision: configRevision,
			}, bundle.GetTrustAnchor(), bundle.GetMembershipCredential()); err != nil {
				return nil, errors.Join(errors.New("appliance managed trust install failed"), err)
			}
		}
		if frame.GetCloseWrite() {
			break
		}
	}
	if !receiptValid || !healthValid {
		return nil, errors.New("appliance bootstrap receipt or health fence did not match")
	}
	return applianceHealth, nil
}

func (controller *Controller) failAppliance(ctx context.Context, installation store.ApplianceInstallation, reason string, cause error) error {
	controller.clearApplianceHealth(installation.ID)
	controller.setApplianceFailure(installation, reason)
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED) {
		return cause
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED)
	_, updateErr := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	return errors.Join(cause, updateErr)
}

func (controller *Controller) sealAppliance(ctx context.Context, installation store.ApplianceInstallation, reason string) error {
	controller.clearApplianceHealth(installation.ID)
	controller.setApplianceFailure(installation, "SEALED_"+reason)
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) && !installation.DesiredEnabled {
		return errors.New("appliance sealed: " + reason)
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
	installation.DesiredEnabled = false
	_, updateErr := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	return errors.Join(errors.New("appliance sealed: "+reason), updateErr)
}

func (controller *Controller) failStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, reason string, cause error) error {
	controller.stopApplianceFrontends(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	stopErr := controller.stopStartedAppliance(ctx, client, identity)
	if current, err := controller.store.GetApplianceInstallation(ctx, installation.ID); err == nil {
		installation = current
	}
	return controller.failAppliance(ctx, installation, reason, errors.Join(cause, stopErr))
}

func (controller *Controller) sealStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, reason string) error {
	controller.stopApplianceFrontends(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	stopErr := controller.stopStartedAppliance(ctx, client, identity)
	return errors.Join(controller.sealAppliance(ctx, installation, reason), stopErr)
}

func (controller *Controller) stopStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, identity *vmmdv1.SystemApplianceIdentity) error {
	if identity == nil || identity.GetContainerInstanceId() == "" {
		return nil
	}
	controller.clearApplianceHealth(identity.GetInstallationId())
	_, err := client.StopSystemAppliance(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: derivedRequestID("failed-system-" + identity.GetInstallationId() + "-" + identity.GetContainerInstanceId()), Identity: identity, TimeoutSeconds: 10})
	return err
}
