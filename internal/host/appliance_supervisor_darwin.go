//go:build darwin

package host

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var errSystemApplianceSealed = errors.New("system appliance integrity is sealed")

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
	if response.GetAppliance() == nil {
		return nil, status.Error(codes.Internal, "Guest omitted the appliance observation")
	}
	return response.GetAppliance(), nil
}

func (controller *Controller) activateAppliance(ctx context.Context, installationID string) error {
	return controller.vmLifecycle.ActivateAppliance(ctx, installationID)
}

func (controller *Controller) stopAppliance(ctx context.Context, installationID string) error {
	installation, err := controller.store.GetApplianceInstallation(ctx, installationID)
	if err != nil {
		return err
	}
	controller.egress.Policies.RevokeEnvironment("appliance:" + installationID)
	serviceErr := controller.withdrawApplianceServiceImports(ctx, installationID)
	controller.stopApplianceFrontends(installationID)
	if installation.State != int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		controller.clearApplianceHealth(installationID)
	}
	controller.stopPrivateControllerRoute(installationID)
	if installation.RegistrationID != "" {
		if err := controller.invalidateRemoteController(ctx, installation.RegistrationID); err != nil {
			return errors.Join(serviceErr, err)
		}
	}
	if controller.manager.Snapshot().GetState() != hostv1.VMState_VM_STATE_RUNNING {
		return serviceErr
	}
	client, connection, err := controller.guestClient(ctx)
	if err != nil {
		return errors.Join(serviceErr, err)
	}
	defer connection.Close()
	appliance, err := getSystemAppliance(ctx, client, installationID)
	if err != nil {
		return errors.Join(serviceErr, err)
	}
	if appliance == nil {
		return serviceErr
	}
	_, err = client.StopSystemAppliance(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: derivedRequestID("stop-system-" + installationID + "-" + appliance.GetIdentity().GetContainerInstanceId()), Identity: appliance.GetIdentity(), TimeoutSeconds: 10})
	return errors.Join(serviceErr, err)
}

func (controller *Controller) startAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation) error {
	lock := controller.integrationLock("appliance-lifecycle:" + installation.ID)
	for !lock.TryLock() {
		if err := waitForControllerLock(ctx); err != nil {
			return err
		}
	}
	defer lock.Unlock()
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
	if !current.DesiredEnabled || current.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) {
		return resourceError(codes.FailedPrecondition, "APPLIANCE_START_FORBIDDEN", "appliance_installation",
			current.ID, "appliance is disabled or sealed")
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
	if err := controller.importApplianceArchive(ctx, client, installation, artifact, grant); err != nil {
		return controller.failAppliance(ctx, installation, "ARTIFACT_IMAGE_IMPORT_FAILED", err)
	}
	identity, err := controller.startApplianceInstance(ctx, client, installation, artifact, grant, installation.Revision+1)
	if err != nil {
		if errors.Is(err, errSystemApplianceSealed) {
			return controller.sealAppliance(ctx, installation, "GUEST_INTEGRITY_FAILED")
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
			return controller.sealStartedAppliance(ctx, client, installation, identity)
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
	return controller.refreshApplianceServiceImports(ctx, installation)
}

func (controller *Controller) startApplianceInstance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, artifact store.ApplianceArtifact, grant store.ApplianceGrant, configRevision uint64) (result *vmmdv1.SystemApplianceIdentity, resultErr error) {
	started := time.Now()
	log.Printf("appliance instance ensure begin: installation=%s digest=%s", installation.ID, artifact.ImageDigest)
	defer func() {
		log.Printf("appliance instance ensure end: installation=%s elapsed=%s succeeded=%t", installation.ID, time.Since(started).Round(time.Millisecond), resultErr == nil)
	}()
	request := systemApplianceLaunchRequest(installation, artifact, grant, controller.manager.Snapshot().GetBootId(), configRevision)
	response, err := client.EnsureSystemAppliance(ctx, request)
	if err != nil {
		return nil, err
	}
	identity := response.GetAppliance().GetIdentity()
	if response.GetAppliance().GetState() == vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_SEALED {
		return nil, errSystemApplianceSealed
	}
	if response.GetAppliance().GetState() != vmmdv1.SystemApplianceState_SYSTEM_APPLIANCE_STATE_RUNNING || identity.GetContainerInstanceId() == "" {
		return nil, errors.New("guest did not start the exact system appliance")
	}
	return identity, nil
}

func systemApplianceLaunchRequest(installation store.ApplianceInstallation, artifact store.ApplianceArtifact, grant store.ApplianceGrant, bootID string, configRevision uint64) *vmmdv1.EnsureSystemApplianceRequest {
	requestID := derivedRequestID(fmt.Sprintf("system-launch/%q/%q/%q/%d", installation.ID, installation.DataVolumeID, bootID, configRevision))
	nonce := sha256.Sum256([]byte("agent-vmm/system-appliance/boot-nonce/v1/" + requestID))
	identity := &vmmdv1.SystemApplianceIdentity{InstallationId: installation.ID,
		BootId: bootID, NamespaceId: systemNamespaceID(installation.ID), ContainerId: systemContainerID(),
		ImageManifestDigest: artifact.ImageDigest, BootNonce: nonce[:]}
	manifest := new(appliancev1.ControllerApplianceManifest)
	_ = proto.Unmarshal(artifact.Manifest, manifest)
	return &vmmdv1.EnsureSystemApplianceRequest{RequestId: requestID, Identity: identity, ContentRefs: manifest.GetContentRefs(),
		ImageReference: artifact.ImageReference, DataVolumeId: installation.DataVolumeID,
		DataVolumeBytes: installation.DataVolumeBytes,
		Limits:          &vmmdv1.NamespaceLimits{Pids: uint32(grant.System.PIDs), DiskBytes: grant.System.DiskBytes},
		LanEgress:       grant.LANEgress, EgressMode: vmmdv1.EgressMode(grant.EgressMode), ConfigRevision: configRevision}
}

func (controller *Controller) publishApplianceInstance(ctx context.Context, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, health *appliancev1.ApplianceHealth) error {
	if !controller.privateControllerRouteCurrent(installation.ID, identity) {
		controller.projectionMu.Lock()
		policyErr := controller.publishSourcePoliciesLocked(ctx, "")
		controller.projectionMu.Unlock()
		if policyErr != nil {
			return policyErr
		}
		if err := controller.startPrivateControllerRoute(ctx, installation, identity); err != nil {
			return err
		}
	}
	if err := controller.refreshApplianceServiceImports(ctx, installation); err != nil {
		return err
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

func (controller *Controller) failAppliance(ctx context.Context, installation store.ApplianceInstallation, reason string, cause error) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installation.ID)
	controller.clearApplianceHealth(installation.ID)
	controller.setApplianceFailure(installation, reason)
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED) {
		return errors.Join(cause, controller.withdrawApplianceServiceImports(ctx, installation.ID))
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED)
	_, updateErr := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	return errors.Join(cause, updateErr, controller.withdrawApplianceServiceImports(ctx, installation.ID))
}

func (controller *Controller) sealAppliance(ctx context.Context, installation store.ApplianceInstallation, reason string) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installation.ID)
	controller.clearApplianceHealth(installation.ID)
	controller.setApplianceFailure(installation, "SEALED_"+reason)
	if installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) && !installation.DesiredEnabled {
		return errors.Join(errors.New("appliance sealed: "+reason), controller.withdrawApplianceServiceImports(ctx, installation.ID))
	}
	installation.State = int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED)
	installation.DesiredEnabled = false
	_, updateErr := controller.store.UpdateApplianceInstallation(ctx, installation, installation.Revision)
	return errors.Join(errors.New("appliance sealed: "+reason), updateErr, controller.withdrawApplianceServiceImports(ctx, installation.ID))
}

func (controller *Controller) failStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity, reason string, cause error) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installation.ID)
	controller.stopApplianceFrontends(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	stopErr := controller.stopStartedAppliance(ctx, client, identity)
	if current, err := controller.store.GetApplianceInstallation(ctx, installation.ID); err == nil {
		installation = current
	}
	return controller.failAppliance(ctx, installation, reason, errors.Join(cause, stopErr))
}

func (controller *Controller) sealStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, installation store.ApplianceInstallation, identity *vmmdv1.SystemApplianceIdentity) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + installation.ID)
	controller.stopApplianceFrontends(installation.ID)
	controller.stopPrivateControllerRoute(installation.ID)
	stopErr := controller.stopStartedAppliance(ctx, client, identity)
	return errors.Join(controller.sealAppliance(ctx, installation, "BOOTSTRAP_SECRET_INTEGRITY_FAILED"), stopErr)
}

func (controller *Controller) stopStartedAppliance(ctx context.Context, client vmmdv1.VMMDServiceClient, identity *vmmdv1.SystemApplianceIdentity) error {
	controller.egress.Policies.RevokeEnvironment("appliance:" + identity.GetInstallationId())
	if identity == nil || identity.GetContainerInstanceId() == "" {
		return nil
	}
	controller.clearApplianceHealth(identity.GetInstallationId())
	_, err := client.StopSystemAppliance(ctx, &vmmdv1.StopSystemApplianceRequest{RequestId: derivedRequestID("failed-system-" + identity.GetInstallationId() + "-" + identity.GetContainerInstanceId()), Identity: identity, TimeoutSeconds: 10})
	return err
}
