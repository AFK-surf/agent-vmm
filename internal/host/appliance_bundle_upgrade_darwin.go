//go:build darwin

package host

import (
	"context"
	"errors"
	"fmt"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) reconcileBundledApplianceArtifact(ctx context.Context) error {
	installations, err := controller.store.ListApplianceInstallations(ctx)
	if err != nil || len(installations) == 0 {
		return err
	}
	material, err := controller.manager.ReadVerifiedBundledApplianceMaterial()
	if err != nil {
		return err
	}
	return controller.reconcileBundledApplianceArtifactManifest(ctx, installations, material.Manifest, material.ImageDigest)
}

func (controller *Controller) reconcileBundledApplianceArtifactManifest(ctx context.Context, installations []store.ApplianceInstallation, encoded []byte, lockedDigest string) error {
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(encoded, manifest); err != nil {
		return err
	}
	canonical, canonicalHash, err := (applianceArtifactVerifier{}).verify(manifest, nil)
	if err != nil {
		return err
	}
	if manifest.GetImageManifestDigest() != lockedDigest || manifest.GetImageReference() != "agent-vmm.local/mcp@"+lockedDigest {
		return errors.New("bundled appliance manifest does not match the Guest image lock")
	}
	var matched []store.ApplianceInstallation
	for _, installation := range installations {
		if installation.ProductID == manifest.GetProductId() {
			matched = append(matched, installation)
		}
	}
	if len(matched) == 0 {
		return nil
	}
	if len(matched) != 1 {
		return fmt.Errorf("bundled appliance product has %d installations", len(matched))
	}
	installation := matched[0]
	if !installation.DesiredEnabled || installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_SEALED) ||
		installation.State == int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPING) {
		return nil
	}
	if !applianceBundleUpgradeState(installation.State) {
		return errors.New("bundled appliance installation state cannot be upgraded")
	}
	grant, err := controller.store.GetApplianceGrant(ctx, installation.ID)
	if err != nil {
		return err
	}
	approved := grantToProto(grant)
	frontends, err := controller.store.ListApplianceFrontends(ctx, installation.ID)
	if err != nil {
		return err
	}
	approved.Frontends = frontendsToProto(frontends)
	if _, _, err := (applianceArtifactVerifier{}).verify(manifest, approved); err != nil {
		return err
	}
	artifactID, err := randomID()
	if err != nil {
		return err
	}
	_, err = controller.store.SwitchBundledApplianceArtifact(ctx, installation.ID, installation.Revision, installation.ArtifactID,
		installation.State, int32(appliancev1.ApplianceState_APPLIANCE_STATE_STOPPED), store.ApplianceArtifact{
			ID: artifactID, CanonicalHash: canonicalHash, ImageDigest: manifest.GetImageManifestDigest(),
			ImageReference: manifest.GetImageReference(), Platform: manifest.GetPlatform(), ProductID: manifest.GetProductId(),
			Release: manifest.GetRelease(), Verified: true, Manifest: canonical,
		})
	return err
}

func applianceBundleUpgradeState(state int32) bool {
	return applianceRecoveryState(state) || state == int32(appliancev1.ApplianceState_APPLIANCE_STATE_FAILED)
}
