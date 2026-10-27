//go:build darwin || linux

package appliance

import (
	"context"
	"database/sql"
	"errors"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"github.com/AFK-surf/agent-vmm/internal/host/store"
	"google.golang.org/protobuf/proto"
)

func (controller *Owner) ReconcileBundledArtifact(ctx context.Context) error {
	material, available, err := controller.manager.ReadVerifiedBundledApplianceMaterial()
	if err != nil {
		return err
	}
	if !available {
		return nil
	}
	return controller.stageBundledApplianceArtifactManifest(ctx, material.Manifest, material.ImageDigest)
}

func (controller *Owner) stageBundledApplianceArtifactManifest(ctx context.Context, encoded []byte, lockedDigest string) error {
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
	_, err = controller.store.GetApplianceArtifactByCanonicalHash(ctx, canonicalHash)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	artifactID, err := opreceipt.NewID()
	if err != nil {
		return err
	}
	_, _, err = controller.store.StageApplianceArtifact(ctx, store.ApplianceArtifact{ID: artifactID, CanonicalHash: canonicalHash, ImageDigest: manifest.GetImageManifestDigest(), ImageReference: manifest.GetImageReference(), Platform: manifest.GetPlatform(), ProductID: manifest.GetProductId(), Release: manifest.GetRelease(), Manifest: canonical}, store.ApplianceOperation{ID: opreceipt.StableID("bundle-stage", artifactID), RequestID: opreceipt.StableID("bundle-stage", artifactID), Kind: int32(appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE), State: int32(appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED), Phase: int32(appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_TERMINAL), Stage: "bundled-artifact-staged"})
	return err
}
