//go:build darwin || linux

package appliance

import (
	"context"
	"slices"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	opreceipt "github.com/AFK-surf/agent-vmm/internal/host/operation"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (controller *Owner) applianceImageRetention(ctx context.Context, installationID string) (*vmmdv1.SystemImageRetention, error) {
	installation, operations, err := controller.store.ApplianceImageRetentionFacts(ctx, installationID)
	if err != nil {
		return nil, err
	}
	artifact, err := controller.store.GetApplianceArtifact(ctx, installation.ArtifactID)
	if err != nil {
		return nil, err
	}
	projection := &vmmdv1.SystemImageRetention{InstallationRevision: installation.Revision, ManifestDigests: []string{artifact.ImageDigest}}
	for _, operation := range operations {
		progress := new(appliancev1.ApplianceUpdateProgress)
		if err := proto.Unmarshal(operation.Receipt, progress); err != nil {
			return nil, err
		}
		target, err := controller.store.GetApplianceArtifact(ctx, progress.GetRequest().GetArtifactId())
		if err != nil {
			return nil, err
		}
		projection.ManifestDigests = append(projection.ManifestDigests, target.ImageDigest)
	}
	slices.Sort(projection.ManifestDigests)
	projection.ManifestDigests = slices.Compact(projection.ManifestDigests)
	return projection, nil
}

func (controller *Owner) reconcileSystemImageRetention(ctx context.Context, client vmmdv1.VMMDServiceClient, installationID string) error {
	retention, err := controller.applianceImageRetention(ctx, installationID)
	if err != nil {
		return err
	}
	request := &vmmdv1.ReconcileSystemImagesRequest{InstallationId: installationID, Retention: retention}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return err
	}
	request.RequestId = opreceipt.DerivedRequestID("system-image-retention:" + string(encoded))
	_, err = client.ReconcileSystemImages(ctx, request)
	if reason, ok := hostv1.ErrorReasonOf(err); ok && reason == hostv1.ErrorReason_ERROR_REASON_SYSTEM_IMAGE_COLLECTION_DEFERRED {
		for _, detail := range status.Convert(err).Details() {
			if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Metadata["projection_applied"] == "true" {
				controller.logger.Warn("system image collection deferred", "installation_id", installationID, "error", err)
				return nil
			}
		}
	}
	return err
}
