//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Server) CopyImage(ctx context.Context, request *vmmdv1.CopyImageRequest) (*vmmdv1.Image, error) {
	result, err := s.copyImage(ctx, request)
	return result, rpcError(err)
}

func (s *Server) copyImage(ctx context.Context, request *vmmdv1.CopyImageRequest) (result *vmmdv1.Image, operationErr error) {
	if err := validateRequestID(request.GetRequestId()); err != nil {
		return nil, err
	}
	for _, namespace := range []string{request.GetTargetNamespaceId()} {
		if err := validateID("namespace_id", namespace); err != nil {
			return nil, err
		}
	}
	expected := digest.Digest(request.GetManifestDigest())
	if expected.Validate() != nil || expected.Algorithm() != digest.SHA256 {
		return nil, errors.New("image copy requires sha256 manifest digest")
	}
	reference, err := normalizeImageReference(request.GetImageReference())
	if err != nil {
		return nil, err
	}
	sourceOwner := request.SourceNamespaceId
	if request.ControllerCacheScope != "" {
		sourceOwner = retainedImageOwner(request.ControllerCacheScope)
	}
	unlockSource, err := s.runtime.lockNamespace(ctx, sourceOwner)
	if err != nil {
		return nil, err
	}
	defer func() {
		if unlockSource != nil {
			unlockSource()
		}
	}()
	controllerFingerprint := request.GetControllerRequestSha256()
	if controllerFingerprint != "" {
		parsed := digest.Digest("sha256:" + controllerFingerprint)
		if parsed.Validate() != nil {
			return nil, errors.New("invalid controller copy input sha256")
		}
	} else {
		return nil, errors.New("controller copy input sha256 is required")
	}
	prior, priorErr := s.runtime.imageOperation(ctx, request.TargetNamespaceId, request.RequestId, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY)
	if priorErr == nil {
		if prior.InputSha256 != controllerFingerprint {
			return nil, status.Error(codes.Aborted, "copy input differs from original admission")
		}
		if prior.State == vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED && prior.WriterSettled {
			return &vmmdv1.Image{Reference: prior.ImageReference, Digest: prior.ManifestDigest, CopyReceipt: prior}, nil
		}
		return nil, status.Error(codes.Aborted, "copy was already accepted; observe original operation")
	}
	if status.Code(priorErr) != codes.NotFound {
		return nil, priorErr
	}
	var sourceRecord coreimages.Image
	if request.ControllerCacheScope != "" {
		if request.SourceNamespaceId != "" {
			return nil, errors.New("retained copy cannot name an allocation namespace")
		}
		sourceRecord, err = s.runtime.retainedImage(ctx, request.ControllerCacheScope, reference, expected.String(), request.RetainedBuildRequestId)
	} else {
		if err := s.runtime.namespaceExists(ctx, request.GetSourceNamespaceId()); err != nil {
			return nil, err
		}
		sourceRecord, err = s.runtime.client.ImageService().Get(namespaceContext(ctx, request.SourceNamespaceId), reference)
	}
	if err != nil {
		if request.ControllerCacheScope != "" && errdefs.IsNotFound(err) {
			return nil, hostv1.NewError(codes.NotFound, hostv1.ErrorReason_ERROR_REASON_IMAGE_CACHE_NOT_ADMITTED, "retained source disappeared before copy admission", hostv1.WithResource("image", reference), hostv1.WithMetadata(map[string]string{"admission": "not_accepted"}))
		}
		return nil, err
	}
	if sourceRecord.Target.Digest != expected {
		return nil, errors.New("source image digest does not match copy contract")
	}
	requestBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	if err != nil {
		return nil, err
	}
	label := fmt.Sprintf("%scopy-request.%x", labelPrefix, sha256.Sum256([]byte(request.GetRequestId())))
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(requestBytes))
	operationContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
	if deadline, found := ctx.Deadline(); found {
		cancel()
		operationContext, cancel = context.WithDeadline(context.WithoutCancel(ctx), deadline)
	}
	defer cancel()
	receipt := &vmmdv1.ImageOperationReceipt{RequestId: request.RequestId, InputSha256: controllerFingerprint, Kind: vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY, ImageReference: reference, ManifestDigest: expected.String()}
	run, prior, err := s.runtime.acceptImageOperation(context.WithoutCancel(ctx), request.TargetNamespaceId, receipt, sourceRecord.Name, cancel)
	if err != nil {
		return nil, err
	}
	if prior != nil {
		if prior.State == vmmdv1.ImageOperationState_IMAGE_OPERATION_STATE_SUCCEEDED && prior.WriterSettled {
			return &vmmdv1.Image{Reference: prior.ImageReference, Digest: prior.ManifestDigest, CopyReceipt: prior}, nil
		}
		return nil, status.Error(codes.Aborted, "copy was already accepted; observe original operation")
	}
	unlockSource()
	unlockSource = nil
	operationContext = context.WithValue(operationContext, imageOperationContextKey{}, vmmdv1.ImageOperationKind_IMAGE_OPERATION_KIND_COPY)
	ctx = operationContext
	defer func() {
		if result != nil {
			receipt.ManifestDigest = result.Digest
		}
		if err := s.runtime.finishImageOperation(request.TargetNamespaceId, receipt, run, operationErr, nil, operationContext.Err() != nil); err != nil {
			operationErr = errors.Join(operationErr, err)
		}
	}()
	targetContext := namespaceContext(ctx, request.GetTargetNamespaceId())
	records, err := s.runtime.client.ImageService().List(targetContext)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if prior, found := record.Labels[label]; found && (prior != fingerprint || record.Name != reference || record.Target.Digest != expected) {
			return nil, errors.New("image copy request was reused with different input")
		}
	}
	_, physicalGrowth, descriptor, err := s.runtime.imageMaterialized(ctx, request.GetTargetNamespaceId(), reference, expected, false)
	if err != nil {
		return nil, err
	}
	if descriptor == nil {
		return nil, errors.New("source image materialization is unavailable")
	}
	header := &vmmdv1.ImportImageHeader{RequestId: request.GetRequestId(), ActorNamespaceId: request.GetTargetNamespaceId(), ImageReference: reference, ManifestDigest: expected.String(), Platform: "linux/arm64"}
	var image *vmmdv1.Image
	materialize := func() error {
		var importErr error
		image, importErr = s.runtime.importImage(ctx, nil, header, reference, *descriptor, false, false)
		return importErr
	}
	if physicalGrowth {
		unlock, admissionErr := s.runtime.lockImportSlot(ctx)
		if admissionErr != nil {
			return nil, admissionErr
		}
		defer unlock()
		err = s.runtime.guardOCIPhysicalGrowthLocked(ctx, header, materialize)
	} else {
		err = materialize()
	}
	if err != nil {
		return nil, err
	}
	record, err := s.runtime.client.ImageService().Get(targetContext, reference)
	if err != nil {
		return nil, err
	}
	if record.Target.Digest != expected {
		return nil, errors.New("copy target changed before receipt publication")
	}
	if record.Labels == nil {
		record.Labels = make(map[string]string)
	}
	record.Labels[label] = fingerprint
	if err := setImageOperationReceipt(&record, "copy", &vmmdv1.ImageOperationReceipt{RequestId: request.GetRequestId(), InputSha256: controllerFingerprint}); err != nil {
		return nil, err
	}
	if receipt := imageOperationReceipt(sourceRecord, "build"); receipt != nil {
		if err := setImageOperationReceipt(&record, "build", receipt); err != nil {
			return nil, err
		}
	}
	if _, err := s.runtime.client.ImageService().Update(targetContext, record, "labels"); err != nil {
		return nil, err
	}
	image.BuildReceipt = imageOperationReceipt(record, "build")
	image.CopyReceipt = imageOperationReceipt(record, "copy")
	return image, nil
}
