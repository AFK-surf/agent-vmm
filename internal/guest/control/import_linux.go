//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/diff"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxImportArchiveBytes = 8 << 30
	importRequestLabel    = labelPrefix + "import"
)

func (s *Server) ImportImage(stream grpc.ClientStreamingServer[vmmdv1.ImportImageRequest, vmmdv1.Image]) error {
	first, err := stream.Recv()
	if err != nil {
		return rpcError(err)
	}
	header := first.GetHeader()
	reference, target, err := validateImportHeader(header, maxImportArchiveBytes)
	if err != nil {
		return invalid(err)
	}
	value, err := s.mutate(stream.Context(), "import-image", header, func() (any, error) {
		ctx := stream.Context()
		if header.GetSystemImport() {
			if _, err := s.runtime.ensureNamespace(ctx, &vmmdv1.EnsureNamespaceRequest{RequestId: header.RequestId, NamespaceId: header.ActorNamespaceId, Limits: header.SystemLimits, EgressMode: header.SystemEgressMode}, true); err != nil {
				return nil, err
			}
		}
		image, hot, err := s.acknowledgeMaterializedImport(ctx, header, reference, target)
		if err != nil || hot {
			return image, err
		}
		unlock, err := s.runtime.lockImportSlot(ctx)
		if err != nil {
			return nil, err
		}
		defer unlock()
		if header.GetSystemImport() {
			if err := s.runtime.prepareSystemImageImport(ctx, header, reference, target.String()); err != nil {
				return nil, err
			}
		}
		reusable, growth, descriptor, err := s.runtime.imageMaterialized(ctx, header.ActorNamespaceId, reference, target, header.SystemImport)
		if err != nil {
			return nil, err
		}
		if reusable && !growth {
			return s.runtime.importImage(ctx, nil, header, reference, *descriptor, header.SystemImport, false)
		}
		reservation, err := s.runtime.reserveImport(ctx, header)
		if err != nil {
			return nil, err
		}
		defer reservation.Release()
		var archive *os.File
		if descriptor == nil {
			var cleanup func()
			archive, cleanup, err = s.runtime.receiveArchive(ctx, func() ([]byte, error) {
				frame, err := stream.Recv()
				if err != nil {
					return nil, err
				}
				return frame.GetArchiveChunk(), nil
			}, header.ArchiveSize, header.ArchiveSha256, reservation)
			if err != nil {
				return nil, err
			}
			defer cleanup()
			descriptor, err = ociarchive.Validate(archive, target)
			if err != nil {
				return nil, invalid(err)
			}
		}
		image = nil
		err = s.runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
			return s.runtime.guardOCIProjectGrowth(transaction, reservation, func() error {
				var err error
				image, err = s.runtime.importImage(ctx, archive, header, reference, *descriptor, header.SystemImport, false)
				return err
			})
		})
		return image, err
	})
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(value.(*vmmdv1.Image))
}

func validImportArchiveChunk(chunk []byte, remaining uint64) bool {
	return len(chunk) > 0 && len(chunk) <= vmmdv1.MaxImportArchiveChunkBytes && uint64(len(chunk)) <= remaining
}

// writeImportChunk keeps the sealed check and the physical write in the same
// GuestDiskBudget critical section. This is the production transition modeled
// by WriteImport in GuestDiskBudget.tla and Download in OCIContentLifecycle.tla.
func writeImportChunk(budget *guestdisk.Budget, reservation *importReservation, writer io.Writer, chunk []byte) (count int, err error) {
	err = budget.Mutate(func(_ *guestdisk.Transaction) error {
		if consumeErr := reservation.Consume(uint64(len(chunk))); consumeErr != nil {
			return consumeErr
		}
		count, err = writer.Write(chunk)
		return err
	})
	return count, err
}

func (r *Runtime) imageMaterialized(ctx context.Context, namespaceID, imageReference string, manifestDigest digest.Digest, system bool) (bool, bool, *ocispec.Descriptor, error) {
	unlock, err := r.lockNamespace(ctx, namespaceID)
	if err != nil {
		return false, false, nil, err
	}
	defer unlock()
	return r.imageMaterializedLocked(ctx, namespaceID, imageReference, manifestDigest, system)
}

func (r *Runtime) imageMaterializedLocked(ctx context.Context, namespaceID, imageReference string, manifestDigest digest.Digest, system bool) (bool, bool, *ocispec.Descriptor, error) {
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return false, false, nil, err
	}
	logical, logicalErr := r.client.GetImage(namespaceContext(ctx, namespaceID), imageReference)
	if logicalErr != nil && !errdefs.IsNotFound(logicalErr) {
		return false, false, nil, logicalErr
	}
	if logicalErr == nil && logical.Target().Digest != manifestDigest {
		return false, false, nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_IMAGE_DIGEST_CONFLICT, "image reference already names different content", hostv1.WithResource("image", imageReference))
	}
	runtimeImage, err := r.client.GetImage(runtimeContext(ctx), runtimeImageReference(manifestDigest))
	if errdefs.IsNotFound(err) {
		return false, true, nil, nil
	}
	if err != nil {
		return false, false, nil, err
	}
	if runtimeImage.Target().Digest != manifestDigest {
		return false, false, nil, status.Error(codes.FailedPrecondition, "digest-owned runtime image metadata is inconsistent")
	}
	unpacked, err := runtimeImage.IsUnpacked(runtimeContext(ctx), containerSnapshotter)
	unpacked, err = reusableSourceUnpacked(unpacked, err)
	if err != nil {
		return false, false, nil, err
	}
	if !unpacked {
		target := runtimeImage.Target()
		_, _, _, missing, checkErr := coreimages.Check(runtimeContext(ctx), runtimeImage.ContentStore(), target,
			platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"}))
		if checkErr != nil {
			return false, false, nil, checkErr
		}
		if len(missing) == 0 {
			return false, true, &target, nil
		}
		return false, true, nil, nil
	}
	target := runtimeImage.Target()
	return true, false, &target, nil
}

func reusableSourceUnpacked(unpacked bool, err error) (bool, error) {
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	return unpacked, err
}

func (r *Runtime) createImportArchive() (*os.File, error) {
	directory := filepath.Join(stateRoot, "imports")
	if err := ensureDirectory(directory, 0o700); err != nil {
		return nil, err
	}
	if err := assignOCIRoot(directory, projectID, assignProject); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(directory, "import-*.oci.part")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	return file, nil
}

func (r *Runtime) importImage(ctx context.Context, archive io.ReadSeeker, header *vmmdv1.ImportImageHeader, imageReference string, manifest ocispec.Descriptor, system, replaceTarget bool) (result *vmmdv1.Image, importErr error) {
	defer r.wakeRuntimeGC()
	unlock, err := r.lockNamespace(ctx, header.GetActorNamespaceId())
	if err != nil {
		return nil, err
	}
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	namespaceID := header.GetActorNamespaceId()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	nsctx := runtimeContext(ctx)
	runtimeReference := runtimeImageReference(manifest.Digest)
	leaseIdentity := leases.WithRandomID()
	if kind, found := ctx.Value(imageOperationContextKey{}).(vmmdv1.ImageOperationKind); found {
		leaseIdentity = leases.WithID(imageOperationLeaseID(namespaceID, header.RequestId, kind))
	}
	stagingLease, err := r.client.LeasesService().Create(nsctx, leaseIdentity, leases.WithExpiration(24*time.Hour))
	if err != nil {
		return nil, err
	}
	stagingContext := leases.WithLease(nsctx, stagingLease.ID)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if releaseErr := r.client.LeasesService().Delete(runtimeContext(cleanup), stagingLease, leases.SynchronousDelete); releaseErr != nil && !errdefs.IsNotFound(releaseErr) {
			importErr = errors.Join(importErr, &imageCleanupFailure{releaseErr})
		}
	}()
	var fingerprint string
	if !replaceTarget {
		input, err := importFingerprint(header)
		if err != nil {
			return nil, err
		}
		fingerprint = importRequestValue(header.RequestId, input)
	}
	unlock()
	unlock = nil
	existing, existingErr := r.client.GetImage(nsctx, runtimeReference)
	if existingErr == nil {
		unpacked, err := existing.IsUnpacked(nsctx, containerSnapshotter)
		unpacked, err = reusableSourceUnpacked(unpacked, err)
		if err != nil {
			return nil, status.Error(codes.Internal, "image unpack state could not be read")
		}
		if unpacked {
			if system {
				if err := validateSystemImageMaterialization(stagingContext, r.client, existing); err != nil {
					return nil, err
				}
			}
			return r.commitImportedImage(ctx, header, imageReference, existing.Target(), system, replaceTarget, fingerprint)
		}
		// An image record without a usable snapshot is an interrupted
		// materialization, not reusable state. Remove it before this operation
		// takes ownership so an unpack failure can clean up atomically and a
		// later retry does not remain pinned to the same partial record.
		if err := r.client.ImageService().Delete(nsctx, runtimeReference, coreimages.SynchronousDelete()); err != nil && !errdefs.IsNotFound(err) {
			return nil, err
		}
		existing = nil
		existingErr = errdefs.ErrNotFound
	} else if !errdefs.IsNotFound(existingErr) {
		return nil, existingErr
	}
	if archive != nil {
		if _, err := archive.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		importStarted := time.Now()
		if system {
			r.logSystemImageStorage("import")
		}
		importedRecords, err := r.client.Import(stagingContext, archive,
			containerd.WithImportPlatform(platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})),
			containerd.WithImageRefTranslator(func(string) string { return "" }))
		if system {
			r.logger.Info("system image content imported", "elapsed", time.Since(importStarted).Round(time.Millisecond), "succeeded", err == nil)
		}
		if err != nil {
			if ociQuotaExceeded(err) {
				return nil, resourceExhausted("OCI materialization reached its hard project quota")
			}
			return nil, status.Error(codes.InvalidArgument, "OCI archive could not be imported")
		}
		if err := cleanupImportedRecords(stagingContext, importedRecords, manifest, func(ctx context.Context, name string) error {
			return r.client.ImageService().Delete(ctx, name, coreimages.SynchronousDelete())
		}); err != nil {
			if errors.Is(err, errImportedManifestMismatch) {
				return nil, status.Error(codes.InvalidArgument, "OCI archive imported an unexpected manifest")
			}
			return nil, err
		}
	}
	createdRecord := false
	materialized := existing
	if existingErr != nil {
		if _, err := r.client.ImageService().Create(stagingContext, coreimages.Image{Name: runtimeReference, Target: manifest}); err != nil {
			return nil, err
		}
		inventory, inventoryErr := r.requireRuntimeInventory()
		if inventoryErr != nil {
			return nil, inventoryErr
		}
		inventory.addRuntimeImage(runtimeReference)
		createdRecord = true
		materialized, err = r.client.GetImage(stagingContext, runtimeReference)
		if err != nil {
			_ = r.client.ImageService().Delete(stagingContext, runtimeReference, coreimages.SynchronousDelete())
			inventory.removeRuntimeImage(runtimeReference)
			return nil, err
		}
	}
	unpackStarted := time.Now()
	if system {
		r.logSystemImageStorage("unpack")
	}
	if err := materialized.Unpack(stagingContext, containerSnapshotter, containerd.WithUnpackApplyOpts(diff.WithSyncFs(true))); err != nil {
		if createdRecord {
			_ = r.client.ImageService().Delete(stagingContext, runtimeReference, coreimages.SynchronousDelete())
			if inventory, inventoryErr := r.requireRuntimeInventory(); inventoryErr == nil {
				inventory.removeRuntimeImage(runtimeReference)
			}
		}
		if system {
			r.logSystemImageStorage("unpack_failed")
			r.logger.Warn("system image unpack failed", "elapsed", time.Since(unpackStarted).Round(time.Millisecond))
		}
		r.logger.Warn("OCI import unpack failed", "reference", imageReference, "error", err)
		if ociQuotaExceeded(err) {
			return nil, resourceExhausted("OCI materialization reached its hard project quota")
		}
		return nil, hostv1.NewError(codes.FailedPrecondition, hostv1.ErrorReason_ERROR_REASON_IMAGE_UNPACK_FAILED, "image content could not be unpacked", hostv1.WithResource("image", imageReference))
	}
	if system {
		r.logSystemImageStorage("unpack_complete")
		r.logger.Info("system image unpacked", "elapsed", time.Since(unpackStarted).Round(time.Millisecond))
	}
	if system {
		if err := validateSystemImageMaterialization(stagingContext, r.client, materialized); err != nil {
			return nil, err
		}
	}
	if err := syncStateFilesystem(); err != nil {
		return nil, fmt.Errorf("persist imported image: %w", err)
	}
	return r.commitImportedImage(ctx, header, imageReference, materialized.Target(), system, replaceTarget, fingerprint)
}

func (r *Runtime) indexCommittedImage(namespaceID, reference, targetDigest string) error {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	if err := inventory.addImage(namespaceID, reference, targetDigest); err != nil {
		return err
	}
	r.releaseImagePin(imagePublicationKey(namespaceID, reference, targetDigest))
	return nil
}

func (r *Runtime) commitLogicalImageRecord(ctx context.Context, current containerd.Image, currentErr error, reference string, target ocispec.Descriptor, fingerprint string) error {
	if currentErr == nil {
		if current.Target().Digest != target.Digest {
			metadata := current.Metadata()
			metadata.Target = target
			_, err := r.client.ImageService().Update(ctx, metadata, "target")
			return err
		}
		if fingerprint == "" {
			return nil
		}
		return r.commitImportRecord(ctx, current, fingerprint)
	}
	if !errdefs.IsNotFound(currentErr) {
		return currentErr
	}
	labels := map[string]string{}
	if fingerprint != "" {
		labels[importRequestLabel] = fingerprint
	}
	_, err := r.client.ImageService().Create(ctx, coreimages.Image{Name: reference, Target: target, Labels: labels})
	return err
}

func ociQuotaExceeded(err error) bool {
	return errdefs.IsResourceExhausted(err) || errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC) || status.Code(err) == codes.ResourceExhausted
}

var errImportedManifestMismatch = errors.New("imported image record target does not match committed manifest")

func cleanupImportedRecords(ctx context.Context, records []coreimages.Image, manifest ocispec.Descriptor, deleteRecord func(context.Context, string) error) error {
	for _, record := range records {
		if record.Target.Digest != manifest.Digest {
			return errImportedManifestMismatch
		}
	}
	for _, record := range records {
		if err := deleteRecord(ctx, record.Name); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("remove incidental imported image record %q: %w", record.Name, err)
		}
	}
	return nil
}

func (r *Runtime) commitImportRecord(ctx context.Context, image containerd.Image, fingerprint string) error {
	metadata := image.Metadata()
	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string)
	}
	if metadata.Labels[importRequestLabel] == fingerprint {
		return nil
	}
	metadata.Labels[importRequestLabel] = fingerprint
	_, err := r.client.ImageService().Update(ctx, metadata, "labels")
	return err
}

func (r *Runtime) commitImportedImage(ctx context.Context, header *vmmdv1.ImportImageHeader, reference string, target ocispec.Descriptor, system, replace bool, fingerprint string) (*vmmdv1.Image, error) {
	namespace := header.ActorNamespaceId
	unlock, err := r.lockNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespace, system); err != nil {
		return nil, err
	}
	logicalContext := namespaceContext(ctx, namespace)
	if !replace {
		input, err := importFingerprint(header)
		if err != nil {
			return nil, err
		}
		records, err := r.client.ImageService().List(logicalContext)
		if err != nil {
			return nil, err
		}
		if err := checkImportIdempotency(records, header.RequestId, input, reference, target.Digest); err != nil {
			return nil, err
		}
	}
	current, currentErr := r.client.GetImage(logicalContext, reference)
	if currentErr == nil && current.Target().Digest != target.Digest && !replace {
		return nil, hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_IMAGE_DIGEST_CONFLICT, "image reference already names different content", hostv1.WithResource("image", reference))
	}
	if (currentErr == nil && current.Target().Digest != target.Digest) || errdefs.IsNotFound(currentErr) {
		if _, err := r.pinImagePublication(namespace, reference, target.Digest.String()); err != nil {
			return nil, err
		}
	}
	if err := r.commitLogicalImageRecord(logicalContext, current, currentErr, reference, target, fingerprint); err != nil {
		return nil, err
	}
	if err := r.indexCommittedImage(namespace, reference, target.Digest.String()); err != nil {
		return nil, err
	}
	if system {
		if err := r.pruneSystemImages(ctx, namespace, reference, ""); err != nil {
			return nil, err
		}
	}
	return r.getImageForRole(ctx, namespace, reference, system)
}
