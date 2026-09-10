//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
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
	maxImportArchiveBytes  = 8 << 30
	maxImportChunkBytes    = 1 << 20
	importRequestLabelBase = "io.afk-surf.agent-vmm.import."
)

func (s *Server) ImportImage(stream grpc.ClientStreamingServer[vmmdv1.ImportImageRequest, vmmdv1.Image]) error {
	first, err := stream.Recv()
	if err != nil {
		return rpcError(err)
	}
	header := first.GetHeader()
	if header == nil {
		return invalid(errors.New("image import header is required"))
	}
	if header.GetSystemImport() {
		if _, err := s.runtime.ensureNamespace(stream.Context(), &vmmdv1.EnsureNamespaceRequest{RequestId: header.GetRequestId(), NamespaceId: header.GetActorNamespaceId(), Limits: header.GetSystemLimits(), EgressMode: header.GetSystemEgressMode()}, true); err != nil {
			return rpcError(err)
		}
	}
	_, err = s.runtime.getNamespaceForRole(stream.Context(), header.GetActorNamespaceId(), header.GetSystemImport())
	if err != nil {
		return rpcError(err)
	}
	imageReference, manifestDigest, err := validateImportHeader(header, maxImportArchiveBytes)
	if err != nil {
		return invalid(err)
	}
	unlockMaterializer, err := s.runtime.lockImportSlot(manifestDigest.String())
	if err != nil {
		return rpcError(err)
	}
	defer unlockMaterializer()
	reusable, physicalGrowth, reusableTarget, err := s.runtime.imageMaterialized(stream.Context(), header.GetActorNamespaceId(), imageReference, manifestDigest, header.GetSystemImport())
	if err != nil {
		return rpcError(err)
	}
	if reusable && !physicalGrowth {
		var value any
		err := func() error {
			var mutationErr error
			value, mutationErr = s.mutate(stream.Context(), "import-image", header, func() (any, error) {
				return s.runtime.importImage(stream.Context(), nil, header, imageReference, ocispec.Descriptor{Digest: manifestDigest}, header.GetSystemImport(), false)
			})
			return mutationErr
		}()
		if err != nil {
			return rpcError(err)
		}
		return stream.SendAndClose(value.(*vmmdv1.Image))
	}
	reservation, err := s.runtime.reserveImport(stream.Context(), header, true)
	if err != nil {
		return rpcError(err)
	}
	defer reservation.Release()
	if reusable {
		var value any
		err = s.runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
			return s.runtime.guardOCIProjectGrowth(transaction, reservation, func() error {
				target := ocispec.Descriptor{Digest: manifestDigest}
				if reusableTarget != nil {
					target = *reusableTarget
				}
				var mutationErr error
				value, mutationErr = s.mutate(stream.Context(), "import-image", header, func() (any, error) {
					return s.runtime.importImage(stream.Context(), nil, header, imageReference, target, header.GetSystemImport(), false)
				})
				return mutationErr
			})
		})
		if err != nil {
			return rpcError(err)
		}
		return stream.SendAndClose(value.(*vmmdv1.Image))
	}
	var archive *os.File
	if err := s.runtime.diskBudget.Mutate(func(_ *guestdisk.Transaction) error {
		if err := s.runtime.openOCIProjectAllowance(reservation); err != nil {
			return err
		}
		var createErr error
		archive, createErr = s.runtime.createImportArchive(header.GetRequestId())
		return createErr
	}); err != nil {
		return rpcError(err)
	}
	defer func() {
		if err := s.runtime.diskBudget.Settle(func() error { return s.runtime.settleOCIProjectQuota(stream.Context()) }); err != nil {
			log.Printf("tighten OCI project quota after interrupted import: %v", err)
		}
	}()
	archivePath := archive.Name()
	defer func() {
		_ = archive.Close()
		_ = os.Remove(archivePath)
	}()
	hash := sha256.New()
	written := uint64(0)
	for {
		request, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			return rpcError(receiveErr)
		}
		chunk, ok := request.GetPayload().(*vmmdv1.ImportImageRequest_ArchiveChunk)
		if !ok || len(chunk.ArchiveChunk) == 0 || len(chunk.ArchiveChunk) > maxImportChunkBytes || uint64(len(chunk.ArchiveChunk)) > header.GetArchiveSize()-written {
			return invalid(errors.New("archive chunks must be non-empty, bounded, and within archive_size"))
		}
		count, writeErr := writeImportChunk(s.runtime.diskBudget, reservation, io.MultiWriter(archive, hash), chunk.ArchiveChunk)
		if writeErr != nil {
			if errors.Is(writeErr, syscall.EDQUOT) || errors.Is(writeErr, syscall.ENOSPC) {
				return rpcError(resourceExhausted("OCI archive exceeds namespace disk quota"))
			}
			return rpcError(fmt.Errorf("write OCI archive: %w", writeErr))
		}
		if count != len(chunk.ArchiveChunk) {
			return rpcError(io.ErrShortWrite)
		}
		written += uint64(count)
	}
	if written != header.GetArchiveSize() || subtle.ConstantTimeCompare(hash.Sum(nil), header.GetArchiveSha256()) != 1 {
		return invalid(errors.New("OCI archive size or sha256 does not match the header"))
	}
	manifest, err := validateOCIArchive(archive, manifestDigest)
	if err != nil {
		return invalid(err)
	}
	var value any
	err = s.runtime.diskBudget.Mutate(func(transaction *guestdisk.Transaction) error {
		return s.runtime.guardOCIProjectGrowth(transaction, reservation, func() error {
			var mutationErr error
			value, mutationErr = s.mutate(stream.Context(), "import-image", header, func() (any, error) {
				image, importErr := s.runtime.importImage(stream.Context(), archive, header, imageReference, *manifest, header.GetSystemImport(), false)
				return image, importErr
			})
			return mutationErr
		})
	})
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(value.(*vmmdv1.Image))
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
	unlock := r.lockNamespace(namespaceID)
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return false, false, nil, err
	}
	logical, logicalErr := r.client.GetImage(namespaceContext(ctx, namespaceID), imageReference)
	if logicalErr != nil && !errdefs.IsNotFound(logicalErr) {
		return false, false, nil, logicalErr
	}
	if logicalErr == nil && logical.Target().Digest != manifestDigest {
		return false, false, nil, imageError(codes.Aborted, "IMAGE_DIGEST_CONFLICT", "image reference already names different content", imageReference)
	}
	runtimeImage, err := r.client.GetImage(runtimeContext(ctx), runtimeImageReference(manifestDigest))
	if errdefs.IsNotFound(err) {
		return false, true, nil, nil
	}
	if err != nil {
		return false, false, nil, err
	}
	if runtimeImage.Target().Digest != manifestDigest {
		return false, false, nil, imageError(codes.FailedPrecondition, "IMAGE_ROOTFS_UNAVAILABLE", "digest-owned runtime image metadata is inconsistent", imageReference)
	}
	unpacked, err := runtimeImage.IsUnpacked(runtimeContext(ctx), namespaceSnapshotter(namespaceID))
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

func reusableTargetDescriptor(record coreimages.Image) *ocispec.Descriptor {
	target := record.Target
	return &target
}

// A namespace image record can outlive its snapshot metadata after an
// interrupted cleanup. It is not a reusable source, but it must not hide a
// later namespace that has the same digest fully materialized.
func reusableSourceUnpacked(unpacked bool, err error) (bool, error) {
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	return unpacked, err
}

func (r *Runtime) createImportArchive(requestID string) (*os.File, error) {
	directory := filepath.Join(stateRoot, "imports")
	if err := ensureDirectory(directory, 0o700); err != nil {
		return nil, err
	}
	if err := assignOCIRoot(directory, projectID, assignProject); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(directory, requestID+"-*.oci.part")
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

func (r *Runtime) importImage(ctx context.Context, archive io.ReadSeeker, header *vmmdv1.ImportImageHeader, imageReference string, manifest ocispec.Descriptor, system, replaceTarget bool) (*vmmdv1.Image, error) {
	unlock := r.lockNamespace(header.GetActorNamespaceId())
	defer unlock()
	namespaceID := header.GetActorNamespaceId()
	if err := r.namespaceExistsForRole(ctx, namespaceID, system); err != nil {
		return nil, err
	}
	logicalContext := namespaceContext(ctx, namespaceID)
	nsctx := runtimeContext(ctx)
	runtimeReference := runtimeImageReference(manifest.Digest)
	stagingLease, err := r.client.LeasesService().Create(nsctx, leases.WithRandomID(), leases.WithExpiration(24*time.Hour))
	if err != nil {
		return nil, err
	}
	stagingContext := leases.WithLease(nsctx, stagingLease.ID)
	defer func() {
		if releaseErr := r.client.LeasesService().Delete(nsctx, stagingLease, leases.SynchronousDelete); releaseErr != nil {
			log.Printf("OCI import staging lease cleanup failed for %q: %v", imageReference, releaseErr)
		}
	}()
	var fingerprint, requestLabel string
	if !replaceTarget {
		fingerprint, err = importFingerprint(header)
		if err != nil {
			return nil, err
		}
		records, err := r.client.ImageService().List(logicalContext)
		if err != nil {
			return nil, err
		}
		requestLabel = fmt.Sprintf("%s%x", importRequestLabelBase, sha256.Sum256([]byte(header.GetRequestId())))
		for _, record := range records {
			storedFingerprint, found := record.Labels[requestLabel]
			if !found {
				continue
			}
			if storedFingerprint != fingerprint || record.Name != imageReference || record.Target.Digest != manifest.Digest {
				return nil, imageError(codes.Aborted, "IMPORT_IDEMPOTENCY_CONFLICT", "import request ID was reused with different input", imageReference)
			}
			break
		}
	}
	logical, logicalErr := r.client.GetImage(logicalContext, imageReference)
	if logicalErr == nil && logical.Target().Digest != manifest.Digest && !replaceTarget {
		return nil, imageError(codes.Aborted, "IMAGE_DIGEST_CONFLICT", "image reference already names different content", imageReference)
	}
	if logicalErr != nil && !errdefs.IsNotFound(logicalErr) {
		return nil, logicalErr
	}
	existing, existingErr := r.client.GetImage(nsctx, runtimeReference)
	if existingErr == nil {
		unpacked, err := existing.IsUnpacked(nsctx, namespaceSnapshotter(namespaceID))
		unpacked, err = reusableSourceUnpacked(unpacked, err)
		if err != nil {
			return nil, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image unpack state could not be read", imageReference)
		}
		if unpacked {
			if err := r.commitLogicalImageRecord(logicalContext, logical, logicalErr, imageReference, existing.Target(), requestLabel, fingerprint); err != nil {
				return nil, err
			}
			return r.getImageForRole(ctx, namespaceID, imageReference, system)
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
		importedRecords, err := r.client.Import(stagingContext, archive,
			containerd.WithImportPlatform(platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})),
			containerd.WithImageRefTranslator(func(string) string { return "" }))
		if err != nil {
			if ociQuotaExceeded(err) {
				return nil, resourceExhausted("OCI materialization reached its hard project quota")
			}
			return nil, imageError(codes.InvalidArgument, "OCI_ARCHIVE_INVALID", "OCI archive could not be imported", imageReference)
		}
		preservedRecord := ""
		if err := cleanupImportedRecords(stagingContext, importedRecords, manifest, preservedRecord, func(ctx context.Context, name string) error {
			return r.client.ImageService().Delete(ctx, name, coreimages.SynchronousDelete())
		}); err != nil {
			if errors.Is(err, errImportedManifestMismatch) {
				return nil, imageError(codes.InvalidArgument, "OCI_MANIFEST_MISMATCH", "OCI archive imported an unexpected manifest", imageReference)
			}
			return nil, err
		}
	}
	createdRecord := false
	materialized := existing
	if existingErr != nil {
		labels := map[string]string{}
		if requestLabel != "" {
			labels[requestLabel] = fingerprint
		}
		if _, err := r.client.ImageService().Create(stagingContext, coreimages.Image{Name: runtimeReference, Target: manifest, Labels: labels}); err != nil {
			return nil, err
		}
		createdRecord = true
		materialized, err = r.client.GetImage(stagingContext, runtimeReference)
		if err != nil {
			_ = r.client.ImageService().Delete(stagingContext, runtimeReference, coreimages.SynchronousDelete())
			return nil, err
		}
	}
	if err := materialized.Unpack(stagingContext, namespaceSnapshotter(namespaceID), containerd.WithUnpackApplyOpts(diff.WithSyncFs(true))); err != nil {
		if createdRecord {
			_ = r.client.ImageService().Delete(stagingContext, runtimeReference, coreimages.SynchronousDelete())
		}
		log.Printf("OCI import unpack failed for %q: %v", imageReference, err)
		if ociQuotaExceeded(err) {
			return nil, resourceExhausted("OCI materialization reached its hard project quota")
		}
		return nil, imageError(codes.FailedPrecondition, "IMAGE_UNPACK_FAILED", "image content could not be unpacked", imageReference)
	}
	if err := r.commitLogicalImageRecord(logicalContext, logical, logicalErr, imageReference, materialized.Target(), requestLabel, fingerprint); err != nil {
		return nil, err
	}
	if err := syncStateFilesystem(); err != nil {
		return nil, fmt.Errorf("persist imported image: %w", err)
	}
	return r.getImageForRole(ctx, namespaceID, imageReference, system)
}

func (r *Runtime) commitLogicalImageRecord(ctx context.Context, current containerd.Image, currentErr error, reference string, target ocispec.Descriptor, requestLabel, fingerprint string) error {
	if currentErr == nil {
		if current.Target().Digest != target.Digest {
			metadata := current.Metadata()
			metadata.Target = target
			_, err := r.client.ImageService().Update(ctx, metadata, "target")
			return err
		}
		if requestLabel == "" {
			return nil
		}
		return r.commitImportRecord(ctx, current, requestLabel, fingerprint)
	}
	if !errdefs.IsNotFound(currentErr) {
		return currentErr
	}
	labels := map[string]string{}
	if requestLabel != "" {
		labels[requestLabel] = fingerprint
	}
	_, err := r.client.ImageService().Create(ctx, coreimages.Image{Name: reference, Target: target, Labels: labels})
	return err
}

func ociQuotaExceeded(err error) bool {
	return errdefs.IsResourceExhausted(err) || errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC) || status.Code(err) == codes.ResourceExhausted
}

var errImportedManifestMismatch = errors.New("imported image record target does not match committed manifest")

func cleanupImportedRecords(ctx context.Context, records []coreimages.Image, manifest ocispec.Descriptor, preservedName string, deleteRecord func(context.Context, string) error) error {
	for _, record := range records {
		if record.Target.Digest != manifest.Digest {
			return errImportedManifestMismatch
		}
	}
	for _, record := range records {
		if record.Name == preservedName {
			continue
		}
		if err := deleteRecord(ctx, record.Name); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("remove incidental imported image record %q: %w", record.Name, err)
		}
	}
	return nil
}

func (r *Runtime) commitImportRecord(ctx context.Context, image containerd.Image, requestLabel, fingerprint string) error {
	metadata := image.Metadata()
	if metadata.Labels == nil {
		metadata.Labels = make(map[string]string)
	}
	if metadata.Labels[requestLabel] == fingerprint {
		return nil
	}
	metadata.Labels[requestLabel] = fingerprint
	_, err := r.client.ImageService().Update(ctx, metadata, "labels")
	return err
}
