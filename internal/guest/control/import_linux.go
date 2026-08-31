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
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/diff"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	namespace, err := s.runtime.GetNamespace(stream.Context(), header.GetActorNamespaceId())
	if err != nil {
		return rpcError(err)
	}
	archiveLimit := min(maxImportArchiveBytes, namespace.GetLimits().GetDiskBytes())
	imageReference, manifestDigest, err := validateImportHeader(header, archiveLimit)
	if err != nil {
		return invalid(err)
	}
	archive, err := s.runtime.createImportArchive(stream.Context(), header.GetActorNamespaceId(), header.GetRequestId(), header.GetArchiveSize(), namespace.GetLimits().GetDiskBytes())
	if err != nil {
		return rpcError(err)
	}
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
		count, writeErr := io.MultiWriter(archive, hash).Write(chunk.ArchiveChunk)
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
	value, err := s.mutate(stream.Context(), "import-image", header, func() (any, error) {
		image, err := s.runtime.importImage(stream.Context(), archive, header, imageReference, *manifest, header.GetSystemImport())
		return image, err
	})
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(value.(*vmmdv1.Image))
}

func (r *Runtime) createImportArchive(ctx context.Context, namespaceID, requestID string, archiveSize, diskLimit uint64) (*os.File, error) {
	labels, err := r.client.NamespaceService().Labels(ctx, namespaceID)
	if err != nil {
		return nil, err
	}
	projectID, _, err := namespaceFromLabels(namespaceID, labels)
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(stateRoot, "imports", namespaceID)
	if err := ensureDirectory(directory, 0o700); err != nil {
		return nil, err
	}
	if err := assignProject(directory, projectID); err != nil {
		return nil, err
	}
	usage, err := projectUsage(r.stateDevice, projectID)
	if err != nil {
		return nil, err
	}
	if usage > diskLimit || archiveSize > diskLimit-usage {
		return nil, resourceExhausted("OCI archive exceeds remaining namespace disk quota")
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

func (r *Runtime) ImportImage(ctx context.Context, archive io.ReadSeeker, header *vmmdv1.ImportImageHeader, imageReference string, manifest ocispec.Descriptor) (*vmmdv1.Image, error) {
	return r.importImage(ctx, archive, header, imageReference, manifest, false)
}

func (r *Runtime) importImage(ctx context.Context, archive io.ReadSeeker, header *vmmdv1.ImportImageHeader, imageReference string, manifest ocispec.Descriptor, system bool) (*vmmdv1.Image, error) {
	unlock := r.lockNamespace(header.GetActorNamespaceId())
	defer unlock()
	if err := r.namespaceExistsForRole(ctx, header.GetActorNamespaceId(), system); err != nil {
		return nil, err
	}
	nsctx := namespaceContext(ctx, header.GetActorNamespaceId())
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
	fingerprint, err := importFingerprint(header)
	if err != nil {
		return nil, err
	}
	records, err := r.client.ImageService().List(nsctx)
	if err != nil {
		return nil, err
	}
	requestLabel := fmt.Sprintf("%s%x", importRequestLabelBase, sha256.Sum256([]byte(header.GetRequestId())))
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
	existing, existingErr := r.client.GetImage(nsctx, imageReference)
	if existingErr == nil {
		if existing.Target().Digest != manifest.Digest {
			return nil, imageError(codes.Aborted, "IMAGE_DIGEST_CONFLICT", "image reference already names different content", imageReference)
		}
		unpacked, err := existing.IsUnpacked(nsctx, containerSnapshotter)
		if err != nil {
			return nil, imageError(codes.Internal, "IMAGE_METADATA_FAILED", "image unpack state could not be read", imageReference)
		}
		if unpacked {
			if err := r.commitImportRecord(nsctx, existing, requestLabel, fingerprint); err != nil {
				return nil, err
			}
			return r.getImageForRole(nsctx, header.GetActorNamespaceId(), imageReference, system)
		}
	} else if !errdefs.IsNotFound(existingErr) {
		return nil, existingErr
	}
	currentBytes, err := r.namespaceImageBytes(nsctx)
	if err != nil {
		return nil, err
	}
	namespace, err := r.getNamespaceForRole(ctx, header.GetActorNamespaceId(), system)
	if err != nil {
		return nil, err
	}
	if existingErr != nil && currentBytes+header.GetArchiveSize() > namespace.GetLimits().GetDiskBytes() {
		return nil, resourceExhausted("OCI archive exceeds namespace disk quota")
	}
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	importedRecords, err := r.client.Import(stagingContext, archive,
		containerd.WithImportPlatform(platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})),
		containerd.WithImageRefTranslator(func(string) string { return "" }))
	if err != nil {
		return nil, imageError(codes.InvalidArgument, "OCI_ARCHIVE_INVALID", "OCI archive could not be imported", imageReference)
	}
	preservedRecord := ""
	if existingErr == nil {
		preservedRecord = imageReference
	}
	if err := cleanupImportedRecords(stagingContext, importedRecords, manifest, preservedRecord, func(ctx context.Context, name string) error {
		return r.client.ImageService().Delete(ctx, name, coreimages.SynchronousDelete())
	}); err != nil {
		if errors.Is(err, errImportedManifestMismatch) {
			return nil, imageError(codes.InvalidArgument, "OCI_MANIFEST_MISMATCH", "OCI archive imported an unexpected manifest", imageReference)
		}
		return nil, err
	}
	staged := containerd.NewImageWithPlatform(r.client, coreimages.Image{Name: imageReference, Target: manifest}, platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"}))
	size, err := staged.Size(stagingContext)
	if err != nil {
		return nil, err
	}
	if existingErr != nil && currentBytes+uint64(size) > namespace.GetLimits().GetDiskBytes() {
		return nil, resourceExhausted("image logical size exceeds namespace disk quota")
	}
	createdRecord := false
	materialized := existing
	if existingErr != nil {
		if _, err := r.client.ImageService().Create(stagingContext, coreimages.Image{Name: imageReference, Target: manifest,
			Labels: map[string]string{requestLabel: fingerprint}}); err != nil {
			return nil, err
		}
		createdRecord = true
		materialized, err = r.client.GetImage(stagingContext, imageReference)
		if err != nil {
			_ = r.client.ImageService().Delete(stagingContext, imageReference, coreimages.SynchronousDelete())
			return nil, err
		}
	}
	if err := materialized.Unpack(stagingContext, containerSnapshotter, containerd.WithUnpackApplyOpts(diff.WithSyncFs(true))); err != nil {
		if createdRecord {
			_ = r.client.ImageService().Delete(stagingContext, imageReference, coreimages.SynchronousDelete())
		}
		log.Printf("OCI import unpack failed for %q: %v", imageReference, err)
		return nil, imageError(codes.FailedPrecondition, "IMAGE_UNPACK_FAILED", "image content could not be unpacked", imageReference)
	}
	if !createdRecord {
		if err := r.commitImportRecord(stagingContext, materialized, requestLabel, fingerprint); err != nil {
			return nil, err
		}
	}
	if err := syncStateFilesystem(); err != nil {
		return nil, fmt.Errorf("persist imported image: %w", err)
	}
	return r.getImageForRole(nsctx, header.GetActorNamespaceId(), imageReference, system)
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
