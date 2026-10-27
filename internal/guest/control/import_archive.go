//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/imageref"
	coreimages "github.com/containerd/containerd/v2/core/images"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func importFingerprint(header *vmmdv1.ImportImageHeader) (string, error) {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(header)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), nil
}

func validateImportHeader(header *vmmdv1.ImportImageHeader, maxArchiveBytes uint64) (string, digest.Digest, error) {
	if header == nil || validateRequestID(header.GetRequestId()) != nil ||
		len(header.GetArchiveSha256()) != 32 || header.GetPlatform() != "linux/arm64" || header.GetArchiveSize() == 0 || header.GetArchiveSize() > maxArchiveBytes {
		return "", "", errors.New("complete bounded linux/arm64 OCI import metadata is required")
	}
	if header.GetSystemImport() {
		if validateSystemNamespace(header.GetActorNamespaceId()) != nil || validateNamespaceLimits(header.GetActorNamespaceId(), header.GetSystemLimits()) != nil || header.GetSystemEgressMode() == vmmdv1.EgressMode_EGRESS_MODE_UNSPECIFIED {
			return "", "", errors.New("system image import identity is invalid")
		}
	} else if validateID("actor_namespace_id", header.GetActorNamespaceId()) != nil {
		return "", "", errors.New("actor_namespace_id is invalid")
	}
	reference, err := imageref.NormalizeReference(header.GetImageReference())
	if err != nil || reference != header.GetImageReference() {
		return "", "", errors.New("image_reference must be canonical and fixed")
	}
	manifestDigest, err := digest.Parse(header.GetManifestDigest())
	if err != nil || manifestDigest.Algorithm() != digest.SHA256 {
		return "", "", errors.New("manifest_digest must be a sha256 digest")
	}
	return reference, manifestDigest, nil
}

func importRequestValue(requestID, fingerprint string) string {
	return fmt.Sprintf("%x:%s", sha256.Sum256([]byte(requestID)), fingerprint)
}

func checkImportIdempotency(records []coreimages.Image, requestID, fingerprint, reference string, target digest.Digest) error {
	key := fmt.Sprintf("%x:", sha256.Sum256([]byte(requestID)))
	value := importRequestValue(requestID, fingerprint)
	for _, record := range records {
		stored := record.Labels[importRequestLabel]
		if strings.HasPrefix(stored, key) && (stored != value || record.Name != reference || record.Target.Digest != target) {
			return hostv1.NewError(codes.Aborted, hostv1.ErrorReason_ERROR_REASON_REQUEST_ID_CONFLICT, "import request ID was reused with different input", hostv1.WithResource("image", reference))
		}
	}
	return nil
}

func (r *Runtime) receiveArchive(ctx context.Context, recv func() ([]byte, error), size uint64, sha []byte, reservation *importReservation) (_ *os.File, _ func(), resultErr error) {
	var archive *os.File
	if err := r.diskBudget.Mutate(func(*guestdisk.Transaction) error {
		if err := r.openOCIProjectAllowance(reservation); err != nil {
			return err
		}
		var err error
		archive, err = r.createImportArchive()
		return err
	}); err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		_ = archive.Close()
		_ = os.Remove(archive.Name())
		if err := r.diskBudget.Settle(func() error { return r.settleOCIProjectQuota() }); err != nil {
			r.logger.Warn("OCI archive quota settlement failed", "error", err)
		}
	}
	defer func() {
		if resultErr != nil {
			cleanup()
		}
	}()
	hash := sha256.New()
	var written uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		chunk, err := recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		if !validImportArchiveChunk(chunk, size-written) {
			return nil, nil, invalid(errors.New("archive chunks must be non-empty, bounded, and within archive_size"))
		}
		count, err := writeImportChunk(r.diskBudget, reservation, io.MultiWriter(archive, hash), chunk)
		if errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC) {
			return nil, nil, resourceExhausted("OCI archive exceeds namespace disk quota")
		}
		if err != nil {
			return nil, nil, fmt.Errorf("write OCI archive: %w", err)
		}
		if count != len(chunk) {
			return nil, nil, io.ErrShortWrite
		}
		written += uint64(count)
	}
	if written != size || subtle.ConstantTimeCompare(hash.Sum(nil), sha) != 1 {
		return nil, nil, invalid(errors.New("OCI archive size or sha256 does not match the header"))
	}
	return archive, cleanup, nil
}
