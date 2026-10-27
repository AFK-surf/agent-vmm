//go:build linux

package control

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	coreimages "github.com/containerd/containerd/v2/core/images"
	digest "github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockingImportWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	writes  int
}

func TestImportArchiveChunkBoundMatchesStreamContract(t *testing.T) {
	if !validImportArchiveChunk(make([]byte, vmmdv1.MaxImportArchiveChunkBytes), vmmdv1.MaxImportArchiveChunkBytes) {
		t.Fatal("maximum stream chunk was rejected")
	}
	if validImportArchiveChunk(make([]byte, vmmdv1.MaxImportArchiveChunkBytes+1), vmmdv1.MaxImportArchiveChunkBytes+1) {
		t.Fatal("oversized stream chunk was accepted")
	}
	if validImportArchiveChunk(make([]byte, 2), 1) {
		t.Fatal("chunk beyond the remaining archive was accepted")
	}
}

func (w *blockingImportWriter) Write(data []byte) (int, error) {
	w.writes++
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(data), nil
}

func TestImportChunkAndPersistentSettlementShareOneDiskBudgetLinearizationPoint(t *testing.T) {
	budget := new(guestdisk.Budget)
	reservationPath := filepath.Join(t.TempDir(), "reservation")
	holdPath := reservationPath + ".hold"
	reservation := &importReservation{path: reservationPath, holdPath: holdPath, remaining: 2}
	if err := writeImportReservation(reservationPath, holdPath, reservation.remaining); err != nil {
		t.Fatal(err)
	}
	w := &blockingImportWriter{entered: make(chan struct{}), release: make(chan struct{})}
	firstDone := make(chan error, 1)
	go func() {
		_, err := writeImportChunk(budget, reservation, w, []byte("a"))
		firstDone <- err
	}()
	<-w.entered

	settleEntered := make(chan struct{})
	settleStarted := make(chan struct{})
	settleDone := make(chan error, 1)
	persistent := errors.New("persistent quota settlement failure")
	go func() {
		close(settleStarted)
		settleDone <- budget.Settle(func() error {
			close(settleEntered)
			return persistent
		})
	}()
	<-settleStarted
	select {
	case <-settleEntered:
		t.Fatal("settlement crossed an in-flight physical import write")
	case <-time.After(50 * time.Millisecond):
	}
	close(w.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if err := <-settleDone; !errors.Is(err, guestdisk.ErrGrowthSealed) || !errors.Is(err, persistent) {
		t.Fatalf("settlement error = %v", err)
	}
	if _, err := writeImportChunk(budget, reservation, io.Discard, []byte("b")); !errors.Is(err, guestdisk.ErrGrowthSealed) {
		t.Fatalf("post-seal chunk error = %v, want sealed", err)
	}
	if reservation.remaining != 1 {
		t.Fatalf("remaining = %d, post-seal callback consumed reservation", reservation.remaining)
	}
}

func TestOCIQuotaFailureIsReportedAsCapacityNotArchiveCorruption(t *testing.T) {
	for _, err := range []error{syscall.EDQUOT, syscall.ENOSPC, status.Error(codes.ResourceExhausted, "hard project quota")} {
		if !ociQuotaExceeded(err) {
			t.Fatalf("quota error %v was not classified as capacity exhaustion", err)
		}
	}
	if ociQuotaExceeded(errors.New("invalid tar")) {
		t.Fatal("archive corruption was classified as quota exhaustion")
	}
}

func TestValidateOCIArchiveUsesHeaderReferenceInsteadOfBuildxAnnotations(t *testing.T) {
	reference := "example.com/agent/pi:latest"
	incidentalReference := "docker.io/buildx/generated:latest"
	archive, manifestDigest := testOCIArchive(t, map[string]string{
		ocispec.AnnotationRefName:  incidentalReference,
		"io.containerd.image.name": incidentalReference,
	}, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	manifest, err := ociarchive.Validate(bytes.NewReader(archive), manifestDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := validateImportHeader(&vmmdv1.ImportImageHeader{RequestId: "request", ActorNamespaceId: "namespace", ImageReference: reference,
		ArchiveSha256: make([]byte, 32), ManifestDigest: manifestDigest.String(), Platform: "linux/arm64", ArchiveSize: uint64(len(archive))}, uint64(len(archive))); err != nil {
		t.Fatal(err)
	}
	var deleted []string
	if err := cleanupImportedRecords(context.Background(), []coreimages.Image{{Name: incidentalReference, Target: *manifest}}, *manifest, func(_ context.Context, name string) error {
		deleted = append(deleted, name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 || deleted[0] != incidentalReference {
		t.Fatalf("deleted records = %v, want only %q", deleted, incidentalReference)
	}
}

func TestCleanupImportedRecordsRejectsTargetOtherThanCommittedManifest(t *testing.T) {
	committed := ocispec.Descriptor{Digest: digest.FromString("committed")}
	deleted := false
	err := cleanupImportedRecords(context.Background(), []coreimages.Image{{Name: "docker.io/buildx/generated:latest", Target: ocispec.Descriptor{Digest: digest.FromString("other")}}}, committed, func(context.Context, string) error {
		deleted = true
		return nil
	})
	if !errors.Is(err, errImportedManifestMismatch) {
		t.Fatalf("error = %v, want imported manifest mismatch", err)
	}
	if deleted {
		t.Fatal("mismatched imported record was deleted before all targets were validated")
	}
}

func TestValidateOCIArchiveRejectsPlatformAndUnsafeEntry(t *testing.T) {
	tests := []struct {
		name     string
		platform ocispec.Platform
		unsafe   bool
	}{
		{name: "platform", platform: ocispec.Platform{OS: "linux", Architecture: "amd64"}},
		{name: "unsafe", platform: ocispec.Platform{OS: "linux", Architecture: "arm64"}, unsafe: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			archive, manifestDigest := testOCIArchive(t, nil, test.platform)
			if test.unsafe {
				archive = appendTarEntry(t, archive, "../escape", []byte("bad"))
			}
			if _, err := ociarchive.Validate(bytes.NewReader(archive), manifestDigest); err == nil {
				t.Fatal("invalid OCI archive accepted")
			}
		})
	}
}

func TestValidateOCIArchiveRejectsMissingCommittedManifest(t *testing.T) {
	archive, _ := testOCIArchive(t, nil, ocispec.Platform{OS: "linux", Architecture: "arm64"})
	if _, err := ociarchive.Validate(bytes.NewReader(archive), digest.FromString("missing")); err == nil {
		t.Fatal("missing committed manifest accepted")
	}
}

func testOCIArchive(t *testing.T, annotations map[string]string, platform ocispec.Platform) ([]byte, digest.Digest) {
	t.Helper()
	config := ocispec.Image{Platform: platform}
	configBytes, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configDigest := digest.FromBytes(configBytes)
	manifest := ocispec.Manifest{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{MediaType: ocispec.MediaTypeImageConfig, Digest: configDigest, Size: int64(len(configBytes))},
		Layers: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageLayerGzip, Digest: digest.FromString("layer"), Size: 5}}}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest.FromBytes(manifestBytes)
	index := ocispec.Index{Versioned: specs.Versioned{SchemaVersion: 2}, MediaType: ocispec.MediaTypeImageIndex,
		Manifests: []ocispec.Descriptor{{MediaType: ocispec.MediaTypeImageManifest, Digest: manifestDigest, Size: int64(len(manifestBytes)), Platform: &platform,
			Annotations: annotations}}}
	indexBytes, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for name, content := range map[string][]byte{
		"oci-layout": []byte(`{"imageLayoutVersion":"1.0.0"}`),
		"index.json": indexBytes,
		"blobs/sha256/" + digest.FromString("layer").Encoded(): []byte("layer"),
		"blobs/sha256/" + manifestDigest.Encoded():             manifestBytes,
		"blobs/sha256/" + configDigest.Encoded():               configBytes,
	} {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes(), manifestDigest
}

func appendTarEntry(t *testing.T, archive []byte, name string, content []byte) []byte {
	t.Helper()
	trimmed := bytes.TrimSuffix(archive, make([]byte, 1024))
	var output bytes.Buffer
	output.Write(trimmed)
	writer := tar.NewWriter(&output)
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestImportIdempotencyUsesOneLatestRequestLabel(t *testing.T) {
	target := digest.FromString("image")
	reference := "example.com/image:latest"
	records := []coreimages.Image{{Name: reference, Target: ocispec.Descriptor{Digest: target}, Labels: map[string]string{importRequestLabel: importRequestValue("newer", "input")}}}
	if err := checkImportIdempotency(records, "newer", "input", reference, target); err != nil {
		t.Fatal(err)
	}
	if err := checkImportIdempotency(records, "older", "older-input", reference, target); err != nil {
		t.Fatalf("older same-digest replay=%v", err)
	}
	for _, change := range []struct {
		fingerprint, reference string
		target                 digest.Digest
	}{{"different", reference, target}, {"input", "example.com/other:latest", target}, {"input", reference, digest.FromString("other")}} {
		if err := checkImportIdempotency(records, "newer", change.fingerprint, change.reference, change.target); status.Code(err) != codes.Aborted {
			t.Fatalf("conflict=%v", err)
		}
	}
}
