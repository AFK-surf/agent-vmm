//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func testImportedImageForcedRestart(t *testing.T, harness *hostHarness) {
	const namespaceID = "restart-import"
	const reference = "agent-vmm.local/restart-import:latest"
	client := harness.runtime(t)
	ensureNamespace(t, client, namespaceID, 256<<20)
	archive, manifestDigest := buildImportArchive(t)
	archiveHash := sha256.Sum256(archive)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	image, err := client.ImportImage(ctx, namespaceID, &hostv1.ImportImageHeader{RequestId: "restart-import", ImageReference: reference,
		ArchiveSha256: archiveHash[:], ManifestDigest: manifestDigest, Platform: "linux/arm64", ArchiveSize: uint64(len(archive))}, bytes.NewReader(archive))
	cancel()
	if err != nil || image.GetDigest() != manifestDigest {
		t.Fatalf("archive import = %+v, %v", image, err)
	}

	harness.stop(t, true)
	harness.start(t)
	client = harness.runtime(t)
	if imageByReference(t, client, namespaceID, reference).GetDigest() != manifestDigest {
		t.Fatal("imported image identity changed across forced VM restart")
	}
	container := createContainerWithImage(t, client, namespaceID, "imported-rootfs", reference, []string{"true"})
	if err := client.DeleteContainer(context.Background(), namespaceID, &hostv1.ContainerMutationRequest{RequestId: "delete-imported-rootfs", ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	current := imageByReference(t, client, namespaceID, reference)
	if err := client.DeleteImage(context.Background(), namespaceID, deleteImageRequest("delete-restart-import", current)); err != nil {
		t.Fatal(err)
	}
	deleteNamespace(t, client, namespaceID)
}

func buildImportArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	directory := t.TempDir()
	layerPath := filepath.Join(directory, "layer.tar")
	layer, err := os.Create(layerPath)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(layer)
	marker := []byte("durable imported rootfs\n")
	if err := writer.WriteHeader(&tar.Header{Name: "marker", Mode: 0o644, Size: int64(len(marker))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(marker); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	_, source, _, _ := runtime.Caller(0)
	repository := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	archivePath := filepath.Join(directory, "image.oci.tar")
	command := exec.Command("go", "run", "./build/guest/oci", layerPath, archivePath)
	command.Dir = repository
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build OCI archive: %v: %s", err, output)
	}
	archive, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	indexBytes := readArchiveEntry(t, archive, "index.json")
	var index struct {
		Manifests []struct {
			Digest string `json:"digest"`
		} `json:"manifests"`
	}
	if err := json.Unmarshal(indexBytes, &index); err != nil || len(index.Manifests) != 1 || index.Manifests[0].Digest == "" {
		t.Fatalf("invalid generated OCI index: %v", err)
	}
	return archive, index.Manifests[0].Digest
}

func readArchiveEntry(t *testing.T, archive []byte, name string) []byte {
	t.Helper()
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == name {
			value, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("OCI archive has no %s", name)
	return nil
}
