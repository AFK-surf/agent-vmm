//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/registryarchive"
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
	if err := client.DeleteContainer(context.Background(), namespaceID, &hostv1.DeleteContainerRequest{RequestId: "delete-imported-rootfs", ContainerId: container.GetId(), ExpectedGenerationId: container.GetGenerationId()}); err != nil {
		t.Fatal(err)
	}
	current := imageByReference(t, client, namespaceID, reference)
	if err := client.DeleteImage(context.Background(), namespaceID, deleteImageRequest("delete-restart-import", current)); err != nil {
		t.Fatal(err)
	}
	deleteNamespace(t, client, namespaceID)
}

func importTestImage(t *testing.T, client *productClient, namespaceID, requestID, reference string, archive []byte, manifestDigest string) *hostv1.Image {
	t.Helper()
	archiveHash := sha256.Sum256(archive)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	image, err := client.ImportImage(ctx, namespaceID, &hostv1.ImportImageHeader{
		RequestId: requestID, ImageReference: reference, ArchiveSha256: archiveHash[:],
		ManifestDigest: manifestDigest, Platform: "linux/arm64", ArchiveSize: uint64(len(archive)),
	}, bytes.NewReader(archive))
	if err != nil || image.GetDigest() != manifestDigest {
		t.Fatalf("archive import = %+v, %v", image, err)
	}
	return image
}

func buildImportArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	return buildExecutableArchive(t, "./test/e2e/testdata/providerstate", "bin/provider-state", false)
}

// buildExecutableArchive packages one static helper. A system appliance image
// also uses it as the entrypoint and carries the Guest's fixed mountpoints.
func buildExecutableArchive(t *testing.T, sourcePackage, executablePath string, appliance bool) ([]byte, string) {
	t.Helper()
	directory := t.TempDir()
	providerState := filepath.Join(directory, "provider-state")
	_, source, _, _ := runtime.Caller(0)
	repository := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	build := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w -buildid=", "-o", providerState, sourcePackage)
	build.Dir = repository
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build provider-state E2E helper: %v: %s", err, output)
	}
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
	helper, err := os.ReadFile(providerState)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteHeader(&tar.Header{Name: executablePath, Mode: 0o755, Size: int64(len(helper))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(helper); err != nil {
		t.Fatal(err)
	}
	if appliance {
		for _, directory := range []string{"opt/", "opt/agent-vmm/", "opt/agent-vmm/content/", "run/", "run/agent-vmm/", "var/", "var/lib/", "var/lib/agent-vmm/", "etc/"} {
			if err := writer.WriteHeader(&tar.Header{Name: directory, Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
				t.Fatal(err)
			}
		}
		for _, file := range []string{"etc/resolv.conf", ".agent-vmm-supervisor"} {
			if err := writer.WriteHeader(&tar.Header{Name: file, Mode: 0o755}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(directory, "image.oci.tar")
	arguments := []string{"run", "./build/guest/oci", layerPath, archivePath}
	if appliance {
		arguments = append(arguments, "/"+executablePath)
	}
	command := exec.Command("go", arguments...)
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

// Registry fixtures are fetched anonymously for linux/arm64 and exported with
// the SDK's OCI layout writer. Each harness caches each fixed reference once.
func (client *productClient) registryArchive(ctx context.Context, reference string) (registryArchive, error) {
	client.archiveMu.Lock()
	defer client.archiveMu.Unlock()
	if cached, ok := client.registryArchives[reference]; ok {
		return cached, nil
	}
	// This Mac reaches public registries through a transparent proxy that can
	// reset long layer downloads. Retry only this harness-side fetch, a bounded
	// number of times; the product import below it is never retried.
	result, err := cachedRegistryArchive(reference)
	if err != nil {
		for attempt := range 3 {
			if result, err = exportRegistryArchive(ctx, client.archiveDirectory, reference); err == nil || ctx.Err() != nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
		}
	}
	if err != nil {
		return registryArchive{}, err
	}
	if client.registryArchives == nil {
		client.registryArchives = make(map[string]registryArchive)
	}
	client.registryArchives[reference] = result
	return result, nil
}

// Digest-pinned registry fixtures are immutable, so their exported archives
// persist across runs. Only the harness download is cached; every run still
// imports through the product path, which checks the committed sha256 and size.
type archiveRecord struct {
	ManifestDigest string `json:"manifest_digest"`
	SHA256         string `json:"sha256"`
	Size           uint64 `json:"size"`
}

func archiveCacheDirectory() string {
	if directory := os.Getenv("AGENT_VMM_E2E_ARCHIVE_CACHE"); directory != "" {
		return directory
	}
	return filepath.Join("..", "..", ".build", "e2e-archives")
}

// archiveCacheKey returns the pinned digest of an immutable reference, or "".
func archiveCacheKey(reference string) string {
	_, pinned, found := strings.Cut(reference, "@sha256:")
	if !found || len(pinned) != 64 {
		return ""
	}
	return pinned
}

func cachedRegistryArchive(reference string) (registryArchive, error) {
	key := archiveCacheKey(reference)
	if key == "" {
		return registryArchive{}, os.ErrNotExist
	}
	path := filepath.Join(archiveCacheDirectory(), key)
	raw, err := os.ReadFile(path + ".json")
	if err != nil {
		return registryArchive{}, err
	}
	var record archiveRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return registryArchive{}, err
	}
	hash, err := hex.DecodeString(record.SHA256)
	info, statErr := os.Stat(path + ".tar")
	if err != nil || statErr != nil || len(hash) != sha256.Size || !strings.HasPrefix(record.ManifestDigest, "sha256:") || uint64(info.Size()) != record.Size {
		return registryArchive{}, errors.Join(errors.New("cached registry archive record is invalid"), err, statErr)
	}
	return registryArchive{path: path + ".tar", digest: record.ManifestDigest, hash: hash, size: record.Size}, nil
}

// exportRegistryArchive writes a tag reference into the harness directory and a
// digest-pinned one into the persistent cache.
func exportRegistryArchive(ctx context.Context, harnessDirectory, reference string) (registryArchive, error) {
	key := archiveCacheKey(reference)
	directory := harnessDirectory
	if key != "" {
		directory = archiveCacheDirectory()
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return registryArchive{}, err
	}
	archive, err := registryarchive.Export(ctx, reference, directory, 8<<30, nil)
	if err != nil {
		return registryArchive{}, err
	}
	result := registryArchive{path: archive.Path, digest: archive.ManifestDigest, hash: archive.SHA256, size: archive.Size}
	if key == "" {
		return result, nil
	}
	// The record is written last, so a reader sees either a complete archive or no entry.
	path := filepath.Join(directory, key)
	if err := os.Rename(archive.Path, path+".tar"); err != nil {
		return registryArchive{}, err
	}
	result.path = path + ".tar"
	encoded, err := json.Marshal(archiveRecord{ManifestDigest: archive.ManifestDigest, SHA256: hex.EncodeToString(archive.SHA256), Size: archive.Size})
	if err != nil {
		return registryArchive{}, err
	}
	temporary, err := os.CreateTemp(directory, key+".json-*")
	if err != nil {
		return registryArchive{}, err
	}
	defer os.Remove(temporary.Name())
	_, writeErr := temporary.Write(encoded)
	if err := errors.Join(writeErr, temporary.Sync(), temporary.Close()); err != nil {
		return registryArchive{}, err
	}
	return result, os.Rename(temporary.Name(), path+".json")
}
