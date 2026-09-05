//go:build darwin && e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"

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

func buildExecutableArchive(t *testing.T, sourcePackage, executablePath string, entrypoint bool) ([]byte, string) {
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
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(directory, "image.oci.tar")
	arguments := []string{"run", "./build/guest/oci", layerPath, archivePath}
	if entrypoint {
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
	ref, err := name.ParseReference(reference)
	if err != nil {
		return registryArchive{}, err
	}
	image, err := remote.Image(ref, remote.WithContext(ctx), remote.WithPlatform(v1.Platform{OS: "linux", Architecture: "arm64"}), remote.WithAuth(authn.Anonymous))
	if err != nil {
		return registryArchive{}, err
	}
	config, err := image.ConfigFile()
	if err != nil {
		return registryArchive{}, err
	}
	// ARM64 v8 is the baseline platform. Build the fixture commitment from
	// the normalized image, while retaining its layers and runtime config.
	if config.OS == "linux" && config.Architecture == "arm64" && config.Variant == "v8" {
		config = config.DeepCopy()
		config.Variant = ""
		image, err = mutate.ConfigFile(image, config)
		if err != nil {
			return registryArchive{}, err
		}
	}
	digest, err := image.Digest()
	if err != nil {
		return registryArchive{}, err
	}
	directory, err := os.MkdirTemp(client.archiveDirectory, "oci-layout-")
	if err != nil {
		return registryArchive{}, err
	}
	defer os.RemoveAll(directory)
	index, err := layout.Write(directory, empty.Index)
	if err != nil {
		return registryArchive{}, err
	}
	if err := index.AppendImage(image, layout.WithPlatform(v1.Platform{OS: "linux", Architecture: "arm64"})); err != nil {
		return registryArchive{}, err
	}
	file, err := os.CreateTemp(client.archiveDirectory, "registry-*.tar")
	if err != nil {
		return registryArchive{}, err
	}
	hash := sha256.New()
	writer := tar.NewWriter(io.MultiWriter(file, hash))
	writeErr := filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		if err := writer.WriteHeader(&tar.Header{Name: filepath.ToSlash(relative), Mode: 0o600, Size: info.Size()}); err != nil {
			return err
		}
		source, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(writer, source)
		return errors.Join(copyErr, source.Close())
	})
	if err := errors.Join(writeErr, writer.Close(), file.Close()); err != nil {
		os.Remove(file.Name())
		return registryArchive{}, err
	}
	info, err := os.Stat(file.Name())
	if err != nil {
		return registryArchive{}, err
	}
	result := registryArchive{path: file.Name(), digest: digest.String(), hash: hash.Sum(nil), size: uint64(info.Size())}
	if client.registryArchives == nil {
		client.registryArchives = make(map[string]registryArchive)
	}
	client.registryArchives[reference] = result
	return result, nil
}
