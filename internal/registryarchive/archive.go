// Package registryarchive exports one linux/arm64 registry image as an OCI
// layout tar, the archive form that ImportImage accepts.
package registryarchive

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

var (
	ErrPlatformUnavailable = errors.New("image has no linux/arm64 variant")
	ErrTooLarge            = errors.New("image archive exceeds the import size limit")
	// ErrDownload marks a failure after the manifest resolved, while blobs
	// were downloading.
	ErrDownload = errors.New("image content download failed")
)

var arm64 = v1.Platform{OS: "linux", Architecture: "arm64"}

// Archive is a complete OCI layout tar and the identity ImportImage commits to.
type Archive struct {
	Path           string
	ManifestDigest string
	SHA256         []byte
	Size           uint64
}

// Export fetches reference anonymously through transport (nil uses the
// default transport) and writes its archive in directory. The caller owns the
// returned file. A failed export leaves no file behind.
func Export(ctx context.Context, reference, directory string, maxBytes uint64, transport http.RoundTripper) (Archive, error) {
	ref, err := name.ParseReference(reference)
	if err != nil {
		return Archive{}, err
	}
	options := []remote.Option{remote.WithContext(ctx), remote.WithAuth(authn.Anonymous), remote.WithPlatform(arm64)}
	if transport != nil {
		options = append(options, remote.WithTransport(transport))
	}
	image, err := arm64Image(ref, options)
	if err != nil {
		return Archive{}, err
	}
	config, err := image.ConfigFile()
	if err != nil {
		return Archive{}, err
	}
	if !matchesARM64(config.Platform()) {
		return Archive{}, ErrPlatformUnavailable
	}
	manifest, err := image.Manifest()
	if err != nil {
		return Archive{}, err
	}
	// Compressed sizes are a lower bound of the archive; reject before download.
	total := uint64(manifest.Config.Size)
	for _, layer := range manifest.Layers {
		total += uint64(layer.Size)
	}
	if total > maxBytes {
		return Archive{}, ErrTooLarge
	}
	// ARM64 v8 is the baseline platform. Commit to the normalized image while
	// retaining its layers and runtime config.
	if config.Variant == "v8" {
		config = config.DeepCopy()
		config.Variant = ""
		if image, err = mutate.ConfigFile(image, config); err != nil {
			return Archive{}, err
		}
	}
	image, err = normalizeOCI(image)
	if err != nil {
		return Archive{}, err
	}
	digest, err := image.Digest()
	if err != nil {
		return Archive{}, err
	}
	staging, err := os.MkdirTemp(directory, "oci-layout-")
	if err != nil {
		return Archive{}, err
	}
	defer os.RemoveAll(staging)
	index, err := layout.Write(staging, empty.Index)
	if err != nil {
		return Archive{}, err
	}
	if err := index.AppendImage(image, layout.WithPlatform(arm64)); err != nil {
		return Archive{}, fmt.Errorf("%w: %w", ErrDownload, err)
	}
	file, err := os.CreateTemp(directory, "registry-*.tar")
	if err != nil {
		return Archive{}, err
	}
	hash := sha256.New()
	counter := &limitWriter{limit: maxBytes}
	writer := tar.NewWriter(io.MultiWriter(file, hash, counter))
	writeErr := filepath.WalkDir(staging, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(staging, path)
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
		return Archive{}, err
	}
	return Archive{Path: file.Name(), ManifestDigest: digest.String(), SHA256: hash.Sum(nil), Size: counter.written}, nil
}

func arm64Image(ref name.Reference, options []remote.Option) (v1.Image, error) {
	descriptor, err := remote.Get(ref, options...)
	if err != nil {
		return nil, err
	}
	if !descriptor.MediaType.IsIndex() {
		return descriptor.Image()
	}
	index, err := descriptor.ImageIndex()
	if err != nil {
		return nil, err
	}
	manifest, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}
	for _, child := range manifest.Manifests {
		if matchesARM64(child.Platform) {
			return index.Image(child.Digest)
		}
	}
	return nil, ErrPlatformUnavailable
}

func matchesARM64(platform *v1.Platform) bool {
	return platform != nil && platform.OS == arm64.OS && platform.Architecture == arm64.Architecture && (platform.Variant == "" || platform.Variant == "v8")
}

type limitWriter struct {
	limit, written uint64
}

func (writer *limitWriter) Write(data []byte) (int, error) {
	if uint64(len(data)) > writer.limit-writer.written {
		return 0, fmt.Errorf("%w: more than %d bytes", ErrTooLarge, writer.limit)
	}
	writer.written += uint64(len(data))
	return len(data), nil
}
