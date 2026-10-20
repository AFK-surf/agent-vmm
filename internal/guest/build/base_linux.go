//go:build linux

package build

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	guestimage "github.com/AFK-surf/agent-vmm/internal/guest/image"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/images/archive"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func (w *Workspace) prepareBase(ctx context.Context) error {
	if w.header.GetBaseImageReference() == "" {
		return nil
	}
	expected := digest.Digest(w.header.GetBaseManifestDigest())
	source, err := w.builder.client.GetImage(namespaces.WithNamespace(ctx, w.header.GetActorNamespaceId()), w.header.GetBaseImageReference())
	if err != nil {
		return err
	}
	if source.Target().Digest != expected {
		return errors.New("namespace base image digest does not match build contract")
	}
	w.baseRoot = filepath.Join(w.root, "base-image")
	file, err := os.Create(filepath.Join(w.root, "base.tar"))
	if err != nil {
		return err
	}
	defer file.Close()
	physical := namespaces.WithNamespace(ctx, guestimage.MaterializedNamespace)
	if err := w.builder.client.Export(physical, file, archive.WithImages([]coreimages.Image{{Name: "base", Target: source.Target()}}), archive.WithPlatform(platforms.OnlyStrict(ocispec.Platform{OS: "linux", Architecture: "arm64"})), archive.WithSkipDockerManifest()); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := extractBaseLayout(file, w.baseRoot); err != nil {
		return err
	}
	return os.Remove(file.Name())
}

func extractBaseLayout(reader io.Reader, root string) error {
	archive := tar.NewReader(reader)
	var total int64
	for entries := 0; ; entries++ {
		item, err := archive.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if entries >= 10000 {
			return errors.New("base OCI layout has too many entries")
		}
		name := strings.TrimSuffix(item.Name, "/")
		valid := name == "oci-layout" || name == "index.json" || name == "blobs" || name == "blobs/sha256"
		if strings.HasPrefix(name, "blobs/sha256/") {
			value := digest.Digest("sha256:" + strings.TrimPrefix(name, "blobs/sha256/"))
			valid = value.Validate() == nil
		}
		if !valid || (item.Typeflag != tar.TypeDir && item.Typeflag != tar.TypeReg && item.Typeflag != tar.TypeRegA) {
			return errors.New("invalid exported base OCI layout entry")
		}
		destination := filepath.Join(root, filepath.FromSlash(name))
		if item.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(destination, 0755); err != nil {
				return err
			}
			continue
		}
		if item.Size < 0 || item.Size > 8<<30 || total > 8<<30-item.Size {
			return errors.New("base OCI layout exceeds size bound")
		}
		total += item.Size
		if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
			return err
		}
		file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0444)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyN(file, archive, item.Size)
		closeErr := file.Close()
		if err := errors.Join(copyErr, closeErr); err != nil {
			return err
		}
	}
}
