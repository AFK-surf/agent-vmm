//go:build linux

package control

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/plugins/content/local"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestMarkImageGraphCollectsParallelSiblings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := local.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	config := []byte(`{"architecture":"arm64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}`)
	configDescriptor := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(config),
		Size:      int64(len(config)),
	}
	if err := content.WriteBlob(ctx, store, "config", bytes.NewReader(config), configDescriptor); err != nil {
		t.Fatal(err)
	}

	layers := make([]ocispec.Descriptor, 64)
	for index := range layers {
		payload := []byte(fmt.Sprintf("layer-%d", index))
		layers[index] = ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageLayer,
			Digest:    digest.FromBytes(payload),
			Size:      int64(len(payload)),
		}
		if err := content.WriteBlob(ctx, store, fmt.Sprintf("layer-%d", index), bytes.NewReader(payload), layers[index]); err != nil {
			t.Fatal(err)
		}
	}

	manifest, err := json.Marshal(ocispec.Manifest{
		Config: configDescriptor,
		Layers: layers,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDescriptor := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifest),
		Size:      int64(len(manifest)),
	}
	if err := content.WriteBlob(ctx, store, "manifest", bytes.NewReader(manifest), manifestDescriptor); err != nil {
		t.Fatal(err)
	}

	references := make(map[digest.Digest]bool)
	if err := markImageGraph(ctx, store, manifestDescriptor, references); err != nil {
		t.Fatal(err)
	}
	if got, want := len(references), len(layers)+2; got != want {
		t.Fatalf("reference count = %d, want %d", got, want)
	}
}
