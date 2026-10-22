package registryarchive

import (
	"archive/tar"
	"bytes"
	"io"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestNormalizeOCIRetainsLayerBytesAndRuntimeConfig(t *testing.T) {
	var tarBytes bytes.Buffer
	tw := tar.NewWriter(&tarBytes)
	payload := []byte("image file contents")
	if err := tw.WriteHeader(&tar.Header{Name: "file", Mode: 0600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name               string
		source, normalized types.MediaType
	}{
		{"gzip", types.DockerLayer, types.OCILayer},
		{"uncompressed", types.DockerUncompressedLayer, types.OCIUncompressedLayer},
		{"foreign-gzip", types.DockerForeignLayer, types.OCIRestrictedLayer},
	} {
		t.Run(test.name, func(t *testing.T) {
			var layer v1.Layer
			var err error
			if test.source == types.DockerUncompressedLayer {
				layer = static.NewLayer(tarBytes.Bytes(), test.source)
			} else {
				layer, err = tarball.LayerFromReader(bytes.NewReader(tarBytes.Bytes()), tarball.WithMediaType(test.source))
				if err != nil {
					t.Fatal(err)
				}
			}
			source, err := mutate.AppendLayers(empty.Image, layer)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := source.ConfigFile()
			if err != nil {
				t.Fatal(err)
			}
			cfg = cfg.DeepCopy()
			cfg.OS = "linux"
			cfg.Architecture = "arm64"
			cfg.Config.Entrypoint = []string{"/bin/app"}
			cfg.Config.Cmd = []string{"argument"}
			cfg.Config.Env = []string{"KEY=value"}
			cfg.Config.User = "1000:1000"
			source, err = mutate.ConfigFile(source, cfg)
			if err != nil {
				t.Fatal(err)
			}
			normalized, err := normalizeOCI(source)
			if err != nil {
				t.Fatal(err)
			}
			before, err := source.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			after, err := normalized.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			if after.MediaType != types.OCIManifestSchema1 || after.Config.MediaType != types.OCIConfigJSON || after.Layers[0].MediaType != test.normalized {
				t.Fatalf("wrong canonical types: %+v", after)
			}
			if after.Config.Digest != before.Config.Digest || after.Config.Size != before.Config.Size || after.Layers[0].Digest != before.Layers[0].Digest || after.Layers[0].Size != before.Layers[0].Size {
				t.Fatal("metadata conversion changed content identity")
			}
			beforeConfig, _ := source.RawConfigFile()
			afterConfig, _ := normalized.RawConfigFile()
			if !bytes.Equal(beforeConfig, afterConfig) {
				t.Fatal("runtime config changed")
			}
			layers, err := normalized.Layers()
			if err != nil {
				t.Fatal(err)
			}
			gotType, err := layers[0].MediaType()
			if err != nil || gotType != test.normalized {
				t.Fatalf("layer interface media type %q, %v", gotType, err)
			}
			original, err := layer.Compressed()
			if err != nil {
				t.Fatal(err)
			}
			defer original.Close()
			originalBytes, err := io.ReadAll(original)
			if err != nil {
				t.Fatal(err)
			}
			converted, err := layers[0].Compressed()
			if err != nil {
				t.Fatal(err)
			}
			defer converted.Close()
			convertedBytes, err := io.ReadAll(converted)
			if err != nil || !bytes.Equal(originalBytes, convertedBytes) {
				t.Fatalf("compressed layer bytes changed: %v", err)
			}
			uncompressed, err := layers[0].Uncompressed()
			if err != nil {
				t.Fatal(err)
			}
			defer uncompressed.Close()
			got, err := io.ReadAll(uncompressed)
			if err != nil || !bytes.Equal(got, tarBytes.Bytes()) {
				t.Fatalf("layer compression semantics changed: %v", err)
			}
			again, err := normalizeOCI(normalized)
			if err != nil {
				t.Fatal(err)
			}
			firstDigest, _ := normalized.Digest()
			secondDigest, _ := again.Digest()
			if firstDigest != secondDigest {
				t.Fatal("OCI normalization changed an already canonical digest")
			}
		})
	}
}
