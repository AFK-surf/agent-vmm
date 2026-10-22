package registryarchive

import (
	"encoding/json"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// ociImageCore changes descriptor media types while retaining the compressed blobs.
type ociImageCore struct {
	v1.Image
	manifest   []byte
	layerTypes map[v1.Hash]types.MediaType
}

func (i *ociImageCore) RawManifest() ([]byte, error)        { return i.manifest, nil }
func (i *ociImageCore) MediaType() (types.MediaType, error) { return types.OCIManifestSchema1, nil }
func (i *ociImageCore) LayerByDigest(hash v1.Hash) (partial.CompressedLayer, error) {
	layer, err := i.Image.LayerByDigest(hash)
	if err != nil {
		return nil, err
	}
	return ociLayer{Layer: layer, mediaType: i.layerTypes[hash]}, nil
}

type ociLayer struct {
	v1.Layer
	mediaType types.MediaType
}

func (l ociLayer) MediaType() (types.MediaType, error) { return l.mediaType, nil }

func normalizeOCI(image v1.Image) (v1.Image, error) {
	manifest, err := image.Manifest()
	if err != nil {
		return nil, err
	}
	manifest = manifest.DeepCopy()
	changed := false
	if manifest.MediaType == types.DockerManifestSchema2 {
		manifest.MediaType = types.OCIManifestSchema1
		changed = true
	}
	if manifest.Config.MediaType == types.DockerConfigJSON {
		manifest.Config.MediaType = types.OCIConfigJSON
		changed = true
	}
	layerTypes := make(map[v1.Hash]types.MediaType, len(manifest.Layers))
	for position := range manifest.Layers {
		layer := &manifest.Layers[position]
		switch layer.MediaType {
		case types.DockerLayer:
			layer.MediaType = types.OCILayer
			changed = true
		case types.DockerUncompressedLayer:
			layer.MediaType = types.OCIUncompressedLayer
			changed = true
		case types.DockerForeignLayer:
			layer.MediaType = types.OCIRestrictedLayer
			changed = true
		}
		layerTypes[layer.Digest] = layer.MediaType
	}
	if !changed {
		return image, nil
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	return partial.CompressedToImage(&ociImageCore{Image: image, manifest: raw, layerTypes: layerTypes})
}
