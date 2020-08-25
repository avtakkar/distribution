package manifestlistwithconfig

import (
	"encoding/json"
	"fmt"

	ociSpecsV2 "github.com/avtakkar/image-spec/specs-go/v2"
	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// OCIInfexWithConfigSchemaVersion test
var OCIInfexWithConfigSchemaVersion = manifest.Versioned{
	SchemaVersion: 2,
	MediaType:     ociSpecsV2.MediaTypeImageIndex,
}

func init() {
	imageIndexWithConfigFunc := func(b []byte) (distribution.Manifest, distribution.Descriptor, error) {
		m := new(DeserializedManifestListWithConfig)
		err := m.UnmarshalJSON(b)
		if err != nil {
			return nil, distribution.Descriptor{}, err
		}

		if m.MediaType != "" && m.MediaType != ociSpecsV2.MediaTypeImageIndex {
			err = fmt.Errorf("if present, mediaType in image index should be '%s' not '%s'",
			ociSpecsV2.MediaTypeImageIndex, m.MediaType)

			return nil, distribution.Descriptor{}, err
		}

		dgst := digest.FromBytes(b)
		return m, distribution.Descriptor{Digest: dgst, Size: int64(len(b)), MediaType: ociSpecsV2.MediaTypeImageIndex}, err
	}
	err := distribution.RegisterManifestSchema(ociSpecsV2.MediaTypeImageIndex, imageIndexWithConfigFunc)
	if err != nil {
		panic(fmt.Sprintf("Unable to register OCI Image Index: %s", err))
	}
}

// ManifestListWithConfig references manifests for various platforms with config.
type ManifestListWithConfig struct {
	manifestlist.ManifestList

	// Config references the image configuration as a blob.
	Config distribution.Descriptor `json:"config"`
}

// DeserializedManifestListWithConfig wraps ManifestListWithConfig with a copy of the original
// JSON.
type DeserializedManifestListWithConfig struct {
	ManifestListWithConfig

	// canonical is the canonical byte representation of the Manifest.
	canonical []byte
}

// UnmarshalJSON populates a new ManifestList struct from JSON data.
func (m *DeserializedManifestListWithConfig) UnmarshalJSON(b []byte) error {
	m.canonical = make([]byte, len(b))
	// store manifest list in canonical
	copy(m.canonical, b)

	// Unmarshal canonical JSON into ManifestList object
	var manifestListWithConfig ManifestListWithConfig
	if err := json.Unmarshal(m.canonical, &manifestListWithConfig); err != nil {
		return err
	}

	m.ManifestListWithConfig = manifestListWithConfig

	return nil
}

// Payload returns the raw content of the manifest list. The contents can be
// used to calculate the content identifier.
func (m DeserializedManifestListWithConfig) Payload() (string, []byte, error) {
	var mediaType string
	if m.MediaType == "" {
		mediaType = v1.MediaTypeImageIndex
	} else {
		mediaType = m.MediaType
	}

	return mediaType, m.canonical, nil
}
