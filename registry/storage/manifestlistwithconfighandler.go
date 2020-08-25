package storage

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/manifestlistwithconfig"
	"github.com/opencontainers/go-digest"
)

// manifestListWithConfigHandler is a ManifestHandler that covers schema2 manifest lists with config.
type manifestListWithConfigHandler struct {
	repository                distribution.Repository
	blobStore                 distribution.BlobStore
	ctx                       context.Context
	referrerMetadataStoreFunc func(manifestDigest digest.Digest, metadataMediaType string) *linkedBlobStore
}

func (ms *manifestListWithConfigHandler) Unmarshal(ctx context.Context, dgst digest.Digest, content []byte) (distribution.Manifest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestListWithConfigHandler).Unmarshal")

	m := &manifestlistwithconfig.DeserializedManifestListWithConfig{}
	if err := m.UnmarshalJSON(content); err != nil {
		return nil, err
	}

	return m, nil
}

func (ms *manifestListWithConfigHandler) Put(ctx context.Context, manifestList distribution.Manifest, skipDependencyVerification bool) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestListWithConfigHandler).Put")

	m, ok := manifestList.(*manifestlistwithconfig.DeserializedManifestListWithConfig)
	if !ok {
		return "", fmt.Errorf("wrong type put to manifestListWithConfigHandler: %T", manifestList)
	}

	if err := ms.verifyManifest(ms.ctx, *m, skipDependencyVerification); err != nil {
		return "", err
	}

	mt, payload, err := m.Payload()
	if err != nil {
		return "", err
	}

	revision, err := ms.blobStore.Put(ctx, mt, payload)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error putting payload into blobstore: %v", err)
		return "", err
	}

	err = ms.linkReferrerMetadata(ctx, *m)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error linking referrer metadata: %v", err)
		return "", err
	}

	return revision.Digest, nil
}

// verifyManifest ensures that the manifest content is valid from the
// perspective of the registry. As a policy, the registry only tries to
// store valid content, leaving trust policies of that content up to
// consumers.
func (ms *manifestListWithConfigHandler) verifyManifest(ctx context.Context, mnfst manifestlistwithconfig.DeserializedManifestListWithConfig, skipDependencyVerification bool) error {
	var errs distribution.ErrManifestVerification

	if mnfst.SchemaVersion != 2 {
		return fmt.Errorf("unrecognized manifest list schema version %d", mnfst.SchemaVersion)
	}

	if !skipDependencyVerification {
		// This manifest service is different from the blob service
		// returned by Blob. It uses a linked blob store to ensure that
		// only manifests are accessible.

		manifestService, err := ms.repository.Manifests(ctx)
		if err != nil {
			return err
		}

		for _, manifestDescriptor := range mnfst.References() {
			exists, err := manifestService.Exists(ctx, manifestDescriptor.Digest)
			if err != nil && err != distribution.ErrBlobUnknown {
				errs = append(errs, err)
			}
			if err != nil || !exists {
				// On error here, we always append unknown blob errors.
				errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: manifestDescriptor.Digest})
			}
		}

		if configDigest := mnfst.Config.Digest; configDigest != "" {
			blobStore := ms.repository.Blobs(ctx)
			_, err = blobStore.Stat(ctx, configDigest)
		}
	}
	if len(errs) != 0 {
		return errs
	}

	return nil
}

func (ms *manifestListWithConfigHandler) linkReferrerMetadata(ctx context.Context, mnfst manifestlistwithconfig.DeserializedManifestListWithConfig) error {
	// Record link as referrer metadata.
	for _, manifestDescriptor := range mnfst.References() {
		if err := ms.referrerMetadataStoreFunc(manifestDescriptor.Digest, mnfst.Config.MediaType).linkBlob(ctx, distribution.Descriptor{Digest: mnfst.Config.Digest}); err != nil {
			return err
		}
	}
	return nil
}
