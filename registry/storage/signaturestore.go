package storage

import (
	"context"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// signatureStore provides methods to manage manifest signatures in a backend storage driver.
type signatureStore struct {
	repository *repository
	blobStore  *blobStore
}

// All returns a list of signature digests that are linked to the specified manifest.
func (ss *signatureStore) All(ctx context.Context, manifestDigest digest.Digest) ([]digest.Digest, error) {
	signatures := []digest.Digest{}

	lbs := ss.linkedBlobStore(ctx, manifestDigest)

	err := lbs.Enumerate(ctx, func(sigDgst digest.Digest) error {
		signatures = append(signatures, sigDgst)
		return nil
	})
	if err == nil {
		return signatures, nil
	}

	switch err.(type) {
	case driver.PathNotFoundError:
		return signatures, nil
	}

	return signatures, err
}

// Link links signature artifact signatureDgst to the given manifest descriptor.
func (ss *signatureStore) Link(ctx context.Context, manifestDgst digest.Digest, signatureDgst digest.Digest) error {
	lbs := ss.linkedBlobStore(ctx, manifestDgst)

	if err := lbs.linkBlob(ctx, distribution.Descriptor{Digest: signatureDgst}); err != nil {
		return err
	}

	return nil
}

// linkedBlobStore returns the linkedBlobStore for signatures linked to the specifed manifest.
// Using this ensures the links are managed via a common code path.
func (ss *signatureStore) linkedBlobStore(ctx context.Context, manifestDgst digest.Digest) *linkedBlobStore {
	return &linkedBlobStore{
		blobStore:  ss.blobStore,
		repository: ss.repository,
		ctx:        ctx,
		linkPathFns: []linkPathFunc{func(name string, signatureDgst digest.Digest) (string, error) {
			// This function resolves the set of linked signatures for the manifest.
			return pathFor(manifestSignatureLinkPathSpec{
				name:      name,
				revision:  manifestDgst,
				signature: signatureDgst,
			})
		}},
		// This is the root directory under which all signatures linked to the manifest are found.
		linkDirectoryPathSpec: manifestSignaturesPathSpec{
			name:     ss.repository.Named().Name(),
			revision: manifestDgst,
		},
		blobAccessController: &linkedBlobStatter{
			blobStore:   ss.blobStore,
			repository:  ss.repository,
			linkPathFns: []linkPathFunc{manifestRevisionLinkPath},
		},
	}
}
