package storage

import (
	"context"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
)

// signatureStore provides methods to manage manifest signatures in a backend storage driver.
type signatureStore struct {
	repository *repository
	blobStore  *blobStore
}

// All returns a list of signature digests for the specified manifest.
func (ss *signatureStore) All(ctx context.Context, dgst digest.Digest) ([]digest.Digest, error) {
	var signatures []digest.Digest

	lbs := ss.linkedBlobStore(ctx, dgst)
	err := lbs.Enumerate(ctx, func(sigDgst digest.Digest) error {
		signatures = append(signatures, sigDgst)
		return nil
	})

	return signatures, err
}

// Link links signature artifact signatureDgst to the given manifest descriptor.
func (ss *signatureStore) Link(ctx context.Context, manifestDgst digest.Digest, signatureDgst digest.Digest) error {
	lbs := ss.linkedBlobStore(ctx, manifestDgst)

	// Verify the signature manifest exists.
	if _, err := lbs.Stat(ctx, signatureDgst); err != nil {
		return err
	}

	if err := lbs.linkBlob(ctx, distribution.Descriptor{Digest: signatureDgst}); err != nil {
		return err
	}

	return nil
}

// linkedBlobStore returns the linkedBlobStore for manifest signatures.
// Using this ensures the links are managed via the same code path.
func (ss *signatureStore) linkedBlobStore(ctx context.Context, manifestDgst digest.Digest) *linkedBlobStore {
	return &linkedBlobStore{
		blobStore:  ss.blobStore,
		repository: ss.repository,
		ctx:        ctx,
		linkPathFns: []linkPathFunc{func(name string, signatureDgst digest.Digest) (string, error) {
			return pathFor(manifestSignatureLinkPathSpec{
				name:      name,
				revision:  manifestDgst,
				signature: signatureDgst,
			})
		}},
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
