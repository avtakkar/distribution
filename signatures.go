package distribution

import (
	"context"

	"github.com/opencontainers/go-digest"
)

// SignatureService provides access to information about manifest signatures.
type SignatureService interface {
	// All returns the list of all signatures linked to the manifest dgst.
	All(ctx context.Context, dgst digest.Digest) ([]digest.Digest, error)

	// Link links a manifest specified by signatureDgst in the signature store of manifest manifestDgst.
	Link(ctx context.Context, manifestDgst digest.Digest, signatureDgst digest.Digest) error
}
