package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/docker/distribution/registry/api/errcode"

	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// signatureDispatcher takes the request context and builds the
// appropriate handler for handling signature requests.
func signatureDispatcher(ctx *Context, r *http.Request) http.Handler {
	signatureHandler := &signatureHandler{
		Context: ctx,
	}
	reference := getReference(ctx)
	signatureHandler.Revision, _ = digest.Parse(reference)

	mhandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(signatureHandler.GetSignatures),
	}

	if !ctx.readOnly {
		signature := getSignature(ctx)
		if signature != "" {
			signatureHandler.Signature, _ = digest.Parse(signature)
		}
		mhandler["PUT"] = http.HandlerFunc(signatureHandler.PutSignature)
	}

	return mhandler
}

type allSignatures struct {
	Digest     string          `json:"digest"`
	Signatures []digest.Digest `json:"signatures"`
}

// signatureHandler handles http operations on image manifest signatures.
type signatureHandler struct {
	*Context
	Revision  digest.Digest
	Signature digest.Digest
}

// GetSignatures gets all signatures on the given manifest.
func (sh *signatureHandler) GetSignatures(w http.ResponseWriter, r *http.Request) {
	signatures := sh.Repository.Signatures(sh.Context)
	dgsts, err := signatures.All(sh.Context.Context, sh.Revision)
	if err != nil {
		sh.Errors = append(sh.Errors, err)
		return
	}
	if dgsts == nil {
		dgsts = []digest.Digest{}
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(allSignatures{Digest: string(sh.Revision), Signatures: dgsts}); err != nil {
		sh.Errors = append(sh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}

// PutSignature links a signature to the given manifest.
func (sh *signatureHandler) PutSignature(w http.ResponseWriter, r *http.Request) {
	signatures := sh.Repository.Signatures(sh.Context)

	err := signatures.Link(sh.Context, sh.Revision, sh.Signature)
	if err != nil {
		sh.Errors = append(sh.Errors, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
