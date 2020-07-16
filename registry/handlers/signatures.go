package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"

	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// signatureDispatcher takes the request context and builds the
// appropriate handler for handling signature requests.
func signatureDispatcher(ctx *Context, r *http.Request) http.Handler {
	signatureHandler := &signatureHandler{
		Context: ctx,
	}
	signatureHandler.reference = getReference(ctx)

	mhandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(signatureHandler.GetSignatures),
	}

	if !ctx.readOnly {
		signatureHandler.signature = getSignature(ctx)
		mhandler["PUT"] = http.HandlerFunc(signatureHandler.PutSignature)
	}

	return mhandler
}

// signaturesAPIResponse is the object returned when fetching all signatures for a manifest.
type signaturesAPIResponse struct {
	Digest     string          `json:"digest"`
	Signatures []digest.Digest `json:"signatures"`
}

// signatureHandler handles http operations on image manifest signatures.
type signatureHandler struct {
	*Context
	reference string
	signature string
}

// GetSignatures gets all signatures on the given manifest.
func (sh *signatureHandler) GetSignatures(w http.ResponseWriter, r *http.Request) {
	revision, err := digest.Parse(sh.reference)
	if err != nil {
		sh.Errors = append(sh.Errors, v2.ErrorCodeDigestInvalid)
		return
	}

	if !sh.checkManifests(revision) {
		return
	}

	// TODO avtakkar support pagination (refer to catalog handler)
	signatures := sh.Repository.Signatures(sh.Context)
	signatureDigests, err := signatures.All(sh.Context.Context, revision)
	if err != nil {
		sh.Errors = append(sh.Errors, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err := enc.Encode(signaturesAPIResponse{Digest: string(revision), Signatures: signatureDigests}); err != nil {
		sh.Errors = append(sh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}

// PutSignature links a signature to the given manifest.
func (sh *signatureHandler) PutSignature(w http.ResponseWriter, r *http.Request) {
	revision, err := digest.Parse(sh.reference)
	if err != nil {
		sh.Errors = append(sh.Errors, v2.ErrorCodeDigestInvalid.WithDetail("the manifest digest is invalid"))
		return
	}

	signatureDigest, err := digest.Parse(sh.signature)
	if err != nil {
		sh.Errors = append(sh.Errors, v2.ErrorCodeDigestInvalid.WithDetail("the signature digest is inavlid"))
		return
	}

	if !sh.checkManifests(revision, signatureDigest) {
		return
	}

	signatures := sh.Repository.Signatures(sh.Context)

	err = signatures.Link(sh.Context, revision, signatureDigest)
	if err != nil {
		sh.Errors = append(sh.Errors, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// checkManifests checks that the given manifests exist in the manifest store.
func (sh *signatureHandler) checkManifests(dgsts ...digest.Digest) bool {
	manifests, err := sh.Repository.Manifests(sh.Context)
	if err != nil {
		sh.Errors = append(sh.Errors, err)
		return false
	}

	for _, dgst := range dgsts {
		if exists, err := manifests.Exists(sh.Context, dgst); err != nil {
			sh.Errors = append(sh.Errors, err)
			return false
		} else if !exists {
			sh.Errors = append(sh.Errors, v2.ErrorCodeManifestUnknown.WithDetail("revision: "+dgst))
			return false
		}
	}

	return true
}
