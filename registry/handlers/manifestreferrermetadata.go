package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// manifestReferrerMetadataDispatcher takes the request context and builds the
// appropriate handler for handling manifest referrer metadata requests.
func manifestReferrerMetadataDispatcher(ctx *Context, r *http.Request) http.Handler {
	handler := &manifestReferrerMetadataHandler{
		Context: ctx,
	}
	reference := getReference(ctx)
	dgst, err := digest.Parse(reference)
	if err != nil {
		// We just have a tag
		handler.Tag = reference
	} else {
		handler.Digest = dgst
	}

	mhandler := handlers.MethodHandler{
		"GET": http.HandlerFunc(handler.All),
	}

	return mhandler
}

type manifestReferrerMetadataResponse struct {
	Digest           digest.Digest   `json:"digest"`
	Tag              string          `json:"tag"`
	ReferrerMetadata []digest.Digest `json:"referrerMetadata"`
	NextLink         string          `json:"nextLink"`
}

// manifestReferrerMetadataHandler handles http operations on image manifest referrer metadata.
type manifestReferrerMetadataHandler struct {
	*Context

	// One of tag or digest gets set, depending on what is present in context.
	Tag    string
	Digest digest.Digest
}

// All fetches the list of manifest referrer metadata objects, filtered by mediaType.
func (mrmh *manifestReferrerMetadataHandler) All(w http.ResponseWriter, r *http.Request) {
	dcontext.GetLogger(mrmh).Debug("All")

	metadataMediaType := r.FormValue("metadatamediatype")
	if metadataMediaType == "" {
		mrmh.Errors = append(mrmh.Errors, v2.ErrorCodeManifestMetadataMediaTypeUnknown)
		return
	}

	if mrmh.Tag != "" {
		tags := mrmh.Repository.Tags(mrmh)
		desc, err := tags.Get(mrmh, mrmh.Tag)
		if err != nil {
			if _, ok := err.(distribution.ErrTagUnknown); ok {
				mrmh.Errors = append(mrmh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(err))
			} else {
				mrmh.Errors = append(mrmh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			}
			return
		}
		mrmh.Digest = desc.Digest
	}

	manifestService, err := mrmh.Repository.Manifests(mrmh)
	if err != nil {
		mrmh.Errors = append(mrmh.Errors, err)
		return
	}

	referrerMetadata, err := manifestService.ReferrerMetadata(mrmh.Context, mrmh.Digest, metadataMediaType)
	if err != nil {
		if _, ok := err.(distribution.ErrManifestUnknownRevision); ok {
			mrmh.Errors = append(mrmh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(err))
		} else {
			mrmh.Errors = append(mrmh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		}
		return
	}

	if referrerMetadata == nil {
		referrerMetadata = []digest.Digest{}
	}

	response := manifestReferrerMetadataResponse{ReferrerMetadata: referrerMetadata, NextLink: "not implemented"}
	if mrmh.Tag != "" {
		response.Tag = mrmh.Tag
	} else {
		response.Digest = mrmh.Digest
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	if err = enc.Encode(response); err != nil {
		mrmh.Errors = append(mrmh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}
