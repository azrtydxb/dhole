package plugins

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
)

// casMediaType describes what a `cas://` artifact's bytes are: an opaque blob.
// The CAS stores content, not a manifest, so there is nothing more specific to
// say without inspecting bytes we deliberately do not interpret here.
const casMediaType = "application/vnd.dhole.plugin.blob"

// hexLen is the length of a sha256 digest in hex. A reference whose hex is any
// other length is malformed, not merely absent — and saying so keeps a typo
// from being reported as a missing plugin.
const hexLen = 64

// casBackend serves `cas://sha256:<hex>` references from the tenant's
// content-addressed store.
//
// There is no tag to resolve here — a CAS reference is already a digest — so
// resolve does the one thing it can usefully do beyond parsing: confirm the
// blob is present for this tenant, which is what turns "this reference is
// well-formed" into "this artifact exists where you can fetch it".
type casBackend struct {
	store cas.Store
}

func (c *casBackend) resolve(ctx context.Context, tenantID, ref, rest string) (Artifact, error) {
	d, err := parseCASDigest(ref, rest)
	if err != nil {
		return Artifact{}, err
	}

	ok, err := c.store.Has(ctx, tenantID, d)
	if err != nil {
		return Artifact{}, fmt.Errorf("plugins: cas resolve %q: %w", ref, err)
	}
	if !ok {
		return Artifact{}, fmt.Errorf("%w: cas resolve %q: not stored for this tenant", ErrNotFound, ref)
	}

	return Artifact{Ref: ref, Digest: d, Scheme: SchemeCAS, MediaType: casMediaType}, nil
}

// fetch opens the blob by the digest the artifact carries. The reference's own
// text is not consulted: the recorded digest is the authority, exactly as on
// the OCI path.
func (c *casBackend) fetch(ctx context.Context, tenantID string, a Artifact) (io.ReadCloser, error) {
	rc, err := c.store.Get(ctx, tenantID, a.Digest)
	switch {
	case errors.Is(err, cas.ErrNotFound):
		// Translated to this package's sentinel so a caller holding a plugins
		// Artifact never has to know which store backed it — and so a miss
		// stays distinguishable from a store that failed to answer.
		return nil, fmt.Errorf("%w: cas fetch %q: %w", ErrNotFound, a.Ref, err)
	case err != nil:
		return nil, fmt.Errorf("plugins: cas fetch %q: %w", a.Ref, err)
	}
	return rc, nil
}

// parseCASDigest accepts `sha256:<hex>` and the bare `<hex>` shorthand, which
// ADR 0011 writes as `cas://<hash>`. Anything else is a malformed reference:
// guessing at an algorithm would address a different blob than the author meant.
func parseCASDigest(ref, rest string) (*dholev1.Digest, error) {
	algo, hex, found := strings.Cut(rest, ":")
	if !found {
		algo, hex = algoSHA256, rest
	}
	if algo != algoSHA256 {
		return nil, fmt.Errorf("%w %q: unsupported digest algorithm %q, %s", ErrMalformedRef, ref, algo, refForm)
	}
	if len(hex) != hexLen || strings.TrimLeft(hex, "0123456789abcdef") != "" {
		return nil, fmt.Errorf("%w %q: %q is not a lowercase 64-character sha256 hex digest, %s", ErrMalformedRef, ref, hex, refForm)
	}
	return newDigest(hex), nil
}
