// Package plugins resolves plugin references to digest-addressed artifacts and
// fetches their bytes.
//
// ADR 0011 makes a plugin reference a scheme-addressed URI —
// `oci://registry/repo:tag` or `cas://sha256:<hex>` — behind one resolver, so
// nothing above this package has to know which distribution path an artifact
// came from. The two schemes therefore resolve to the same Artifact shape and
// fetch through the same call.
//
// The property the package is built around: a tag is resolved to a digest
// exactly once, when a definition is saved, and never again. Fetch takes the
// digest that resolution recorded and ignores whatever the tag points at now.
// Resolving a tag at dispatch time would mean a re-run of a year-old pipeline
// silently executing different code than it ran the first time, and every cache
// key folded over that reference would be a lie.
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

// Scheme is the distribution path an artifact came from. It is recorded on the
// artifact so Fetch can dispatch without re-parsing, and so a stored artifact
// says plainly where its bytes live.
type Scheme string

const (
	// SchemeOCI addresses an artifact in an OCI registry.
	SchemeOCI Scheme = "oci"
	// SchemeCAS addresses an artifact in the tenant's content-addressed store.
	SchemeCAS Scheme = "cas"
)

// refForm is the whole accepted grammar, quoted verbatim in every malformed-ref
// error: the reader is holding a reference they believed was valid, so the
// message has to show the shapes that are.
const refForm = `expected "oci://<registry>/<repository>:<tag>", "oci://<registry>/<repository>@sha256:<hex>" or "cas://sha256:<hex>"`

var (
	// ErrTenantRequired reports a call made without a tenant scope. There is no
	// unscoped read: an unscoped fetch would be the one call site that reads
	// across tenants the day a second tenant exists.
	ErrTenantRequired = errors.New("plugins: tenant scope required")

	// ErrMalformedRef reports a reference that is not a plugin reference at all.
	ErrMalformedRef = errors.New("plugins: malformed reference")

	// ErrUnknownScheme reports a scheme this resolver does not serve. It is a
	// distinct error from a malformed reference because the reference is
	// well-formed — it just names somewhere we refuse to fetch from. An unknown
	// scheme is never defaulted to oci or cas: that would fetch code from
	// somewhere the author never named.
	ErrUnknownScheme = errors.New("plugins: unknown reference scheme")

	// ErrUnresolvedTag reports a dispatch-time fetch of a reference whose digest
	// was never recorded. This is the refusal that keeps a re-run reproducible.
	ErrUnresolvedTag = errors.New("plugins: unresolved tag")

	// ErrNotFound reports that the artifact does not exist where the reference
	// says it does. Callers match it with errors.Is to tell a plugin that was
	// never mirrored — an operator problem — apart from a store or registry that
	// is unreachable, which is a different one.
	ErrNotFound = errors.New("plugins: artifact not found")
)

// Artifact is a resolved plugin: a reference, the digest it resolved to, and
// enough to fetch it again without consulting a mutable name.
//
// The Ref is kept as the author wrote it, tag and all, because that is what a
// human recognises in a lockfile and in an audit trail. It is never what Fetch
// pulls by — Digest is.
//
// Digest travels as *dholev1.Digest, not by value: the generated protobuf
// message embeds a MessageState, so copying one is a vet copylocks error.
type Artifact struct {
	// Ref is the original scheme-addressed reference.
	Ref string
	// Digest is the content digest resolution pinned. Never nil on an artifact
	// returned by Resolve.
	Digest *dholev1.Digest
	// Scheme is the distribution path Ref names.
	Scheme Scheme
	// MediaType describes the bytes Fetch returns.
	MediaType string
}

// Resolver turns references into digest-pinned artifacts and fetches their
// bytes. Resolve is a save-time operation and may consult mutable names; Fetch
// is a dispatch-time operation and must not.
type Resolver interface {
	// Resolve pins ref to a digest. For an `oci://` reference carrying a tag
	// this is the one moment the tag is read.
	Resolve(ctx context.Context, tenantID, ref string) (Artifact, error)

	// Fetch opens the artifact's bytes, addressed by a.Digest alone. It fails
	// with ErrUnresolvedTag when no digest was recorded rather than falling
	// back to the reference's tag.
	Fetch(ctx context.Context, tenantID string, a Artifact) (io.ReadCloser, error)
}

// resolver dispatches on scheme to the two backends.
type resolver struct {
	oci *ociBackend
	cas *casBackend
}

// Option configures a resolver.
type Option func(*resolver)

// WithInsecureRegistries permits plain HTTP to the named registry hosts
// (`host` or `host:port`). It exists for the test registry and for airgapped
// deployments that terminate TLS elsewhere; a host not named here is contacted
// over HTTPS.
func WithInsecureRegistries(hosts ...string) Option {
	return func(r *resolver) {
		for _, h := range hosts {
			if h != "" {
				r.oci.insecureHosts[h] = struct{}{}
			}
		}
	}
}

// NewResolver returns a Resolver reading `cas://` artifacts from store and
// `oci://` artifacts from the registry each reference names.
func NewResolver(store cas.Store, opts ...Option) Resolver {
	r := &resolver{
		oci: &ociBackend{insecureHosts: map[string]struct{}{}},
		cas: &casBackend{store: store},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Resolve dispatches on the reference's scheme.
func (r *resolver) Resolve(ctx context.Context, tenantID, ref string) (Artifact, error) {
	if err := requireTenant(tenantID); err != nil {
		return Artifact{}, err
	}
	scheme, rest, err := splitScheme(ref)
	if err != nil {
		return Artifact{}, err
	}
	switch scheme {
	case SchemeOCI:
		return r.oci.resolve(ctx, ref, rest)
	case SchemeCAS:
		return r.cas.resolve(ctx, tenantID, ref, rest)
	default:
		return Artifact{}, fmt.Errorf("%w %q: %s", ErrUnknownScheme, scheme, refForm)
	}
}

// Fetch opens the artifact by its recorded digest. Nothing here reads a tag.
func (r *resolver) Fetch(ctx context.Context, tenantID string, a Artifact) (io.ReadCloser, error) {
	if err := requireTenant(tenantID); err != nil {
		return nil, err
	}
	if a.Digest == nil || a.Digest.GetHex() == "" {
		return nil, fmt.Errorf("%w: %q has no recorded digest; a reference is pinned once when the definition is saved and only digests are dispatched", ErrUnresolvedTag, a.Ref)
	}
	switch a.Scheme {
	case SchemeOCI:
		return r.oci.fetch(ctx, a)
	case SchemeCAS:
		return r.cas.fetch(ctx, tenantID, a)
	default:
		return nil, fmt.Errorf("%w %q: %s", ErrUnknownScheme, a.Scheme, refForm)
	}
}

// requireTenant refuses an empty scope rather than substituting a default. A
// default tenant here would be invisible at every call site above.
func requireTenant(tenantID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return ErrTenantRequired
	}
	return nil
}

// splitScheme separates the scheme from the remainder without accepting a
// reference that is merely scheme-shaped: an empty remainder names nothing.
func splitScheme(ref string) (Scheme, string, error) {
	scheme, rest, found := strings.Cut(ref, "://")
	if !found || scheme == "" {
		return "", "", fmt.Errorf("%w %q: %s", ErrMalformedRef, ref, refForm)
	}
	if strings.TrimSpace(rest) == "" {
		return "", "", fmt.Errorf("%w %q: nothing after the scheme, %s", ErrMalformedRef, ref, refForm)
	}
	return Scheme(scheme), rest, nil
}

// newDigest is the one place a hex string becomes a protobuf digest, so the
// algorithm is recorded rather than assumed by each caller.
func newDigest(hex string) *dholev1.Digest {
	return &dholev1.Digest{Algo: algoSHA256, Hex: hex}
}

// NewDigest builds a sha256 *dholev1.Digest from its hex. Exported for callers
// reconstructing an artifact from storage, which is how a saved revision comes
// back before dispatch.
func NewDigest(hex string) *dholev1.Digest { return newDigest(hex) }
