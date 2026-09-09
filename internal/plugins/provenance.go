package plugins

import (
	"context"
	"errors"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// WHERE A DISPATCH-TIME POLICY DECISION GETS ITS SUPPLY-CHAIN FACTS.
//
// ADR 0012 has policy consulted by the scheduler as well as at definition-save
// time, and two of its inputs — `signed` and `upstream` — exist only for the
// dispatch half. At save time there is nothing to answer them with: the
// reference has not been pinned to a digest, nothing has been mirrored, and no
// signature has been checked. By dispatch all three have happened, and this is
// what reads them back.
//
// Both answers are conservative in the same direction. A reference this
// deployment does not federate, an upstream nobody registered, a digest the
// lockfile never pinned, a signature store that cannot answer: each yields
// "not signed", never an error the caller might mistake for permission and
// never a guessed upstream. The caller (the scheduler) hands the pair to
// policy, and a tier that requires a signature refuses what this could not
// vouch for.

// Provenance answers what the artifact behind a plugin reference says about
// itself: whether this tenant holds a signature over the exact pinned bytes
// from a signer IT named for that upstream, and which upstream the bytes came
// from.
//
// It deliberately holds no allowed-signer list of its own. Trust is
// per-upstream (see Upstream.AllowedIdentities), and pooling the lists here
// would mean registering one federation widened the trust of every other.
type Provenance struct {
	sigs Signatures
	ups  Upstreams
}

// NewProvenance returns the provenance source over a tenant's signature store
// and its federation registry.
func NewProvenance(sigs Signatures, ups Upstreams) *Provenance {
	return &Provenance{sigs: sigs, ups: ups}
}

// Provenance reports whether pluginRef — pinned by the run's revision to
// pinnedDigest — carries a signature this tenant accepts, and the URL of the
// upstream it came from.
//
// The digest is the caller's, taken from the lockfile, and the tag is never
// consulted: a signature checked over whatever a tag means at dispatch time
// would be a signature over bytes the revision never approved (ADR 0011).
//
// The signature is re-checked on every call rather than remembered. A key
// compromise is answered by withdrawing the record, and an artifact that
// dispatched an hour ago has to stop dispatching now.
func (p *Provenance) Provenance(
	ctx context.Context, tenantID, pluginRef, pinnedDigest string,
) (bool, string, error) {
	if err := requireTenant(tenantID); err != nil {
		return false, "", err
	}
	if p.sigs == nil || p.ups == nil {
		return false, "", errors.New("plugins: provenance needs a signature store and an upstream registry")
	}

	u, ok, err := p.federation(ctx, tenantID, pluginRef)
	if err != nil {
		return false, "", err
	}
	if !ok {
		// Not a federated plugin: an inline command, a `cas://` artifact, a
		// namespace nobody registered. There is no allowed-signer list to
		// judge it by, so this tenant holds no accepted signature over it —
		// which is the honest answer, and the one a tier that requires
		// signatures refuses.
		return false, "", nil
	}

	digest, ok := parseDigestText(pinnedDigest)
	if !ok {
		// A reference the lockfile does not cover is a reference nothing has
		// pinned. There are no exact bytes to check a signature over.
		return false, u.URL, nil
	}

	err = p.sigs.Verify(ctx, tenantID, digest, u.AllowedIdentities)
	switch {
	case err == nil:
		return true, u.URL, nil
	case errors.Is(err, ErrVerificationFailed):
		// Verify collapses every refusal — including a store that could not
		// be read — into this one error, on purpose: "I could not tell" and
		// "no" are the same answer on the way to running somebody's code.
		return false, u.URL, nil
	default:
		return false, u.URL, err
	}
}

// federation finds the upstream a plugin reference belongs to, and reports
// whether it belongs to one at all.
func (p *Provenance) federation(
	ctx context.Context, tenantID, pluginRef string,
) (Upstream, bool, error) {
	// A scheme-addressed reference is not a federated one. It is separated
	// here rather than left to splitPluginRef, which would read `oci://a/b:v1`
	// as the namespace `oci:` and go looking for an upstream by that name.
	if strings.Contains(pluginRef, "://") {
		return Upstream{}, false, nil
	}
	namespace, _, _, err := splitPluginRef(pluginRef)
	if err != nil {
		return Upstream{}, false, nil
	}
	u, err := p.ups.Upstream(ctx, tenantID, namespace)
	switch {
	case errors.Is(err, ErrUnknownUpstream):
		return Upstream{}, false, nil
	case err != nil:
		return Upstream{}, false, err
	}
	return u, true, nil
}

// parseDigestText reads the "<algo>:<hex>" form a lockfile records, and
// reports whether it was one. A half-populated digest is refused rather than
// completed with a default algorithm: a signature is keyed on the whole thing.
func parseDigestText(text string) (*dholev1.Digest, bool) {
	algo, hex, found := strings.Cut(text, ":")
	if !found || algo == "" || hex == "" {
		return nil, false
	}
	return &dholev1.Digest{Algo: algo, Hex: hex}, true
}
