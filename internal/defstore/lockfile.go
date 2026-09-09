package defstore

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
)

// THE MOMENT A DEFINITION STOPS DEPENDING ON THE OUTSIDE WORLD.
//
// A plugin reference an author writes is usually a tag — `oci://reg/img:v1` —
// and a tag is a mutable pointer somebody else controls. The resolver reads it
// exactly once, here, when the definition is saved (ADR 0011), and records the
// digest it saw. Everything downstream dispatches on that digest: the tag is
// never read again, so retagging :v1 upstream tomorrow changes nothing about
// what an already-saved revision runs, and the cache key folded over the
// lockfile stays a true statement about the code that produced the outputs.
//
// Two rules make that guarantee hold rather than merely describe it:
//
//   - Resolution happens BEFORE anything is written. A revision whose lockfile
//     covers some references and not others looks pinned and is not; that is
//     worse than no lockfile at all, because nothing downstream would ever
//     notice. So a single unresolvable reference fails the whole Save, by name,
//     and leaves the store untouched.
//   - An existing revision is never re-pinned. Saving identical content returns
//     the revision that already exists, with the digests it already recorded —
//     a re-save must not quietly upgrade a year-old revision to whatever its
//     tags mean today.

// Option configures a definition store.
type Option func(*SQLStore)

// WithResolver gives the store the resolver it pins plugin references with at
// save time.
//
// It is an option rather than a constructor argument because a store with no
// resolver is a meaningful configuration — a deployment whose definitions carry
// no plugin references, and the tests of everything in this package that is not
// about plugins. What it must never become is a silent fallback: with a
// resolver configured, an unresolvable reference fails the save.
func WithResolver(r plugins.Resolver) Option {
	return func(s *SQLStore) { s.resolver = r }
}

// ResolveLockfile pins every plugin reference in p to a digest, returning the
// map from reference to digest that the revision stores.
//
// The digest is recorded as `<algo>:<hex>`, the same text a registry and a
// `cas://` reference use, so a human reading a lockfile beside a registry UI is
// comparing like with like and no caller has to guess an algorithm.
//
// A reference appearing on two steps is one artifact and is resolved once: a
// second round trip could pin the two steps to different digests if the tag
// moved in between, which is one definition running two versions of the same
// plugin.
//
// A pipeline with no plugin references yields an empty lockfile and no error.
// Nothing to pin is pinned trivially; refusing it would make "nothing to
// resolve" indistinguishable from "resolution failed".
func ResolveLockfile(
	ctx context.Context, tenantID string, p *dholev1.Pipeline, r plugins.Resolver,
) (map[string]string, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	if p == nil {
		return nil, fmt.Errorf("resolve lockfile: nil pipeline")
	}

	refs := pluginRefs(p)
	// Callers range over the lockfile; it is empty, never nil, so an
	// unresolved pipeline and one with nothing to resolve stay distinct.
	lockfile := make(map[string]string, len(refs))
	if len(refs) == 0 {
		return lockfile, nil
	}
	if r == nil {
		return nil, fmt.Errorf("resolve lockfile: %d plugin reference(s) to pin and no resolver configured", len(refs))
	}

	for _, ref := range refs {
		artifact, err := r.Resolve(ctx, tenantID, ref)
		if err != nil {
			// Named, because the operator has to know WHICH reference to fix,
			// and returned before anything is written, because a lockfile
			// covering only the references that happened to resolve would look
			// like a pinned revision.
			return nil, fmt.Errorf("resolve lockfile: plugin %q: %w", ref, err)
		}
		digest := artifact.Digest
		if digest == nil || digest.GetHex() == "" {
			return nil, fmt.Errorf("resolve lockfile: plugin %q resolved to no digest", ref)
		}
		lockfile[ref] = digest.GetAlgo() + ":" + digest.GetHex()
	}
	return lockfile, nil
}

// pluginRefs returns the distinct, non-empty plugin references of p in a
// deterministic order.
//
// Sorted, not in step order: the returned lockfile is a map and so unordered by
// definition, but the ORDER OF RESOLUTION is observable — in the round trips
// made, and in which reference is named when a save fails against a registry
// that is half down. Two saves of the same definition should fail the same way.
func pluginRefs(p *dholev1.Pipeline) []string {
	refs := make([]string, 0, len(p.GetSteps()))
	for _, step := range p.GetSteps() {
		ref := strings.TrimSpace(step.GetPluginRef())
		if ref == "" {
			// A step that declares no plugin has nothing to pin. It is not an
			// error here: what a step must carry is the DAG's business.
			continue
		}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	return slices.Compact(refs)
}

// LockfileChange is one reference whose pinned digest differs between two
// revisions. Old is empty when the reference is newly pinned and New is empty
// when it is gone.
type LockfileChange struct {
	Ref string
	Old string
	New string
}

// LockfileDiff reports what changed between two lockfiles, ordered by
// reference.
//
// An upgrade nobody can see is how a supply chain moves without anyone
// noticing, so the diff exists to be shown to a human at approval time — which
// is why it is ordered rather than handed over as a map, and why a change to a
// reference that survives is one entry carrying both digests instead of an
// unrelated removal and addition.
func LockfileDiff(before, after map[string]string) []LockfileChange {
	refs := make([]string, 0, len(before)+len(after))
	for ref := range before {
		refs = append(refs, ref)
	}
	for ref := range after {
		if _, ok := before[ref]; !ok {
			refs = append(refs, ref)
		}
	}
	sort.Strings(refs)

	changes := make([]LockfileChange, 0, len(refs))
	for _, ref := range refs {
		old, updated := before[ref], after[ref]
		if old == updated {
			continue
		}
		changes = append(changes, LockfileChange{Ref: ref, Old: old, New: updated})
	}
	return changes
}

// FormatLockfileDiff renders a diff for a human, one line per change. Both
// digests are always shown: the one being left behind is the evidence that
// tells an approver whether they are looking at the upgrade they asked for.
func FormatLockfileDiff(changes []LockfileChange) string {
	var b strings.Builder
	for _, c := range changes {
		switch {
		case c.Old == "":
			fmt.Fprintf(&b, "+ %s %s\n", c.Ref, c.New)
		case c.New == "":
			fmt.Fprintf(&b, "- %s %s\n", c.Ref, c.Old)
		default:
			fmt.Fprintf(&b, "~ %s %s -> %s\n", c.Ref, c.Old, c.New)
		}
	}
	return b.String()
}
