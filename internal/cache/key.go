// Package cache turns a step's declared inputs into a content-addressed key,
// so a step whose inputs have been seen before is skipped and its recorded
// outputs reused (ADR 0009).
//
// The whole primitive rests on one property: the key must capture everything
// that could change the step's output. Anything it misses is not a slower
// cache, it is a wrong answer served fast — which is strictly worse than
// having no cache at all. Two rules follow, and both are enforced here rather
// than left to the caller:
//
//   - Only PURE steps are cacheable (ADR 0002). A step with an external effect
//     must not be skipped, and a step whose effect class was never declared has
//     promised nothing, so it is refused alongside the effectful ones.
//   - A step with no stable environment identity cannot be cached at all. If
//     nobody can name the environment the outputs were produced in, no key can
//     cover a change to it.
package cache

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sort"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrNotCacheable reports that a step may not be cached at all. Callers match
// it with errors.Is to tell "do not cache this" apart from a broken store: the
// first is a normal, expected outcome for most steps in an automation
// pipeline, not a failure.
var ErrNotCacheable = errors.New("cache: step is not cacheable")

// keySchema domain-separates this key encoding from every other use of
// SHA-256 in the system, and gives a future encoding change somewhere to
// announce itself: bump it and every old entry becomes unreachable rather than
// silently misread.
const keySchema = "dhole.cache.key/v1"

// keyAlgo is the digest algorithm the key is reported under. It matches the
// CAS so a key and a blob digest are the same kind of value.
const keyAlgo = "sha256"

// Key computes the cache key for one step: a SHA-256 over a canonical,
// unambiguous encoding of the plugin reference, the resolved environment
// identity, the set of input digests, and the plugin lockfile.
//
// The encoding is framed — every variable-length field is written with its
// byte length in front of it. Concatenating fields raw would let two different
// step definitions hash identically (inputs "ab","c" and "a","bc" produce the
// same bytes), and a collision here serves one step's outputs for another
// step's work.
//
// Inputs are sorted, and the lockfile — a Go map, so unordered by definition —
// is sorted too, because the order the scheduler happens to hand them over is
// not part of the step's meaning. The sort is over the digest's whole value,
// not a prefix of it: a prefix comparison declares near-identical digests
// equal and leaves them in the caller's order, which would make the key depend
// on that order again.
func Key(
	step *dholev1.Step,
	envIdentity string,
	inputs []*dholev1.Digest,
	lockfile map[string]string,
) (*dholev1.Digest, error) {
	if step == nil {
		return nil, fmt.Errorf("%w: no step given", ErrNotCacheable)
	}
	if class := step.GetEffectClass(); class != dholev1.EffectClass_EFFECT_CLASS_PURE {
		return nil, fmt.Errorf("%w: only pure steps are cacheable, step %q is %s",
			ErrNotCacheable, step.GetId(), class)
	}
	if envIdentity == "" {
		return nil, fmt.Errorf("%w: step %q has no stable environment identity",
			ErrNotCacheable, step.GetId())
	}

	h := sha256.New()
	writeField(h, []byte(keySchema))
	writeField(h, []byte(step.GetPluginRef()))
	writeField(h, []byte(envIdentity))

	// Sort a copy: the caller's slice is theirs, and reordering it under them
	// would be an invisible side effect of asking a question.
	ordered := slices.Clone(inputs)
	sort.Slice(ordered, func(i, j int) bool {
		return compareDigest(ordered[i], ordered[j]) < 0
	})
	writeCount(h, len(ordered))
	for _, d := range ordered {
		// Both halves are framed separately: algorithm "ab"/hex "c" must not
		// hash the same as algorithm "a"/hex "bc".
		writeField(h, []byte(d.GetAlgo()))
		writeField(h, []byte(d.GetHex()))
	}

	names := make([]string, 0, len(lockfile))
	for name := range lockfile {
		names = append(names, name)
	}
	sort.Strings(names)
	writeCount(h, len(names))
	for _, name := range names {
		writeField(h, []byte(name))
		writeField(h, []byte(lockfile[name]))
	}

	return &dholev1.Digest{Algo: keyAlgo, Hex: fmt.Sprintf("%x", h.Sum(nil))}, nil
}

// writeField frames one variable-length field with its length, so the hashed
// material can only be read apart one way.
func writeField(h hash.Hash, b []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(b)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(b)
}

// writeCount frames the number of elements in a repeated section, so an empty
// element cannot be confused with a missing one.
func writeCount(h hash.Hash, n int) {
	var count [8]byte
	binary.BigEndian.PutUint64(count[:], uint64(n)) //nolint:gosec // n is a slice length, never negative.
	_, _ = h.Write(count[:])
}

// compareDigest is a total order over a digest's whole value — algorithm
// first, then the complete hex — so equal-looking prefixes never tie.
func compareDigest(a, b *dholev1.Digest) int {
	if c := cmpString(a.GetAlgo(), b.GetAlgo()); c != 0 {
		return c
	}
	return cmpString(a.GetHex(), b.GetHex())
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
