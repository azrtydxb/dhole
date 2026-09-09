package cache_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
)

// pureStep is the only shape the cache will ever accept, so every key test
// that is not about refusal starts from it.
func pureStep() *dholev1.Step {
	return &dholev1.Step{
		Id:          "build",
		Name:        "build",
		PluginRef:   "oci://ghcr.io/acme/go-build@sha256:abc",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	}
}

// digest builds a digest by hand. Digests travel by pointer: the generated
// message embeds a MessageState, and copying one is a vet copylocks error.
func digest(hex string) *dholev1.Digest {
	return &dholev1.Digest{Algo: "sha256", Hex: hex}
}

// The two inputs below share a long common prefix and differ only near the
// end. A sort that compares a hex prefix rather than the whole value would
// call them equal, leave them in the order they were handed over, and so
// produce a different key for a reordered slice — which the ordering test
// below would then catch.
const (
	hexA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01"
	hexB = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02"
)

// TestKeyIsStableAcrossOrderingAndUnstableOnInputChange pins both halves of
// the cache's correctness. Stability is what makes a hit possible at all: the
// order the scheduler happens to hand inputs over is not part of the step's
// meaning. Instability on every other axis is what keeps a hit honest — a key
// that misses something that changed the output is a wrong answer served
// fast, which is worse than no cache (ADR 0009).
func TestKeyIsStableAcrossOrderingAndUnstableOnInputChange(t *testing.T) {
	step := pureStep()
	const env = "sha256:image-identity"
	lockfile := map[string]string{"acme/build": "1.4.2", "acme/test": "0.9.0"}

	base, err := cache.Key(step, env, []*dholev1.Digest{digest(hexA), digest(hexB)}, lockfile)
	require.NoError(t, err)
	require.Equal(t, "sha256", base.GetAlgo())
	require.Len(t, base.GetHex(), 64)

	reordered, err := cache.Key(step, env, []*dholev1.Digest{digest(hexB), digest(hexA)}, lockfile)
	require.NoError(t, err)
	require.Equal(t, base.GetHex(), reordered.GetHex(), "input order is not part of the step's meaning")

	changedInput, err := cache.Key(step, env,
		[]*dholev1.Digest{digest(hexA), digest(strings.Replace(hexB, "02", "03", 1))}, lockfile)
	require.NoError(t, err)
	require.NotEqual(t, base.GetHex(), changedInput.GetHex(), "a changed input must change the key")

	changedEnv, err := cache.Key(step, "sha256:other-image",
		[]*dholev1.Digest{digest(hexA), digest(hexB)}, lockfile)
	require.NoError(t, err)
	require.NotEqual(t, base.GetHex(), changedEnv.GetHex(), "a changed environment must change the key")

	changedLock, err := cache.Key(step, env, []*dholev1.Digest{digest(hexA), digest(hexB)},
		map[string]string{"acme/build": "1.4.3", "acme/test": "0.9.0"})
	require.NoError(t, err)
	require.NotEqual(t, base.GetHex(), changedLock.GetHex(), "a changed plugin version must change the key")

	changedPlugin := pureStep()
	changedPlugin.PluginRef = "oci://ghcr.io/acme/go-build@sha256:def"
	changedRef, err := cache.Key(changedPlugin, env, []*dholev1.Digest{digest(hexA), digest(hexB)}, lockfile)
	require.NoError(t, err)
	require.NotEqual(t, base.GetHex(), changedRef.GetHex(), "a changed plugin reference must change the key")
}

// TestKeyEncodingIsUnambiguous is the collision test. Hashing concatenated
// fields lets two different inputs produce identical bytes — ["ab","c"] and
// ["a","bc"] are the classic pair — and a collision here silently serves one
// step's outputs for another step's work. Every field must be framed so the
// encoding can be read back apart.
func TestKeyEncodingIsUnambiguous(t *testing.T) {
	const env = "env"
	none := []*dholev1.Digest{}

	// The lockfile pair boundary: "ab"->"c" against "a"->"bc".
	splitOne, err := cache.Key(pureStep(), env, none, map[string]string{"ab": "c"})
	require.NoError(t, err)
	splitTwo, err := cache.Key(pureStep(), env, none, map[string]string{"a": "bc"})
	require.NoError(t, err)
	require.NotEqual(t, splitOne.GetHex(), splitTwo.GetHex(), "lockfile key/value boundary must be framed")

	// The boundary between two fields of different meaning: plugin ref and
	// environment identity.
	stepAB := pureStep()
	stepAB.PluginRef = "ab"
	stepA := pureStep()
	stepA.PluginRef = "a"
	fieldOne, err := cache.Key(stepAB, "c", none, nil)
	require.NoError(t, err)
	fieldTwo, err := cache.Key(stepA, "bc", none, nil)
	require.NoError(t, err)
	require.NotEqual(t, fieldOne.GetHex(), fieldTwo.GetHex(), "field boundary must be framed")

	// The boundary between two input digests, and between a digest's own
	// algorithm and hex halves.
	inputOne, err := cache.Key(pureStep(), env,
		[]*dholev1.Digest{{Algo: "ab", Hex: "c"}, {Algo: "d", Hex: "e"}}, nil)
	require.NoError(t, err)
	inputTwo, err := cache.Key(pureStep(), env,
		[]*dholev1.Digest{{Algo: "a", Hex: "bc"}, {Algo: "d", Hex: "e"}}, nil)
	require.NoError(t, err)
	require.NotEqual(t, inputOne.GetHex(), inputTwo.GetHex(), "digest halves must be framed")

	// Adding an empty input is a different input set, not the same one.
	withEmpty, err := cache.Key(pureStep(), env, []*dholev1.Digest{{Algo: "", Hex: ""}}, nil)
	require.NoError(t, err)
	withoutEmpty, err := cache.Key(pureStep(), env, none, nil)
	require.NoError(t, err)
	require.NotEqual(t, withEmpty.GetHex(), withoutEmpty.GetHex(), "an extra empty input must change the key")
}

// TestKeyRefusesNonPureStep holds the line ADR 0002 draws: caching is a
// property of purity, and a step that touches the world outside its declared
// outputs must never be skipped on a hit. UNSPECIFIED is in here deliberately
// — a step whose class was never set has made no promise, and defaulting an
// unset field to "pure" would cache exactly the steps nobody thought about.
func TestKeyRefusesNonPureStep(t *testing.T) {
	for _, class := range []dholev1.EffectClass{
		dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
		dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED,
	} {
		t.Run(class.String(), func(t *testing.T) {
			step := pureStep()
			step.EffectClass = class

			k, err := cache.Key(step, "sha256:image", []*dholev1.Digest{digest(hexA)}, nil)
			require.Error(t, err)
			require.Nil(t, k)
			require.Contains(t, err.Error(), "only pure steps are cacheable")
			require.Contains(t, err.Error(), class.String(), "the refusal must name the class that caused it")
		})
	}
}

// TestKeyRefusesEmptyEnvironmentIdentity refuses the case where the key cannot
// be honest. If the environment a step ran in has no stable identity, nothing
// in the key covers a change to it, so a hit would reuse outputs built under
// an environment nobody can name. Not caching is the only safe answer.
func TestKeyRefusesEmptyEnvironmentIdentity(t *testing.T) {
	k, err := cache.Key(pureStep(), "", []*dholev1.Digest{digest(hexA)}, nil)
	require.Error(t, err)
	require.Nil(t, k)
	require.Contains(t, err.Error(), "no stable environment identity")
}

// TestKeyRefusesNilStep keeps a missing step from hashing as an empty one,
// which would give every caller that forgot a step the same key.
func TestKeyRefusesNilStep(t *testing.T) {
	k, err := cache.Key(nil, "sha256:image", nil, nil)
	require.Error(t, err)
	require.Nil(t, k)
}
