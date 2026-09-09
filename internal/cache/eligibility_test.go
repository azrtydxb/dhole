package cache_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/executor"
)

// TestPoolLeaseMarksStepNonCacheable is the whole point of the pool scope: a
// warm sandbox is faster because it keeps what the last occupant left behind,
// and that state is an input to the step that no key can hash. Caching such a
// step would serve one run's leftovers as another run's answer.
func TestPoolLeaseMarksStepNonCacheable(t *testing.T) {
	eligible, reason := cache.Eligible(pureStep(), executor.LeasePool, "sha256:image")

	require.False(t, eligible)
	require.Equal(t, "pool lease reuses state that cannot be hashed", reason)
}

// TestPureStepUnderStepLeaseIsEligible is the one combination that qualifies:
// a pure step, a sandbox created and destroyed for it alone, and an
// environment somebody can name.
func TestPureStepUnderStepLeaseIsEligible(t *testing.T) {
	eligible, reason := cache.Eligible(pureStep(), executor.LeaseStep, "sha256:image")

	require.True(t, eligible)
	require.Empty(t, reason, "an eligible step has nothing to explain")
}

// TestEffectfulStepIsNeverEligible: the effect class outranks everything else.
// A step with an external effect is not cached however clean its sandbox is,
// and the reason has to name the class so the run view says which promise the
// step declined to make.
func TestEffectfulStepIsNeverEligible(t *testing.T) {
	for _, tc := range []struct {
		class dholev1.EffectClass
		name  string
	}{
		{dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT, "IDEMPOTENT"},
		{dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, "AT_MOST_ONCE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			step := pureStep()
			step.EffectClass = tc.class

			eligible, reason := cache.Eligible(step, executor.LeaseStep, "sha256:image")

			require.False(t, eligible)
			require.Contains(t, reason, tc.name)
		})
	}
}

// TestUnknownLeaseScopeIsNotEligible. Every scope but the step scope carries
// state from a previous occupant, so a scope this package does not recognise
// has to be refused rather than assumed clean: guessing wrong here does not
// slow a run down, it serves a wrong answer quickly.
func TestUnknownLeaseScopeIsNotEligible(t *testing.T) {
	for _, scope := range []executor.LeaseScope{"", "warp-drive"} {
		eligible, reason := cache.Eligible(pureStep(), scope, "sha256:image")

		require.False(t, eligible, "scope %q", scope)
		require.NotEmpty(t, reason)
	}
}

// TestStepWithoutEnvironmentIdentityIsNotEligible. If nobody can name the
// environment the outputs were produced in, no key covers a change to it — the
// host toolchain moves and the cache keeps serving what the old one built.
func TestStepWithoutEnvironmentIdentityIsNotEligible(t *testing.T) {
	eligible, reason := cache.Eligible(pureStep(), executor.LeaseStep, "")

	require.False(t, eligible)
	require.Contains(t, reason, "no stable environment identity")
}
