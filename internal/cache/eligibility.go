package cache

import (
	"fmt"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// Eligible reports whether a step may be cached, and when it may not, why.
//
// The reason is not a courtesy. Cacheability is decided by three things a
// pipeline author can change without noticing — the effect class they
// declared, the sandbox lease they asked for, and whether the backend has an
// environment anyone can name — and a step that quietly stopped being cached
// presents as a run that got slower for no reason. There is nothing to look
// at, so the conclusion is "the system is slow" rather than "this step opted
// out". Returning the reason lets the dispatch record it and the run view
// display it.
//
// Every refusal here is deliberately one-directional: when the answer is not
// certainly yes it is no. A false negative costs the work the cache would have
// saved; a false positive serves one run's outputs as another run's answer,
// which is worse than having no cache at all.
func Eligible(step *dholev1.Step, lease executor.LeaseScope, envIdentity string) (bool, string) {
	if step == nil {
		return false, "no step to cache"
	}
	if reason := effectReason(step.GetEffectClass()); reason != "" {
		return false, reason
	}
	if reason := leaseReason(lease); reason != "" {
		return false, reason
	}
	// Last, because it is the least specific: a pooled step is not cacheable
	// whether or not its backend has an image digest, and naming the pool is
	// the more useful answer.
	if envIdentity == "" {
		return false, "no stable environment identity to hash the step against"
	}
	return true, ""
}

// effectReason refuses everything but a declared pure step. A step whose class
// was never set has promised nothing, and an undeclared promise is not a
// promise (ADR 0002).
func effectReason(class dholev1.EffectClass) string {
	switch class {
	case dholev1.EffectClass_EFFECT_CLASS_PURE:
		return ""
	case dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE:
		return fmt.Sprintf("effect class %s has an external effect that no cache key can capture",
			shortEffect(class))
	case dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED:
		return "effect class is undeclared, so the step has not promised it is pure"
	default:
		return fmt.Sprintf("unknown effect class %s cannot be shown to be pure", class)
	}
}

// leaseReason refuses every scope that outlives the step, because everything a
// previous occupant left in the sandbox is an input the key cannot see.
//
// An unspecified or unrecognised scope is refused with the rest. Executors
// read the zero value as the step scope for the purpose of ACQUIRING a
// sandbox, and that default is safe there; here it would mean inferring that
// carried state is absent from the fact that nobody said, which is the one
// inference this function must never make.
func leaseReason(scope executor.LeaseScope) string {
	switch scope {
	case executor.LeaseStep:
		return ""
	case executor.LeasePool:
		return "pool lease reuses state that cannot be hashed"
	case executor.LeaseJob:
		return "job lease carries state between the steps of a job that cannot be hashed"
	case executor.LeasePipeline:
		return "pipeline lease carries state between the steps of a run that cannot be hashed"
	case executor.LeaseService:
		return "service lease carries state across runs that cannot be hashed"
	default:
		return fmt.Sprintf("lease scope %q is not known to be free of carried state", scope)
	}
}

// shortEffect is the enum name without its prefix: "IDEMPOTENT" rather than
// "EFFECT_CLASS_IDEMPOTENT", which is what a person reading a run view wants.
func shortEffect(class dholev1.EffectClass) string {
	return strings.TrimPrefix(class.String(), "EFFECT_CLASS_")
}
