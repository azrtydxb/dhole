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

// StepEnvironment is the environment identity a step's cache key is hashed
// against: the image the step itself named, or — when it named none — the
// identity its engine's tier announced (ADR 0021).
//
// The tier's identity describes the engine's OWN sandbox image, which is the
// right answer only for a step that accepted it. A step that names its own
// image runs somewhere else entirely, and keying it against the tier's digest
// hashed two different computations to one key: two steps on one Kubernetes
// engine, one building with Go and one with Node, shared a cache entry, and
// whichever ran second was handed the first one's outputs. That is the worst
// failure a content-addressed cache has, and it is silent.
//
// A step naming a MUTABLE tag gets no identity at all, so Eligible refuses it.
// The plane resolves nothing here: it holds no registry credentials, a
// lookup would blow the 10ms policy-and-cache budget, and a lookup at plan
// time is not the answer the engine gets at run time anyway. A tag is
// therefore a name whose meaning can change under the key, and the only
// honest thing to hash is nothing. Pin the digest and the step caches.
func StepEnvironment(step *dholev1.Step, tierIdentity string) string {
	image := step.GetImage()
	if image == "" {
		return tierIdentity
	}
	if !pinned(image) {
		return ""
	}
	return image
}

// pinned reports whether an image reference names content rather than a moving
// label — "repo@algo:hex".
//
// It is a string test and not a registry parse on purpose. This package is the
// cache, not a container client: ADR 0006 keeps container vocabulary out of
// the core, and an executor backend with no images at all still passes a step
// through here. What matters is a reference no one can repoint, and a digest
// suffix is what that looks like in every backend that has images.
func pinned(image string) bool {
	at := strings.LastIndex(image, "@")
	if at <= 0 {
		return false
	}
	algo, hex, ok := strings.Cut(image[at+1:], ":")
	return ok && algo != "" && hex != ""
}
