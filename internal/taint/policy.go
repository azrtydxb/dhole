package taint

import (
	"context"
	"fmt"
	"strings"

	"github.com/azrtydxb/dhole/internal/policy"
)

// Tier is the trust tier a dispatch is judged under when it names none. It
// exists so that a Dispatch that simply did not say is still judged by
// something, and by the strictest thing available.
const Tier = "untrusted"

// The rules the built-in taint policy is written as. They keep the names the
// Go implementation reported, because a run log, an agent's refusal and an
// operator's runbook already name them.
const (
	// RulePrivilegedEngine: no untrusted data reaches an engine advertising
	// PRIVILEGED, whatever the step's effect class.
	RulePrivilegedEngine = "taint.privileged-engine"
	// RuleEffectfulStep: only a PURE step may consume untrusted data.
	RuleEffectfulStep = "taint.effectful-step"
)

// BuiltinPolicy is ADR 0015's taint rules, expressed in CEL (ADR 0019).
//
// They were four decisions in Go — clean data passes, a pure step may read
// untrusted data, an effectful step may not, and nothing tainted reaches a
// privileged engine — which is four decisions an operator could not see, audit
// or change. As policy they are two guards whose complements are the two
// permits: a dispatch carrying no taint satisfies both by their first clause,
// and a pure step on an unprivileged engine satisfies both by their second.
//
// Both are written to say NOTHING about clean data. `!input.tainted || ...`
// rather than a rule about effect classes: a taint rule that also refused a
// clean at-most-once step would be a second, invisible effect-class policy.
//
// An UNSPECIFIED effect class is not PURE and is therefore refused. Guessing
// the other way costs an attacker-triggered at-most-once action.
func BuiltinPolicy() policy.TierPolicy {
	return policy.TierPolicy{
		// The revision is what the engine caches compiled programs under. It
		// is a constant because these rules only change when this build does.
		Revision: "builtin/1",
		Rules: []policy.Rule{
			{
				ID:         RulePrivilegedEngine,
				Expression: `!input.tainted || !("PRIVILEGED" in input.engine_capabilities)`,
				Reason: "untrusted data may not be dispatched to an engine advertising " +
					"PRIVILEGED; clear it through a sanitisation gate first",
			},
			{
				ID:         RuleEffectfulStep,
				Expression: `!input.tainted || input.effect_class == "PURE"`,
				Reason: "an effectful step may not act on untrusted data until a " +
					"sanitisation gate clears it",
			},
		},
	}
}

// defaultingSource is what makes the built-ins a FLOOR rather than a default
// somebody has to remember to configure.
//
// A tier with no taint rules of its own is judged by BuiltinPolicy. Without
// this, a deployment that had never written a taint policy would evaluate an
// empty rule set — and the policy engine denies an empty rule set, which fails
// closed but refuses every clean dispatch too, so the first operator to meet
// it would turn taint checking off. Failing closed has to be survivable to be
// kept.
//
// It wraps a source of TAINT policy alone, never the deployment's general
// tier policy: injecting these rules there would turn "no policy configured
// for this tier" from a denial into a permit for everything else the tier
// decides.
type defaultingSource struct {
	inner policy.Source
}

// Policy returns the tier's configured taint rules, or the built-in ones.
func (s defaultingSource) Policy(
	ctx context.Context, tenantID, tier string,
) (policy.TierPolicy, bool, error) {
	if s.inner == nil {
		return BuiltinPolicy(), true, nil
	}
	p, ok, err := s.inner.Policy(ctx, tenantID, tier)
	if err != nil {
		// A source that cannot answer is not an excuse to fall back to
		// anything: the engine turns this into a denial with the error in its
		// reason, which is the fail-closed path.
		return policy.TierPolicy{}, false, err
	}
	if !ok || len(p.Rules) == 0 {
		return BuiltinPolicy(), true, nil
	}
	return p, true, nil
}

// Checker answers whether tainted data may reach a dispatch, by evaluating
// policy (ADR 0012) rather than by deciding in Go.
//
// It owns its own policy engine over its own source, so that the taint rules
// are not mixed into the tier policy the save guard and the secret resolver
// consult. Sharing one source would either weaken those (a tier with only
// taint rules would start permitting whatever else it decides) or make the
// built-in floor unavailable when a tier's policy exists but says nothing
// about taint.
type Checker struct {
	engine policy.Engine
}

// NewChecker returns a checker reading taint policy from src and recording
// every decision through aud.
//
// A nil src is the deployment that configured no taint policy at all: the
// built-in rules apply. aud is required — a decision nobody can answer for
// later is exactly what ADR 0012 exists to prevent.
func NewChecker(src policy.Source, aud policy.Auditor) (*Checker, error) {
	engine, err := policy.New(defaultingSource{inner: src}, aud)
	if err != nil {
		return nil, fmt.Errorf("taint: %w", err)
	}
	return &Checker{engine: engine}, nil
}

// NewCheckerFrom returns a checker over an already-built policy engine. It is
// for a caller that has arranged its own source and auditor — a hosted
// deployment reading tenant-authored taint policy — and takes responsibility
// for that source answering with the built-in floor when a tier has no rules
// of its own.
func NewCheckerFrom(engine policy.Engine) (*Checker, error) {
	if engine == nil {
		return nil, fmt.Errorf("taint: a checker needs a policy engine")
	}
	return &Checker{engine: engine}, nil
}

// Check answers whether tainted data may reach this dispatch.
//
// The decision is the policy engine's, and it is audited there, so a taint
// refusal reads and audits like every other refusal rather than being a
// second vocabulary for "no". The reason is enriched with the subject and the
// triggers that admitted the data before it is returned: a rule can only say
// what it refuses in general, and the person reading a stuck run needs to
// know which webhook it was.
//
// An error means the decision could not be made or could not be recorded. The
// Decision returned alongside it never allows, so a caller that reads only
// the decision still refuses.
//
// A clearance is not a state Check can see: a gate returns cleared VALUES, and
// what makes this allow is that the data no longer carries a mark.
func (c *Checker) Check(ctx context.Context, d Dispatch) (policy.Decision, error) {
	sources := map[string]struct{}{}
	for _, v := range d.Inputs {
		for _, s := range Sources(v) {
			sources[s] = struct{}{}
		}
	}
	for _, ref := range d.InputRefs {
		for _, s := range RefSources(ref) {
			sources[s] = struct{}{}
		}
	}
	named := sorted(sources)

	tier := d.Tier
	if tier == "" {
		tier = Tier
	}
	decision, err := c.engine.Evaluate(ctx, policy.Input{
		Tier:               tier,
		TenantID:           d.TenantID,
		Subject:            d.Subject,
		EffectClass:        d.EffectClass,
		Tainted:            len(named) > 0,
		TaintSources:       named,
		EngineCapabilities: d.EngineCapabilities,
		PrincipalKind:      d.PrincipalKind,
		PrincipalUntrusted: d.PrincipalUntrusted,
	})
	if err != nil {
		return policy.Decision{Rule: decision.Rule, Reason: decision.Reason}, err
	}
	return describe(decision, d.Subject, named), nil
}

// describe puts the facts a rule cannot interpolate back into the reason.
func describe(d policy.Decision, subject string, sources []string) policy.Decision {
	if len(sources) == 0 {
		d.Reason = fmt.Sprintf("step %q carries no tainted input: %s", subject, d.Reason)
		return d
	}
	d.Reason = fmt.Sprintf("step %q carries data tainted by %s: %s",
		subject, strings.Join(sources, ", "), d.Reason)
	return d
}
