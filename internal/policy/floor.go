package policy

import "context"

// UntrustedTier is the tier the spec's security constraint names: work in it
// cannot reach privileged engines, unsigned plugins, or at-most-once steps.
const UntrustedTier = "untrusted"

// UntrustedTierRules are that constraint as rules (ADR 0031). Each says
// nothing about any other tier, so composing them onto every tier's decision
// costs a short-circuit and changes no other tier's answer.
//
// At-most-once is refused outright rather than "without an approval gate": a
// pipeline step has no route through a gate at dispatch. The approval route is
// `builtin:agent`'s, enforced in its own code (ADR 0015, ADR 0025).
func UntrustedTierRules() []Rule {
	return []Rule{
		{
			ID:         "tier.untrusted-signed-plugins",
			Expression: `input.tier != "` + UntrustedTier + `" || input.signed`,
			Reason:     "work in the untrusted tier may run only plugins with a verified signature",
		},
		{
			ID:         "tier.untrusted-unprivileged-engines",
			Expression: `input.tier != "` + UntrustedTier + `" || !("PRIVILEGED" in input.engine_capabilities)`,
			Reason:     "work in the untrusted tier may not reach an engine advertising PRIVILEGED",
		},
		{
			ID:         "tier.untrusted-no-at-most-once",
			Expression: `input.tier != "` + UntrustedTier + `" || input.effect_class != "AT_MOST_ONCE"`,
			Reason: "work in the untrusted tier may not run an at-most-once step; " +
				"only an agent's approval gate can admit one",
		},
	}
}

// WithFloor composes floor beneath every tier policy inner holds: the floor's
// rules are evaluated first, and no rule of the tier's can answer a question
// the floor refuses (ADR 0031).
//
// It only ever narrows. A tier inner has no rules for stays without a policy —
// and so denied — rather than being permitted by a floor whose rules all hold:
// that is the mistake taint's own defaultingSource is kept apart from tier
// policy to avoid.
//
// The composed revision names both halves, because the engine caches compiled
// programs under it: a floor that changed with the build, or a tier policy
// reloaded under its old revision string, must not be evaluated from programs
// compiled against the rules they replaced.
func WithFloor(inner Source, floor TierPolicy) Source {
	return flooredSource{inner: inner, floor: floor}
}

type flooredSource struct {
	inner Source
	floor TierPolicy
}

func (s flooredSource) Policy(ctx context.Context, tenantID, tier string) (TierPolicy, bool, error) {
	p, ok, err := s.inner.Policy(ctx, tenantID, tier)
	if err != nil || !ok || len(p.Rules) == 0 {
		return p, ok, err
	}
	rules := make([]Rule, 0, len(s.floor.Rules)+len(p.Rules))
	rules = append(rules, s.floor.Rules...)
	rules = append(rules, p.Rules...)
	return TierPolicy{Revision: s.floor.Revision + "+" + p.Revision, Rules: rules}, true, nil
}
