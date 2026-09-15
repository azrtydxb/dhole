package taint_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/taint"
)

// flooredDefault is the dispatch policy `dhole serve` runs when nobody
// configured one: the permissive built-in default, beneath the floor.
func flooredDefault(t *testing.T, tier string) *policy.CELEngine {
	t.Helper()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set(tier, policy.Default()))
	engine, err := policy.New(policy.WithFloor(src, taint.DispatchFloor()), policy.DiscardAudit{})
	require.NoError(t, err)
	return engine
}

// TestTheFloorHoldsUnderAPermissivePolicy is the constraint the owner's
// "every step allowed" must not undo (ADR 0031): tainted data does not reach an
// effectful step or a privileged engine, and untrusted-tier work does not run
// unsigned, privileged or at-most-once — however permissive the document on
// top. Each case is paired with the clean or trusted twin the default DOES
// allow, so a floor that refused everything could not pass.
func TestTheFloorHoldsUnderAPermissivePolicy(t *testing.T) {
	ctx := context.Background()
	privileged := []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	tainted := []string{"git:github:pushes"}

	for _, c := range []struct {
		name    string
		in      policy.Input
		refused string
	}{
		{"tainted data reaching an effectful step", policy.Input{
			Tier: "trusted", EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			Tainted: true, TaintSources: tainted,
		}, taint.RuleEffectfulStep},
		{"tainted data reaching a privileged engine", policy.Input{
			Tier: "trusted", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			Tainted: true, TaintSources: tainted, EngineCapabilities: privileged,
		}, taint.RulePrivilegedEngine},
		{"a secret for a step reading tainted data on a privileged engine", policy.Input{
			Tier: "trusted", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE, SecretName: "harbor-robot",
			Tainted: true, TaintSources: tainted, EngineCapabilities: privileged,
		}, taint.RulePrivilegedEngine},
		{"an unsigned plugin in the untrusted tier", policy.Input{
			Tier: policy.UntrustedTier, EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		}, "tier.untrusted-signed-plugins"},
		{"a privileged engine in the untrusted tier", policy.Input{
			Tier: policy.UntrustedTier, EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE, Signed: true,
			EngineCapabilities: privileged,
		}, "tier.untrusted-unprivileged-engines"},
		{"an at-most-once step in the untrusted tier", policy.Input{
			Tier: policy.UntrustedTier, EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, Signed: true,
		}, "tier.untrusted-no-at-most-once"},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := c.in
			in.TenantID, in.Subject = "acme", "step:deploy"
			decision, err := flooredDefault(t, in.Tier).Evaluate(ctx, in)
			require.NoError(t, err)
			require.False(t, decision.Allow, "the permissive default let through what the floor refuses")
			require.Equal(t, c.refused, decision.Rule)
		})
	}

	for _, c := range []struct {
		name string
		in   policy.Input
	}{
		{"clean data reaching an at-most-once step on a privileged engine", policy.Input{
			Tier: "trusted", EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			EngineCapabilities: privileged,
		}},
		{"tainted data reaching a pure step on an unprivileged engine", policy.Input{
			Tier: "trusted", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE, Tainted: true, TaintSources: tainted,
		}},
		{"a signed pure step in the untrusted tier", policy.Input{
			Tier: policy.UntrustedTier, EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE, Signed: true,
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := c.in
			in.TenantID, in.Subject = "acme", "step:build"
			decision, err := flooredDefault(t, in.Tier).Evaluate(ctx, in)
			require.NoError(t, err)
			require.True(t, decision.Allow, "the floor refused what it has no rule about: %s", decision.Reason)
		})
	}
}

// TestTheFloorDoesNotTurnAMissingPolicyIntoAPermit: every floor rule holds for
// clean trusted work, so a floor composed onto a tier with no rules would admit
// everything. It must leave that tier denied.
func TestTheFloorDoesNotTurnAMissingPolicyIntoAPermit(t *testing.T) {
	engine, err := policy.New(policy.WithFloor(policy.NewStaticSource(), taint.DispatchFloor()), policy.DiscardAudit{})
	require.NoError(t, err)
	decision, err := engine.Evaluate(context.Background(), policy.Input{
		Tier: "trusted", TenantID: "acme", Subject: "step:build",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	})
	require.NoError(t, err)
	require.False(t, decision.Allow, "a tier with no policy was permitted by the floor alone")
}

// TestTheFlooredDefaultStaysWithinThePolicyBudget holds the dispatch decision
// to the spec's 10ms including cache lookup, with the floor's rules added.
func TestTheFlooredDefaultStaysWithinThePolicyBudget(t *testing.T) {
	engine := flooredDefault(t, "trusted")
	in := policy.Input{
		Tier: "trusted", TenantID: "acme", Subject: "step:build",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT, Tainted: true,
		TaintSources:       []string{"git:github:pushes"},
		EngineCapabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
	}
	ctx := context.Background()
	_, err := engine.Evaluate(ctx, in) // compile once; the budget is for a warm cache
	require.NoError(t, err)

	const n = 1000
	start := time.Now()
	for range n {
		_, err := engine.Evaluate(ctx, in)
		require.NoError(t, err)
	}
	perDecision := time.Since(start) / n
	require.Less(t, perDecision, 10*time.Millisecond, "a floored decision took %s", perDecision)
}
