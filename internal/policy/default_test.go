package policy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
)

// TestTheDefaultDocumentPermitsEverythingAndIsValid is the owner's decision
// held to what it says (ADR 0032): the plane ships a real, readable document
// that allows every step and every secret, and it is evaluated, not skipped.
// A default that compiled to "no rules" would deny everything instead, and one
// that was not parseable by the same reader as an operator's file would be a
// second format nobody could copy from.
func TestTheDefaultDocumentPermitsEverythingAndIsValid(t *testing.T) {
	raw := policy.DefaultDocument()
	require.Contains(t, string(raw), "default.permit", "the shipped document does not name its rule")

	doc, err := policy.ParseDocument(raw)
	require.NoError(t, err, "the built-in document does not parse with the operator's reader")
	parsed, err := doc.TierPolicy()
	require.NoError(t, err)
	require.Equal(t, policy.Default(), parsed, "Default() is not the document the binary prints")
	require.NotEmpty(t, parsed.Rules, "a default with no rules denies everything")
	require.NoError(t, policy.Validate(parsed))

	src := policy.NewStaticSource()
	require.NoError(t, src.Set("trusted", policy.Default()))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	for _, in := range []policy.Input{
		{Subject: "step:build", EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}},
		{Subject: "secret:harbor-robot", SecretName: "harbor-robot"},
		{Subject: "step:unsigned", PluginRef: "command:{}"},
	} {
		in.Tier, in.TenantID = "trusted", "acme"
		decision, err := engine.Evaluate(context.Background(), in)
		require.NoError(t, err)
		require.True(t, decision.Allow, "the permissive default refused %s: %s", in.Subject, decision.Reason)
		require.Contains(t, decision.Reason, policy.Default().Revision,
			"an allow does not name the revision that allowed it")
	}
}

// TestADocumentThatCannotWorkIsRefusedWhereItIsRead: a file with no rules, an
// unknown field or a rule that does not compile is a configuration error at
// the reader, never a plane that starts and refuses every step.
func TestADocumentThatCannotWorkIsRefusedWhereItIsRead(t *testing.T) {
	for name, raw := range map[string]string{
		"no rules":        "revision: r1\nrules: []\n",
		"not compiling":   "revision: r1\nrules:\n  - id: broken\n    expression: 'input.'\n",
		"no id":           "revision: r1\nrules:\n  - expression: 'true'\n",
		"a misspelt key":  "revision: r1\nrules:\n  - id: a\n    expression: 'true'\n    reasn: a typo\n",
		"not a document":  "- just\n- a list\n",
		"non-bool answer": "revision: r1\nrules:\n  - id: str\n    expression: '\"yes\"'\n",
	} {
		t.Run(name, func(t *testing.T) {
			doc, err := policy.ParseDocument([]byte(raw))
			if err == nil {
				_, err = doc.TierPolicy()
			}
			require.Error(t, err, "a document that cannot work was accepted")
		})
	}
	doc, err := policy.ParseDocument([]byte("revision: r1\nrules:\n  - id: broken\n    expression: 'input.'\n"))
	require.NoError(t, err, "a document that is well-formed YAML is refused before its rules are compiled")
	_, err = doc.TierPolicy()
	require.ErrorContains(t, err, `"broken"`, "the refusal does not name the rule")
}

// TestTheFloorIsEvaluatedFirstAndOnlyNarrows (ADR 0032): a floor is composed
// onto a tier that has rules, is decided before them, cannot be answered by
// them, and a tier with NO rules stays a denial.
func TestTheFloorIsEvaluatedFirstAndOnlyNarrows(t *testing.T) {
	floor := policy.TierPolicy{Revision: "floor/1", Rules: []policy.Rule{{
		ID: "floor.no-privileged", Expression: `!("PRIVILEGED" in input.capabilities)`, Reason: "never",
	}}}
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("trusted", policy.Default()))
	engine, err := policy.New(policy.WithFloor(src, floor), policy.DiscardAudit{})
	require.NoError(t, err)
	ctx := context.Background()

	decision, err := engine.Evaluate(ctx, policy.Input{
		Tier: "trusted", TenantID: "acme", Subject: "step:x",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
	})
	require.NoError(t, err)
	require.False(t, decision.Allow, "a permissive tier policy answered a question the floor refuses")
	require.Equal(t, "floor.no-privileged", decision.Rule)

	decision, err = engine.Evaluate(ctx, policy.Input{Tier: "trusted", TenantID: "acme", Subject: "step:y"})
	require.NoError(t, err)
	require.True(t, decision.Allow, decision.Reason)
	require.True(t, strings.Contains(decision.Reason, "floor/1") && strings.Contains(decision.Reason, "default/1"),
		"the composed revision does not name both halves: %s", decision.Reason)

	decision, err = engine.Evaluate(ctx, policy.Input{Tier: "elsewhere", TenantID: "acme", Subject: "step:z"})
	require.NoError(t, err)
	require.False(t, decision.Allow, "the floor turned a tier with no policy into a permit")
}
