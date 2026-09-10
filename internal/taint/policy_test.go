package taint_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/taint"
)

// recordingAudit is the audit trail, kept in memory so a test can read it
// back. It is a real Auditor, not a stub that swallows: the property under
// test is that a taint decision REACHES the trail, and a discard would make
// every assertion about it vacuous.
type recordingAudit struct {
	mu      sync.Mutex
	records []policy.AuditRecord
	err     error
}

func (a *recordingAudit) Record(_ context.Context, r policy.AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.records = append(a.records, r)
	return nil
}

func (a *recordingAudit) all() []policy.AuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]policy.AuditRecord(nil), a.records...)
}

// checkDispatch is the taint decision as a deployment that configured no taint
// policy gets it: the built-in rules, evaluated through the policy engine. It
// is what the rest of this package's tests ask, and it fails the test on an
// evaluation error rather than reading one as a refusal, so a broken engine
// cannot look like a working rule.
func checkDispatch(t *testing.T, d taint.Dispatch) policy.Decision {
	t.Helper()
	if d.TenantID == "" {
		d.TenantID = "tenant-a"
	}
	checker, err := taint.NewChecker(nil, policy.DiscardAudit{})
	require.NoError(t, err)
	decision, err := checker.Check(context.Background(), d)
	require.NoError(t, err)
	return decision
}

// taintedRef is one output ref admitted by source.
func taintedRef(source string) []*dholev1.OutputRef {
	return []*dholev1.OutputRef{taint.MarkRef(&dholev1.OutputRef{Port: "payload"}, source)}
}

// TestNoTaintPolicyConfiguredKeepsTheBuiltInRules is the safe default, and it
// is the whole reason taint may be moved into policy at all.
//
// A deployment that has configured no taint policy — the ordinary deployment —
// must behave at least as strictly as the four built-in rules did when they
// were Go code: clean data passes, a pure step may read untrusted data, an
// effectful step may not, and nothing tainted reaches a privileged engine
// whatever its class. An empty source that admitted everything would turn ADR
// 0015's guarantee into a configuration option nobody set.
func TestNoTaintPolicyConfiguredKeepsTheBuiltInRules(t *testing.T) {
	ctx := context.Background()
	audit := &recordingAudit{}
	// A REAL source that simply has nothing configured, rather than nil: this
	// is the deployment that wired policy up and never wrote a taint rule.
	checker, err := taint.NewChecker(policy.NewStaticSource(), audit)
	require.NoError(t, err)

	marked := taint.Mark(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")

	for _, tc := range []struct {
		name      string
		dispatch  taint.Dispatch
		allow     bool
		rule      string
		mentions  string
		mentions2 string
	}{
		{
			name: "clean data runs anywhere",
			dispatch: taint.Dispatch{
				Subject:     "step:deploy",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
				Inputs: map[string]*structpb.Value{
					"ref": structpb.NewStringValue("refs/heads/main"),
				},
			},
			allow: true,
		},
		{
			name: "a pure step may read untrusted data",
			dispatch: taint.Dispatch{
				Subject:     "step:parse",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Inputs:      map[string]*structpb.Value{"ref": marked},
			},
			allow: true,
		},
		{
			name: "an effectful step may not",
			dispatch: taint.Dispatch{
				Subject:     "step:deploy",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
				Inputs:      map[string]*structpb.Value{"ref": marked},
			},
			rule:     "taint.effectful-step",
			mentions: "git:github:pushes",
		},
		{
			name: "an unspecified class counts as effectful",
			dispatch: taint.Dispatch{
				Subject:   "step:unknown",
				InputRefs: taintedRef("http:public:hooks"),
			},
			rule:     "taint.effectful-step",
			mentions: "http:public:hooks",
		},
		{
			name: "nothing tainted reaches a privileged engine, pure or not",
			dispatch: taint.Dispatch{
				Subject:            "step:parse",
				EffectClass:        dholev1.EffectClass_EFFECT_CLASS_PURE,
				EngineCapabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
				InputRefs:          taintedRef("http:public:hooks"),
			},
			rule:      "taint.privileged-engine",
			mentions:  "http:public:hooks",
			mentions2: "PRIVILEGED",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.dispatch
			d.TenantID = "tenant-a"
			d.Tier = "untrusted"

			decision, err := checker.Check(ctx, d)
			require.NoError(t, err)
			require.Equal(t, tc.allow, decision.Allow, "decision: %+v", decision)
			if tc.allow {
				return
			}
			require.Equal(t, tc.rule, decision.Rule,
				"a denial names the rule that decided it, and the built-in rules keep "+
					"the names the Go ones reported")
			require.Contains(t, decision.Reason, tc.mentions,
				"the refusal must name the trigger that admitted the value")
			if tc.mentions2 != "" {
				require.Contains(t, decision.Reason, tc.mentions2)
			}
		})
	}

	// Every decision, allow and deny alike, is on the trail. "Why was this
	// allowed" is the harder of the two questions (ADR 0012).
	records := audit.all()
	require.Len(t, records, 5, "every taint decision is recorded: %+v", records)
	denials := 0
	for _, r := range records {
		require.Equal(t, "tenant-a", r.TenantID, "there is no unscoped audit row")
		require.Equal(t, "untrusted", r.Tier)
		require.NotEmpty(t, r.Subject, "an audit row names what was decided")
		if !r.Allow {
			denials++
			require.NotEmpty(t, r.Rule, "a recorded denial names the rule that denied")
			require.NotEmpty(t, r.Reason)
		}
	}
	require.Equal(t, 3, denials, "three of the five dispatches were refused")
}

// TestOperatorTaintPolicyDecides is what the plumbing is for: a rule an
// operator can read, audit and change, rather than four decisions buried in
// Go. A configured taint policy is what evaluates, and it can refuse what the
// built-ins would have allowed.
func TestOperatorTaintPolicyDecides(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("untrusted", policy.TierPolicy{
		Revision: "rev-1",
		Rules: []policy.Rule{{
			ID:         "acme.no-public-hooks",
			Expression: `!input.tainted || !("http:public:hooks" in input.taint_sources)`,
			Reason:     "this deployment does not run on data from the public hook endpoint",
		}},
	}), "the taint keys must exist in the CEL environment or the rule cannot compile")

	audit := &recordingAudit{}
	checker, err := taint.NewChecker(src, audit)
	require.NoError(t, err)

	// A PURE step, which every built-in rule would have allowed.
	decision, err := checker.Check(ctx, taint.Dispatch{
		TenantID:    "tenant-a",
		Tier:        "untrusted",
		Subject:     "step:parse",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		InputRefs:   taintedRef("http:public:hooks"),
	})
	require.NoError(t, err)
	require.False(t, decision.Allow,
		"the operator's rule is what decides once one is configured")
	require.Equal(t, "acme.no-public-hooks", decision.Rule)
	require.Contains(t, decision.Reason, "public hook endpoint")

	records := audit.all()
	require.Len(t, records, 1)
	require.Equal(t, "acme.no-public-hooks", records[0].Rule)
	require.False(t, records[0].Allow)
}

// TestBuiltInTaintRulesAreValidCEL: the built-ins are policy, not a second
// vocabulary. They compile in the same environment a tenant's rules do, which
// is also the proof that the keys they read are on the public contract.
func TestBuiltInTaintRulesAreValidCEL(t *testing.T) {
	p := taint.BuiltinPolicy()
	require.NotEmpty(t, p.Rules)
	require.NoError(t, policy.Validate(p))
	for _, r := range p.Rules {
		require.NotEmpty(t, r.Reason, "rule %q denies without saying why", r.ID)
	}
}

// TestUnauditableTaintDecisionDoesNotAllow: a decision nobody can answer for
// later must not take effect. The audit failure is an error AND a refusal —
// a caller that only looked at the decision must not read a permit out of it.
func TestUnauditableTaintDecisionDoesNotAllow(t *testing.T) {
	ctx := context.Background()
	audit := &recordingAudit{err: errors.New("the audit database is gone")}
	checker, err := taint.NewChecker(policy.NewStaticSource(), audit)
	require.NoError(t, err)

	decision, err := checker.Check(ctx, taint.Dispatch{
		TenantID:    "tenant-a",
		Tier:        "untrusted",
		Subject:     "step:parse",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		Inputs: map[string]*structpb.Value{
			"ref": structpb.NewStringValue("refs/heads/main"),
		},
	})
	require.Error(t, err)
	require.False(t, decision.Allow, "an unauditable decision permits nothing")
}

// TestTaintCheckRefusesAnUnscopedDispatch: every decision and every audit row
// carries a tenant. An empty one is a caller bug, and a bug must not be read
// as a permit.
func TestTaintCheckRefusesAnUnscopedDispatch(t *testing.T) {
	checker, err := taint.NewChecker(policy.NewStaticSource(), &recordingAudit{})
	require.NoError(t, err)

	decision, err := checker.Check(context.Background(), taint.Dispatch{
		Subject:     "step:parse",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	})
	require.ErrorIs(t, err, policy.ErrTenantRequired)
	require.False(t, decision.Allow)
}

// TestATaintCheckCarriesTheAskingPrincipalIntoTheRule is the other half of
// ADR 0025's "taint follows the credential": the taint checker is the one
// place an agent step asks whether it may act, so a rule that cannot see WHO
// is acting cannot express the ADR's own example — refuse an at-most-once
// effect to an agent while allowing it to a person.
//
// The dispatch below carries no tainted value at all. That is deliberate: the
// credential's own untrustworthiness is not the data's, and a check that only
// looked at the inputs would let an agent take any action it liked as long as
// it had read nothing.
func TestATaintCheckCarriesTheAskingPrincipalIntoTheRule(t *testing.T) {
	ctx := context.Background()

	src := policy.NewStaticSource()
	require.NoError(t, src.Set("standard", policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "agents-do-not-deploy",
			Expression: `input.principal_kind != "agent" || input.effect_class != "AT_MOST_ONCE"`,
			Reason:     "an agent may not take an at-most-once action",
		}},
	}))
	checker, err := taint.NewChecker(src, policy.DiscardAudit{})
	require.NoError(t, err)

	dispatch := taint.Dispatch{
		TenantID:    "tenant-a",
		Tier:        "standard",
		Subject:     "start_run",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
	}

	person := dispatch
	person.PrincipalKind = "user"
	decision, err := checker.Check(ctx, person)
	require.NoError(t, err)
	require.True(t, decision.Allow, "a person was refused by a rule about agents")

	robot := dispatch
	robot.PrincipalKind = "agent"
	robot.PrincipalUntrusted = true
	decision, err = checker.Check(ctx, robot)
	require.NoError(t, err)
	require.False(t, decision.Allow, "an agent took an action the same rule denies it")
	require.Equal(t, "agents-do-not-deploy", decision.Rule)
}
