package policy_test

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// openDB gives the test a migrated SQLite database: the definition store and
// the policy audit log share one, exactly as they do in a deployment.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dhole.db")
	migrated, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	require.NoError(t, migrated.Close())

	db, err := sql.Open("sqlite", "file:"+url.PathEscape(path)+
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// noPrivileged is the tier policy the plan names: privileged steps are refused.
func noPrivileged() policy.TierPolicy {
	return policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "no-privileged",
			Expression: `!("PRIVILEGED" in input.capabilities)`,
			Reason:     "the privileged capability is not permitted in this tier",
		}},
	}
}

func privilegedPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: "pipe-1",
		Steps: []*dholev1.Step{{
			Id:           "build",
			PluginRef:    "oci://example/builder@sha256:abc",
			EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		}},
	}
}

// TestForbiddenCapabilityRefusedAtSave is the whole point of declaring
// capabilities in a manifest (ADR 0012): a step asking for PRIVILEGED under a
// tier that forbids it is refused when the definition is saved, not discovered
// in production.
func TestForbiddenCapabilityRefusedAtSave(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)

	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", noPrivileged()))

	engine, err := policy.New(src, policy.NewSQLAudit(db))
	require.NoError(t, err)

	decision, err := engine.Evaluate(ctx, policy.Input{
		Tier:         "production",
		TenantID:     "acme",
		Subject:      "step:build",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
		PluginRef:    "oci://example/builder@sha256:abc",
		Signed:       true,
		Upstream:     "example.com",
	})
	require.NoError(t, err)
	require.False(t, decision.Allow)
	require.Equal(t, "no-privileged", decision.Rule, "the decision must name the rule that made it")
	require.Contains(t, decision.Reason, "privileged")

	// And the same refusal reaches the definition store's save path.
	guarded := policy.NewSaveGuard(engine, defstore.New(db), policy.FixedTier("production"))
	_, err = guarded.Save(ctx, "acme", privilegedPipeline(), "author@example.com")
	var denied *policy.DeniedError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "no-privileged", denied.Decision.Rule)
}

// TestPolicyDenialAuditedWithinLatencyBudget checks the two properties that
// make a policy point usable: every denial is answerable from one log
// (ADR 0012), and evaluation is cheap enough to sit in the dispatch path
// (10ms, from the spec's constraints).
//
// Both numbers are measured, and both are reported: the full path including
// the audit write, and evaluation alone. Excluding the audit write from the
// budget would be measuring something the dispatcher never does.
func TestPolicyDenialAuditedWithinLatencyBudget(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	audit := policy.NewSQLAudit(db)

	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", noPrivileged()))
	engine, err := policy.New(src, audit)
	require.NoError(t, err)

	in := policy.Input{
		Tier:         "production",
		TenantID:     "acme",
		Subject:      "step:build",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		PluginRef:    "oci://example/builder@sha256:abc",
	}
	decision, err := engine.Evaluate(ctx, in)
	require.NoError(t, err)
	require.False(t, decision.Allow)

	records, err := audit.Records(ctx, "acme", 10)
	require.NoError(t, err)
	require.Len(t, records, 1, "a denial must leave exactly one audit row")
	require.Equal(t, "no-privileged", records[0].Rule)
	require.Equal(t, "production", records[0].Tier)
	require.Equal(t, "step:build", records[0].Subject)
	require.False(t, records[0].Allow)

	// The cache is warm after the evaluation above.
	const runs = 1000
	start := time.Now()
	for i := 0; i < runs; i++ {
		if _, err := engine.Evaluate(ctx, in); err != nil {
			require.NoError(t, err)
		}
	}
	full := time.Since(start) / runs

	// The same loop with the audit write removed, so the report can say which
	// half of the budget goes where rather than quietly dropping work.
	evalOnly, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)
	_, err = evalOnly.Evaluate(ctx, in)
	require.NoError(t, err)
	start = time.Now()
	for i := 0; i < runs; i++ {
		if _, err := evalOnly.Evaluate(ctx, in); err != nil {
			require.NoError(t, err)
		}
	}
	evaluation := time.Since(start) / runs

	t.Logf("evaluate+audit %v per decision; evaluate only %v per decision", full, evaluation)
	require.Less(t, full, 10*time.Millisecond,
		"policy evaluation including the audit write must fit the 10ms budget")

	records, err = audit.Records(ctx, "acme", runs+10)
	require.NoError(t, err)
	require.Len(t, records, runs+1, "every decision is audited, not only the first")
}

// TestPolicyEvaluationErrorFailsClosed is the safety property of the whole
// subsystem: a policy that cannot be evaluated must never admit. A rule
// referring to a field that does not exist is a broken policy, and a broken
// policy denies.
func TestPolicyEvaluationErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "broken",
			Expression: `input.no_such_field == "x"`,
			Reason:     "unreachable",
		}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	decision, err := engine.Evaluate(ctx, policy.Input{
		Tier: "production", TenantID: "acme", Subject: "step:build",
	})
	require.NoError(t, err)
	require.False(t, decision.Allow, "an evaluation error must never admit")
	require.Contains(t, decision.Reason, "policy error")
	require.Equal(t, "broken", decision.Rule)
}

// TestUnsignedPluginDeniedInProductionTierAllowedInDev: the same input, the
// same plugin, decided differently by tier alone (ADR 0012).
func TestUnsignedPluginDeniedInProductionTierAllowedInDev(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "signed-plugins-only",
			Expression: `input.signed`,
			Reason:     "an unsigned plugin may not run in production",
		}},
	}))
	require.NoError(t, src.Set("dev", policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "anything-goes",
			Expression: `true`,
			Reason:     "dev accepts unsigned plugins",
		}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	in := policy.Input{
		TenantID:  "acme",
		Subject:   "step:build",
		PluginRef: "oci://example/builder@sha256:abc",
		Signed:    false,
		Upstream:  "example.com",
	}

	in.Tier = "production"
	prod, err := engine.Evaluate(ctx, in)
	require.NoError(t, err)
	require.False(t, prod.Allow)
	require.Equal(t, "signed-plugins-only", prod.Rule)

	in.Tier = "dev"
	dev, err := engine.Evaluate(ctx, in)
	require.NoError(t, err)
	require.True(t, dev.Allow)
}

// TestTierWithoutPolicyDenies: the absence of a policy is not permission. An
// unconfigured tier and an unknown tier both deny, or forgetting to configure
// a tier would silently open it.
func TestTierWithoutPolicyDenies(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", noPrivileged()))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	for _, tier := range []string{"", "staging"} {
		decision, err := engine.Evaluate(ctx, policy.Input{
			Tier: tier, TenantID: "acme", Subject: "step:build",
		})
		require.NoError(t, err)
		require.False(t, decision.Allow, "tier %q has no policy and must deny", tier)
		require.Contains(t, decision.Reason, "no policy")
	}

	// A tier configured with an empty rule set is the same case: nothing
	// permitted it.
	require.NoError(t, src.Set("empty", policy.TierPolicy{Revision: "rev1"}))
	decision, err := engine.Evaluate(ctx, policy.Input{
		Tier: "empty", TenantID: "acme", Subject: "step:build",
	})
	require.NoError(t, err)
	require.False(t, decision.Allow)
}

// TestProgramCacheHonoursPolicyRevision: the compiled-program cache is keyed on
// (tier, revision). Keyed on tier alone, a policy edit would go on being
// evaluated with the program compiled from the policy it replaced.
func TestProgramCacheHonoursPolicyRevision(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules:    []policy.Rule{{ID: "permit-all", Expression: `true`, Reason: "permitted"}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	in := policy.Input{Tier: "production", TenantID: "acme", Subject: "step:build"}
	first, err := engine.Evaluate(ctx, in)
	require.NoError(t, err)
	require.True(t, first.Allow)

	// The tier's policy is edited; the revision changes with it.
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev2",
		Rules: []policy.Rule{{
			ID: "deny-all", Expression: `false`, Reason: "the tier is now closed",
		}},
	}))

	second, err := engine.Evaluate(ctx, in)
	require.NoError(t, err)
	require.False(t, second.Allow, "the edited policy must take effect")
	require.Equal(t, "deny-all", second.Rule)
}

// TestConcurrentEvaluationIsSafe exercises the program cache from many
// goroutines at once — the dispatch path is concurrent, so run this with -race.
func TestConcurrentEvaluationIsSafe(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", noPrivileged()))
	require.NoError(t, src.Set("dev", policy.TierPolicy{
		Revision: "rev1",
		Rules:    []policy.Rule{{ID: "permit-all", Expression: `true`, Reason: "dev"}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tier := "production"
			if i%2 == 0 {
				tier = "dev"
			}
			for j := 0; j < 32; j++ {
				if _, err := engine.Evaluate(ctx, policy.Input{
					Tier: tier, TenantID: "acme", Subject: "step:build",
					Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
				}); err != nil {
					t.Errorf("evaluate: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestAuditIsTenantScoped: there is no unscoped record and no unscoped query.
func TestAuditIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	db := openDB(t)
	audit := policy.NewSQLAudit(db)

	err := audit.Record(ctx, policy.AuditRecord{
		Tier: "production", Subject: "step:build", Rule: "no-privileged",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")

	_, err = audit.Records(ctx, "", 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")

	// An evaluation without a tenant is a caller bug, not a wildcard.
	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", noPrivileged()))
	engine, err := policy.New(src, audit)
	require.NoError(t, err)
	_, err = engine.Evaluate(ctx, policy.Input{Tier: "production", Subject: "step:build"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")

	// One tenant's audit never reads back another's.
	_, err = engine.Evaluate(ctx, policy.Input{
		Tier: "production", TenantID: "acme", Subject: "step:build",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
	})
	require.NoError(t, err)
	other, err := audit.Records(ctx, "other", 10)
	require.NoError(t, err)
	require.Empty(t, other)
}

// TestNonBooleanExpressionRejectedAtCompileTime: a rule that does not answer
// yes or no is a configuration error. Caught when the policy is set, it is a
// message to whoever wrote it; caught at evaluation it would be an outage
// shaped like a deny.
func TestNonBooleanExpressionRejectedAtCompileTime(t *testing.T) {
	src := policy.NewStaticSource()
	err := src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules:    []policy.Rule{{ID: "arithmetic", Expression: `1 + 1`, Reason: "nonsense"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bool")
	require.Contains(t, err.Error(), "arithmetic")

	// A syntactically broken rule is caught in the same place.
	err = src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules:    []policy.Rule{{ID: "broken", Expression: `input.signed &&`, Reason: "nonsense"}},
	})
	require.Error(t, err)
}

// TestExpensiveRuleIsBounded: CEL cannot loop forever — it has no recursion and
// no unbounded loop — but a comprehension over nested lists can still be made
// arbitrarily expensive. The cost limit turns that into a bounded, failed-closed
// evaluation instead of a stalled dispatcher.
func TestExpensiveRuleIsBounded(t *testing.T) {
	ctx := context.Background()
	src := policy.NewStaticSource()
	const nested = `[1,2,3,4,5,6,7,8,9,10].all(a,
		[1,2,3,4,5,6,7,8,9,10].all(b,
		[1,2,3,4,5,6,7,8,9,10].all(c,
		[1,2,3,4,5,6,7,8,9,10].all(d,
		[1,2,3,4,5,6,7,8,9,10].all(e, a + b + c + d + e > 0)))))`
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules:    []policy.Rule{{ID: "expensive", Expression: nested, Reason: "unreachable"}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	done := make(chan policy.Decision, 1)
	go func() {
		d, evalErr := engine.Evaluate(ctx, policy.Input{
			Tier: "production", TenantID: "acme", Subject: "step:build",
		})
		if evalErr != nil {
			t.Errorf("evaluate: %v", evalErr)
		}
		done <- d
	}()
	select {
	case d := <-done:
		require.False(t, d.Allow, "an over-budget rule must not admit")
		require.Contains(t, d.Reason, "policy error")
	case <-time.After(10 * time.Second):
		t.Fatal("evaluation did not terminate: the cost limit is not enforced")
	}
}

// TestAPolicyRuleCanRefuseAnAgentWhatItAllowsAPerson is ADR 0025's taint half:
// "an agent's token is marked untrusted, so the policy engine can refuse an
// at-most-once effect or an unsigned plugin to an agent while allowing it to a
// person, without every call carrying provenance."
//
// The two inputs below differ in NOTHING except who is asking. Before the
// principal reached the input map there was no expression an operator could
// write that told them apart: `input.principal_untrusted` did not exist, and a
// rule naming a key the map does not hold is an evaluation error, which denies
// — so the agent and the person were refused together or admitted together.
func TestAPolicyRuleCanRefuseAnAgentWhatItAllowsAPerson(t *testing.T) {
	ctx := context.Background()

	src := policy.NewStaticSource()
	require.NoError(t, src.Set("production", policy.TierPolicy{
		Revision: "rev1",
		Rules: []policy.Rule{{
			ID:         "no-at-most-once-for-an-agent",
			Expression: `!input.principal_untrusted || input.effect_class != "AT_MOST_ONCE"`,
			Reason:     "an untrusted principal may not take an at-most-once action",
		}},
	}))
	engine, err := policy.New(src, policy.DiscardAudit{})
	require.NoError(t, err)

	deploy := policy.Input{
		Tier:        "production",
		TenantID:    "acme",
		Subject:     "action:start_run",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
	}

	person := deploy
	person.PrincipalKind = "user"
	decision, err := engine.Evaluate(ctx, person)
	require.NoError(t, err)
	require.True(t, decision.Allow, "a person was refused by a rule about agents")

	robot := deploy
	robot.PrincipalKind = "agent"
	robot.PrincipalUntrusted = true
	decision, err = engine.Evaluate(ctx, robot)
	require.NoError(t, err)
	require.False(t, decision.Allow, "an agent held a capability the same rule denies it")
	require.Equal(t, "no-at-most-once-for-an-agent", decision.Rule)
}
