package server_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/taint"
)

// THE DISPATCH POLICY THE SINGLE BINARY RUNS (ADR 0031).
//
// Every assertion here is made through the embedded plane: a real scheduler, a
// real engine over the loopback bus, and the audit trail read back out of the
// run database. The mechanism was built and tested in the scheduler long before
// `dhole serve` passed it a policy engine, which is the gap these close.

// auditTrail reads every policy decision the plane recorded for the tenant.
func auditTrail(ctx context.Context, t *testing.T, dir string) []policy.AuditRecord {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(dir, "dhole.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	rows, err := policy.NewSQLAudit(db).Records(ctx, tenantID, 1000)
	require.NoError(t, err)
	return rows
}

// policyDenial is the run's STEP_POLICY_DENIED, or fails the test.
func policyDenial(t *testing.T, events []runstore.Event, stepID string) scheduler.PolicyDenied {
	t.Helper()
	for _, e := range events {
		if e.Type == scheduler.StepPolicyDenied && e.StepID == stepID {
			d, err := scheduler.UnmarshalPolicyDenied(e.Payload)
			require.NoError(t, err)
			return d
		}
	}
	t.Fatalf("step %q has no %s event: %s", stepID, scheduler.StepPolicyDenied, describe(events))
	return scheduler.PolicyDenied{}
}

// TestTheDefaultPolicyIsEvaluatedAndRecordedForAStepAndItsSecret: a plane
// nobody configured a policy for runs the built-in default — it EVALUATES it,
// and the allow for the step and for the secret it declares are both on the
// audit trail, naming the revision that allowed them. The owner's reason for a
// permissive default was that trail.
func TestTheDefaultPolicyIsEvaluatedAndRecordedForAStepAndItsSecret(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlane(ctx, t, dir, src)

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-under-the-default"))
	require.NoError(t, err)
	requireStepSucceeded(t, awaitRunCompleted(ctx, t, srv, runID), "push")

	decided := map[string]policy.AuditRecord{}
	for _, r := range auditTrail(ctx, t, dir) {
		decided[r.Subject] = r
	}
	for _, subject := range []string{"step:push", "secret:harbor-robot"} {
		r, ok := decided[subject]
		require.True(t, ok, "the plane recorded no policy decision about %s: %v", subject, decided)
		require.True(t, r.Allow, "the permissive default refused %s: %s", subject, r.Reason)
		require.Equal(t, server.DefaultTier, r.Tier)
		require.Contains(t, r.Reason, policy.Default().Revision,
			"the allow for %s does not name the policy that allowed it", subject)
	}
}

// TestAnOperatorPolicyRefusingASecretFailsTheRun: a policy an operator
// supplies REPLACES the default, and a rule on one secret's name refuses the
// step declaring it — recorded as the step's STEP_POLICY_DENIED naming the
// secret, the run failed, and nothing dispatched.
func TestAnOperatorPolicyRefusingASecretFailsTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlaneWith(ctx, t, dir, func(cfg *server.Config) {
		cfg.StepSecrets = src
		cfg.Policy = &policy.TierPolicy{Revision: "operator/1", Rules: []policy.Rule{{
			ID:         "no-registry-robot",
			Expression: `input.secret_name != "harbor-robot"`,
			Reason:     "the registry robot is not for pipelines",
		}}}
	})

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-refused-its-secret"))
	require.NoError(t, err)
	events := awaitRunEnded(ctx, t, srv, runID, runstore.RunFailed)
	for _, e := range events {
		if e.Type == runstore.StepDispatched || e.Type == runstore.StepSucceeded {
			t.Fatalf("a step whose secret policy refused went ahead: %s", describe(events))
		}
	}
	denial := policyDenial(t, events, "push")
	require.Equal(t, "no-registry-robot", denial.Rule)
	require.Equal(t, "harbor-robot", denial.Secret, "the refusal does not name the secret it refused")
}

// taintedPipeline is one step reading a value a trigger binds, under class.
func taintedPipeline(id string, class dholev1.EffectClass) *dholev1.Pipeline {
	p := webhookPipeline(id)
	p.GetSteps()[0].EffectClass = class
	return p
}

// TestATaintedInputReachesTheRuleAsInputTainted: a value an untrusted trigger
// admitted, bound to a step's port, reaches an operator's rule as
// `input.tainted` with its source; the same pipeline started with a clean value
// runs. And under the permissive default, the floor still refuses an effectful
// step that reads it (ADR 0015) — the default cannot switch that off.
func TestATaintedInputReachesTheRuleAsInputTainted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	tainted := map[string]*structpb.Value{
		"event": taint.Mark(structpb.NewStringValue("refs/heads/main"), "http:hook"),
	}
	clean := map[string]*structpb.Value{"event": structpb.NewStringValue("refs/heads/main")}

	t.Run("an operator rule reads it", func(t *testing.T) {
		dir := t.TempDir()
		srv := startSecretPlaneWith(ctx, t, dir, func(cfg *server.Config) {
			cfg.Policy = &policy.TierPolicy{Revision: "operator/1", Rules: []policy.Rule{{
				ID:         "nothing-from-the-hook",
				Expression: `!input.tainted || !("http:hook" in input.taint_sources)`,
				Reason:     "nothing the public hook sent may run here",
			}}}
		})
		pipeline := taintedPipeline("reads-the-hook", dholev1.EffectClass_EFFECT_CLASS_PURE)

		runID, err := srv.SubmitWithInputs(ctx, tenantID, pipeline, tainted, "http:hook")
		require.NoError(t, err)
		events := awaitRunEnded(ctx, t, srv, runID, runstore.RunFailed)
		require.Equal(t, "nothing-from-the-hook", policyDenial(t, events, "work").Rule,
			"a step bound to a tainted value reached the rule without input.tainted")

		runID, err = srv.SubmitWithInputs(ctx, tenantID, pipeline, clean, "")
		require.NoError(t, err)
		requireStepSucceeded(t, awaitRunCompleted(ctx, t, srv, runID), "work")
	})

	t.Run("the floor holds under the default", func(t *testing.T) {
		dir := t.TempDir()
		srv := startSecretPlaneWith(ctx, t, dir, func(*server.Config) {})

		runID, err := srv.SubmitWithInputs(ctx, tenantID,
			taintedPipeline("acts-on-the-hook", dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT), tainted, "http:hook")
		require.NoError(t, err)
		events := awaitRunEnded(ctx, t, srv, runID, runstore.RunFailed)
		denial := policyDenial(t, events, "work")
		require.Equal(t, taint.RuleEffectfulStep, denial.Rule,
			"the permissive default let an effectful step act on untrusted data")
		require.True(t, strings.Contains(denial.Reason, "sanitisation"), denial.Reason)

		runID, err = srv.SubmitWithInputs(ctx, tenantID,
			taintedPipeline("reads-the-hook-purely", dholev1.EffectClass_EFFECT_CLASS_PURE), tainted, "http:hook")
		require.NoError(t, err)
		requireStepSucceeded(t, awaitRunCompleted(ctx, t, srv, runID), "work")
	})
}

// TestTheServerRefusesAPolicyThatCannotWork: a policy that will not compile, or
// has no rules, is refused when the plane is configured — not discovered as a
// plane that denies every step.
func TestTheServerRefusesAPolicyThatCannotWork(t *testing.T) {
	for name, p := range map[string]policy.TierPolicy{
		"not compiling": {Revision: "r", Rules: []policy.Rule{{ID: "broken", Expression: "input."}}},
		"no rules":      {Revision: "r"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			_, err := server.New(server.Config{
				Mode: server.ModeEmbedded, StoreDSN: filepath.Join(dir, "dhole.db"),
				BlobRoot: filepath.Join(dir, "state"), Policy: &p, NoAPI: true,
			})
			require.Error(t, err, "a policy that cannot work was accepted")
		})
	}
}
