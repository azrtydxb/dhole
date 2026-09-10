package scheduler_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// THE GAP THESE TESTS EXIST TO CLOSE.
//
// ADR 0012 has policy consulted by "the scheduler, the registry, the
// dispatcher and the secret resolver". Only the definition-save half was ever
// wired, because the scheduler did not exist when policy was built — and save
// time is precisely where two of the facts a tier cares about cannot be known:
// whether the artifact is SIGNED, and which UPSTREAM it came from. Both are
// resolved after the save, so a tier whose rule is "unsigned plugins may not
// run in production" was enforced nowhere.
//
// So these assert on the BUS. A fake dispatcher would prove that a fake was
// not called; only the absence of a message on `job.dispatch.*` proves that
// nothing reached an engine.

const (
	signedStep     = "a-signed"
	unsignedStep   = "b-unsigned"
	signedRef      = "acme/signed:v1"
	unsignedRef    = "acme/unsigned:v1"
	signedDigest   = "sha256:aaaa"
	unsignedDigest = "sha256:bbbb"
	trustedMirror  = "oci://mirror.example/acme"
	otherUpstream  = "oci://elsewhere.example/acme"

	ruleSigned   = "supply-chain.signed-plugins"
	ruleUpstream = "supply-chain.approved-upstreams"

	reasonUnsigned = "unsigned plugins may not run in production"
	reasonUpstream = "a plugin may only run from an approved upstream"
)

// The real source of Signed and Upstream is internal/plugins, over its
// signature store and its upstream registry. This is the compile-time proof
// that what the scheduler consumes is what that package produces — the
// interface is expressed in plain results precisely so plugins need not import
// this package to satisfy it.
var _ scheduler.Provenances = (*plugins.Provenance)(nil)

// supplyChain is two INDEPENDENT root steps naming two different plugins. Two
// roots rather than one is the point: a policy is asked per step, so a test
// over a single step cannot tell an engine that consulted policy from one that
// denies — or allows — everything it is handed.
func supplyChain() *dholev1.Pipeline {
	step := func(id, ref string) *dholev1.Step {
		return &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   ref,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{{Name: "out"}},
		}
	}
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{
			step(signedStep, signedRef),
			step(unsignedStep, unsignedRef),
		},
	}
}

// staticRevisions is the lockfile the run pinned: the digest a signature is
// checked over is the one the revision recorded, never whatever the tag means
// now (ADR 0011).
type staticRevisions struct {
	lockfile map[string]string
}

func (r staticRevisions) Revision(
	_ context.Context, tenantID, revisionID string,
) (defstore.Revision, error) {
	if tenantID == "" {
		return defstore.Revision{}, runstore.ErrTenantRequired
	}
	if revisionID != testRevision {
		return defstore.Revision{}, errors.New("no such revision")
	}
	return defstore.Revision{
		ID:         testRevision,
		PipelineID: testPipeline,
		Lockfile:   r.lockfile,
	}, nil
}

// provenanceQuery is one question the scheduler asked about an artifact.
type provenanceQuery struct {
	tenantID string
	ref      string
	digest   string
}

// staticProvenance answers per REFERENCE, never uniformly. A source that said
// "signed" to everything would let a hardcoded Input.Signed pass every test in
// this file.
type staticProvenance struct {
	mu       sync.Mutex
	signed   map[string]bool
	upstream map[string]string
	asked    []provenanceQuery
	err      error
}

func (p *staticProvenance) Provenance(
	_ context.Context, tenantID, pluginRef, pinnedDigest string,
) (bool, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, provenanceQuery{
		tenantID: tenantID, ref: pluginRef, digest: pinnedDigest,
	})
	if p.err != nil {
		return false, "", p.err
	}
	return p.signed[pluginRef], p.upstream[pluginRef], nil
}

func (p *staticProvenance) queries() []provenanceQuery {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]provenanceQuery(nil), p.asked...)
}

// recordingAudit is the audit trail ADR 0012 asks for, in memory.
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

// failingSource is a policy source that cannot answer. Its whole purpose is
// the fail-closed case: an error must deny, exactly as it does at save time.
type failingSource struct{ err error }

func (s failingSource) Policy(
	_ context.Context, tenantID, _ string,
) (policy.TierPolicy, bool, error) {
	if tenantID == "" {
		return policy.TierPolicy{}, false, policy.ErrTenantRequired
	}
	return policy.TierPolicy{}, false, s.err
}

// tenantSource answers only for the tenant that owns the policy. A source that
// ignored the tenant it was handed would make every tier in a hosted
// deployment share one rule set.
type tenantSource struct {
	tenantID string
	p        policy.TierPolicy
}

func (s tenantSource) Policy(
	_ context.Context, tenantID, tier string,
) (policy.TierPolicy, bool, error) {
	if tenantID == "" {
		return policy.TierPolicy{}, false, policy.ErrTenantRequired
	}
	if tenantID != s.tenantID || tier != testTier {
		return policy.TierPolicy{}, false, nil
	}
	return s.p, true, nil
}

// signedOnlyPolicy is the tier a supply chain is actually run under: a plugin
// this tenant holds no accepted signature over does not run, and one from an
// upstream nobody approved does not run either.
func signedOnlyPolicy() policy.TierPolicy {
	return policy.TierPolicy{
		Revision: "test/1",
		Rules: []policy.Rule{
			{ID: ruleSigned, Expression: `input.signed`, Reason: reasonUnsigned},
			{
				ID:         ruleUpstream,
				Expression: `input.upstream == "` + trustedMirror + `"`,
				Reason:     reasonUpstream,
			},
		},
	}
}

// policyHarness is the ordinary harness plus the two things a dispatch-time
// decision reads and a save-time one cannot.
type policyHarness struct {
	*harness
	prov  *staticProvenance
	audit *recordingAudit
}

type policyOptions struct {
	source     policy.Source
	auditor    *recordingAudit
	provenance *staticProvenance
	pipeline   *dholev1.Pipeline
}

func newPolicyHarness(ctx context.Context, t *testing.T, opts policyOptions) *policyHarness {
	t.Helper()

	if opts.pipeline == nil {
		opts.pipeline = supplyChain()
	}
	if opts.source == nil {
		opts.source = tenantSource{tenantID: testTenant, p: signedOnlyPolicy()}
	}
	if opts.auditor == nil {
		opts.auditor = &recordingAudit{}
	}
	if opts.provenance == nil {
		opts.provenance = &staticProvenance{
			signed:   map[string]bool{signedRef: true, unsignedRef: false},
			upstream: map[string]string{signedRef: trustedMirror, unsignedRef: trustedMirror},
		}
	}

	store, err := runstore.NewSQLite(t.TempDir() + "/run.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	engine, err := policy.New(opts.source, opts.auditor)
	require.NoError(t, err)

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	fleet := staticFleet{instances: []registry.Instance{readyEngine("engine-1")}}
	defs := staticDefs{pipeline: opts.pipeline}

	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		Policy:      engine,
		Provenance:  opts.provenance,
		Revisions: staticRevisions{lockfile: map[string]string{
			signedRef:   signedDigest,
			unsignedRef: unsignedDigest,
		}},
	})
	require.NoError(t, err)

	h := &harness{
		store: store, bus: recorder, outbox: ob, leases: leases,
		sched: sched, url: srv.URL(), fleet: fleet, defs: defs,
	}
	h.seedRun(ctx, t)
	return &policyHarness{harness: h, prov: opts.provenance, audit: opts.auditor}
}

// dispatchSubjects is what actually went to the bus, subject and all. The
// property under test is "nothing reached an engine", and only the wire says
// that.
func (b *recordingBus) dispatchSubjects() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, p := range b.published {
		if strings.HasPrefix(p.subject, "job.dispatch.") {
			out = append(out, p.subject)
		}
	}
	return out
}

// policyDenial reads back the refusal recorded on the run's log for a step.
func (h *policyHarness) policyDenial(
	ctx context.Context, t *testing.T, stepID string,
) scheduler.PolicyDenied {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	for _, e := range events {
		if e.Type != scheduler.StepPolicyDenied || e.StepID != stepID {
			continue
		}
		denial, err := scheduler.UnmarshalPolicyDenied(e.Payload)
		require.NoError(t, err)
		return denial
	}
	t.Fatalf("step %q has no %s event", stepID, scheduler.StepPolicyDenied)
	return scheduler.PolicyDenied{}
}

// runFailed reports whether the run was closed as failed, and over which steps.
func (h *policyHarness) runFailed(ctx context.Context, t *testing.T) ([]string, bool) {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	for _, e := range events {
		if e.Type != scheduler.RunFailed {
			continue
		}
		failure, err := scheduler.UnmarshalRunFailure(e.Payload)
		require.NoError(t, err)
		return failure.Steps, true
	}
	return nil, false
}

// TestPolicyDeniedStepIsNeverDispatched is the test this task exists for. One
// run, two steps, one policy: the signed plugin goes to an engine and the
// unsigned one does not reach the bus at all, with the rule that refused it on
// the run's log.
func TestPolicyDeniedStepIsNeverDispatched(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	dispatched := h.drain(ctx, t)

	require.Equal(t, []string{signedStep}, dispatched,
		"only the step policy permitted may reach an engine")
	require.Len(t, h.bus.dispatchSubjects(), 1,
		"the denied step must put no message on job.dispatch.*")

	denial := h.policyDenial(ctx, t, unsignedStep)
	require.Equal(t, ruleSigned, denial.Rule, "the refusal names the rule that decided")
	require.Contains(t, denial.Reason, reasonUnsigned)
	require.Equal(t, unsignedRef, denial.PluginRef)
	require.False(t, denial.Signed)

	steps, failed := h.runFailed(ctx, t)
	require.True(t, failed, "a run holding a step that will never be dispatched must not hang")
	require.Equal(t, []string{unsignedStep}, steps)
}

// TestPolicyAllowedStepIsDispatchedNormally is the control. Without it every
// test here would pass on a scheduler that denied everything.
func TestPolicyAllowedStepIsDispatchedNormally(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		provenance: &staticProvenance{
			signed:   map[string]bool{signedRef: true, unsignedRef: true},
			upstream: map[string]string{signedRef: trustedMirror, unsignedRef: trustedMirror},
		},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.ElementsMatch(t, []string{signedStep, unsignedStep}, h.drain(ctx, t),
		"a step every rule permits is dispatched exactly as it was before policy existed")

	_, failed := h.runFailed(ctx, t)
	require.False(t, failed, "nothing was denied, so nothing failed the run")
}

// TestUpstreamAndSignedComeFromTheResolvedArtifact is why this decision belongs
// at dispatch rather than at save. Both facts are asked for by the step's
// plugin reference AND the digest the revision's lockfile pinned, and both
// reach the rule that reads them.
func TestUpstreamAndSignedComeFromTheResolvedArtifact(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		provenance: &staticProvenance{
			// BOTH are signed, so the signature rule cannot be what decides.
			// One comes from an upstream nobody approved: the only rule that
			// can tell them apart is the one reading input.upstream.
			signed:   map[string]bool{signedRef: true, unsignedRef: true},
			upstream: map[string]string{signedRef: trustedMirror, unsignedRef: otherUpstream},
		},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{signedStep}, h.drain(ctx, t),
		"the step from the unapproved upstream is the one that is refused")

	denial := h.policyDenial(ctx, t, unsignedStep)
	require.Equal(t, ruleUpstream, denial.Rule)
	require.Equal(t, otherUpstream, denial.Upstream,
		"the upstream the decision turned on is recorded with the refusal")

	asked := h.prov.queries()
	require.NotEmpty(t, asked)
	for _, q := range asked {
		require.Equal(t, testTenant, q.tenantID, "every provenance question is tenant-scoped")
	}
	require.Contains(t, asked, provenanceQuery{
		tenantID: testTenant, ref: signedRef, digest: signedDigest,
	}, "the signature is checked over the digest the lockfile pinned, not the tag")
}

// TestPolicyErrorAtDispatchDenies is the fail-closed property. internal/policy
// already fails closed at save time; a dispatcher that treated "I could not
// tell" as permission would undo it on the only path that runs code.
func TestPolicyErrorAtDispatchDenies(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		source: failingSource{err: errors.New("the policy store is unreachable")},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t), "a policy that cannot answer dispatches nothing")
	require.Empty(t, h.bus.dispatchSubjects())

	denial := h.policyDenial(ctx, t, signedStep)
	require.Contains(t, denial.Reason, "policy error")
	require.Contains(t, denial.Reason, "the policy store is unreachable")

	_, failed := h.runFailed(ctx, t)
	require.True(t, failed)
}

// TestProvenanceErrorAtDispatchDenies is the same property one layer out: a
// signature store that cannot answer leaves Signed unknown, and an unknown
// fact must never be read as a permitted one.
func TestProvenanceErrorAtDispatchDenies(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		provenance: &staticProvenance{err: errors.New("the signature store is unreachable")},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t))

	denial := h.policyDenial(ctx, t, signedStep)
	require.Contains(t, denial.Reason, "the signature store is unreachable")
}

// TestAnUnauditableDecisionDeniesAndSaysWhy. ADR 0012 exists so that "why was
// this allowed" has an answer; a decision that could not be written down has
// no answer, so it must not take effect. And the run has to say what actually
// broke — an operator reading "policy denied" while the real fault is a
// read-only audit database will go looking at the rules.
func TestAnUnauditableDecisionDeniesAndSaysWhy(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		auditor: &recordingAudit{err: errors.New("the audit database is read-only")},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t),
		"a decision that cannot be recorded has not been made")
	require.Empty(t, h.bus.dispatchSubjects())

	denial := h.policyDenial(ctx, t, signedStep)
	require.Contains(t, denial.Reason, "the audit database is read-only",
		"the refusal names the failure that caused it, not just the refusal")

	_, failed := h.runFailed(ctx, t)
	require.True(t, failed)
}

// TestDispatchDecisionIsAuditedForAllowAndDeny: ADR 0012 wants one trail from
// one evaluation point, and an allow recorded as faithfully as a denial. A
// trail that held only refusals answers "why did this run" with silence.
func TestDispatchDecisionIsAuditedForAllowAndDeny(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	_ = h.drain(ctx, t)

	bySubject := map[string]policy.AuditRecord{}
	for _, r := range h.audit.all() {
		require.Equal(t, testTenant, r.TenantID, "there is no unscoped audit row")
		require.Equal(t, testTier, r.Tier)
		bySubject[r.Subject] = r
	}

	allowed, ok := bySubject["step:"+signedStep]
	require.True(t, ok, "the step that WAS dispatched is on the audit trail")
	require.True(t, allowed.Allow)
	require.Equal(t, signedRef, allowed.PluginRef)

	denied, ok := bySubject["step:"+unsignedStep]
	require.True(t, ok, "the step that was refused is on the audit trail")
	require.False(t, denied.Allow)
	require.Equal(t, ruleSigned, denied.Rule)
	require.Equal(t, unsignedRef, denied.PluginRef)
}

// TestDispatchPolicyIsTenantScoped: the tier's rules are read for the tenant
// whose run this is, and a tenant with no policy of its own is denied rather
// than judged by somebody else's.
func TestDispatchPolicyIsTenantScoped(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{
		source: tenantSource{tenantID: "other-tenant", p: signedOnlyPolicy()},
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t),
		"this tenant has no policy of its own and is not judged by another's")

	denial := h.policyDenial(ctx, t, signedStep)
	require.Contains(t, denial.Reason, "no policy configured")
}

// TestASchedulerWithPolicyNeedsTheFactsPolicyReads refuses the half-wired
// configuration. A policy engine handed an Input whose Signed is always false
// and whose Upstream is always empty is not a supply-chain policy; it is a
// policy that refuses everything or reads a fact nobody populated.
func TestASchedulerWithPolicyNeedsTheFactsPolicyReads(t *testing.T) {
	ctx := testContext(t)
	engine, err := policy.New(
		tenantSource{tenantID: testTenant, p: signedOnlyPolicy()}, &recordingAudit{})
	require.NoError(t, err)

	store, err := runstore.NewSQLite(t.TempDir() + "/run.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	base := scheduler.Config{
		Store:       store,
		Outbox:      outbox.New(store, &recordingBus{}, "test-plane"),
		Leases:      leases,
		Fleet:       staticFleet{},
		Definitions: staticDefs{pipeline: supplyChain()},
		Tier:        testTier,
		Policy:      engine,
	}

	withoutProvenance := base
	withoutProvenance.Revisions = staticRevisions{}
	_, err = scheduler.New(withoutProvenance)
	require.ErrorContains(t, err, "provenance")

	withoutRevisions := base
	withoutRevisions.Provenance = &staticProvenance{}
	_, err = scheduler.New(withoutRevisions)
	require.ErrorContains(t, err, "revision")

	provenanceWithoutPolicy := base
	provenanceWithoutPolicy.Policy = nil
	provenanceWithoutPolicy.Provenance = &staticProvenance{}
	provenanceWithoutPolicy.Revisions = nil
	_, err = scheduler.New(provenanceWithoutPolicy)
	require.Error(t, err, "a provenance source with no policy is a wiring that does nothing")
}

// BenchmarkDispatchPolicyEvaluation measures the constraint the spec states:
// one evaluation, cache lookup included, inside 10ms. The engine is the real
// CEL one with the same Input the dispatch path builds.
func BenchmarkDispatchPolicyEvaluation(b *testing.B) {
	engine, err := policy.New(
		tenantSource{tenantID: testTenant, p: signedOnlyPolicy()}, policy.DiscardAudit{})
	if err != nil {
		b.Fatal(err)
	}
	in := policy.Input{
		Tier:        testTier,
		TenantID:    testTenant,
		Subject:     "step:" + signedStep,
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		PluginRef:   signedRef,
		Signed:      true,
		Upstream:    trustedMirror,
	}
	ctx := context.Background()
	if _, err := engine.Evaluate(ctx, in); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for range b.N {
		start := time.Now()
		d, err := engine.Evaluate(ctx, in)
		if err != nil || !d.Allow {
			b.Fatalf("decision %+v err %v", d, err)
		}
		if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
			b.Fatalf("one evaluation took %s, over the 10ms budget", elapsed)
		}
	}
}
