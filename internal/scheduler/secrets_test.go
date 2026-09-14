package scheduler_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/secrets"
)

const registryPassword = "correcthorsebatterystaple"

// pushPipeline is one step that asks for the registry credential by name.
func pushPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{{
			Id:           "push",
			Name:         "push",
			PluginRef:    "cmd://echo",
			EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
			Secrets:      []*dholev1.StepSecret{{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"}},
		}},
	}
}

// redeemingEngine can take a step that carries a secret.
func redeemingEngine(id string) registry.Instance {
	e := readyEngine(id)
	e.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	return e
}

// newSecretHarness is the scheduler over a real store, a real lease manager and
// a recording bus, with the step-secret issuer the test hands it.
func newSecretHarness(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, issuer scheduler.StepSecrets,
) *harness {
	t.Helper()
	return newSecretHarnessWith(ctx, t, pipeline, issuer, secretHarnessOptions{})
}

// secretHarnessOptions bend the secret harness for the cases that need a lease
// to expire, or a dispatch to fail to commit.
type secretHarnessOptions struct {
	ttl      time.Duration
	leases   func(lease.Manager) lease.Manager
	builtins scheduler.BuiltinSteps
	gate     scheduler.Gate
}

func newSecretHarnessWith(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, issuer scheduler.StepSecrets,
	opts secretHarnessOptions,
) *harness {
	t.Helper()
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
	var manager lease.Manager = leases
	if opts.leases != nil {
		manager = opts.leases(leases)
	}

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	fleet := staticFleet{instances: []registry.Instance{redeemingEngine("e1")}}
	defs := staticDefs{pipeline: pipeline}
	sched, err := scheduler.New(scheduler.Config{
		Store: store, Outbox: ob, Leases: manager, Fleet: fleet, Definitions: defs,
		Tier: testTier, OS: "linux", Arch: "amd64",
		Secrets: issuer, LeaseTTL: opts.ttl,
		Builtins: opts.builtins, Gate: opts.gate,
	})
	require.NoError(t, err)

	h := &harness{
		store: store, bus: recorder, outbox: ob, leases: leases,
		sched: sched, url: srv.URL(), fleet: fleet, defs: defs,
	}
	h.seedRun(ctx, t)
	return h
}

// storedDispatches reads the outbox as it sits in the database, BEFORE anything
// drains it: this is the dispatch at rest, which is durable and replayable.
func storedDispatches(ctx context.Context, t *testing.T, store runstore.Store) [][]byte {
	t.Helper()
	var payloads [][]byte
	require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
		rows, err := tx.Query(ctx, `SELECT payload FROM outbox`)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var p []byte
			if err := rows.Scan(&p); err != nil {
				return err
			}
			payloads = append(payloads, p)
		}
		return rows.Err()
	}))
	return payloads
}

// TestTheStoredDispatchCarriesHandlesAndNeverTheValue is the producer half of
// "engines never receive secret values": a step declaring a secret goes out
// with one SecretRef bound to the variable it named, expiring within the
// issuance bound, and neither the stored dispatch nor the run log holds the
// value.
func TestTheStoredDispatchCarriesHandlesAndNeverTheValue(t *testing.T) {
	ctx := testContext(t)
	src := secrets.NewMapSource()
	src.Set(testTenant, "harbor-robot", registryPassword)
	broker := secrets.NewBroker()
	h := newSecretHarness(ctx, t, pushPipeline(), secrets.NewStepIssuer(broker, src))

	before := time.Now()
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	stored := storedDispatches(ctx, t, h.store)
	require.Len(t, stored, 1, "the step was not dispatched")
	require.False(t, bytes.Contains(stored[0], []byte(registryPassword)),
		"the dispatch at rest in the outbox contains the secret VALUE")

	d := &dholev1.JobDispatch{}
	require.NoError(t, proto.Unmarshal(stored[0], d))
	require.Len(t, d.GetSecrets(), 1, "the dispatch carries no SecretRef for the declared secret")
	ref := d.GetSecrets()[0]
	require.Equal(t, "REGISTRY_PASSWORD", ref.GetName())
	require.NotEmpty(t, ref.GetHandle())
	require.LessOrEqual(t, ref.GetExpiresAt(), before.Add(scheduler.DefaultSecretTTL).Add(time.Second).Unix(),
		"a handle outlives the bound it is issued under")
	require.Greater(t, ref.GetExpiresAt(), before.Unix())

	value, err := broker.Redeem(ref.GetHandle())
	require.NoError(t, err)
	require.Equal(t, registryPassword, value, "the handle on the dispatch is not the one the broker issued")

	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	for _, e := range events {
		require.False(t, bytes.Contains(e.Payload, []byte(registryPassword)),
			"event %s contains the secret value", e.Type)
	}
}

// TestAStepDeclaringAMissingSecretIsRefusedBeforeAnythingIsDispatched: no
// outbox row, no dispatch event, a refusal naming the secret, and a failed run.
func TestAStepDeclaringAMissingSecretIsRefusedBeforeAnythingIsDispatched(t *testing.T) {
	ctx := testContext(t)
	h := newSecretHarness(ctx, t, pushPipeline(),
		secrets.NewStepIssuer(secrets.NewBroker(), secrets.NewMapSource()))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	requireRefusedNaming(ctx, t, h, "harbor-robot")
}

// TestAPlaneThatIssuesNoStepSecretsRefusesADeclaringStep is the honest default:
// a scheduler wired with no issuer does not dispatch the step without its
// secret.
func TestAPlaneThatIssuesNoStepSecretsRefusesADeclaringStep(t *testing.T) {
	ctx := testContext(t)
	h := newSecretHarness(ctx, t, pushPipeline(), nil)

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	requireRefusedNaming(ctx, t, h, "harbor-robot")
}

// TestASecretDeclaredWithoutTheCapabilityIsRefused: the step would otherwise
// reach an engine that cannot redeem, and policy would never see the ask.
func TestASecretDeclaredWithoutTheCapabilityIsRefused(t *testing.T) {
	ctx := testContext(t)
	src := secrets.NewMapSource()
	src.Set(testTenant, "harbor-robot", registryPassword)
	p := pushPipeline()
	p.GetSteps()[0].Capabilities = nil
	h := newSecretHarness(ctx, t, p, secrets.NewStepIssuer(secrets.NewBroker(), src))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	requireRefusedNaming(ctx, t, h, "CAPABILITY_SECRETS")
}

func requireRefusedNaming(ctx context.Context, t *testing.T, h *harness, named string) {
	t.Helper()
	require.Empty(t, storedDispatches(ctx, t, h.store), "a step missing its secret was dispatched anyway")

	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var refused, failed bool
	for _, e := range events {
		switch e.Type {
		case runstore.StepDispatched:
			t.Fatalf("a step missing its secret recorded a dispatch")
		case scheduler.StepSecretUnavailable:
			require.Equal(t, "push", e.StepID)
			reason, err := scheduler.UnmarshalSecretUnavailable(e.Payload)
			require.NoError(t, err)
			require.Contains(t, reason.Reason, named)
			refused = true
		case scheduler.RunFailed:
			failed = true
		}
	}
	require.True(t, refused, "no %s event explains the refusal", scheduler.StepSecretUnavailable)
	require.True(t, failed, "the run was left open with a step that will never run")

	// And the refusal is final: another pass does not dispatch it either.
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, storedDispatches(ctx, t, h.store))
}

// TestAnEndedAttemptsUnspentHandlesAreRevoked: a handle is minted for one
// attempt, and it is a bearer credential until it expires. An attempt that
// ended without its engine redeeming — it failed before the sandbox came up,
// it was cancelled, its engine died — used to leave that credential live for
// the rest of its ten minutes (ADR 0028).
func TestAnEndedAttemptsUnspentHandlesAreRevoked(t *testing.T) {
	const ttl = 250 * time.Millisecond
	for _, end := range []struct {
		name string
		end  func(ctx context.Context, t *testing.T, h *harness, d *dholev1.JobDispatch)
	}{
		{"succeeded", reportPhase(dholev1.Phase_PHASE_SUCCEEDED)},
		{"failed", reportPhase(dholev1.Phase_PHASE_FAILED)},
		{"cancelled", reportPhase(dholev1.Phase_PHASE_CANCELLED)},
		{"lost", func(ctx context.Context, t *testing.T, h *harness, _ *dholev1.JobDispatch) {
			time.Sleep(2 * ttl)
			lost, err := h.sched.SweepOrphans(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, lost, "the attempt was not recorded lost")
		}},
	} {
		t.Run(end.name, func(t *testing.T) {
			ctx := testContext(t)
			src := secrets.NewMapSource()
			src.Set(testTenant, "harbor-robot", registryPassword)
			broker := secrets.NewBroker()
			h := newSecretHarnessWith(ctx, t, pushPipeline(), secrets.NewStepIssuer(broker, src),
				secretHarnessOptions{ttl: ttl})

			require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
			require.Equal(t, []string{"push"}, h.drain(ctx, t))
			d := h.latestDispatch(t, "push")
			require.Len(t, d.GetSecrets(), 1)
			h.report(ctx, t, h.sched, d, dholev1.Phase_PHASE_ACCEPTED)

			end.end(ctx, t, h, d)

			_, err := broker.Redeem(d.GetSecrets()[0].GetHandle())
			require.Error(t, err, "the handle of an attempt that %s is still redeemable", end.name)
		})
	}
}

func reportPhase(phase dholev1.Phase) func(context.Context, *testing.T, *harness, *dholev1.JobDispatch) {
	return func(ctx context.Context, t *testing.T, h *harness, d *dholev1.JobDispatch) {
		t.Helper()
		h.report(ctx, t, h.sched, d, phase)
	}
}

// fencedLeases loses every commit: the lease is proven stale inside the
// dispatch's own transaction, which is what another plane taking the step
// looks like from here.
type fencedLeases struct{ lease.Manager }

func (fencedLeases) Validate(context.Context, lease.Token) error { return lease.ErrFenced }

// recordingIssuer keeps the handles it issued, so a case can try them after the
// dispatch that carried them never happened.
type recordingIssuer struct {
	*secrets.StepIssuer
	mu     sync.Mutex
	issued []*dholev1.SecretRef
}

func (r *recordingIssuer) Issue(
	ctx context.Context, scope secrets.Scope, step *dholev1.Step, ttl time.Duration,
) ([]*dholev1.SecretRef, error) {
	refs, err := r.StepIssuer.Issue(ctx, scope, step, ttl)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.issued = append(r.issued, refs...)
	return refs, err
}

// TestHandlesIssuedForADispatchThatDidNotCommitAreRevoked: the handles are
// minted while the dispatch is built, before the transaction that writes it.
// A dispatch that then loses its lease writes nothing and publishes nothing,
// and the handles it minted belong to no attempt anybody will ever run.
func TestHandlesIssuedForADispatchThatDidNotCommitAreRevoked(t *testing.T) {
	ctx := testContext(t)
	src := secrets.NewMapSource()
	src.Set(testTenant, "harbor-robot", registryPassword)
	broker := secrets.NewBroker()
	issuer := &recordingIssuer{StepIssuer: secrets.NewStepIssuer(broker, src)}
	h := newSecretHarnessWith(ctx, t, pushPipeline(), issuer, secretHarnessOptions{
		leases: func(m lease.Manager) lease.Manager { return fencedLeases{Manager: m} },
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, storedDispatches(ctx, t, h.store), "a fenced dispatch was written")

	issuer.mu.Lock()
	defer issuer.mu.Unlock()
	require.Len(t, issuer.issued, 1, "the dispatch never got as far as issuing, so this proves nothing")
	_, err := broker.Redeem(issuer.issued[0].GetHandle())
	require.Error(t, err, "a handle minted for a dispatch that never committed is still redeemable")
}

// takingBuiltins takes every `builtin:` step it is offered, the way the plane's
// own dispatcher does, and remembers that it did.
type takingBuiltins struct {
	mu    sync.Mutex
	taken []string
}

func (b *takingBuiltins) Take(_ context.Context, _, _ string, step *dholev1.Step) (bool, error) {
	if !strings.HasPrefix(step.GetPluginRef(), "builtin:") {
		return false, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.taken = append(b.taken, step.GetId())
	return true, nil
}

// armingGate arms every `builtin:wait` step, and remembers that it did.
type armingGate struct {
	mu    sync.Mutex
	armed []string
}

func (g *armingGate) Arm(_ context.Context, _ runstore.Tx, _, _ string, step *dholev1.Step) (bool, error) {
	if step.GetPluginRef() != "builtin:wait" {
		return false, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = append(g.armed, step.GetId())
	return true, nil
}

// TestABuiltinStepDeclaringASecretIsRefused: a `builtin:` step runs on the
// plane and never has a JobDispatch, so the secrets it declared were dropped
// on the floor — the author believed a credential was delivered and nothing
// was. It is refused like any other step whose secrets cannot be given, before
// a plane worker takes it or a gate is armed (ADR 0028).
func TestABuiltinStepDeclaringASecretIsRefused(t *testing.T) {
	for _, ref := range []string{"builtin:llm", "builtin:wait"} {
		t.Run(ref, func(t *testing.T) {
			ctx := testContext(t)
			src := secrets.NewMapSource()
			src.Set(testTenant, "harbor-robot", registryPassword)
			p := pushPipeline()
			p.GetSteps()[0].PluginRef = ref
			builtins, gate := &takingBuiltins{}, &armingGate{}
			h := newSecretHarnessWith(ctx, t, p, secrets.NewStepIssuer(secrets.NewBroker(), src),
				secretHarnessOptions{builtins: builtins, gate: gate})

			require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
			requireRefusedNaming(ctx, t, h, "builtin:")

			builtins.mu.Lock()
			defer builtins.mu.Unlock()
			gate.mu.Lock()
			defer gate.mu.Unlock()
			require.Empty(t, builtins.taken, "the plane took a builtin step whose declared secrets it cannot give")
			require.Empty(t, gate.armed, "a gate declaring secrets was armed as if they had been given")
		})
	}
}
