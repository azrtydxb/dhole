package scheduler_test

import (
	"bytes"
	"context"
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

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	fleet := staticFleet{instances: []registry.Instance{redeemingEngine("e1")}}
	defs := staticDefs{pipeline: pipeline}
	sched, err := scheduler.New(scheduler.Config{
		Store: store, Outbox: ob, Leases: leases, Fleet: fleet, Definitions: defs,
		Tier: testTier, OS: "linux", Arch: "amd64",
		Secrets: issuer,
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
