package server_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/server"
)

// stepSecretValue is recognisable in any byte stream it leaks into.
const stepSecretValue = "correcthorsebatterystaple-e2e"

// secretPipeline asks for the registry credential by NAME and proves it got
// it by writing the value ROT13'd: a step that echoed the value to prove it
// had it would make every leak check below untestable.
func secretPipeline(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:   "push",
			Name: "push",
			PluginRef: `command:{"args":["/bin/sh","-c",` +
				`"printf '%s' \"$REGISTRY_PASSWORD\" | tr 'A-Za-z' 'N-ZA-Mn-za-m' > outputs/proof"]}`,
			EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
			Secrets:      []*dholev1.StepSecret{{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"}},
			Outputs: []*dholev1.Port{{
				Name: "proof",
				Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "text/plain"}}},
			}},
		}},
	}
}

func rot13(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return 'a' + (r-'a'+13)%26
		case r >= 'A' && r <= 'Z':
			return 'A' + (r-'A'+13)%26
		}
		return r
	}, s)
}

// TestAStepReceivesTheSecretItDeclaresAndTheValueIsRecordedNowhere runs the
// whole path through the single binary: a step names a secret, the operator
// configured it for the tenant, the plane issues a handle, the embedded engine
// redeems it over the loopback bus, and the process sees the value.
//
// Then it looks for the value everywhere Dhole keeps anything: every run event,
// every outbox row as stored, the whole run database (which holds the cache
// entries and their keys), and every file under the plane's state directory
// (the CAS and the authoritative logs).
func TestAStepReceivesTheSecretItDeclaresAndTheValueIsRecordedNowhere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlane(ctx, t, dir, src)

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-with-secret"))
	require.NoError(t, err)
	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "push")

	proof := outputBytes(ctx, t, srv, events, "push", "proof")
	require.Equal(t, rot13(stepSecretValue), string(proof),
		"the step did not see the declared secret's value in $REGISTRY_PASSWORD")

	for _, e := range events {
		require.False(t, bytes.Contains(e.Payload, []byte(stepSecretValue)),
			"run event %s contains the secret value", e.Type)
	}

	outbox := storedOutbox(ctx, t, dir)
	var carried bool
	for _, row := range outbox {
		require.False(t, bytes.Contains(row, []byte(stepSecretValue)), "a stored outbox row contains the secret value")
		d := &dholev1.JobDispatch{}
		if proto.Unmarshal(row, d) == nil && d.GetStepId() == "push" && len(d.GetSecrets()) == 1 {
			carried = true
		}
	}
	require.True(t, carried, "no stored dispatch carried a SecretRef for the step")

	// Everything on disk: the run database and its WAL (events, outbox,
	// cache entries and their keys) and the state directory (CAS, logs).
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path) //nolint:gosec // a path this test created
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte(stepSecretValue)) {
			t.Errorf("%s contains the secret value", path)
		}
		return nil
	}))
}

// TestARedemptionOnAnotherTenantsSubjectIsRefusedEndToEnd runs ADR 0030
// through the single binary. The hosted engine redeems the step's handle on
// its tenant's subject — and only there — and the plane refuses a handle
// presented on any other tenant's, in both directions.
func TestARedemptionOnAnotherTenantsSubjectIsRefusedEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlane(ctx, t, dir, src)

	// Observers on both subjects, answering nothing: the plane is the only
	// responder, and these only count what the engine asked where.
	watch, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(watch.Close)
	var scoped, legacy atomic.Int32
	stopScoped, err := watch.SubscribeEphemeral(ctx, bus.SubjectSecretRedeemFor(tenantID), func([]byte) { scoped.Add(1) })
	require.NoError(t, err)
	t.Cleanup(stopScoped)
	stopLegacy, err := watch.SubscribeEphemeral(ctx, bus.SubjectSecretRedeem(), func([]byte) { legacy.Add(1) })
	require.NoError(t, err)
	t.Cleanup(stopLegacy)

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-on-the-tenant-subject"))
	require.NoError(t, err)
	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "push")
	require.Equal(t, rot13(stepSecretValue), string(outputBytes(ctx, t, srv, events, "push", "proof")),
		"the step did not see the declared secret's value in $REGISTRY_PASSWORD")
	require.Positive(t, scoped.Load(), "the engine never redeemed on its tenant's subject")
	require.Zero(t, legacy.Load(), "the engine redeemed on the unscoped legacy subject")

	conn, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	redeemer := secrets.NewBusRedeemer(conn, bus.SubjectSecretRedeem())

	mine, err := srv.Secrets().Issue(tenantID, "REGISTRY_PASSWORD", stepSecretValue, time.Minute)
	require.NoError(t, err)
	_, err = redeemer.Redeem(ctx, "globex", mine)
	require.Error(t, err, "tenant globex redeemed a handle the plane issued for tenant %s", tenantID)
	require.NotContains(t, err.Error(), stepSecretValue)
	_, err = redeemer.Redeem(ctx, tenantID, mine)
	require.Error(t, err, "a handle presented on another tenant's subject stayed redeemable")

	theirs, err := srv.Secrets().Issue("globex", "REGISTRY_PASSWORD", "globex-value", time.Minute)
	require.NoError(t, err)
	_, err = redeemer.Redeem(ctx, tenantID, theirs)
	require.Error(t, err, "tenant %s redeemed a handle the plane issued for tenant globex", tenantID)
}

// TestAStepDeclaringASecretThePlaneDoesNotHoldIsNeverDispatched: the run fails
// naming the secret, and no dispatch for it was ever written.
func TestAStepDeclaringASecretThePlaneDoesNotHoldIsNeverDispatched(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	srv := startSecretPlane(ctx, t, dir, secrets.NewMapSource())

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-without-secret"))
	require.NoError(t, err)
	events := awaitRunEnded(ctx, t, srv, runID, runstore.RunFailed)

	var reason string
	for _, e := range events {
		switch e.Type {
		case runstore.StepDispatched, runstore.StepSucceeded:
			t.Fatalf("a step missing its secret went ahead: %s", describe(events))
		case scheduler.StepSecretUnavailable:
			r, err := scheduler.UnmarshalSecretUnavailable(e.Payload)
			require.NoError(t, err)
			reason = r.Reason
		}
	}
	require.Contains(t, reason, "harbor-robot", "the refusal does not name the secret: %s", describe(events))
	for _, row := range storedOutbox(ctx, t, dir) {
		d := &dholev1.JobDispatch{}
		if proto.Unmarshal(row, d) == nil && d.GetRunId() == runID {
			t.Fatalf("a dispatch for the refused step was written to the outbox")
		}
	}
}

// TestAModelCredentialIsNotAStepSecret: the plane's OWN source (ADR 0024) holds
// what an operator gave it for model calls, and a pipeline step naming that
// secret is refused rather than handed it (ADR 0027). Upgrading must not widen
// who can read a model provider's key.
func TestAModelCredentialIsNotAStepSecret(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	model := secrets.NewMapSource()
	model.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlaneWith(ctx, t, dir, func(cfg *server.Config) { cfg.SecretSource = model })

	runID, err := srv.Submit(ctx, tenantID, secretPipeline("push-with-model-key"))
	require.NoError(t, err)
	events := awaitRunEnded(ctx, t, srv, runID, runstore.RunFailed)
	for _, e := range events {
		if e.Type == runstore.StepSucceeded || e.Type == runstore.StepDispatched {
			t.Fatalf("a step was handed a secret configured only for the plane's model calls: %s", describe(events))
		}
	}
}

func startSecretPlane(ctx context.Context, t *testing.T, dir string, src secrets.Source) *server.Server {
	t.Helper()
	return startSecretPlaneWith(ctx, t, dir, func(cfg *server.Config) { cfg.StepSecrets = src })
}

func startSecretPlaneWith(ctx context.Context, t *testing.T, dir string, with func(*server.Config)) *server.Server {
	t.Helper()
	cfg := server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	}
	with(&cfg)
	srv, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { stopPlane(t, srv) })
	return srv
}

// storedOutbox reads every outbox row as the database holds it.
func storedOutbox(ctx context.Context, t *testing.T, dir string) [][]byte {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(dir, "dhole.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	rows, err := db.QueryContext(ctx, `SELECT payload FROM outbox`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var out [][]byte
	for rows.Next() {
		var p []byte
		require.NoError(t, rows.Scan(&p))
		out = append(out, p)
	}
	require.NoError(t, rows.Err())
	return out
}
