package tenancy_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// A tenant's storage total only ever grew. The collector deleted blobs and
// wrote nothing that offset them, so a tenant who reclaimed a terabyte was
// still charged for it and was eventually refused every write against a store
// that was nearly empty.
func TestCollectingABlobCreditsBackTheBytesItWasChargedFor(t *testing.T) {
	ctx := context.Background()
	store := newUsageStore(t)
	digest := &dholev1.Digest{Algo: "sha256", Hex: "aa11"}

	charge(t, store, "acme", digest, 1_000)

	before, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Equal(t, int64(1_000), before)

	require.NoError(t, tenancy.NewReclaimer(store).Collected(ctx, "acme", digest))

	after, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Zero(t, after, "the tenant is still charged for storage they no longer hold")
}

// The credit must equal the charge. Crediting a number the collector happened
// to know, rather than the one the ledger recorded, leaves a balance nobody
// can explain.
func TestTheCreditIsTheAmountTheLedgerCharged(t *testing.T) {
	ctx := context.Background()
	store := newUsageStore(t)
	big := &dholev1.Digest{Algo: "sha256", Hex: "bb22"}
	small := &dholev1.Digest{Algo: "sha256", Hex: "cc33"}

	charge(t, store, "acme", big, 900)
	charge(t, store, "acme", small, 100)

	require.NoError(t, tenancy.NewReclaimer(store).Collected(ctx, "acme", small))

	left, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Equal(t, int64(900), left, "collecting the small blob credited the wrong amount")
}

// Two sweeps over one blob must not credit it twice. The collector is allowed
// to see a blob it has already removed — its own comment says the row can
// outlive the bytes — so this is a real path, not a hypothetical one.
func TestASecondSweepOverTheSameBlobDoesNotCreditItTwice(t *testing.T) {
	ctx := context.Background()
	store := newUsageStore(t)
	digest := &dholev1.Digest{Algo: "sha256", Hex: "dd44"}

	charge(t, store, "acme", digest, 500)
	charge(t, store, "acme", &dholev1.Digest{Algo: "sha256", Hex: "ee55"}, 500)

	r := tenancy.NewReclaimer(store)
	require.NoError(t, r.Collected(ctx, "acme", digest))
	require.NoError(t, r.Collected(ctx, "acme", digest))

	left, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Equal(t, int64(500), left, "the same blob was credited twice")
}

// Blobs predate the metering, so a sweep that tidies one up has nothing to
// credit and must not fail the collection over it.
func TestCollectingABlobNobodyWasChargedForIsNotAnError(t *testing.T) {
	ctx := context.Background()
	store := newUsageStore(t)

	err := tenancy.NewReclaimer(store).Collected(ctx, "acme",
		&dholev1.Digest{Algo: "sha256", Hex: "ff66"})
	require.NoError(t, err)

	total, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Zero(t, total)
}

// One tenant's collection must never credit another's balance.
func TestACreditStaysInsideItsTenant(t *testing.T) {
	ctx := context.Background()
	store := newUsageStore(t)
	digest := &dholev1.Digest{Algo: "sha256", Hex: "1177"}

	charge(t, store, "acme", digest, 700)
	charge(t, store, "globex", digest, 700)

	require.NoError(t, tenancy.NewReclaimer(store).Collected(ctx, "acme", digest))

	acme, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Zero(t, acme)

	globex, err := store.CASBytes(ctx, "globex")
	require.NoError(t, err)
	require.Equal(t, int64(700), globex, "one tenant's sweep credited another's balance")
}

// The interface is declared in cas and implemented here, so the dependency
// runs one way: metering knows about storage, storage does not know about
// metering. A signature drift would otherwise only show up at the call site.
var _ cas.Collector = (*tenancy.Reclaimer)(nil)

// newUsageStore opens a metering store over its own database.
func newUsageStore(t *testing.T) *tenancy.Store {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dhole.db")

	runs, err := runstore.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := cas.OpenIndex(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := tenancy.NewStore(db, runstore.DialectSQLite)
	require.NoError(t, err)
	return store
}

// charge records what the guard records when a blob is written.
func charge(t *testing.T, s *tenancy.Store, tenantID string, d *dholev1.Digest, bytes int64) {
	t.Helper()
	_, err := s.RecordUsage(context.Background(), tenantID, tenancy.Usage{
		Kind:     tenancy.KindCASBytes,
		StepID:   d.GetHex(),
		Quantity: bytes,
		At:       time.Now().UTC(),
	})
	require.NoError(t, err)
}

// A nil *Reclaimer in the collector's interface field is a NON-nil interface
// holding nil, so the collector's own `!= nil` check does not catch it. The
// method guards its receiver instead; without that, a deployment whose
// metering store failed to open would panic on its first sweep.
func TestANilReclaimerIsSafeToCall(t *testing.T) {
	var r *tenancy.Reclaimer
	var c cas.Collector = r

	// The collector guards with a plain `g.Collected != nil`, which a typed
	// nil passes — so this call really happens in a deployment whose metering
	// store failed to open. It must return, not panic. (testify's NotNil does
	// a deep nil check and would assert about testify rather than about this.)
	require.NoError(t, c.Collected(context.Background(), "acme",
		&dholev1.Digest{Algo: "sha256", Hex: "aa11"}))
}

// The whole path, through the real collector: a guarded store charges for a
// write, an expired run's blob is swept, and the ledger nets back to nothing.
// The unit tests above prove the credit; this proves the collector actually
// asks for one.
func TestARealSweepCreditsTheTenantsLedger(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dhole.db")

	runs, err := runstore.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := cas.OpenIndex(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := tenancy.NewStore(db, runstore.DialectSQLite)
	require.NoError(t, err)
	enforcer, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: store})
	require.NoError(t, err)

	guarded, err := tenancy.GuardCAS(cas.NewFilesystem(filepath.Join(dir, "blobs")), enforcer)
	require.NoError(t, err)

	gc := &cas.GC{
		Store: guarded, Runs: runs, DB: db,
		Collected: tenancy.NewReclaimer(store),
	}

	digest, err := guarded.Put(ctx, "acme", strings.NewReader("bytes that will be reclaimed"))
	require.NoError(t, err)

	charged, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Positive(t, charged, "the guard did not charge for the write, so there is nothing to credit")

	require.NoError(t, gc.Reference(ctx, "acme", digest, "run-1"))
	require.NoError(t, runs.Append(ctx, "acme", runstore.Event{
		RunID: "run-1", StepID: "step", Attempt: 1, Sequence: 1,
		Type: runstore.RunCompleted, At: time.Now().UTC().Add(-48 * time.Hour),
	}))

	// Bounded, and the bound is the test. Crediting inside the collector's own
	// transaction deadlocks rather than fails: the credit reads the ledger in
	// the same database, a SQLite run store holds exactly one connection, and
	// the query waits for a connection the open transaction will not release.
	// Nothing times out down there — no busy timeout applies to a wait in Go's
	// connection pool — so without this the sweep hangs until the whole
	// PACKAGE hits its deadline and the reason is a stack dump.
	swept := make(chan int, 1)
	sweepErr := make(chan error, 1)
	go func() {
		n, err := gc.Collect(ctx, "acme", time.Hour)
		swept <- n
		sweepErr <- err
	}()

	var collected int
	select {
	case collected = <-swept:
		require.NoError(t, <-sweepErr)
	case <-time.After(20 * time.Second):
		t.Fatal("the sweep never returned: the credit is running inside the collector's transaction, " +
			"and it is waiting for a connection that transaction holds")
	}
	require.Equal(t, 1, collected)

	after, err := store.CASBytes(ctx, "acme")
	require.NoError(t, err)
	require.Zero(t, after,
		"the collector reclaimed the bytes and the tenant is still charged %d for them", after)
}
