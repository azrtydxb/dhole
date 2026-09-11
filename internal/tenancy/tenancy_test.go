package tenancy_test

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/tenancy"
	"github.com/azrtydxb/dhole/internal/tenant"
)

// harness is one dialect's wiring: the tenancy tables, the run event log they
// derive usage from, and the handle both share.
type harness struct {
	dialect runstore.Dialect
	db      *sql.DB
	store   *tenancy.Store
	log     runstore.Store
}

// forEachDialect runs fn against SQLite and — when a DSN is configured —
// against Postgres. Every storage test here runs on both: a statement that
// only ever met SQLite is a statement nobody has checked pgx will bind.
func forEachDialect(t *testing.T, fn func(t *testing.T, h harness)) {
	t.Helper()

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "dhole.db")
		db, err := runstore.OpenSQLite(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		log, err := runstore.NewSQLite(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = log.Close() })

		store, err := tenancy.NewStore(db, runstore.DialectSQLite)
		require.NoError(t, err)
		fn(t, harness{dialect: runstore.DialectSQLite, db: db, store: store, log: log})
	})

	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		ctx := context.Background()
		db, err := runstore.OpenPostgres(ctx, dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		log, err := runstore.NewPostgres(ctx, dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = log.Close() })

		store, err := tenancy.NewStore(db, runstore.DialectPostgres)
		require.NoError(t, err)
		fn(t, harness{dialect: runstore.DialectPostgres, db: db, store: store, log: log})
	})
}

// uniqueTenant keeps every dialect run in its own scope. Postgres is shared
// and persistent between runs, so a quota that was only respected because the
// tenant had no history would prove nothing.
func uniqueTenant(t *testing.T) string {
	t.Helper()
	id := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '-'
		}
	}, t.Name())
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	// The id is a NATS subject token and a storage path segment, and
	// tenant.Validate caps it at 63 characters.
	if len(id) > 63-len(suffix)-1 {
		id = id[:63-len(suffix)-1]
	}
	return id + "-" + suffix
}

// TestProvisionCreatesIsolatedTenant is the whole point of provisioning: a new
// tenant comes out of it with its own NATS account, and the isolation Task 22
// asserts holds against it — its credentials reach its own subjects and
// nothing else.
func TestProvisionCreatesIsolatedTenant(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		srv, err := bus.StartEmbedded(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(srv.Close)

		p, err := tenancy.NewProvisioner(tenancy.ProvisionerConfig{Store: h.store, Bus: srv})
		require.NoError(t, err)

		idA, idB := uniqueTenant(t)+"a", uniqueTenant(t)+"b"
		a, err := p.Provision(ctx, idA)
		require.NoError(t, err)
		b, err := p.Provision(ctx, idB)
		require.NoError(t, err)

		require.Equal(t, tenant.AccountName(idA), a.NATSAccount)
		require.Equal(t, tenant.AccountName(idB), b.NATSAccount)
		require.NotEmpty(t, a.PlaneCredentials)
		require.NotEqual(t, a.PlaneCredentials, b.PlaneCredentials,
			"both tenants were given the same credentials")
		require.False(t, a.CreatedAt.IsZero())

		// It is durable, not just returned: a second process reads the same row.
		stored, err := h.store.Tenant(ctx, idA)
		require.NoError(t, err)
		require.Equal(t, a.ID, stored.ID)
		require.Equal(t, a.NATSAccount, stored.NATSAccount)

		// And a default quota exists, because a tenant with no quota row is a
		// tenant with no limit at all.
		q, err := h.store.Quota(ctx, idA)
		require.NoError(t, err)
		require.Equal(t, tenancy.DefaultQuota(), q)

		// Task 22's isolation assertions, against a tenant this package made.
		connA := dial(t, a.PlaneCredentials)
		watcherA := dial(t, a.PlaneCredentials)
		connB := dial(t, b.PlaneCredentials)

		subject := bus.SubjectDispatch("trusted", "capsdeadbeef")
		inA := subscribe(t, watcherA, subject)
		inB := subscribe(t, connB, subject)
		require.NoError(t, connA.Flush())
		require.NoError(t, connB.Flush())
		require.NoError(t, watcherA.Flush())

		require.NoError(t, connA.Publish(subject, []byte("dispatch for a")))
		require.NoError(t, connA.Flush())

		select {
		case got := <-inA:
			require.Equal(t, "dispatch for a", string(got))
		case <-time.After(3 * time.Second):
			t.Fatal("the publishing tenant did not receive its own dispatch: the test proves nothing")
		}
		select {
		case got := <-inB:
			t.Fatalf("tenant %q received tenant %q's dispatch: %q", idB, idA, got)
		case <-time.After(300 * time.Millisecond):
		}

		// An id that could forge a subject never reaches the server.
		for _, bad := range []string{"", "a>", "a*", "a.b", "../etc"} {
			_, err := p.Provision(ctx, bad)
			require.Error(t, err, "Provision accepted the tenant id %q", bad)
		}
	})
}

// TestAnEngineCredentialTheProvisionerIssuesCannotReachAnotherTier is the
// property a distributed deployment needs and the one the account credential
// cannot give it.
//
// Provisioning used to return ONE credential, the tenant's account, and that
// is what a deployment handed an engine. It reaches the tenant's whole subject
// space — `job.dispatch.>`, every tier of it — because it is also the identity
// the control plane's own components connect as. An engine holding it is
// separated from other tiers by nothing but its own choice of filter subject,
// which is the arrangement [S-5] refuses: the refusal must come from the bus,
// not from application code.
//
// tenant.ProvisionTierUser proved the mechanism; this proves the DEPLOYMENT
// PATH uses it. Both halves are asserted — the tier's own two work queues
// still bind, and another tier's does not — because a credential that reached
// nothing at all would pass the second half on its own.
func TestAnEngineCredentialTheProvisionerIssuesCannotReachAnotherTier(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		srv, err := bus.StartEmbedded(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(srv.Close)

		p, err := tenancy.NewProvisioner(tenancy.ProvisionerConfig{Store: h.store, Bus: srv})
		require.NoError(t, err)

		id := uniqueTenant(t)
		provisioned, err := p.Provision(ctx, id)
		require.NoError(t, err)

		// The plane declares both tiers' streams inside the tenant's account,
		// with the account credential it keeps. That is the other half of the
		// property: narrowing what an ENGINE gets must not narrow what the
		// PLANE connects as — it publishes to every tier.
		plane, err := bus.Connect(ctx, provisioned.PlaneCredentials)
		require.NoError(t, err)
		t.Cleanup(plane.Close)
		require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

		engineCreds, err := p.EngineCredentials(ctx, id, "untrusted")
		require.NoError(t, err)
		require.NotEqual(t, provisioned.PlaneCredentials, engineCreds,
			"the deployment path handed an engine the tenant's ACCOUNT credential, "+
				"which reaches every tier's job.dispatch.> — tier isolation is then "+
				"the engine's own choice of filter subject, not a boundary")

		untrustedStream, err := bus.DispatchStreamName("untrusted")
		require.NoError(t, err)
		trustedStream, err := bus.DispatchStreamName("trusted")
		require.NoError(t, err)

		engine, err := bus.Connect(ctx, engineCreds)
		require.NoError(t, err)
		t.Cleanup(engine.Close)

		// Both of its own queues: an engine binds the tier's unrestricted
		// subject AND the subject naming its executor kind, so a credential
		// that allowed only one of them would stop kind-targeted work reaching
		// anybody in the tier.
		for name, subject := range map[string]string{
			"engines-untrusted-abc":    bus.SubjectDispatch("untrusted", "abc"),
			"engines-untrusted-abc-vm": bus.SubjectDispatchKind("untrusted", "abc", "vm"),
		} {
			sub, subErr := engine.SubscribePull(ctx, untrustedStream, name, subject)
			require.NoError(t, subErr, "an engine was refused its own tier's queue %q", subject)
			t.Cleanup(func() { _ = sub.Close() })
		}

		_, err = engine.SubscribePull(ctx, trustedStream, "engines-trusted-abc",
			bus.SubjectDispatch("trusted", "abc"))
		require.ErrorIs(t, err, bus.ErrPermissionDenied,
			"the credential the deployment path issued an untrusted engine bound a work "+
				"queue filtered to the trusted tier")

		// Idempotent, for the reason provisioning is: an operator re-runs it,
		// a controller reconciles, and a second call that issued a new
		// password would lock out every engine already holding the old one.
		again, err := p.EngineCredentials(ctx, id, "untrusted")
		require.NoError(t, err)
		require.Equal(t, engineCreds, again,
			"re-issuing an engine credential replaced it, locking out the engines holding it")

		// A tenant nobody provisioned gets no credential at all: issuing one
		// would create an account out of a typo in a tenant id.
		_, err = p.EngineCredentials(ctx, "no-such-tenant", "untrusted")
		require.ErrorIs(t, err, tenancy.ErrUnknownTenant)

		// And a tier that cannot be spelled as a subject token is refused
		// rather than spelled into a permission: a tier of `>` would widen the
		// credential to everything it exists to exclude.
		for _, bad := range []string{"", "*", ">", "a.b", "a b"} {
			_, err = p.EngineCredentials(ctx, id, bad)
			require.Error(t, err, "EngineCredentials issued a credential for the tier %q", bad)
		}
	})
}

// TestProvisioningIsIdempotentAndDoesNotResetTheTenant pins what a retried
// provisioning call must NOT do. Provisioning is at-least-once like everything
// else here — an operator re-runs it, a controller reconciles — and a second
// call that reset the tenant's quota to the default would silently hand back
// limits an operator had deliberately lowered, or lock out engines holding
// credentials for the account it replaced.
func TestProvisioningIsIdempotentAndDoesNotResetTheTenant(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		srv, err := bus.StartEmbedded(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(srv.Close)

		p, err := tenancy.NewProvisioner(tenancy.ProvisionerConfig{Store: h.store, Bus: srv})
		require.NoError(t, err)

		id := uniqueTenant(t)
		first, err := p.Provision(ctx, id)
		require.NoError(t, err)

		tightened := tenancy.Quota{MaxConcurrentSteps: 2, MaxRunsPerDay: 3, MaxCASBytes: 4096}
		require.NoError(t, h.store.SetQuota(ctx, id, tightened))

		second, err := p.Provision(ctx, id)
		require.NoError(t, err)

		require.Equal(t, first.ID, second.ID)
		require.Equal(t, first.NATSAccount, second.NATSAccount,
			"re-provisioning moved the tenant to a different NATS account")
		require.Equal(t, first.PlaneCredentials, second.PlaneCredentials,
			"re-provisioning issued new credentials, locking out everything holding the old ones")
		require.Equal(t, first.CreatedAt.UTC(), second.CreatedAt.UTC(),
			"re-provisioning rewrote the tenant's creation time")

		q, err := h.store.Quota(ctx, id)
		require.NoError(t, err)
		require.Equal(t, tightened, q, "re-provisioning reset the tenant's quotas")
	})
}

// TestQuotaExceededRejectsNewRunsWithoutAffectingRunning states both halves of
// a daily run quota. A tenant at its limit is refused a NEW run, and the runs
// it already has in flight are untouched: an admission control that refused
// work already admitted would kill runs mid-flight to enforce a limit those
// runs were already counted against.
func TestQuotaExceededRejectsNewRunsWithoutAffectingRunning(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		require.NoError(t, h.store.SetQuota(ctx, id, tenancy.Quota{
			MaxConcurrentSteps: 4, MaxRunsPerDay: 2, MaxCASBytes: 1 << 20,
		}))

		enf, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: h.store})
		require.NoError(t, err)

		for _, runID := range []string{"run-a", "run-b"} {
			d, err := enf.AdmitRun(ctx, id, runID)
			require.NoError(t, err)
			require.True(t, d.Allowed, "run %s was refused below the quota: %s", runID, d.Reason)
		}

		refused, err := enf.AdmitRun(ctx, id, "run-c")
		require.NoError(t, err)
		require.False(t, refused.Allowed, "a third run was admitted against a quota of two")
		require.ErrorIs(t, refused.Err(), tenancy.ErrQuotaExceeded)

		// The in-flight runs are unaffected: re-admitting one already counted
		// still passes, and its work still meters.
		again, err := enf.AdmitRun(ctx, id, "run-a")
		require.NoError(t, err)
		require.True(t, again.Allowed,
			"a run already admitted was refused re-entry, which would kill it mid-flight: %s", again.Reason)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)
		require.NoError(t, meter.Record(ctx, id, tenancy.Usage{
			Kind: tenancy.KindStepSeconds, RunID: "run-a", StepID: "build", Attempt: 1,
			Quantity: 1500, Billable: true, At: time.Now().UTC(),
		}))

		// And the refusal did not consume quota of its own: exactly the two
		// admitted runs are counted.
		used, err := h.store.RunsStartedToday(ctx, id, time.Now().UTC())
		require.NoError(t, err)
		require.Equal(t, 2, used, "the refused run was counted against the quota anyway")
	})
}

// TestQuotaRefusalNamesTheQuotaAndTheLimit is the visibility half. A quota
// enforced silently is an outage nobody can explain: the person looking at the
// refused run has to be able to read which limit stopped it and what that
// limit is, without access to the source.
func TestQuotaRefusalNamesTheQuotaAndTheLimit(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		require.NoError(t, h.store.SetQuota(ctx, id, tenancy.Quota{
			MaxConcurrentSteps: 1, MaxRunsPerDay: 1, MaxCASBytes: 64,
		}))

		var seen []tenancy.Decision
		enf, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{
			Store:   h.store,
			Observe: func(_ context.Context, d tenancy.Decision) { seen = append(seen, d) },
		})
		require.NoError(t, err)

		ok, err := enf.AdmitRun(ctx, id, "run-a")
		require.NoError(t, err)
		require.True(t, ok.Allowed)

		refused, err := enf.AdmitRun(ctx, id, "run-b")
		require.NoError(t, err)
		require.False(t, refused.Allowed)

		require.Equal(t, tenancy.QuotaMaxRunsPerDay, refused.Quota)
		require.Equal(t, int64(1), refused.Limit)
		require.Equal(t, int64(1), refused.Observed)
		require.Contains(t, refused.Reason, string(tenancy.QuotaMaxRunsPerDay),
			"the refusal does not name the quota that refused it")
		require.Contains(t, refused.Reason, "1",
			"the refusal does not name the limit it hit")
		require.Contains(t, refused.Reason, id,
			"the refusal does not name the tenant it refused")

		require.Len(t, seen, 1, "the refusal was not reported to the observer: it is silent")
		require.Equal(t, refused.Reason, seen[0].Reason)

		// A step refusal names its own quota, not the run one.
		step, err := enf.AdmitStep(ctx, id, 1)
		require.NoError(t, err)
		require.False(t, step.Allowed)
		require.Equal(t, tenancy.QuotaMaxConcurrentSteps, step.Quota)
		require.Contains(t, step.Reason, string(tenancy.QuotaMaxConcurrentSteps))
	})
}

// TestCASQuotaBlocksWriteBeforeExceeding requires that a blob which would take
// the tenant past MaxCASBytes fails with a quota error and leaves NOTHING
// stored. A gate that let the write land and complained afterwards would bill
// for bytes it had promised to refuse, and the operator's only remedy would be
// to delete them by hand.
func TestCASQuotaBlocksWriteBeforeExceeding(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		require.NoError(t, h.store.SetQuota(ctx, id, tenancy.Quota{
			MaxConcurrentSteps: 4, MaxRunsPerDay: 100, MaxCASBytes: 64,
		}))

		enf, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: h.store})
		require.NoError(t, err)
		inner := cas.NewFilesystem(t.TempDir())
		guarded, err := tenancy.GuardCAS(inner, enf)
		require.NoError(t, err)

		// Under the limit: stored, and counted.
		small := strings.Repeat("a", 20)
		digest, err := guarded.Put(ctx, id, strings.NewReader(small))
		require.NoError(t, err)
		has, err := inner.Has(ctx, id, digest)
		require.NoError(t, err)
		require.True(t, has)

		used, err := h.store.CASBytes(ctx, id)
		require.NoError(t, err)
		require.Equal(t, int64(20), used)

		// Storing the SAME bytes again is not a second charge: content
		// addressing means one copy, and an invoice that grew because a
		// pipeline re-uploaded an identical artifact would be wrong.
		_, err = guarded.Put(ctx, id, strings.NewReader(small))
		require.NoError(t, err)
		used, err = h.store.CASBytes(ctx, id)
		require.NoError(t, err)
		require.Equal(t, int64(20), used, "re-storing identical bytes was billed twice")

		// Over the limit: refused, and not stored.
		big := strings.Repeat("b", 50)
		_, err = guarded.Put(ctx, id, strings.NewReader(big))
		require.Error(t, err)
		require.ErrorIs(t, err, tenancy.ErrQuotaExceeded)
		require.Contains(t, err.Error(), string(tenancy.QuotaMaxCASBytes),
			"the refusal does not name the quota that refused it")
		require.Contains(t, err.Error(), "64", "the refusal does not name the limit it hit")

		bigDigest := digestOf(t, inner, id, big)
		has, err = inner.Has(ctx, id, bigDigest)
		require.NoError(t, err)
		require.False(t, has, "the refused blob was written anyway")

		used, err = h.store.CASBytes(ctx, id)
		require.NoError(t, err)
		require.Equal(t, int64(20), used, "the refused blob was billed for")

		// Empty tenant is refused before anything is read.
		_, err = guarded.Put(ctx, "", strings.NewReader("x"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "tenant scope required")
	})
}

// TestMeteredUsageMatchesActualStepSeconds is the invoice check: a step the
// log says took two seconds is billed as two step-seconds, derived from the
// log rather than from a counter somebody remembered to increment.
func TestMeteredUsageMatchesActualStepSeconds(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		start := time.Now().UTC().Truncate(time.Second)
		writeAttempt(t, h.log, id, "run-1", "build", 1, start, start.Add(2*time.Second), runstore.StepSucceeded, false)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)
		sum, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)

		require.InDelta(t, 2.0, sum.StepSeconds, 0.2, "billed step-seconds are more than 10%% off two seconds")
		require.Equal(t, 1, sum.BilledAttempts)

		// And it is durable, not just returned.
		billed, err := h.store.BilledStepSeconds(ctx, id)
		require.NoError(t, err)
		require.InDelta(t, 2.0, billed, 0.2)
	})
}

// TestRedeliveredStatusIsNotBilledTwice is the property that decides whether
// this system can bill at all. Delivery is at-least-once by design — the
// outbox says so, and an orphan sweep re-dispatches a step under a new fence —
// so a meter that counts deliveries bills a customer twice for one step. The
// same status is delivered twice here, and the whole log is metered twice,
// because those are two different redeliveries and a meter can be idempotent
// against one without being idempotent against the other.
func TestRedeliveredStatusIsNotBilledTwice(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		start := time.Now().UTC().Truncate(time.Second)
		writeAttempt(t, h.log, id, "run-1", "build", 1, start, start.Add(2*time.Second), runstore.StepSucceeded, false)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)

		first, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)
		require.Equal(t, 1, first.BilledAttempts)

		// The same log, metered again: a redelivered status, or a second
		// control plane advancing the same run.
		second, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)
		require.Equal(t, 0, second.BilledAttempts,
			"metering the same log twice billed the step twice")

		// And the direct record path, called twice with the same usage.
		u := tenancy.Usage{
			Kind: tenancy.KindStepSeconds, RunID: "run-1", StepID: "test", Attempt: 1,
			Quantity: 1000, Billable: true, At: start,
		}
		require.NoError(t, meter.Record(ctx, id, u))
		require.NoError(t, meter.Record(ctx, id, u))

		billed, err := h.store.BilledStepSeconds(ctx, id)
		require.NoError(t, err)
		require.InDelta(t, 3.0, billed, 0.2,
			"a redelivered usage record was billed twice")

		records, err := h.store.UsageForRun(ctx, id, "run-1")
		require.NoError(t, err)
		require.Len(t, records, 2, "a redelivery wrote a second usage row")
	})
}

// TestLostAttemptIsNotBilledAndItsRedispatchIs is the retry decision, written
// down. An attempt the CUSTOMER's step ended — succeeded or failed — is
// billable, because the compute really was spent running their code. An
// attempt the PLATFORM lost — the engine died, the lease expired, the orphan
// sweep re-dispatched it under a new fence — is not: the customer did not
// cause that work and must not pay for our failure. The redispatch that
// follows is a fresh attempt and is billed on its own terms.
func TestLostAttemptIsNotBilledAndItsRedispatchIs(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		start := time.Now().UTC().Truncate(time.Second)

		// Attempt 1 ran for five seconds and was then lost with the engine.
		writeAttempt(t, h.log, id, "run-1", "build", 1, start, start.Add(5*time.Second), scheduler.StepAttemptLost, false)
		// Attempt 2 was re-dispatched and took two seconds.
		writeAttempt(t, h.log, id, "run-1", "build", 2, start.Add(5*time.Second), start.Add(7*time.Second), runstore.StepSucceeded, false)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)
		sum, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)

		require.Equal(t, 1, sum.BilledAttempts, "the lost attempt was billed")
		require.Equal(t, 1, sum.UnbilledAttempts)
		require.InDelta(t, 2.0, sum.StepSeconds, 0.2,
			"the five seconds lost with the engine were charged to the customer")

		billed, err := h.store.BilledStepSeconds(ctx, id)
		require.NoError(t, err)
		require.InDelta(t, 2.0, billed, 0.2)

		// The lost attempt is still RECORDED, marked unbillable: an invoice
		// query that cannot see it cannot answer "why did this run cost less
		// than it took".
		records, err := h.store.UsageForRun(ctx, id, "run-1")
		require.NoError(t, err)
		require.Len(t, records, 2)
	})
}

// TestFailedAttemptIsBillable is the other side of that decision, stated on
// its own so removing it fails. A step that exits non-zero consumed exactly
// the compute a successful one would have; not billing it would let a pipeline
// run a fleet for free by failing on purpose.
func TestFailedAttemptIsBillable(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		start := time.Now().UTC().Truncate(time.Second)
		writeAttempt(t, h.log, id, "run-1", "build", 1, start, start.Add(3*time.Second), runstore.StepFailed, false)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)
		sum, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)

		require.Equal(t, 1, sum.BilledAttempts, "a step the customer's own code failed was not billed")
		require.InDelta(t, 3.0, sum.StepSeconds, 0.3)
	})
}

// TestCacheHitIsNotMeteredAsIfItRan pins the one case where the log looks
// exactly like a real execution and must not be billed like one. A cache hit
// writes the same STEP_DISPATCHED and STEP_SUCCEEDED pair a dispatch writes —
// deliberately, so nothing replaying the log needs to know about the cache —
// and the only thing separating them is the CacheHit flag on the dispatch
// payload. Billing a hit charges the customer for work nobody did.
func TestCacheHitIsNotMeteredAsIfItRan(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h harness) {
		ctx := context.Background()
		id := uniqueTenant(t)
		start := time.Now().UTC().Truncate(time.Second)
		writeAttempt(t, h.log, id, "run-1", "build", 1, start, start.Add(2*time.Second), runstore.StepSucceeded, true)

		meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: h.store, Log: h.log})
		require.NoError(t, err)
		sum, err := meter.MeterRun(ctx, id, "run-1")
		require.NoError(t, err)

		require.Equal(t, 1, sum.CacheHits)
		require.Equal(t, 0, sum.BilledAttempts, "a cache hit was billed as if the step had run")
		require.Zero(t, sum.StepSeconds)

		billed, err := h.store.BilledStepSeconds(ctx, id)
		require.NoError(t, err)
		require.Zero(t, billed)

		// It IS recorded, at zero: an invoice has to be able to show the
		// customer what the cache saved them.
		records, err := h.store.UsageForRun(ctx, id, "run-1")
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, tenancy.KindCacheHit, records[0].Kind)
		require.False(t, records[0].Billable)
	})
}

// TestEveryTenancyMethodRefusesAnEmptyTenant sweeps this package's exported
// methods by reflection and requires each to refuse an empty tenant, the way
// Task 22's sweep does for the stores. A method added later that forgets the
// scope fails here rather than in production, where it would read or bill
// across every tenant at once.
func TestEveryTenancyMethodRefusesAnEmptyTenant(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dhole.db")
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	log, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })

	store, err := tenancy.NewStore(db, runstore.DialectSQLite)
	require.NoError(t, err)
	meter, err := tenancy.NewMeter(tenancy.MeterConfig{Store: store, Log: log})
	require.NoError(t, err)
	enf, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: store})
	require.NoError(t, err)
	prov, err := tenancy.NewProvisioner(tenancy.ProvisionerConfig{Store: store})
	require.NoError(t, err)
	guard, err := tenancy.GuardCAS(cas.NewFilesystem(t.TempDir()), enf)
	require.NoError(t, err)

	for _, subject := range []any{store, meter, enf, prov, guard} {
		typ := reflect.TypeOf(subject)
		for i := range typ.NumMethod() {
			method := typ.Method(i)
			if !takesContextAndTenant(method.Type) {
				continue
			}
			t.Run(typ.Elem().Name()+"."+method.Name, func(t *testing.T) {
				err := callWithEmptyTenant(t, subject, method)
				require.Error(t, err, "%s accepted an empty tenant", method.Name)
				require.Contains(t, err.Error(), "tenant scope required",
					"%s refused an empty tenant without saying why", method.Name)
			})
		}
	}
}

// takesContextAndTenant reports whether a method's first two arguments are a
// context and a string, which is the shape every tenant-scoped call here has.
func takesContextAndTenant(typ reflect.Type) bool {
	return typ.NumIn() >= 3 &&
		typ.In(1) == reflect.TypeOf((*context.Context)(nil)).Elem() &&
		typ.In(2).Kind() == reflect.String &&
		typ.NumOut() >= 1 &&
		typ.Out(typ.NumOut()-1) == reflect.TypeOf((*error)(nil)).Elem()
}

// callWithEmptyTenant invokes one method with an empty tenant and plausible
// values for everything else, and returns the error it produced.
func callWithEmptyTenant(t *testing.T, subject any, method reflect.Method) error {
	t.Helper()
	typ := method.Type
	args := make([]reflect.Value, typ.NumIn())
	args[0] = reflect.ValueOf(subject)
	args[1] = reflect.ValueOf(context.Background())
	args[2] = reflect.ValueOf("")
	for i := 3; i < typ.NumIn(); i++ {
		args[i] = plausible(typ.In(i))
	}
	out := reflect.ValueOf(subject).Method(method.Index).Call(args[1:])
	last := out[len(out)-1]
	if last.IsNil() {
		return nil
	}
	err, ok := last.Interface().(error)
	require.True(t, ok)
	return err
}

// plausible is a non-zero value of the given type, so that a method refusing
// the empty tenant is refusing the TENANT rather than some other empty field.
func plausible(typ reflect.Type) reflect.Value {
	switch {
	case typ == reflect.TypeOf(time.Time{}):
		return reflect.ValueOf(time.Now().UTC())
	case typ == reflect.TypeOf((*io.Reader)(nil)).Elem():
		return reflect.ValueOf(io.Reader(strings.NewReader("bytes")))
	case typ == reflect.TypeOf(&dholev1.Digest{}):
		return reflect.ValueOf(&dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("ab", 32)})
	case typ == reflect.TypeOf(tenancy.Usage{}):
		return reflect.ValueOf(tenancy.Usage{
			Kind: tenancy.KindStepSeconds, RunID: "run-1", StepID: "build",
			Attempt: 1, Quantity: 1000, At: time.Now().UTC(),
		})
	case typ == reflect.TypeOf(tenancy.Quota{}):
		return reflect.ValueOf(tenancy.DefaultQuota())
	case typ.Kind() == reflect.String:
		return reflect.ValueOf("some-value").Convert(typ)
	case typ.Kind() == reflect.Int:
		return reflect.ValueOf(1).Convert(typ)
	case typ.Kind() == reflect.Int64:
		return reflect.ValueOf(int64(1)).Convert(typ)
	default:
		return reflect.New(typ).Elem()
	}
}

// writeAttempt appends the events one attempt of one step produces: the
// dispatch, and the terminal event that ends it. It writes them through the
// real run store, because the meter's whole claim is that it can reconstruct a
// bill from that log and a fake log would let it claim that against nothing.
func writeAttempt(
	t *testing.T,
	log runstore.Store,
	tenantID, runID, stepID string,
	attempt uint32,
	dispatchedAt, endedAt time.Time,
	terminal runstore.EventType,
	cacheHit bool,
) {
	t.Helper()
	ctx := context.Background()
	payload, err := scheduler.MarshalDispatched(scheduler.Dispatched{
		Attempt: attempt, Fence: uint64(attempt), Cacheable: cacheHit, CacheHit: cacheHit,
	})
	require.NoError(t, err)
	require.NoError(t, log.Append(ctx, tenantID, runstore.Event{
		RunID: runID, StepID: stepID, Attempt: attempt, Sequence: 0,
		Type: runstore.StepDispatched, Payload: payload, At: dispatchedAt,
	}))
	require.NoError(t, log.Append(ctx, tenantID, runstore.Event{
		RunID: runID, StepID: stepID, Attempt: attempt, Sequence: 0,
		Type: terminal, At: endedAt,
	}))
}

// digestOf is the digest bytes WOULD have, computed by storing them under a
// throwaway tenant, so a test can ask whether a refused blob landed.
func digestOf(t *testing.T, store cas.Store, tenantID, content string) *dholev1.Digest {
	t.Helper()
	d, err := store.Put(context.Background(), tenantID+"-probe", strings.NewReader(content))
	require.NoError(t, err)
	return d
}

func dial(t *testing.T, creds string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(creds, nats.Timeout(3*time.Second))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func subscribe(t *testing.T, conn *nats.Conn, subject string) chan []byte {
	t.Helper()
	in := make(chan []byte, 1)
	sub, err := conn.Subscribe(subject, func(m *nats.Msg) { in <- m.Data })
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return in
}

// TestMirroredSchedulerContractsHaveNotDrifted guards the one place this
// package copies rather than imports. Quotas are enforced inside the
// scheduler, so the scheduler imports tenancy and tenancy cannot import it
// back; the meter therefore mirrors the STEP_ATTEMPT_LOST event type and the
// cache_hit field of the dispatch payload. Both are persistence contracts, but
// a mirror nobody checks is a mirror that drifts, so this checks it.
func TestMirroredSchedulerContractsHaveNotDrifted(t *testing.T) {
	require.Equal(t, scheduler.StepAttemptLost, tenancy.StepAttemptLost,
		"the meter mirrors an event type the scheduler has since renamed")

	payload, err := scheduler.MarshalDispatched(scheduler.Dispatched{Attempt: 1, CacheHit: true})
	require.NoError(t, err)
	require.Contains(t, string(payload), `"cache_hit":true`,
		"the meter reads cache_hit off the dispatch payload and the scheduler no longer writes it")
}
