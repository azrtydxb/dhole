package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestEventAndPublishCommitTogether is the whole reason this package exists.
// The bus is not the source of truth (ADR 0005): if a run event were committed
// and its dispatch published as two separate acts, a crash between them would
// lose the step silently, and no amount of retrying recovers what was never
// recorded. The event and the intent to publish are therefore ONE transaction —
// which means a rollback has to take both with it, never leave one behind.
//
// The failure this catches is the enqueue writing on its own connection
// instead of the caller's transaction: everything still passes on the happy
// path, and the store only disagrees with the bus after a rollback nobody
// tested.
func TestEventAndPublishCommitTogether(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := context.Background()
		store := open(t)
		recorder := &recordingBus{}
		ob := outbox.New(store, recorder)
		tenant := uniqueTenant(t)

		sentinel := errors.New("caller aborted the transaction")
		err := store.WithTx(ctx, func(tx runstore.Tx) error {
			if err := tx.Append(ctx, tenant, runstore.Event{
				RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 1,
				Type: runstore.StepDispatched,
				At:   time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
			}); err != nil {
				return err
			}
			if err := ob.Enqueue(ctx, tx, tenant,
				bus.SubjectDispatch("trusted", "caps"),
				dispatch("run-1", "build", "fence-1")); err != nil {
				return err
			}
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)

		replayed, err := store.Replay(ctx, tenant, "run-1")
		require.NoError(t, err)
		require.Empty(t, replayed, "a rolled-back transaction must leave no event behind")

		drained, err := ob.Drain(ctx)
		require.NoError(t, err)
		require.Zero(t, drained, "a rolled-back transaction must leave nothing to publish")
		require.Empty(t, recorder.subjects(), "nothing reaches the bus for a transaction that never committed")

		// The committed case is the other half of the same claim: both
		// survive together, or the guarantee is vacuous.
		require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
			if err := tx.Append(ctx, tenant, runstore.Event{
				RunID: "run-2", StepID: "build", Attempt: 1, Sequence: 2,
				Type: runstore.StepDispatched,
				At:   time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
			}); err != nil {
				return err
			}
			return ob.Enqueue(ctx, tx, tenant,
				bus.SubjectDispatch("trusted", "caps"),
				dispatch("run-2", "build", "fence-2"))
		}))

		replayed, err = store.Replay(ctx, tenant, "run-2")
		require.NoError(t, err)
		require.Len(t, replayed, 1)
		drained, err = ob.Drain(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, drained)
	})
}

// TestEnqueueRejectsAnUnscopedTenant holds the outbox to the same rule as
// every other stored record: there is no unscoped row and no unscoped subject,
// even while only one tenant exists.
func TestEnqueueRejectsAnUnscopedTenant(t *testing.T) {
	ctx := context.Background()
	store := openSQLite(t, t.TempDir())
	ob := outbox.New(store, &recordingBus{})

	err := store.WithTx(ctx, func(tx runstore.Tx) error {
		return ob.Enqueue(ctx, tx, "", bus.SubjectDispatch("trusted", "caps"), dispatch("run-1", "build", "f"))
	})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
}

// TestDrainPublishesThenMarksSent runs against a real embedded NATS with a
// real JetStream work queue, because the assertion is that the bytes actually
// travel — a fake bus can only prove that a method was called.
//
// The second Drain returning 0 is the part that matters: a row published but
// never marked sent republishes forever.
func TestDrainPublishesThenMarksSent(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := context.Background()
		store := open(t)
		drainAll(ctx, t, store) // Postgres is shared: start from an empty table.

		b := connectEmbedded(t, startEmbedded(t))
		require.NoError(t, b.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))
		sub, err := b.SubscribePull(ctx, "DISPATCH", "drain-test", "job.dispatch.>")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sub.Close()) })

		ob := outbox.New(store, b)
		tenant := uniqueTenant(t)
		subject := bus.SubjectDispatch("trusted", "caps")
		want := dispatch("run-1", "build", "fence-7")

		require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
			return ob.Enqueue(ctx, tx, tenant, subject, want)
		}))

		drained, err := ob.Drain(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, drained)

		fetchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		msg, err := sub.Next(fetchCtx)
		require.NoError(t, err)
		var got dholev1.JobDispatch
		require.NoError(t, proto.Unmarshal(msg.Data(), &got))
		require.NoError(t, msg.Ack())
		require.True(t, proto.Equal(want, &got), "the published payload must be the enqueued message")
		// The subject and payload have to carry enough for the consumer to
		// deduplicate a republished delivery on its own: at-least-once is the
		// honest guarantee, so (run, step, attempt, fence) must survive.
		require.Equal(t, "fence-7", got.GetFenceToken())

		drained, err = ob.Drain(ctx)
		require.NoError(t, err)
		require.Zero(t, drained, "a published row must be marked sent, not published again on the next drain")
	})
}

// TestDrainRetriesAfterBusFailure is the property the whole design rests on: a
// publish that fails must stay OWED. The row is durable state, the publish is
// a retryable side effect, and a bus outage may only make a message late.
//
// The bus is a real NATS that is really shut down, and the retry runs against
// a freshly started one over the SAME store — which is exactly what a bus
// restart looks like from the control plane's side.
func TestDrainRetriesAfterBusFailure(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := context.Background()
		store := open(t)
		drainAll(ctx, t, store)

		down := startEmbedded(t)
		downBus := connectEmbedded(t, down)
		require.NoError(t, downBus.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))

		ob := outbox.New(store, downBus)
		tenant := uniqueTenant(t)
		subject := bus.SubjectDispatch("trusted", "caps")
		want := dispatch("run-1", "build", "fence-9")
		require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
			return ob.Enqueue(ctx, tx, tenant, subject, want)
		}))

		down.Close()

		failCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		drained, err := ob.Drain(failCtx)
		require.Error(t, err, "a drain that could not publish must say so, not report success")
		require.Zero(t, drained)

		// The bus comes back. Nothing was lost; the message is merely late.
		up := startEmbedded(t)
		upBus := connectEmbedded(t, up)
		require.NoError(t, upBus.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))
		sub, err := upBus.SubscribePull(ctx, "DISPATCH", "retry-test", "job.dispatch.>")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, sub.Close()) })

		retried := outbox.New(store, upBus)
		drained, err = retried.Drain(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, drained, "a row left unsent by a failed publish must still be owed")

		fetchCtx, cancelFetch := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFetch()
		msg, err := sub.Next(fetchCtx)
		require.NoError(t, err)
		var got dholev1.JobDispatch
		require.NoError(t, proto.Unmarshal(msg.Data(), &got))
		require.NoError(t, msg.Ack())
		require.True(t, proto.Equal(want, &got))
	})
}

// TestDrainLeavesAFailedPublishOwed is TestDrainRetriesAfterBusFailure's
// sharper twin, and it exists because that test alone did not catch a real
// break. Shutting a real NATS down makes the publish hang until the drain's
// context expires, and an expired context then fails the mark-sent write too,
// so the whole transaction rolls back and the row survives no matter what
// order the implementation used. The bug that hides behind that — marking a
// row sent BEFORE the bus accepted it — is the silent loss this package
// exists to prevent, so it needs a failure that is immediate and leaves the
// context healthy.
//
// The break it catches: move `sent = append(...)` above the Publish call and
// the row is stamped sent for a message that never went anywhere. The step is
// gone, with nothing owed and nothing to retry.
func TestDrainLeavesAFailedPublishOwed(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := context.Background()
		store := open(t)
		drainAll(ctx, t, store)

		tenant := uniqueTenant(t)
		subject := bus.SubjectDispatch("trusted", "caps")
		enqueue := func() {
			require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
				return outbox.New(store, &recordingBus{}).
					Enqueue(ctx, tx, tenant, subject, dispatch("run-1", "build", "fence-1"))
			}))
		}
		enqueue()

		// The bus refuses instantly and the context stays healthy, so nothing
		// but the implementation's own ordering decides what happens next.
		down := outbox.New(store, &failingBus{})
		drained, err := down.Drain(ctx)
		require.Error(t, err, "a publish the bus refused is not a drain that succeeded")
		require.Zero(t, drained, "a row is only drained once the bus has accepted it")

		back := &recordingBus{}
		drained, err = outbox.New(store, back).Drain(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, drained, "a row whose publish failed must still be owed")
		require.Equal(t, []string{subject}, back.subjects())
	})
}

// TestDrainKeepsPublishingAfterOneFailure covers a batch that fails partway:
// the rows the bus took are marked sent, the rest stay owed. Neither half may
// be lost, and the sent half must not go out twice as a matter of course.
func TestDrainStopsAtTheFirstFailureAndOwesTheRest(t *testing.T) {
	ctx := context.Background()
	store := openSQLite(t, t.TempDir())
	flaky := &flakyBus{failAfter: 2}

	require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
		ob := outbox.New(store, flaky)
		tenant := uniqueTenant(t)
		for i := range 5 {
			if err := ob.Enqueue(ctx, tx, tenant, bus.SubjectDispatch("trusted", "caps"),
				dispatch("run-"+strconv.Itoa(i), "build", "fence-"+strconv.Itoa(i))); err != nil {
				return err
			}
		}
		return nil
	}))

	drained, err := outbox.New(store, flaky).Drain(ctx)
	require.Error(t, err)
	require.Equal(t, 2, drained, "the rows the bus accepted are drained")

	back := &recordingBus{}
	drained, err = outbox.New(store, back).Drain(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, drained, "the rows the bus refused are still owed, and only those")
}

// TestConcurrentDrainersPublishEachRowOnce covers two control planes draining
// the same table at the same time. Duplicate delivery is survivable — the
// guarantee is at-least-once — but publishing every row twice as a matter of
// course is not a retry, it is a design that doubles the load. Each dialect
// claims rows before publishing; this asserts the claim actually excludes.
func TestConcurrentDrainersPublishEachRowOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := context.Background()
		writer := open(t)
		drainAll(ctx, t, writer)

		const rows = 40
		tenant := uniqueTenant(t)
		require.NoError(t, writer.WithTx(ctx, func(tx runstore.Tx) error {
			ob := outbox.New(writer, &recordingBus{})
			for i := range rows {
				if err := ob.Enqueue(ctx, tx, tenant,
					bus.SubjectDispatch("trusted", "caps"),
					dispatch("run-"+strconv.Itoa(i), "build", "fence-"+strconv.Itoa(i))); err != nil {
					return err
				}
			}
			return nil
		}))

		// The bus is deliberately slow. With an instant one the first drainer
		// finishes its whole batch before the others have begun, and the test
		// passes without two of them ever overlapping — which is to say
		// without testing the claim at all. A millisecond per publish opens a
		// window wide enough for the claim to be the thing that decides.
		shared := &slowBus{delay: time.Millisecond}
		start := make(chan struct{})
		var (
			wg     sync.WaitGroup
			total  atomic.Int64
			errsMu sync.Mutex
			errs   []error
		)
		for range 4 {
			store := open(t)
			ob := outbox.New(store, shared)
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for {
					n, err := ob.Drain(ctx)
					if err != nil {
						errsMu.Lock()
						errs = append(errs, err)
						errsMu.Unlock()
						return
					}
					if n == 0 {
						return
					}
					total.Add(int64(n))
				}
			}()
		}
		close(start)
		wg.Wait()
		require.Empty(t, errs)

		require.Equal(t, int64(rows), total.Load(), "every row is claimed by exactly one drainer")
		require.Len(t, shared.subjects(), rows, "a claimed row must not be published by a second drainer")
	})
}

// TestRunStopsOnContextCancellation: the polling drainer is a background
// goroutine in the control plane. It has to end when the process is shutting
// down rather than outlive it.
func TestRunStopsOnContextCancellation(t *testing.T) {
	store := openSQLite(t, t.TempDir())
	ob := outbox.New(store, &recordingBus{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ob.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the drainer must stop when its context is cancelled")
	}
}

// TestRunDoesNotSpinWhenTheBusIsDown: a bus outage must cost one attempt per
// tick, not a hot loop that burns a core and floods the log for as long as the
// outage lasts.
func TestRunDoesNotSpinWhenTheBusIsDown(t *testing.T) {
	ctx := context.Background()
	store := openSQLite(t, t.TempDir())
	failing := &failingBus{}
	ob := outbox.New(store, failing)

	require.NoError(t, store.WithTx(ctx, func(tx runstore.Tx) error {
		return ob.Enqueue(ctx, tx, uniqueTenant(t), bus.SubjectDispatch("trusted", "caps"),
			dispatch("run-1", "build", "fence-1"))
	}))

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- ob.Run(runCtx) }()

	time.Sleep(time.Second)
	cancel()
	<-done

	attempts := failing.calls.Load()
	require.Positive(t, attempts, "the drainer must keep retrying a failed publish")
	require.Less(t, attempts, int64(30),
		"a drainer that retries on a 200ms tick attempts about 5 times a second, not thousands")
}

// dispatch builds the message the outbox actually carries. It is returned by
// pointer: every generated message embeds a mutex, so a value copy trips go
// vet's copylocks check, which is part of the gate.
func dispatch(runID, stepID, fence string) *dholev1.JobDispatch {
	return &dholev1.JobDispatch{
		RunId: runID, StepId: stepID, Attempt: 1, FenceToken: fence,
		Tenant: &dholev1.Tenant{Id: "tenant"},
	}
}

// storeOpener returns a handle to the SAME database on every call, so a test
// can hold several independent connections to it.
type storeOpener func(t *testing.T) runstore.Store

// eachStore runs a case against both implementations. SQLite is the
// development and homelab store and Postgres the tuned target; an outbox that
// only holds on one of them is an outbox that loses steps in production.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		dir := t.TempDir()
		fn(t, func(t *testing.T) runstore.Store { return openSQLite(t, dir) })
	})
	t.Run("postgres", func(t *testing.T) {
		d := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
		if d == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			store, err := runstore.NewPostgres(context.Background(), d)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			return store
		})
	})
}

func openSQLite(t *testing.T, dir string) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(filepath.Join(dir, "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// drainAll empties the table before a case asserts on counts. Postgres is a
// shared, persistent database: a count that includes what an earlier run left
// behind is not testing what it claims to.
func drainAll(ctx context.Context, t *testing.T, store runstore.Store) {
	t.Helper()
	ob := outbox.New(store, &recordingBus{})
	for {
		n, err := ob.Drain(ctx)
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
}

func startEmbedded(t *testing.T) *bus.Embedded {
	t.Helper()
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return srv
}

func connectEmbedded(t *testing.T, srv *bus.Embedded) *bus.NATS {
	t.Helper()
	conn, err := bus.Connect(context.Background(), srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

var tenantSeq atomic.Uint64

func uniqueTenant(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("outbox-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}

// recordingBus is the bus for cases whose assertion is about what was or was
// not handed over. Cases asserting that bytes really travel use a real
// embedded NATS instead.
type recordingBus struct {
	mu   sync.Mutex
	sent []string
}

var _ bus.Bus = (*recordingBus)(nil)

func (r *recordingBus) Publish(_ context.Context, subject string, _ proto.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, subject)
	return nil
}

func (r *recordingBus) subjects() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func (r *recordingBus) Request(context.Context, string, proto.Message, proto.Message) error {
	return errors.New("not used")
}

func (r *recordingBus) SubscribePull(context.Context, string, string, string) (bus.Subscription, error) {
	return nil, errors.New("not used")
}

func (r *recordingBus) SubscribeEphemeral(context.Context, string, func([]byte)) (func(), error) {
	return nil, errors.New("not used")
}

// flakyBus accepts failAfter messages and then refuses, with no delay either
// way, so a test can see what a drain does with a half-published batch.
type flakyBus struct {
	recordingBus
	failAfter int
	taken     atomic.Int64
}

func (f *flakyBus) Publish(ctx context.Context, subject string, msg proto.Message) error {
	if f.taken.Add(1) > int64(f.failAfter) {
		return errors.New("bus: refused")
	}
	return f.recordingBus.Publish(ctx, subject, msg)
}

// slowBus takes its time accepting a message, so concurrent drainers actually
// overlap instead of finishing one after another by accident.
type slowBus struct {
	recordingBus
	delay time.Duration
}

func (s *slowBus) Publish(ctx context.Context, subject string, msg proto.Message) error {
	time.Sleep(s.delay)
	return s.recordingBus.Publish(ctx, subject, msg)
}

// failingBus is a bus that is down and counts how hard the drainer leans on it.
type failingBus struct {
	recordingBus
	calls atomic.Int64
}

func (f *failingBus) Publish(context.Context, string, proto.Message) error {
	f.calls.Add(1)
	return errors.New("bus: down")
}
