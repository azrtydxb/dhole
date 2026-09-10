package wait_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/gate"
)

// gateWait is how long the gate in these pipelines waits. It is long enough
// that no case here can pass by the timer having quietly come due: a gate that
// was skipped and a gate that fired are both "the step is finished", and the
// only way to tell them apart is to make firing impossible.
const gateWait = time.Hour

// gatedPipeline is a durable gate and the step behind it. The gate is a step
// whose PLUGIN REF says it waits — which is the whole fix: gatedness is a fact
// about the definition, so no reading of the log can miss it.
func gatedPipeline() *dholev1.Pipeline {
	p := waitingPipeline()
	hold := p.GetSteps()[0]
	hold.PluginRef = gate.PluginRef
	hold.Config = map[string]string{gate.ConfigDuration: gateWait.String()}
	return p
}

// TestAGateIsArmedInsteadOfDispatchedByTheSameDecisionThatFoundItReady is the
// atomicity the finding asked for, stated as a property of one Advance.
//
// The gate used to be armed by whoever started the run, in a transaction of
// its own, while the scheduler decided readiness in another. Sequence numbers
// are allocated inside a transaction and VISIBILITY is not, so the gate could
// hold a lower sequence than the STEP_DISPATCHED of the very step it gated:
// the log read as "gated, then dispatched anyway", and the wait was skipped
// entirely. Both acceptance pipelines papered over it by arming behind a
// five-second predecessor.
//
// There is no window left to lose, because there is no second writer. The
// readiness decision and the arming are the same act, and what makes the step
// a gate is its plugin ref rather than an event somebody else has to have
// committed first.
func TestAGateIsArmedInsteadOfDispatchedByTheSameDecisionThatFoundItReady(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)

		p := newPlaneFor(ctx, t, open(t), startBus(t), tenant, gatedPipeline())
		p.seedRun(ctx, t, tenant)

		before := time.Now().UTC()
		require.NoError(t, p.sched.Advance(ctx, tenant, testRun))

		require.Empty(t, p.drain(ctx, t),
			"a gate waits; nothing about it is work for an engine")
		require.Equal(t, 1, p.countEvents(ctx, t, tenant, testRun, scheduler.StepAwaitingTimer),
			"the advance that found the gate ready is what armed it")
		require.Zero(t, p.countEvents(ctx, t, tenant, testRun, runstore.StepDispatched),
			"the gated step was dispatched: the wait was skipped entirely")

		due, pending, err := p.timers.Pending(ctx, tenant, testRun, "hold")
		require.NoError(t, err)
		require.True(t, pending, "a gate event with no timer row waits for ever")
		require.WithinDuration(t, before.Add(gateWait), due, time.Minute,
			"the gate waits for the duration its step declared")

		// Advancing again changes nothing. The gate is now in the log as
		// well as in the definition, and both readings have to agree.
		require.NoError(t, p.sched.Advance(ctx, tenant, testRun))
		require.Empty(t, p.drain(ctx, t))
		require.Equal(t, 1, p.countEvents(ctx, t, tenant, testRun, scheduler.StepAwaitingTimer),
			"a second pass re-armed a gate that was already armed")
		require.Zero(t, p.countEvents(ctx, t, tenant, testRun, runstore.RunCompleted),
			"a run holding a gate is neither finished nor idle")
	})
}

// armingStore is the run store with the finding's exact interval opened up: a
// gate is armed by SOMEBODY ELSE in the moment between the scheduler reading
// the run's log and the scheduler writing to it.
//
// It is what turns an ordering bug into a deterministic one. Advance replays
// outside the transaction it later appends in, and the gap between the two is
// a definition lookup, a graph build and a plan — microseconds, hit
// intermittently on a real deployment and essentially never in a test that
// merely ran two things at once.
type armingStore struct {
	runstore.Store

	once   sync.Once
	arm    func(context.Context) error
	armErr error
}

func (s *armingStore) Replay(ctx context.Context, tenantID, runID string) ([]runstore.Event, error) {
	events, err := s.Store.Replay(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}
	// AFTER the read and BEFORE the caller can act on it. The arming commits
	// here, so its event holds a lower sequence than anything this Advance
	// goes on to write — and the caller's copy of the log does not contain it.
	s.once.Do(func() { s.armErr = s.arm(ctx) })
	if s.armErr != nil {
		return nil, s.armErr
	}
	return events, nil
}

// TestAGateArmedAfterTheSchedulerReadTheLogStillStopsTheStepFromRunning holds
// the fix to the harder half of the property.
//
// Recognising the gate from the log alone would pass the test above and fail
// here, because this log is the one the scheduler read before the gate existed
// in it. The step is a gate because its definition says so, which is a fact
// that was true before the run started and cannot be raced.
func TestAGateArmedAfterTheSchedulerReadTheLogStillStopsTheStepFromRunning(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		store := open(t)

		racing := &armingStore{Store: store}
		p := newPlaneFor(ctx, t, racing, startBus(t), tenant, gatedPipeline())
		p.seedRun(ctx, t, tenant)

		// The other writer: the run's starter arming the wait, on its own
		// connection, in its own transaction — which is what every gated
		// pipeline had to do before a gate was a step type.
		outsider := p.timers
		racing.arm = func(ctx context.Context) error {
			return outsider.Schedule(ctx, tenant, testRun, "hold", time.Now().UTC().Add(gateWait))
		}

		require.NoError(t, p.sched.Advance(ctx, tenant, testRun))

		require.Empty(t, p.drain(ctx, t),
			"the gate was armed while the scheduler held a log that predated it, "+
				"and the scheduler dispatched the gated step anyway")
		require.Zero(t, p.countEvents(ctx, t, tenant, testRun, runstore.StepDispatched),
			"the gated step was dispatched despite its timer")
	})
}

// barrierStore holds the first n callers of Replay together and then releases
// all of them, so two advances of one run demonstrably read the same log
// before either of them writes.
//
// The pattern is internal/scheduler/terminal_test.go's, for the same reason:
// two goroutines started at once are a coin toss, and an ordering bug that is
// only sometimes reproduced is not tested at all.
type barrierStore struct {
	runstore.Store

	mu      sync.Mutex
	arrived int
	held    int
	release chan struct{}
}

func newBarrierStore(store runstore.Store, hold int) *barrierStore {
	return &barrierStore{Store: store, held: hold, release: make(chan struct{})}
}

func (b *barrierStore) Replay(ctx context.Context, tenantID, runID string) ([]runstore.Event, error) {
	events, err := b.Store.Replay(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.arrived++
	last := b.arrived == b.held
	hold := b.arrived <= b.held
	b.mu.Unlock()

	switch {
	case last:
		close(b.release)
	case hold:
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return events, nil
}

// TestTwoAdvancesThatBothFindOneGateReadyArmItOnce is migration 0023, and it
// is the same check-then-act migrations 0020 and 0021 closed one level up.
//
// Advance has two triggers — the open-run tick and a status arriving from an
// engine — and they meet on one run. Both replay a log with no gate in it,
// both conclude the gate is ready, and both arm it. Each append is given its
// own sequence, so nothing collides and the run's log says the step began
// waiting twice: two due times for one wait, in the one place a person looks
// to find out what a stopped run is stopped on.
func TestTwoAdvancesThatBothFindOneGateReadyArmItOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		store := open(t)

		seed := newPlaneFor(ctx, t, store, startBus(t), tenant, gatedPipeline())
		seed.seedRun(ctx, t, tenant)

		p := newPlaneFor(ctx, t, newBarrierStore(store, 2), startBus(t), tenant, gatedPipeline())

		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = p.sched.Advance(ctx, tenant, testRun)
			}()
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err, "neither advance is wrong to conclude the gate is ready")
		}

		require.Equal(t, 1, seed.countEvents(ctx, t, tenant, testRun, scheduler.StepAwaitingTimer),
			"both advances armed the same gate; only one may say so in the log")
		require.Zero(t, seed.countEvents(ctx, t, tenant, testRun, runstore.StepDispatched),
			"the gated step was dispatched")
	})
}
