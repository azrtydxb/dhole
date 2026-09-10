package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// singleStep is the pipeline the double-completion bug was observed on: one
// step, no edges, so the run is over the moment that step succeeds and there
// is exactly one pass in which two advances can both conclude it.
func singleStep() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{{
			Id:          "a",
			Name:        "a",
			PluginRef:   "cmd://echo",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{{Name: "out"}},
		}},
	}
}

// replayBarrier is the run store with a rendezvous inside Replay: the first n
// callers are held there until all n have arrived, and then all of them go on.
//
// It exists because the race is otherwise a coin toss. Advance reads the log
// OUTSIDE the transaction it later writes in, and the interval between the two
// is a definition lookup, a graph build and a plan — microseconds, hit
// intermittently in production and almost never in a test. Holding the readers
// together turns "sometimes" into "every time", which is the only version of
// this test worth having.
type replayBarrier struct {
	runstore.Store

	mu      sync.Mutex
	arrived int
	held    int
	release chan struct{}
}

func newReplayBarrier(store runstore.Store, hold int) *replayBarrier {
	return &replayBarrier{Store: store, held: hold, release: make(chan struct{})}
}

func (b *replayBarrier) Replay(ctx context.Context, tenantID, runID string) ([]runstore.Event, error) {
	events, err := b.Store.Replay(ctx, tenantID, runID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.arrived++
	last := b.arrived == b.held
	wait := b.arrived <= b.held
	b.mu.Unlock()

	switch {
	case last:
		close(b.release)
	case wait:
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return events, nil
}

// TestTwoAdvancesOfOneFinishedRunWriteOneTerminalEvent is the production bug:
// a run whose log carried RUN_COMPLETED twice.
//
// Nothing in the deployment was unusual — one control-plane replica, one step,
// one engine. Advance has two independent triggers, the open-run tick and a
// status arriving from an engine, and they fire on the same run at the same
// time. Each replayed the log, each found nothing ready and nothing in flight,
// and each appended the completion the other had not written yet.
//
// A second terminal event is not cosmetic. Every consumer treats the terminal
// event as the end of the run, and the SSE stream closes the client's
// connection on it — so the second one is a close of a stream that has already
// gone.
func TestTwoAdvancesOfOneFinishedRunWriteOneTerminalEvent(t *testing.T) {
	ctx := testContext(t)
	h := newHarnessWith(ctx, t, singleStep(), readyEngine("engine-1"))

	// The run as the deployment left it: the one step dispatched and
	// succeeded, and nothing yet saying the run is over.
	dispatched, err := scheduler.MarshalDispatched(scheduler.Dispatched{Attempt: 1})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Attempt: 1,
		Type: runstore.StepDispatched, Payload: dispatched, At: time.Now().UTC(),
	}))
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Attempt: 1,
		Type: runstore.StepSucceeded, At: time.Now().UTC(),
	}))

	sched := h.planeOverStore(ctx, t, newReplayBarrier(h.store, 2))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sched.Advance(ctx, testTenant, testRun)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err, "neither advance is wrong to conclude the run is over")
	}

	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var completions int
	for _, e := range events {
		if e.Type == runstore.RunCompleted {
			completions++
		}
	}
	require.Equal(t, 1, completions,
		"both advances concluded the run was complete; only one may say so in the log")
}

// planeOverStore is another control plane on a store of the caller's choosing,
// sharing everything else with the harness. It is what lets a test put a seam
// in the store the scheduler reads through.
func (h *harness) planeOverStore(
	_ context.Context, t *testing.T, store runstore.Store,
) *scheduler.Scheduler {
	t.Helper()
	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      h.outbox,
		Leases:      h.leases,
		Fleet:       h.fleet,
		Definitions: h.defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)
	return sched
}
