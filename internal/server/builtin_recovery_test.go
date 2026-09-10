// Recovery of a step the PLANE was running when it died.
//
// A step dispatched to an engine is recovered because it is leased: the holder
// stops renewing, lease.Expire hands the orphan to a sweeper, STEP_ATTEMPT_LOST
// goes in the log and the ordinary rules decide what happens next. A builtin
// step wrote STEP_DISPATCHED and held no lease at all, so SweepOrphans could
// not see it — and no engine would ever report a status for it either, because
// no engine ever had it. The run was stuck forever, with nothing anywhere that
// could notice.
package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// recoveryLeaseTTL is short so the sweeper on the SECOND plane notices the
// dead holder in a few seconds rather than in the default thirty. It is the
// deployment knob, not a test hook: the same value governs an engine's leases
// in this plane.
const recoveryLeaseTTL = 2 * time.Second

// TestABuiltinStepInFlightWhenThePlaneDiesIsRecoveredByANewPlane is clause (e),
// stated as the only thing that matters about it: the run finishes.
//
// The first plane takes the step, writes its STEP_DISPATCHED and is stopped
// while the model call is still outstanding — so no terminal event is ever
// written for that attempt, which is exactly the log a killed plane leaves
// behind. A second plane over the same store then has to notice, say so, and
// let the run continue. Without a lease on a builtin step it cannot: nothing
// in the system can tell that attempt from one still running.
func TestABuiltinStepInFlightWhenThePlaneDiesIsRecoveredByANewPlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	held := &blockingModel{id: "test-model", entered: make(chan struct{}, 1)}
	first := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.StoreDSN = filepath.Join(dir, "dhole.db")
		cfg.BlobRoot = filepath.Join(dir, "state")
		cfg.LeaseTTL = recoveryLeaseTTL
		cfg.Models = held.factory()
	})

	runID, err := first.Submit(ctx, tenantID, llmPipeline("builtin-recovery", "classify this release"))
	require.NoError(t, err)

	// In flight: the plane wrote the dispatch and the model call is parked.
	awaitStepEvent(ctx, t, first, runID, "classify", runstore.StepDispatched)
	select {
	case <-held.entered:
	case <-ctx.Done():
		t.Fatal("the plane never reached the model call")
	}

	// Nothing has decided the step, which is what makes the rest of this a
	// test of recovery rather than of a step that had already finished.
	events, err := first.Events(ctx, tenantID, runID)
	require.NoError(t, err)
	require.False(t, hasEvent(events, "classify", runstore.StepSucceeded),
		"the step finished before the plane stopped, so this test proves nothing")
	require.False(t, hasEvent(events, "classify", runstore.StepFailed),
		"the step failed before the plane stopped, so this test proves nothing")

	// And now the plane goes away with the step still open. Stopping is the
	// only way a test can end a plane in-process, and it leaves the log a kill
	// would leave: a STEP_DISPATCHED with no verdict after it.
	stopPlane(t, first)

	// A NEW plane over the same store, with a model that answers.
	answering := &scriptedModel{id: "test-model"}
	answering.always(`{"severity":"high","done":true}`)
	second := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.StoreDSN = filepath.Join(dir, "dhole.db")
		cfg.BlobRoot = filepath.Join(dir, "state")
		cfg.LeaseTTL = recoveryLeaseTTL
		cfg.Models = answering.factory()
	})

	recovered := awaitRunCompleted(ctx, t, second, runID)
	requireStepSucceeded(t, recovered, "classify")

	// And the recovery is the ordinary one, written down: the dead attempt is
	// recorded as lost rather than quietly replaced, which is what an operator
	// reads afterwards to find out why the step ran twice.
	lost := requireEvent(t, recovered, "classify", scheduler.StepAttemptLost)
	var record scheduler.AttemptLost
	require.NoError(t, json.Unmarshal(lost.Payload, &record))
	require.Equal(t, uint32(1), record.Attempt)
	require.NotZero(t, record.Fence, "a lost attempt with no fence held no lease")

	require.GreaterOrEqual(t, countStepEvents(recovered, "classify", runstore.StepDispatched), 2,
		"the recovered run never dispatched a second attempt; log: %s", describe(recovered))
}

// countStepEvents counts one kind of event for one step.
func countStepEvents(events []runstore.Event, stepID string, kind runstore.EventType) int {
	n := 0
	for _, e := range events {
		if e.StepID == stepID && e.Type == kind {
			n++
		}
	}
	return n
}

// blockingModel is a language model whose call never returns until the caller's
// context is cancelled — a step that is genuinely in flight when the plane
// stops, rather than one that has already finished.
type blockingModel struct {
	id string
	// entered is signalled once the call is parked, so the test stops the
	// plane at a moment it knows the step is running.
	entered chan struct{}
	once    sync.Once
}

func (m *blockingModel) factory() server.ModelFactory {
	return func(context.Context, server.ModelRequest) (provider.LanguageModel, error) {
		return m, nil
	}
}

func (m *blockingModel) ModelID() string      { return m.id }
func (m *blockingModel) ProviderName() string { return "stub" }

func (m *blockingModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: true}
}

func (m *blockingModel) Generate(ctx context.Context, _ provider.Call) (*provider.Response, error) {
	m.once.Do(func() { m.entered <- struct{}{} })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (m *blockingModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, fmt.Errorf("blocking model %q does not stream", m.id)
}

// TestABuiltinStepThatOutlivesItsLeaseIsNotSweptOutFromUnderItself is the
// other half of leasing one: a lease that is claimed and never renewed makes
// every step slower than the TTL look dead, and a model call routinely is.
//
// The run must finish with ONE attempt. A second dispatch here would mean the
// plane declared its own working step lost and paid for the model call twice.
func TestABuiltinStepThatOutlivesItsLeaseIsNotSweptOutFromUnderItself(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Well past the TTL and past one orphan sweep, so an unrenewed lease is
	// certain to have expired and been swept while the step is still running.
	slow := &slowModel{id: "test-model", answer: `{"severity":"low","done":true}`, delay: 8 * time.Second}
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.LeaseTTL = recoveryLeaseTTL
		cfg.Models = slow.factory()
	})

	runID, err := srv.Submit(ctx, tenantID, llmPipeline("builtin-slow", "classify this release"))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "classify")
	require.Equal(t, 1, countStepEvents(events, "classify", runstore.StepDispatched),
		"a step that was still running was dispatched again; log: %s", describe(events))
	require.False(t, hasEvent(events, "classify", scheduler.StepAttemptLost),
		"a step that was running the whole time was recorded as lost; log: %s", describe(events))
}

// slowModel answers, eventually. It is the ordinary case a lease renewal
// exists for: the work is fine, it is simply longer than the TTL.
type slowModel struct {
	id     string
	answer string
	delay  time.Duration
}

func (m *slowModel) factory() server.ModelFactory {
	return func(context.Context, server.ModelRequest) (provider.LanguageModel, error) {
		return m, nil
	}
}

func (m *slowModel) ModelID() string      { return m.id }
func (m *slowModel) ProviderName() string { return "stub" }

func (m *slowModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: true}
}

func (m *slowModel) Generate(ctx context.Context, _ provider.Call) (*provider.Response, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.delay):
	}
	return &provider.Response{
		Content:      []provider.ContentPart{provider.TextPart{Text: m.answer}},
		FinishReason: provider.FinishStop,
		Usage:        provider.Usage{InputTokens: 60, OutputTokens: 20, TotalTokens: 80},
		Raw:          json.RawMessage(fmt.Sprintf(`{"model":%q}`, m.id+"-20260101")),
	}, nil
}

func (m *slowModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, fmt.Errorf("slow model %q does not stream", m.id)
}
