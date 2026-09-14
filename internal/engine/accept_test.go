package engine_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// acceptancePlane plays the control plane's half of ADR 0028: it answers every
// acceptance request with whatever verdict the test gave the fence, and
// remembers every request it was asked, so a case can tell an engine that asked
// once from one that asked again on a redelivery — or never asked at all.
type acceptancePlane struct {
	mu       sync.Mutex
	verdicts map[string]dholev1.Acceptance
	asked    []*dholev1.JobStatus
}

func (h *harness) serveAcceptance(ctx context.Context, t *testing.T, verdicts map[string]dholev1.Acceptance) *acceptancePlane {
	t.Helper()
	p := &acceptancePlane{verdicts: verdicts}
	stop, err := h.plane.Respond(ctx, bus.SubjectAcceptWildcard(), func(raw []byte) (proto.Message, error) {
		st := &dholev1.JobStatus{}
		if err := proto.Unmarshal(raw, st); err != nil {
			return nil, err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.asked = append(p.asked, st)
		return &dholev1.AcceptReply{Acceptance: p.verdicts[st.GetFenceToken()]}, nil
	})
	require.NoError(t, err)
	t.Cleanup(stop)
	return p
}

func (p *acceptancePlane) askedFor(fence string) []*dholev1.JobStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []*dholev1.JobStatus
	for _, st := range p.asked {
		if st.GetFenceToken() == fence {
			out = append(out, st)
		}
	}
	return out
}

// TestADispatchSupersededBeforeItIsAcceptedIsNeverStarted is the kw defect.
// A step's attempt was recorded lost and re-dispatched, and the engine went on
// to start the superseded dispatch as well: a sandbox pod ran `build` for
// nobody, for five minutes, in a slot. The plane's refusal of a stale fence
// only ever discarded the RESULT; for an at-most-once step the effect had
// already happened a second time.
//
// Three things are asserted, because each is a way to get this half right:
// no sandbox is acquired; the dispatch leaves the queue rather than coming
// back after the ack wait (asked exactly once across several ack waits); and
// the engine's only slot is free for the next dispatch.
func TestADispatchSupersededBeforeItIsAcceptedIsNeverStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const ackWait = time.Second
	h := newHarness(t, bus.WithAckWait(ackWait))

	stale := newDispatch("run-superseded", "build", "echo", "ran for nobody")
	stale.ConfirmAcceptance = true
	next := newDispatch("run-superseded", "after", "echo", "hi")
	next.ConfirmAcceptance = true

	plane := h.serveAcceptance(ctx, t, map[string]dholev1.Acceptance{
		stale.GetFenceToken(): dholev1.Acceptance_ACCEPTANCE_FENCED,
		next.GetFenceToken():  dholev1.Acceptance_ACCEPTANCE_CURRENT,
	})
	staleStatuses := h.statuses(ctx, t, "run-superseded", "build")
	nextStatuses := h.statuses(ctx, t, "run-superseded", "after")

	exec := &recordingExecutor{Executor: process.New()}
	h.start(ctx, t, engine.Config{
		EngineID: "engine-superseded",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: exec,
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		AckWait:  ackWait,
	})

	h.publishDispatch(ctx, t, stale)
	require.Eventually(t, func() bool {
		return len(plane.askedFor(stale.GetFenceToken())) > 0 || len(exec.acquired()) > 0
	}, 30*time.Second, 50*time.Millisecond, "the engine neither asked about the dispatch nor started it")

	// Several ack waits: a dispatch the engine did not acknowledge off the
	// queue comes back, and would be asked about again.
	time.Sleep(4 * ackWait)
	require.Empty(t, exec.acquired(),
		"the engine acquired a sandbox for a dispatch the plane answered FENCED: "+
			"a superseded attempt ran for nobody")
	require.Len(t, plane.askedFor(stale.GetFenceToken()), 1,
		"the refused dispatch was delivered again: it must be acknowledged off the queue, not left or nacked")
	select {
	case st := <-staleStatuses:
		t.Fatalf("a refused dispatch published %s; it never started and nobody is waiting for it", st.GetPhase())
	default:
	}

	// The one slot is free: the current dispatch of another step runs.
	h.publishDispatch(ctx, t, next)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, nextStatuses).GetPhase(),
		"the refused dispatch kept the engine's only slot")
	require.Len(t, exec.acquired(), 1)
}

// TestTheCurrentDispatchOfAConfirmedStepRunsNormally is the other side of the
// refusal: a dispatch the plane confirms runs exactly as it always did, and
// what it asked with is its own ACCEPTED status, fence echoed unchanged.
func TestTheCurrentDispatchOfAConfirmedStepRunsNormally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	current := newDispatch("run-current", "build", "echo", "hi")
	current.Attempt = 2
	current.ConfirmAcceptance = true
	plane := h.serveAcceptance(ctx, t, map[string]dholev1.Acceptance{
		current.GetFenceToken(): dholev1.Acceptance_ACCEPTANCE_CURRENT,
	})
	statuses := h.statuses(ctx, t, "run-current", "build")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-current",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	h.publishDispatch(ctx, t, current)

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	asked := plane.askedFor(current.GetFenceToken())
	require.Len(t, asked, 1, "the engine must ask once before it starts")
	require.Equal(t, dholev1.Phase_PHASE_ACCEPTED, asked[0].GetPhase())
	require.Equal(t, "run-current", asked[0].GetRunId())
	require.Equal(t, "build", asked[0].GetStepId())
	require.Equal(t, uint32(2), asked[0].GetAttempt())
}

// TestADispatchFromAPlaneThatDoesNotConfirmStartsWithoutAsking is the new
// engine against an older plane. That plane never sets confirm_acceptance and
// serves nothing on job.accept.*, and its engine credentials may not even
// permit publishing there — where a request costs the whole timeout. So an
// engine asks only a plane that said it answers: here the plane below WOULD
// refuse, and the step must run anyway because nobody told the engine to ask.
func TestADispatchFromAPlaneThatDoesNotConfirmStartsWithoutAsking(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	old := newDispatch("run-old-plane", "build", "echo", "hi")
	plane := h.serveAcceptance(ctx, t, map[string]dholev1.Acceptance{
		old.GetFenceToken(): dholev1.Acceptance_ACCEPTANCE_FENCED,
	})
	statuses := h.statuses(ctx, t, "run-old-plane", "build")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-old-plane",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	published := time.Now()
	h.publishDispatch(ctx, t, old)

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	require.Less(t, time.Since(published), 5*time.Second)
	require.Empty(t, plane.askedFor(old.GetFenceToken()),
		"the engine asked a plane that never said it answers")
}

// TestAnUnansweredConfirmationStartsAPureStep: a plane that set the flag and is
// not there to answer — restarting, partitioned from this engine — must not
// stop pure work. The fence on the way back still discards a stale result, and
// the engine keeps working while the plane is down (ADR 0004).
func TestAnUnansweredConfirmationStartsAPureStep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	d := newDispatch("run-no-plane", "build", "echo", "hi")
	d.ConfirmAcceptance = true
	statuses := h.statuses(ctx, t, "run-no-plane", "build")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-no-plane",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	h.publishDispatch(ctx, t, d)

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase(),
		"a pure step waited forever for a plane that was not there")
}

// TestAnAtMostOnceStepWaitsForAnAnswerBeforeItStarts: the one class that may
// not start on no answer. Its effect cannot be discarded afterwards, and ADR
// 0002 already requires it to hold its lease before it executes. It waits,
// holding its delivery, and starts as soon as a plane confirms it.
func TestAnAtMostOnceStepWaitsForAnAnswerBeforeItStarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	d := newDispatch("run-amo", "charge", "echo", "charged")
	d.Step.EffectClass = dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	d.ConfirmAcceptance = true
	statuses := h.statuses(ctx, t, "run-amo", "charge")

	exec := &recordingExecutor{Executor: process.New()}
	h.start(ctx, t, engine.Config{
		EngineID: "engine-amo",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: exec,
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	h.publishDispatch(ctx, t, d)

	time.Sleep(3 * time.Second)
	require.Empty(t, exec.acquired(),
		"an at-most-once step started with nobody to confirm its fence")

	plane := h.serveAcceptance(ctx, t, map[string]dholev1.Acceptance{
		d.GetFenceToken(): dholev1.Acceptance_ACCEPTANCE_CURRENT,
	})
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase(),
		"the at-most-once step never started once a plane confirmed it")
	require.NotEmpty(t, plane.askedFor(d.GetFenceToken()))
	require.Len(t, exec.acquired(), 1)
}
