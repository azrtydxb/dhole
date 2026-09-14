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
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// TestADispatchOnTheLastOfEightQueuesIsPickedUpPromptlyByAOneSlotEngine is the
// kw failure. An engine advertising {NETWORK, SECRETS} binds eight consumers —
// four capability subsets, each with a plain and a kind-targeted queue — and
// with one slot the pump used to take that slot BEFORE fetching, wait for a
// message, and hand the slot on if none came. The consumer holding work waited
// its turn behind seven empty ones: at a three-second yield that was up to 24
// seconds, and the plane declares a dispatch nobody accepted lost at 30. A step
// was dispatched at 08:18:37, declared lost at 08:19:10 and accepted by the
// engine at 08:19:13.
//
// The dispatch goes to the LAST queue the engine binds, and it is timed from
// publish to ACCEPTED over several rounds, so a pickup that depends on where a
// rotation happens to be fails at least one of them. Half a second is several
// hundred times what an idle engine actually takes, and it is tight enough to
// refuse the 250ms-yield mitigation as well as the original three seconds —
// eight turns of a quarter second are still two seconds — so the fix cannot
// pass by yielding faster: nothing may wait for a slot before a message exists.
func TestADispatchOnTheLastOfEightQueuesIsPickedUpPromptlyByAOneSlotEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const bound = 500 * time.Millisecond
	caps := []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_SECRETS}

	h := newHarness(t)
	registrations := h.registrations(ctx, t)
	h.start(ctx, t, engine.Config{
		EngineID: "engine-eight-queues",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: capableExecutor{Executor: process.New(), caps: caps[:1]},
		Secrets:  nothingToRedeem{},
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	reg := receive(ctx, t, registrations, "registration")
	require.ElementsMatch(t, caps, reg.GetCapabilities(),
		"the premise: the engine advertises both, so it binds four subsets times two queues")

	dispatch := func(step string) (*dholev1.JobDispatch, <-chan *dholev1.JobStatus) {
		d := newDispatch("run-eight", step, "echo", "hi")
		d.Step.Capabilities = caps
		statuses := h.statuses(ctx, t, "run-eight", step)
		h.publishDispatchToKind(ctx, t, d, process.Kind)
		return d, statuses
	}

	// Warm-up, untimed: binding eight consumers is start-up, not pickup.
	_, warm := dispatch("warm")
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, warm).GetPhase())

	for _, step := range []string{"round-1", "round-2", "round-3", "round-4"} {
		// Decorrelate the rounds from any rotation a slot-first pump runs.
		time.Sleep(700 * time.Millisecond)

		published := time.Now()
		_, statuses := dispatch(step)
		awaitPhase(ctx, t, statuses, dholev1.Phase_PHASE_ACCEPTED)
		took := time.Since(published)
		require.Less(t, took, bound,
			"%s on the last of eight queues took %s to be accepted by an idle one-slot engine: "+
				"consumers of empty queues are spending the slot", step, took)
		require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	}
}

// TestAnEngineWithTwoSlotsNeverRunsMoreThanTwoStepsAtOnce: decoupling fetching
// from slots must not decouple RUNNING from them. A pump that fetches without a
// slot and then simply starts what it fetched turns a slot count into a
// suggestion, and a two-slot engine on a small node runs every step it can see.
//
// Three long steps on three of the engine's four queues, every one of them
// fetched: the engine's bus keeps fetching when the engine abandons a fetch,
// which is what a delivery already in flight at the moment the engine filled
// looks like. Without that the case proves less than it seems — an engine that
// stops fetching when full never holds a third step at all, and would pass
// with no slot bound whatsoever.
func TestAnEngineWithTwoSlotsNeverRunsMoreThanTwoStepsAtOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	h := newHarness(t)
	fetchCtx, stopFetching := context.WithCancel(ctx)
	defer stopFetching()
	stubborn := &lateFetchBus{Bus: h.engineBus, ctx: fetchCtx, fetching: make(chan string, 8), fetched: make(chan string, 8)}

	network := []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK}
	exec := &concurrencyExecutor{Executor: capableExecutor{Executor: process.New(), caps: network}}
	h.start(ctx, t, engine.Config{
		EngineID: "engine-two-slots",
		Tier:     tier,
		Bus:      stubborn,
		Executor: exec,
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    2,
	})
	for range 4 {
		receive(ctx, t, stubborn.fetching, "each of the four queues fetching")
	}

	steps := []string{"long-plain", "long-kind", "long-network"}
	all := make([]<-chan *dholev1.JobStatus, 0, len(steps))
	for _, step := range steps {
		all = append(all, h.statuses(ctx, t, "run-slots", step))
	}
	h.publishDispatch(ctx, t, newDispatch("run-slots", "long-plain", "sleep", "2"))
	h.publishDispatchToKind(ctx, t, newDispatch("run-slots", "long-kind", "sleep", "2"), process.Kind)
	onNetwork := newDispatch("run-slots", "long-network", "sleep", "2")
	onNetwork.Step.Capabilities = network
	h.publishDispatch(ctx, t, onNetwork)
	for range steps {
		receive(ctx, t, stubborn.fetched, "every long step fetched")
	}

	for i, statuses := range all {
		require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase(), steps[i])
	}
	require.Equal(t, 2, exec.highWater(),
		"three long steps fetched by a two-slot engine must run two at a time, never three")
}

// TestADispatchFetchedWhileEverySlotIsBusyIsNotRedeliveredWhileItWaits. A pump
// that fetches before it has a slot can hold a message it cannot start yet —
// a fetch already on its way when the last slot was taken. That message's ack
// wait starts at DELIVERY, not when the engine gets round to it, so renewal
// has to start at fetch: begun only once the step runs, the server hands the
// waiting dispatch to another engine after one ack wait, and the step runs
// twice — or, if the plane has meanwhile re-dispatched, three times.
//
// The late fetch is forced rather than raced for: the engine's bus here keeps
// fetching when the engine abandons a fetch, which is exactly what a delivery
// already in flight at that moment looks like. The redelivery it guards
// against is the real server's.
func TestADispatchFetchedWhileEverySlotIsBusyIsNotRedeliveredWhileItWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const ackWait = time.Second
	h := newHarness(t, bus.WithAckWait(ackWait))

	fetchCtx, stopFetching := context.WithCancel(ctx)
	defer stopFetching()
	stubborn := &lateFetchBus{Bus: h.engineBus, ctx: fetchCtx, fetching: make(chan string, 8), fetched: make(chan string, 8)}

	first := h.statuses(ctx, t, "run-late", "occupy")
	second := h.statuses(ctx, t, "run-late", "waiting")
	h.start(ctx, t, engine.Config{
		EngineID: "engine-late",
		Tier:     tier,
		Bus:      stubborn,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		AckWait:  ackWait,
	})

	plainSubject := bus.SubjectDispatch(tier, engine.CapsHash(nil))
	kindSubject := bus.SubjectDispatchKind(tier, engine.CapsHash(nil), process.Kind)

	// Both queues must be fetching before the slot is taken. A pump that had
	// not reached its queue when the engine filled would, correctly, not
	// fetch at all — and the case would pass without testing anything.
	listening := map[string]bool{
		receive(ctx, t, stubborn.fetching, "a pump fetching"): true,
		receive(ctx, t, stubborn.fetching, "a pump fetching"): true,
	}
	require.Equal(t, map[string]bool{plainSubject: true, kindSubject: true}, listening)

	h.publishDispatch(ctx, t, newDispatch("run-late", "occupy", "sleep", "6"))
	awaitPhase(ctx, t, first, dholev1.Phase_PHASE_ACCEPTED)
	require.Equal(t, plainSubject, receive(ctx, t, stubborn.fetched, "the occupying fetch"))

	// The only slot is taken. This one arrives on the kind queue's fetch.
	h.publishDispatchToKind(ctx, t, newDispatch("run-late", "waiting", "echo", "hi"), process.Kind)
	select {
	case subject := <-stubborn.fetched:
		require.Equal(t, kindSubject, subject)
	case <-time.After(10 * time.Second):
		t.Fatal("the engine never fetched the second dispatch while its slot was busy")
	}

	// Another engine on the same durable queue. For several ack waits, while
	// the waiting dispatch still has no slot, the server must not offer it.
	stream, err := engine.DispatchStream(tier)
	require.NoError(t, err)
	rival, err := bus.Connect(ctx, h.srv.URL(), bus.WithAckWait(ackWait))
	require.NoError(t, err)
	t.Cleanup(rival.Close)
	sub, err := rival.SubscribePull(ctx, stream, "engines-"+tier+"-"+engine.CapsHash(nil)+"-"+process.Kind, kindSubject)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	watch, stopWatching := context.WithTimeout(ctx, 3*ackWait+500*time.Millisecond)
	stolen, err := sub.Next(watch)
	stopWatching()
	if err == nil {
		_ = stolen.Nak()
		t.Fatal("a dispatch the engine fetched and was holding for a slot was redelivered to another engine: " +
			"its delivery was not being renewed while it waited")
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NoError(t, sub.Close())

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, first).GetPhase())
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, second).GetPhase(),
		"the held dispatch must run on the engine that held it once the slot frees")
	stopFetching()
}

// TestAnEngineThatStopsWhileADispatchWaitsForASlotGivesItStraightBack. A
// dispatch an engine fetched but never started is not work in progress, and an
// engine that stops with one must not leave it outstanding: its renewal stops
// with the engine, and nothing would hand it to anyone else until the ack wait
// ran out — thirty seconds by default, which is exactly when the plane
// declares a dispatch nobody accepted lost.
func TestAnEngineThatStopsWhileADispatchWaitsForASlotGivesItStraightBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// The DEFAULT ack wait: a dispatch left outstanding would not come back
	// within this test's patience.
	h := newHarness(t)
	fetchCtx, stopFetching := context.WithCancel(ctx)
	defer stopFetching()
	stubborn := &lateFetchBus{Bus: h.engineBus, ctx: fetchCtx, fetching: make(chan string, 8), fetched: make(chan string, 8)}

	occupied := h.statuses(ctx, t, "run-stop", "occupy")
	stop := h.start(ctx, t, engine.Config{
		EngineID: "engine-stopping",
		Tier:     tier,
		Bus:      stubborn,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})
	for range 2 {
		receive(ctx, t, stubborn.fetching, "both queues fetching")
	}

	h.publishDispatch(ctx, t, newDispatch("run-stop", "occupy", "sleep", "30"))
	awaitPhase(ctx, t, occupied, dholev1.Phase_PHASE_ACCEPTED)
	receive(ctx, t, stubborn.fetched, "the occupying fetch")
	h.publishDispatchToKind(ctx, t, newDispatch("run-stop", "waiting", "echo", "hi"), process.Kind)
	receive(ctx, t, stubborn.fetched, "the dispatch that has to wait")

	stopFetching()
	stop()

	stream, err := engine.DispatchStream(tier)
	require.NoError(t, err)
	successor, err := bus.Connect(ctx, h.srv.URL())
	require.NoError(t, err)
	t.Cleanup(successor.Close)
	kindSubject := bus.SubjectDispatchKind(tier, engine.CapsHash(nil), process.Kind)
	sub, err := successor.SubscribePull(ctx, stream, "engines-"+tier+"-"+engine.CapsHash(nil)+"-"+process.Kind, kindSubject)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	waitCtx, stopWaiting := context.WithTimeout(ctx, 5*time.Second)
	defer stopWaiting()
	msg, err := sub.Next(waitCtx)
	require.NoError(t, err, "the dispatch the stopped engine never started was not given back")
	var got dholev1.JobDispatch
	require.NoError(t, proto.Unmarshal(msg.Data(), &got))
	require.Equal(t, "waiting", got.GetStepId())
	require.NoError(t, msg.Ack())
}

// TestASecondEngineOnTheSameQueueGetsWorkWhileTheFirstIsFull: fetching must
// not hoard. A full engine that keeps fetching holds each message it gets for
// as long as its running step takes — renewing it, so the server never gives
// it to anyone — while an idle engine on the same queue sits with nothing to
// do. That is the opposite of a work queue, and it is exactly what a naive
// fetch-first pump does — or one that stops starting fetches when it fills but
// leaves the ones already open to finish.
func TestASecondEngineOnTheSameQueueGetsWorkWhileTheFirstIsFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	full := make(chan string, 8)
	idle := make(chan string, 8)
	occupied := h.statuses(ctx, t, "run-hoard", "occupy")
	// The long step goes to the full engine's OWN kind queue, and is waiting
	// before that engine starts. It binds its plain queue first, so by the time
	// the kind queue hands this over the plain queue's fetch is already open —
	// and the engine fills with a fetch in flight on the queue it shares. A
	// pump that stopped only NEW fetches when full would leave that one open
	// and take the next shared message with it.
	h.publishDispatchToKind(ctx, t, newDispatch("run-hoard", "occupy", "sleep", "25"), "alpha")
	h.start(ctx, t, engine.Config{
		EngineID: "engine-full", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "alpha", acquired: full},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})
	awaitPhase(ctx, t, occupied, dholev1.Phase_PHASE_ACCEPTED)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-idle", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "beta", acquired: idle},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})
	awaitEngines(ctx, t, registrations, "engine-full", "engine-idle")

	started := time.Now()
	steps := []string{"quick-1", "quick-2", "quick-3", "quick-4"}
	all := make([]<-chan *dholev1.JobStatus, 0, len(steps))
	for _, step := range steps {
		all = append(all, h.statuses(ctx, t, "run-hoard", step))
		h.publishDispatch(ctx, t, newDispatch("run-hoard", step, "echo", "hi"))
	}
	for i, statuses := range all {
		select {
		case s := <-awaitTerminalAsync(ctx, statuses):
			require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, s.GetPhase(), steps[i])
		case <-time.After(15*time.Second - time.Since(started)):
			t.Fatalf("%s did not finish while the full engine's long step was still running: "+
				"the full engine is holding work the idle one could have run", steps[i])
		}
	}
	require.Equal(t, "alpha", receive(ctx, t, full, "the occupying acquisition"))
	require.Empty(t, full, "the full engine ran something other than its long step")
	require.Len(t, idle, len(steps), "every quick step belongs on the idle engine")
}

// awaitTerminalAsync is awaitTerminal on a goroutine, so a caller can bound it
// more tightly than receive's own limit.
func awaitTerminalAsync(ctx context.Context, ch <-chan *dholev1.JobStatus) <-chan *dholev1.JobStatus {
	out := make(chan *dholev1.JobStatus, 1)
	go func() {
		for {
			select {
			case s := <-ch:
				switch s.GetPhase() {
				case dholev1.Phase_PHASE_SUCCEEDED, dholev1.Phase_PHASE_FAILED, dholev1.Phase_PHASE_CANCELLED:
					out <- s
					return
				case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// capableExecutor advertises sandbox capabilities its delegate does not have,
// so an engine binds more than one capability subset without a backend this
// test cannot start.
type capableExecutor struct {
	executor.Executor
	caps []dholev1.Capability
}

func (e capableExecutor) Capabilities() []dholev1.Capability { return e.caps }

// nothingToRedeem makes an engine advertise CAPABILITY_SECRETS. No dispatch in
// these cases carries a secret, so it is never asked.
type nothingToRedeem struct{}

func (nothingToRedeem) Redeem(context.Context, string, *dholev1.SecretRef) (string, error) {
	return "", nil
}

// concurrencyExecutor records the most sandboxes it ever had live at once.
type concurrencyExecutor struct {
	executor.Executor
	mu     sync.Mutex
	live   int
	peaked int
}

func (e *concurrencyExecutor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	sb, err := e.Executor.Acquire(ctx, spec)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.live++
	e.peaked = max(e.peaked, e.live)
	e.mu.Unlock()
	return &countedSandbox{Sandbox: sb, owner: e}, nil
}

func (e *concurrencyExecutor) highWater() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.peaked
}

type countedSandbox struct {
	executor.Sandbox
	owner    *concurrencyExecutor
	released sync.Once
}

func (s *countedSandbox) Release(ctx context.Context) error {
	s.released.Do(func() {
		s.owner.mu.Lock()
		s.owner.live--
		s.owner.mu.Unlock()
	})
	return s.Sandbox.Release(ctx)
}

// lateFetchBus is an engine's bus whose fetches do not stop when the engine
// abandons them. It reports each queue's first fetch, and the subject of every
// message it hands over.
type lateFetchBus struct {
	bus.Bus
	ctx      context.Context //nolint:containedctx // the fetches' lifetime is the test's, deliberately not the engine's.
	fetching chan string
	fetched  chan string
}

func (b *lateFetchBus) SubscribePull(ctx context.Context, stream, consumer, subject string) (bus.Subscription, error) {
	sub, err := b.Bus.SubscribePull(ctx, stream, consumer, subject)
	if err != nil {
		return nil, err
	}
	return &lateFetchSub{Subscription: sub, bus: b, subject: subject}, nil
}

type lateFetchSub struct {
	bus.Subscription
	bus     *lateFetchBus
	subject string
	began   sync.Once
}

func (s *lateFetchSub) Next(context.Context) (bus.Message, error) {
	s.began.Do(func() { s.bus.fetching <- s.subject })
	msg, err := s.Subscription.Next(s.bus.ctx)
	if err == nil {
		s.bus.fetched <- s.subject
	}
	return msg, err
}
