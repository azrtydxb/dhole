package api_test

import (
	"context"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
)

// setProperty is one client's edit, made against the base revision it last
// saw — which is the only thing a second editor's edit can be judged against.
func setProperty(stepID, value string) *dholev1.Operation {
	return &dholev1.Operation{Kind: &dholev1.Operation_SetProperty{
		SetProperty: &dholev1.SetProperty{
			StepId: stepID, Property: "plugin_ref", Value: value,
		},
	}}
}

// stepPluginRef reads one step's plugin_ref out of a definition.
func stepPluginRef(t *testing.T, p *dholev1.Pipeline, stepID string) string {
	t.Helper()
	for _, step := range p.GetSteps() {
		if step.GetId() == stepID {
			return step.GetPluginRef()
		}
	}
	t.Fatalf("no step %q in the definition", stepID)
	return ""
}

// TestConcurrentOperationsOnDifferentStepsBothApply is multiplayer editing's
// first property: two people editing the same pipeline at the same time, on
// parts of it that have nothing to do with each other, both get their edit.
//
// Both edits name the SAME base revision, because that is what concurrent
// means: neither editor saw the other's change before making their own. A
// control plane that refuses the second one is correct about the head having
// moved and useless as a multiplayer editor — the second person is told to
// rebase an edit that never touched anything the first person touched.
func TestConcurrentOperationsOnDifferentStepsBothApply(t *testing.T) {
	h := newRealHarness(t)
	_, base := seed(t, h, tenantA)
	ctx := context.Background()

	first, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    setProperty("a", "oci://example/fetch:v1"),
	}, tokenAlice))
	require.NoError(t, err)

	// Carol never saw first's revision: her editor is still on base.
	second, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    setProperty("b", "oci://example/build:v1"),
	}, tokenCarol))
	require.NoError(t, err, "an edit to another step from the same base was refused")

	// The final revision carries BOTH. A second edit that succeeded by
	// overwriting the first would satisfy the line above and lose alice's
	// work, which is the failure this assertion exists for.
	head, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: "pipe-1",
	}, tokenAlice))
	require.NoError(t, err)
	require.Equal(t, second.Msg.GetRevision().GetId(), head.Msg.GetRevision().GetId(),
		"the head is not the revision the second edit produced")
	require.Equal(t, "oci://example/fetch:v1", stepPluginRef(t, head.Msg.GetPipeline(), "a"),
		"the first editor's change is not in the final revision")
	require.Equal(t, "oci://example/build:v1", stepPluginRef(t, head.Msg.GetPipeline(), "b"),
		"the second editor's change is not in the final revision")
	require.NotEqual(t, first.Msg.GetRevision().GetId(), second.Msg.GetRevision().GetId())
}

// TestConcurrentOperationsOnSameStepConflict is the other half, and it is the
// half that has to refuse.
//
// Two people setting the same property of the same step have made an edit
// that cannot be merged: whichever way a control plane resolved it, one of
// them would be told their change had landed when it had not. So the SECOND
// one is refused — not the first, and not both — and the refusal carries the
// revision the pipeline is actually at, because a client told only "you are
// stale" can do nothing but re-read blind, whereas a client handed the newer
// revision can rebase onto it and show the user what changed.
func TestConcurrentOperationsOnSameStepConflict(t *testing.T) {
	h := newRealHarness(t)
	_, base := seed(t, h, tenantA)
	ctx := context.Background()

	first, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    setProperty("c", "oci://alice/one:v1"),
	}, tokenAlice))
	require.NoError(t, err, "the first editor must win")

	_, err = h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    setProperty("c", "oci://carol/two:v1"),
	}, tokenCarol))
	require.Error(t, err, "two editors both changed the same step and both were told they had")
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))

	// The newer revision, attached to the refusal, in a form a client can act
	// on rather than a sentence it would have to parse.
	var conflict *connect.Error
	require.ErrorAs(t, err, &conflict)
	var attached *dholev1.Revision
	for _, detail := range conflict.Details() {
		value, valueErr := detail.Value()
		if valueErr != nil {
			continue
		}
		if rev, ok := value.(*dholev1.Revision); ok {
			attached = rev
		}
	}
	require.NotNil(t, attached, "the conflict carried no revision for the client to rebase onto")
	require.Equal(t, first.Msg.GetRevision().GetId(), attached.GetId(),
		"the conflict named a revision that is not the one the pipeline is at")
	require.Equal(t, "pipe-1", attached.GetPipelineId())

	// And the winner's edit is what stands. A refusal that had nonetheless
	// written carol's value would pass every assertion above.
	head, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: "pipe-1",
	}, tokenAlice))
	require.NoError(t, err)
	require.Equal(t, first.Msg.GetRevision().GetId(), head.Msg.GetRevision().GetId())
	require.Equal(t, "oci://alice/one:v1", stepPluginRef(t, head.Msg.GetPipeline(), "c"))
}

// --- presence -------------------------------------------------------------
//
// Presence is who else is in this pipeline right now, and it is EPHEMERAL by
// construction: it lives on a bus subject nobody may make durable, and in the
// memory of the streams that are open. Nothing about it is stored, because a
// stored presence is a person who appears to be editing a pipeline they closed
// last Tuesday.

// fakeBus is an in-process pub/sub with EXACT subject matching and no
// knowledge of tenants at all.
//
// The last part is what makes it worth having. A bus that scoped anything
// itself would answer the tenant question on the server's behalf, and a server
// that had stopped putting the tenant in the subject would still look correct
// here. This one delivers a publish to whoever subscribed to that exact
// string, so a subject missing its tenant lands two tenants on one subject —
// which is precisely the failure the tenant test asserts against.
type fakeBus struct {
	t  *testing.T
	mu sync.Mutex

	next int
	subs map[string]map[int]func([]byte)

	published  []string
	subscribed []string
}

func newFakeBus(t *testing.T) *fakeBus {
	return &fakeBus{t: t, subs: map[string]map[int]func([]byte){}}
}

func (b *fakeBus) Publish(_ context.Context, subject string, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.published = append(b.published, subject)
	fns := make([]func([]byte), 0, len(b.subs[subject]))
	for _, fn := range b.subs[subject] {
		fns = append(fns, fn)
	}
	b.mu.Unlock()
	for _, fn := range fns {
		fn(data)
	}
	return nil
}

func (b *fakeBus) SubscribeEphemeral(
	_ context.Context, subject string, fn func([]byte),
) (func(), error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	id := b.next
	if b.subs[subject] == nil {
		b.subs[subject] = map[int]func([]byte){}
	}
	b.subs[subject][id] = fn
	b.subscribed = append(b.subscribed, subject)
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.subs[subject], id)
	}, nil
}

// subscriptions is how many subscriptions this bus has taken, ever.
func (b *fakeBus) subscriptions() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribed)
}

// subjects reports every subject this bus was asked to carry presence on.
func (b *fakeBus) subjects() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, s := range append(append([]string{}, b.published...), b.subscribed...) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

const (
	// presenceTTL is short enough that a test can wait out an expiry and long
	// enough that a slow machine does not expire a live session.
	presenceTTL = 300 * time.Millisecond

	// neverExpires is a TTL no test outlives, for the tests that must see a
	// departure ANNOUNCED rather than aged out. Without it a server that never
	// says goodbye passes every one of them a third of a second later, and
	// nothing distinguishes the clean case from the expiry that covers the
	// unclean one.
	neverExpires = 30 * time.Second
)

// presenceHarness is an API server with a presence bus behind it.
func presenceHarness(t *testing.T) (*harness, *fakeBus) {
	t.Helper()
	return presenceHarnessTTL(t, presenceTTL)
}

func presenceHarnessTTL(t *testing.T, ttl time.Duration) (*harness, *fakeBus) {
	t.Helper()
	bus := newFakeBus(t)
	defs := sqliteDefs(t)
	srv, err := api.NewServer(api.Config{
		Definitions:  defs,
		Auth:         fakeAuth{},
		Presence:     bus,
		PresenceTTL:  ttl,
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return &harness{
		client: dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL),
		defs:   defs,
	}, bus
}

// watcher is one open WatchPresence stream, drained into a channel.
type watcher struct {
	events chan *dholev1.PresenceEvent
	stop   context.CancelFunc
	failed chan error
}

// watch opens a presence stream and drains it in the background. sessionID is
// the caller's own editing session; the empty string is an observer that
// announces nothing.
//
// The call itself happens on the background goroutine because a server stream
// does not answer until it has something to say, and an editor watching an
// empty pipeline is meant to wait. It returns once the SERVER has subscribed —
// presence is ephemeral, so an announcement made before the subscription is
// simply not there, and a test racing that would be testing the race.
func watch(t *testing.T, h *harness, bus *fakeBus, token, pipelineID, sessionID string) *watcher {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{
		events: make(chan *dholev1.PresenceEvent, 64),
		stop:   cancel,
		failed: make(chan error, 1),
	}
	before := bus.subscriptions()
	go func() {
		defer close(w.events)
		stream, err := h.client.WatchPresence(ctx, authed(&dholev1.WatchPresenceRequest{
			PipelineId: pipelineID, SessionId: sessionID,
		}, token))
		if err != nil {
			w.failed <- err
			return
		}
		for stream.Receive() {
			w.events <- stream.Msg().GetEvent()
		}
		w.failed <- stream.Err()
	}()
	require.Eventually(t, func() bool { return bus.subscriptions() > before },
		5*time.Second, 2*time.Millisecond, "the stream never subscribed to a presence subject")
	t.Cleanup(cancel)
	return w
}

// await waits for the first event satisfying want, and fails if none arrives.
func (w *watcher) await(t *testing.T, why string, want func(*dholev1.PresenceEvent) bool) *dholev1.PresenceEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	var seen []string
	for {
		select {
		case event, ok := <-w.events:
			if !ok {
				t.Fatalf("%s: the stream ended first; saw %v", why, seen)
			}
			seen = append(seen, event.String())
			if want(event) {
				return event
			}
		case <-deadline:
			t.Fatalf("%s: nothing matched within the deadline; saw %v", why, seen)
		}
	}
}

// requireNone fails if anything matching want arrives within d.
func (w *watcher) requireNone(
	t *testing.T, d time.Duration, why string, want func(*dholev1.PresenceEvent) bool,
) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case event, ok := <-w.events:
			if ok && want(event) {
				t.Fatalf("%s: received %v", why, event)
			}
		case <-deadline:
			return
		}
	}
}

// anything matches every event, for the cases where nothing at all may come.
func anything(*dholev1.PresenceEvent) bool { return true }

// announce is one editor saying what they have selected.
func announce(t *testing.T, h *harness, token, pipelineID, sessionID, selection string) {
	t.Helper()
	_, err := h.client.UpdatePresence(context.Background(), authed(&dholev1.UpdatePresenceRequest{
		PipelineId: pipelineID,
		SessionId:  sessionID,
		Selection:  selection,
		Cursor:     &dholev1.Cursor{X: 12, Y: 34},
	}, token))
	require.NoError(t, err)
}

// TestPresenceShowsAnotherEditorAndForgetsThemWhenTheyLeave is the property
// that makes presence presence rather than a membership list: it is true only
// while somebody is there.
//
// The departure is asserted twice over, because the two halves fail
// differently. A peer who closes their editor must disappear from the people
// still in it; and a NEW editor arriving afterwards must be told about nobody
// at all, because a presence that outlived its session would be replayed to
// them out of whatever remembered it.
func TestPresenceShowsAnotherEditorAndForgetsThemWhenTheyLeave(t *testing.T) {
	h, bus := presenceHarness(t)
	seed(t, h, tenantA)

	alice := watch(t, h, bus, tokenAlice, "pipe-1", "alice-tab-1")
	carol := watch(t, h, bus, tokenCarol, "pipe-1", "carol-tab-1")
	announce(t, h, tokenCarol, "pipe-1", "carol-tab-1", "b")

	// Carol announces herself when she joins and again when she selects
	// something, so this waits for the selection rather than for the first
	// thing that carries her session.
	shown := alice.await(t, "alice never saw carol select anything", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "carol-tab-1" && !e.GetGone() && e.GetSelection() == "b"
	})
	require.Equal(t, "carol", shown.GetPrincipal())
	require.InDelta(t, 12.0, shown.GetCursor().GetX(), 0.001)

	// An editor never draws a cursor for itself, so its own announcements are
	// not sent back to it.
	announce(t, h, tokenAlice, "pipe-1", "alice-tab-1", "a")
	carol.await(t, "carol never saw alice", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "alice-tab-1" && !e.GetGone()
	})
	for {
		select {
		case event := <-alice.events:
			require.NotEqual(t, "alice-tab-1", event.GetSessionId(),
				"an editor was shown its own presence")
			continue
		default:
		}
		break
	}

	// Carol closes her editor.
	carol.stop()
	alice.await(t, "carol left and was still shown", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "carol-tab-1" && e.GetGone()
	})

	// And nothing remembered her: a stream opened now is told about nobody,
	// which is what "the subject carries no durable state" means from the
	// outside.
	dave := watch(t, h, bus, tokenAlice, "pipe-1", "")
	dave.requireNone(t, 300*time.Millisecond,
		"a new editor was replayed a presence from before it connected",
		func(e *dholev1.PresenceEvent) bool { return e.GetSessionId() == "carol-tab-1" })
	// Alice, who is STILL here, does reach it — so the silence above is
	// carol being gone rather than the stream being dead.
	dave.await(t, "a new editor was told about nobody at all", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "alice-tab-1"
	})

	// From the inside: presence is only ever published and subscribed
	// ephemerally. A durable subscription would make it a log.
	require.NotEmpty(t, bus.subjects())
}

// TestALeavingEditorIsAnnouncedRatherThanWaitedOut is the clean departure, on
// a server whose announcements never expire in the life of this test.
//
// The TTL is what makes it worth writing. With a short one, a server that
// never says goodbye at all passes: the editor who left ages out a third of a
// second later and the two cases are indistinguishable. Here nothing ages out,
// so the only way carol can disappear is if her stream ending said so.
func TestALeavingEditorIsAnnouncedRatherThanWaitedOut(t *testing.T) {
	h, bus := presenceHarnessTTL(t, neverExpires)
	seed(t, h, tenantA)

	alice := watch(t, h, bus, tokenAlice, "pipe-1", "alice-tab-1")
	carol := watch(t, h, bus, tokenCarol, "pipe-1", "carol-tab-1")
	alice.await(t, "alice never saw carol arrive", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "carol-tab-1" && !e.GetGone()
	})

	carol.stop()
	alice.await(t, "carol's editor closed and nobody was told", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "carol-tab-1" && e.GetGone()
	})
}

// TestPresenceOfAnEditorThatVanishesUncleanlyExpires is the case a departure
// message cannot cover.
//
// A closed tab, a killed browser, a laptop lid, a proxy that drops the
// connection without an RST: the plane holding that editor's stream may never
// run another line of code on their behalf, so nothing publishes their
// departure. Presence therefore expires: every live session refreshes its own
// announcement, and one that stops being refreshed is reported gone by
// everyone watching, without anyone having to have been told.
func TestPresenceOfAnEditorThatVanishesUncleanlyExpires(t *testing.T) {
	h, bus := presenceHarness(t)
	seed(t, h, tenantA)

	alice := watch(t, h, bus, tokenAlice, "pipe-1", "alice-tab-1")
	// Wait until the stream is really subscribed before the ghost speaks.
	require.Eventually(t, func() bool { return len(bus.subjects()) > 0 },
		2*time.Second, 5*time.Millisecond)
	subject := bus.subjects()[0]

	// A ghost: an announcement from a plane whose editor then vanished. It is
	// published onto the subject the watcher itself subscribed to, so the test
	// hardcodes no spelling of it.
	require.NoError(t, bus.Publish(context.Background(), subject, &dholev1.PresenceEvent{
		Principal: "mallory", SessionId: "ghost-tab", Selection: "c",
	}))
	alice.await(t, "the ghost was never shown", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "ghost-tab" && !e.GetGone()
	})

	// Nobody refreshes it, so it ages out — and the watcher SAYS so rather
	// than leaving a stale cursor on the canvas forever.
	alice.await(t, "a vanished editor was shown forever", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "ghost-tab" && e.GetGone()
	})
}

// TestPresenceIsTenantScoped is the run-stream isolation of stream.go, applied
// to the one subject a person's cursor travels on.
//
// Two tenants may hold a pipeline of the same id — ids are scoped, so this is
// ordinary rather than adversarial. The subject they announce on must not be:
// a presence subject keyed by pipeline id alone would put both tenants'
// editors on one string, and the leak would be invisible until somebody
// noticed a stranger's cursor.
func TestPresenceIsTenantScoped(t *testing.T) {
	h, bus := presenceHarness(t)
	seed(t, h, tenantA)
	seed(t, h, tenantB) // the same pipeline id, in another tenant

	alice := watch(t, h, bus, tokenAlice, "pipe-1", "alice-tab-1")
	bob := watch(t, h, bus, tokenBob, "pipe-1", "bob-tab-1")
	announce(t, h, tokenBob, "pipe-1", "bob-tab-1", "b")

	// Bob's own tenant sees Bob: this is a live subject and not a broken one,
	// which is what makes the silence below meaningful.
	_ = watch(t, h, bus, tokenCarol, "pipe-1", "carol-tab-1")
	announce(t, h, tokenCarol, "pipe-1", "carol-tab-1", "a")
	bob.requireNone(t, presenceTTL, "an editor of another tenant reached bob", anything)
	alice.await(t, "alice never saw carol, who is in her tenant", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "carol-tab-1"
	})
	for {
		select {
		case event := <-alice.events:
			require.NotEqual(t, "bob-tab-1", event.GetSessionId(),
				"another tenant's editor appeared in this tenant's presence")
			require.NotEqual(t, "bob", event.GetPrincipal())
			continue
		default:
		}
		break
	}

	// The subject itself, not only what came out of it: two tenants must not
	// share one string, or the isolation is an accident of who subscribed.
	subjects := bus.subjects()
	require.Len(t, subjects, 2, "two tenants shared one presence subject: %v", subjects)

	// And a pipeline that exists in one tenant only is not reachable from the
	// other at all — the same answer GetPipeline gives, for the same reason.
	_, err := h.defs.Save(context.Background(), tenantA, &dholev1.Pipeline{
		Id: "solo", Tenant: &dholev1.Tenant{Id: tenantA},
	}, "alice")
	require.NoError(t, err)
	stream, err := h.client.WatchPresence(context.Background(), authed(&dholev1.WatchPresenceRequest{
		PipelineId: "solo", SessionId: "bob-tab-2",
	}, tokenBob))
	require.NoError(t, err)
	require.False(t, stream.Receive(), "bob was streamed another tenant's presence")
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(stream.Err()))
}

// TestPresenceEventCarriesNothingBeyondTheHandle holds the event to what a
// recipient needs and no more.
//
// Drawing a cursor needs a label, an opaque session to keep two tabs apart,
// and what that editor has selected — a step id in a pipeline the recipient
// can already read, which is why the selection is not a leak. Everything else
// identity.Principal holds is the plane's business: the tenant, the kind of
// credential, anything a future field adds. The check is driven off the
// message descriptor, so a field added later is refused here until somebody
// decides it belongs.
func TestPresenceEventCarriesNothingBeyondTheHandle(t *testing.T) {
	h, bus := presenceHarness(t)
	seed(t, h, tenantA)

	carol := watch(t, h, bus, tokenCarol, "pipe-1", "carol-tab-1")
	watch(t, h, bus, tokenAlice, "pipe-1", "alice-tab-1")
	announce(t, h, tokenAlice, "pipe-1", "alice-tab-1", "a")

	// BOTH announcements alice makes: the one her stream publishes when it
	// opens, and the one UpdatePresence publishes when she selects something.
	// They are built in two places, and a field leaking into either of them is
	// a field on the wire.
	joined := carol.await(t, "carol never saw alice arrive", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "alice-tab-1"
	})
	event := carol.await(t, "carol never saw alice select anything", func(e *dholev1.PresenceEvent) bool {
		return e.GetSessionId() == "alice-tab-1" && e.GetSelection() == "a"
	})
	for _, e := range []*dholev1.PresenceEvent{joined, event} {
		require.Equal(t, "alice", e.GetPrincipal(), "the handle is the subject and nothing else")
		encoded, err := proto.Marshal(e)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), tenantA, "the event carries the tenant")
		require.NotContains(t, string(encoded), tokenAlice, "the event carries the credential")
	}

	allowed := map[string]bool{
		"principal": true, "selection": true, "cursor": true,
		"session_id": true, "gone": true,
	}
	fields := event.ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		require.True(t, allowed[string(fields.Get(i).Name())],
			"PresenceEvent carries %q, which nobody decided a recipient is entitled to",
			fields.Get(i).Name())
	}

}
