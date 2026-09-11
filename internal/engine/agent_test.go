package engine_test

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/process"
	"github.com/azrtydxb/dhole/internal/wire"
)

// tenantID scopes every stored object and every dispatch in this file. There is
// no unscoped write, even with one tenant.
const tenantID = "t1"

// tier is the trust tier these engines run in.
const tier = "untrusted"

// TestOutboundOnlyEngineRegistersAndExecutes is the shape of the whole
// contract in one case: the engine dials the bus, nothing dials it, it
// announces itself on engine.registration, and a dispatch that arrives over
// that outbound connection comes back as a terminal JobStatus echoing the
// fence token it was handed.
func TestOutboundOnlyEngineRegistersAndExecutes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)
	statuses := h.statuses(ctx, t, "run-1", "build")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-1",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    2,
	})

	reg := receive(ctx, t, registrations, "registration")
	require.Equal(t, "engine-1", reg.GetEngineId())
	require.Equal(t, tier, reg.GetTier())
	require.Equal(t, uint32(2), reg.GetSlots())
	require.Contains(t, reg.GetProtocolVersions(), wire.ProtocolVersion)
	require.Contains(t, reg.GetEngineTypes(), process.Kind)

	d := newDispatch("run-1", "build", "echo", "hi")
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase())
	require.Equal(t, int32(0), status.GetExitCode())
	require.Equal(t, d.GetFenceToken(), status.GetFenceToken(),
		"the fence token must be echoed unchanged")
	require.Equal(t, uint32(1), status.GetAttempt())
	require.Empty(t, status.GetError())
}

// TestAgentStreamsLogsToEphemeralSubjectAndWritesAuthoritativeCopy holds the
// two-copies rule. The chunks on job.logs.* are the live copy and may be
// dropped; the object under output_prefix is the authoritative one, and it must
// be COMPLETE before the terminal status is published — a reader that acts on
// the status must never find a half-written log.
//
// The blobstore here is deliberately slow, so an implementation that publishes
// the status first and finishes the object afterwards loses the race and the
// test fails rather than passing by luck.
func TestAgentStreamsLogsToEphemeralSubjectAndWritesAuthoritativeCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	h.blobs = slowStore{Store: h.blobs, delay: 750 * time.Millisecond}

	chunks := h.logs(ctx, t, "run-2", "print")
	statuses := h.statuses(ctx, t, "run-2", "print")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-2",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	// Two writes separated in time, so two live chunks genuinely arrive
	// without the engine having to buffer output on line boundaries.
	d := newDispatch("run-2", "print", "/bin/sh", "-c", "printf 'a\n'; sleep 0.3; printf 'b\n'")
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase())
	require.NotEmpty(t, status.GetLogKey())
	require.True(t, strings.HasPrefix(status.GetLogKey(), d.GetOutputPrefix()),
		"the authoritative log lives under the dispatch's output_prefix")

	// Read immediately: the object must already be complete and closed.
	r, err := h.blobs.Read(ctx, tenantID, status.GetLogKey())
	require.NoError(t, err, "the authoritative log must exist before the terminal status")
	defer func() { _ = r.Close() }()
	authoritative := readAll(t, r)
	require.Equal(t, "a\nb\n", string(authoritative))

	// The live copy: at least two chunks, monotonic seq, same bytes.
	deadline := time.After(5 * time.Second)
	var live []byte
	var seqs []uint64
	for len(seqs) < 2 {
		select {
		case c := <-chunks:
			require.Equal(t, "run-2", c.GetRunId())
			require.Equal(t, "print", c.GetStepId())
			require.Equal(t, uint32(1), c.GetAttempt())
			require.Equal(t, dholev1.Stream_STREAM_STDOUT, c.GetStream())
			seqs = append(seqs, c.GetSeq())
			live = append(live, c.GetData()...)
		case <-deadline:
			t.Fatalf("wanted at least two live LogChunks, got %d", len(seqs))
		}
	}
	for i := 1; i < len(seqs); i++ {
		require.Greater(t, seqs[i], seqs[i-1], "LogChunk seq must be monotonic")
	}
	require.Equal(t, "a\nb\n", string(live))
}

// TestAgentHeartbeatsListInFlightSteps: a heartbeat is the plane's only
// evidence a step is still being worked on. One that omits the step, or
// carries the wrong fence, makes the plane orphan live work.
func TestAgentHeartbeatsListInFlightSteps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	beats := h.heartbeats(ctx, t, "engine-3")
	statuses := h.statuses(ctx, t, "run-3", "slow")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-3",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-3", "slow", "sleep", "5")
	h.publishDispatch(ctx, t, d)

	// The step is accepted before it runs, so from here a heartbeat must list
	// it until the terminal status.
	accepted := awaitPhase(ctx, t, statuses, dholev1.Phase_PHASE_ACCEPTED)
	require.Equal(t, d.GetFenceToken(), accepted.GetFenceToken())

	deadline := time.After(20 * time.Second)
	for {
		var beat *dholev1.EngineHeartbeat
		select {
		case beat = <-beats:
		case <-deadline:
			t.Fatal("no heartbeat listed the in-flight step")
		}
		require.Equal(t, "engine-3", beat.GetEngineId())
		for _, f := range beat.GetInFlight() {
			if f.GetRunId() != "run-3" || f.GetStepId() != "slow" {
				continue
			}
			require.Equal(t, uint32(1), f.GetAttempt())
			require.Equal(t, d.GetFenceToken(), f.GetFenceToken(),
				"an InFlight entry echoes the dispatch's fence token unchanged")
			return
		}
	}
}

// TestAgentRefusesDispatchWithUnsupportedProtocolVersion: refusing loudly is
// the point. Dropping the message would be indistinguishable from a dead
// engine and the step would hang until its lease expired.
func TestAgentRefusesDispatchWithUnsupportedProtocolVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-4", "future")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-4",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-4", "future", "echo", "hi")
	d.ProtocolVersion = 99
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase())
	require.Contains(t, status.GetError(), "unsupported protocol")
	require.Equal(t, d.GetFenceToken(), status.GetFenceToken())
}

// TestAgentDoesNotAckUntilTerminalStatusIsPublished is the ordering rule, and
// the one most likely to be broken quietly later. An engine that acks first and
// then dies loses the step: nothing is on the bus and no status ever arrived.
//
// Killing the process mid-dispatch is not something a Go test can do to itself,
// so the failure is injected where it has the same shape: the terminal status
// publish fails. The dispatch must then still be outstanding, and a healthy
// engine taking over must receive it again and finish it.
func TestAgentDoesNotAckUntilTerminalStatusIsPublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A short ack wait so the undelivered dispatch comes back quickly.
	h := newHarness(t, bus.WithAckWait(time.Second))
	statuses := h.statuses(ctx, t, "run-5", "build")

	broken := &failTerminalStatus{Bus: h.engineBus}
	stop := h.start(ctx, t, engine.Config{
		EngineID: "engine-5a",
		Tier:     tier,
		Bus:      broken,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-5", "build", "echo", "hi")
	h.publishDispatch(ctx, t, d)

	// It got as far as accepting, so it did receive and start the work.
	awaitPhase(ctx, t, statuses, dholev1.Phase_PHASE_ACCEPTED)
	require.Eventually(t, broken.refused, 20*time.Second, 20*time.Millisecond,
		"the engine never reached the terminal status publish")
	stop()

	// The replacement engine must be handed the same dispatch. If the first
	// engine had acked before publishing, this dispatch is gone forever.
	healthy, err := bus.Connect(ctx, h.srv.URL(), bus.WithAckWait(time.Second))
	require.NoError(t, err)
	t.Cleanup(healthy.Close)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-5b",
		Tier:     tier,
		Bus:      healthy,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase())
	require.Equal(t, d.GetFenceToken(), status.GetFenceToken())
}

// TestAgentMaterialisesInputsAndReportsOutputs pins the other half of ADR 0001:
// a step gets exactly the inputs it declared, and what it produced is reported
// by content digest rather than left on a host the next step cannot see.
func TestAgentMaterialisesInputsAndReportsOutputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-6", "copy")

	digest, err := h.cas.Put(ctx, tenantID, strings.NewReader("payload\n"))
	require.NoError(t, err)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-6",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	// inputs/<port> and outputs/<port>, which is the layout the wire contract
	// states: a step that reads "src" at the sandbox root reads nothing.
	d := newDispatch("run-6", "copy", "/bin/sh", "-c", "cat inputs/src > outputs/dst")
	d.Step.Inputs = []*dholev1.Port{{Name: "src"}}
	d.Step.Outputs = []*dholev1.Port{{Name: "dst"}}
	d.Inputs = []*dholev1.InputRef{{Port: "src", Digest: digest}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase(), status.GetError())
	require.Len(t, status.GetOutputs(), 1)
	out := status.GetOutputs()[0]
	require.Equal(t, "dst", out.GetPort())
	require.Equal(t, digest.GetHex(), out.GetDigest().GetHex(),
		"copying the input verbatim must yield the same content digest")

	r, err := h.cas.Get(ctx, tenantID, out.GetDigest())
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	require.Equal(t, "payload\n", string(readAll(t, r)))
}

// TestAnInputAndAnOutputSharingAPortNameDoNotCollide is why the layout is two
// directories rather than the sandbox root. An in-place transform names its
// input and its output the same thing; flat, the engine materialised the input
// over the output's path and then collected the untouched input back as the
// step's result, so a step that did nothing reported success with its own
// input as its output.
func TestAnInputAndAnOutputSharingAPortNameDoNotCollide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-ports", "transform")

	digest, err := h.cas.Put(ctx, tenantID, strings.NewReader("before\n"))
	require.NoError(t, err)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-ports",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-ports", "transform", "/bin/sh", "-c", "printf 'after\n' > outputs/doc")
	d.Step.Inputs = []*dholev1.Port{{Name: "doc"}}
	d.Step.Outputs = []*dholev1.Port{{Name: "doc"}}
	d.Inputs = []*dholev1.InputRef{{Port: "doc", Digest: digest}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase(), status.GetError())
	require.Len(t, status.GetOutputs(), 1)
	out := status.GetOutputs()[0]
	require.NotEqual(t, digest.GetHex(), out.GetDigest().GetHex(),
		"the reported output is the input's own digest: the input was materialised over the output's path")

	r, err := h.cas.Get(ctx, tenantID, out.GetDigest())
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	require.Equal(t, "after\n", string(readAll(t, r)))
}

// TestAStepThatOutlivesItsTimeoutIsKilledAndReportedFailedWith137: an engine
// that enforces nothing holds its slot and renews its lease for the step's
// full runtime, so a runaway step is indistinguishable from a slow one. The
// phase is FAILED rather than CANCELLED because a cancellation says an
// operator asked for this and is not retried.
func TestAStepThatOutlivesItsTimeoutIsKilledAndReportedFailedWith137(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-timeout", "sleeper")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-timeout",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-timeout", "sleeper", "/bin/sh", "-c", "printf 'sleeping\n'; sleep 60")
	d.Step.TimeoutSeconds = 1
	started := time.Now()
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase(), status.GetError())
	require.Equal(t, int32(137), status.GetExitCode(),
		"a step the engine killed reports 137 whatever the platform did to it")
	require.Contains(t, status.GetError(), "timed out")
	require.Less(t, time.Since(started), 30*time.Second,
		"the step ran to completion: the timeout was not enforced")

	// The evidence is durable before the status naming it is published.
	logs, err := h.blobs.Read(ctx, tenantID, status.GetLogKey())
	require.NoError(t, err)
	defer func() { _ = logs.Close() }()
	require.Contains(t, string(readAll(t, logs)), "sleeping")
}

// TestAgentRefusesSecretsItCannotRedeemWithoutLeakingTheHandle: the process
// executor advertises no capabilities, so it must never be handed a secret. If
// one arrives anyway the honest answer is a failed status — and neither the
// status nor the authoritative log may carry the handle, because both are
// durable and archived.
func TestAgentRefusesSecretsItCannotRedeemWithoutLeakingTheHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-7", "secretive")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-7",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	const handle = "handle-do-not-log-me"
	d := newDispatch("run-7", "secretive", "echo", "hi")
	d.Secrets = []*dholev1.SecretRef{{Name: "TOKEN", Handle: handle, ExpiresAt: time.Now().Add(time.Minute).Unix()}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase())
	require.NotContains(t, status.GetError(), handle)
	require.Contains(t, status.GetError(), "TOKEN")

	if status.GetLogKey() != "" {
		r, err := h.blobs.Read(ctx, tenantID, status.GetLogKey())
		require.NoError(t, err)
		defer func() { _ = r.Close() }()
		require.NotContains(t, string(readAll(t, r)), handle)
	}
}

// TestAStepNamingAnEngineKindRunsOnAnEngineOfThatKindWhenTwoKindsShareATier is
// the test whose absence let the bug ship. Match filters on engine type and is
// right when exercised alone, but it only decides whether a step CAN be
// placed: on kw, a step naming engine_type vm in a tier holding both a
// vm-backed and a kubernetes-backed engine ran on the kubernetes one, because
// both engines pulled the SAME job.dispatch.<tier>.<caps> work queue and
// whichever grabbed it first ran it. Nothing short of two engines of different
// kinds in one tier, over a real bus, catches that.
func TestAStepNamingAnEngineKindRunsOnAnEngineOfThatKindWhenTwoKindsShareATier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	vm := make(chan string, 4)
	kubernetes := make(chan string, 4)
	h.start(ctx, t, engine.Config{
		EngineID: "engine-vm", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "vm", acquired: vm},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})
	h.start(ctx, t, engine.Config{
		EngineID: "engine-kubernetes", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "kubernetes", acquired: kubernetes},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})
	// Both engines are up and bound BEFORE anything is published, so an engine
	// that does not run the step is one that was offered it and left it alone,
	// not one that had not started yet.
	awaitEngines(ctx, t, registrations, "engine-vm", "engine-kubernetes")

	statuses := h.statuses(ctx, t, "run-kind", "probe")
	h.publishDispatchToKind(ctx, t, newDispatch("run-kind", "probe", "echo", "hi"), "vm")

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	require.Equal(t, "vm", receive(ctx, t, vm, "the vm engine acquiring a sandbox"))
	require.Empty(t, kubernetes,
		"an engine offering kubernetes must never be handed a step that named vm")

	// The other direction, on the same pair: the route is a route, not a
	// preference for whichever engine happens to be named first.
	otherStatuses := h.statuses(ctx, t, "run-kind", "other")
	h.publishDispatchToKind(ctx, t, newDispatch("run-kind", "other", "echo", "hi"), "kubernetes")

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, otherStatuses).GetPhase())
	require.Equal(t, "kubernetes", receive(ctx, t, kubernetes, "the kubernetes engine acquiring a sandbox"))
	require.Empty(t, vm, "the vm engine must not have run the step that named kubernetes")
}

// TestAnEngineWhoseKindIsNotASubjectTokenIsRefusedAtConstruction. The kind is
// now part of a subject and of a durable consumer name. A kind carrying a dot
// would split the subject into tokens nothing publishes to and produce a
// durable name NATS rejects — an engine that registers, heartbeats, reports
// ready and takes no kind-targeted work, forever. Refused loudly at the one
// place a backend's kind enters the bus instead.
func TestAnEngineWhoseKindIsNotASubjectTokenIsRefusedAtConstruction(t *testing.T) {
	h := newHarness(t)
	_, err := engine.New(engine.Config{
		EngineID: "engine-bad-kind", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "vm.micro"},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "vm.micro")
}

// TestAStepNamingNoEngineKindStillReachesAnEngineOfAnyKind is the common case,
// and the one a kind-routing regression would break silently: a step that
// names no kind must keep going to the unrestricted subject every engine
// subscribes to, whatever backend that engine happens to run.
func TestAStepNamingNoEngineKindStillReachesAnEngineOfAnyKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	acquired := make(chan string, 4)
	h.start(ctx, t, engine.Config{
		EngineID: "engine-vm-only", Tier: tier, Bus: h.engineBus,
		Executor: kindedExecutor{Executor: process.New(), kind: "vm", acquired: acquired},
		Blobs:    h.blobs, CAS: h.cas, Slots: 1,
	})

	statuses := h.statuses(ctx, t, "run-anykind", "probe")
	h.publishDispatch(ctx, t, newDispatch("run-anykind", "probe", "echo", "hi"))

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	require.Equal(t, "vm", receive(ctx, t, acquired, "the engine acquiring a sandbox"))
}

// --- harness ---------------------------------------------------------------

type harness struct {
	srv       *bus.Embedded
	plane     *bus.NATS
	engineBus *bus.NATS
	blobs     blobstore.Store
	cas       cas.Store
}

// newHarness starts an embedded bus, the dispatch work queue, and the two
// stores an engine writes through. Everything is torn down through t.Cleanup so
// no server or goroutine leaks into the next case.
func newHarness(t *testing.T, opts ...bus.Option) *harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{tier}))

	engineBus, err := bus.Connect(ctx, srv.URL(), opts...)
	require.NoError(t, err)
	t.Cleanup(engineBus.Close)

	return &harness{
		srv:       srv,
		plane:     plane,
		engineBus: engineBus,
		blobs:     blobstore.NewFilesystem(t.TempDir()),
		cas:       cas.NewFilesystem(t.TempDir()),
	}
}

// start runs an agent until the returned function is called or the test ends.
func (h *harness) start(ctx context.Context, t *testing.T, cfg engine.Config) func() {
	t.Helper()
	agent, err := engine.New(cfg)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- agent.Run(runCtx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("agent %s: %v", cfg.EngineID, err)
				}
			case <-time.After(30 * time.Second):
				t.Errorf("agent %s did not stop", cfg.EngineID)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func (h *harness) publishDispatch(ctx context.Context, t *testing.T, d *dholev1.JobDispatch) {
	t.Helper()
	subject := bus.SubjectDispatch(tier, engine.CapsHash(d.GetStep().GetCapabilities()))
	require.NoError(t, h.plane.Publish(ctx, subject, d))
}

// publishDispatchToKind publishes where only engines of one backend kind are
// listening. The kind is a token of the subject, exactly as the tier and the
// capability set are, because a dispatch refused on RECEIPT has already been
// taken off the work queue and its redelivery is a race rather than a route.
func (h *harness) publishDispatchToKind(ctx context.Context, t *testing.T, d *dholev1.JobDispatch, kind string) {
	t.Helper()
	subject := bus.SubjectDispatchKind(tier, engine.CapsHash(d.GetStep().GetCapabilities()), kind)
	require.NoError(t, h.plane.Publish(ctx, subject, d))
}

// awaitEngines blocks until every named engine has announced itself.
func awaitEngines(ctx context.Context, t *testing.T, regs <-chan *dholev1.EngineRegistration, ids ...string) {
	t.Helper()
	waiting := map[string]bool{}
	for _, id := range ids {
		waiting[id] = true
	}
	for len(waiting) > 0 {
		delete(waiting, receive(ctx, t, regs, "registration").GetEngineId())
	}
}

// kindedExecutor wears a backend KIND its delegate does not have and
// records every acquisition, so a test can say WHICH of two engines ran a
// step. A JobStatus carries no engine id, so without this the two engines
// sharing a tier are indistinguishable — the same blindness that let a step
// naming vm run on the kubernetes engine with every unit test green.
type kindedExecutor struct {
	executor.Executor
	kind     string
	acquired chan string
}

func (e kindedExecutor) Kind() string { return e.kind }

func (e kindedExecutor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	select {
	case e.acquired <- e.kind:
	default:
	}
	return e.Executor.Acquire(ctx, spec)
}

func (h *harness) statuses(ctx context.Context, t *testing.T, run, step string) <-chan *dholev1.JobStatus {
	return subscribe(ctx, t, h.plane, bus.SubjectStatus(run, step), func() *dholev1.JobStatus { return &dholev1.JobStatus{} })
}

func (h *harness) logs(ctx context.Context, t *testing.T, run, step string) <-chan *dholev1.LogChunk {
	return subscribe(ctx, t, h.plane, bus.SubjectLogs(run, step), func() *dholev1.LogChunk { return &dholev1.LogChunk{} })
}

// registrations and heartbeats UNFRAME what the engine published. Asking for
// the payload here rather than the frame is itself an assertion: an engine
// that published a bare payload would deliver a frame with no body, and every
// case below would receive a nil registration or heartbeat.
func (h *harness) registrations(ctx context.Context, t *testing.T) <-chan *dholev1.EngineRegistration {
	framed := subscribe(ctx, t, h.plane, bus.SubjectEngineRegistration(),
		func() *dholev1.EngineMessage { return &dholev1.EngineMessage{} })
	return unframe(framed, func(m *dholev1.EngineMessage) *dholev1.EngineRegistration {
		return m.GetRegistration()
	})
}

func (h *harness) heartbeats(ctx context.Context, t *testing.T, engineID string) <-chan *dholev1.EngineHeartbeat {
	framed := subscribe(ctx, t, h.plane, bus.SubjectEngineHeartbeat(engineID),
		func() *dholev1.EngineMessage { return &dholev1.EngineMessage{} })
	return unframe(framed, func(m *dholev1.EngineMessage) *dholev1.EngineHeartbeat {
		return m.GetHeartbeat()
	})
}

// unframe takes one arm of the frame's oneof, dropping anything that does not
// carry it.
func unframe[T proto.Message](in <-chan *dholev1.EngineMessage, body func(*dholev1.EngineMessage) T) <-chan T {
	out := make(chan T, 256)
	go func() {
		for msg := range in {
			payload := body(msg)
			if payload.ProtoReflect() == nil || !payload.ProtoReflect().IsValid() {
				continue
			}
			select {
			case out <- payload:
			default:
			}
		}
	}()
	return out
}

// subscribe collects messages off an ephemeral subscription into a buffered
// channel. Generated messages are only ever handled by pointer: copying one
// would trip vet's copylocks.
func subscribe[T proto.Message](ctx context.Context, t *testing.T, conn *bus.NATS, subject string, fresh func() T) <-chan T {
	t.Helper()
	out := make(chan T, 256)
	stop, err := conn.SubscribeEphemeral(ctx, subject, func(data []byte) {
		msg := fresh()
		if err := proto.Unmarshal(data, msg); err != nil {
			return
		}
		select {
		case out <- msg:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(stop)
	return out
}

func receive[T any](ctx context.Context, t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		var zero T
		t.Fatalf("no %s before the deadline", what)
		return zero
	case <-time.After(30 * time.Second):
		var zero T
		t.Fatalf("no %s within 30s", what)
		return zero
	}
}

func awaitPhase(ctx context.Context, t *testing.T, ch <-chan *dholev1.JobStatus, want dholev1.Phase) *dholev1.JobStatus {
	t.Helper()
	for {
		s := receive(ctx, t, ch, "JobStatus")
		if s.GetPhase() == want {
			return s
		}
	}
}

func awaitTerminal(ctx context.Context, t *testing.T, ch <-chan *dholev1.JobStatus) *dholev1.JobStatus {
	t.Helper()
	for {
		s := receive(ctx, t, ch, "terminal JobStatus")
		switch s.GetPhase() {
		case dholev1.Phase_PHASE_SUCCEEDED, dholev1.Phase_PHASE_FAILED, dholev1.Phase_PHASE_CANCELLED:
			return s
		case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
		}
	}
}

func newDispatch(run, step string, command ...string) *dholev1.JobDispatch {
	return &dholev1.JobDispatch{
		RunId:      run,
		StepId:     step,
		Attempt:    1,
		FenceToken: "fence-" + run + "-" + step,
		Step: &dholev1.Step{
			Id:          step,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		},
		OutputPrefix:    path.Join("runs", run, step),
		ProtocolVersion: wire.ProtocolVersion,
		Tenant:          &dholev1.Tenant{Id: tenantID},
		Command:         command,
	}
}

func readAll(t *testing.T, r io.Reader) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out
		}
	}
}

// slowStore makes the authoritative log take a visible amount of time to
// write, so "status published before the object was finished" is a failure
// rather than a race the test usually wins.
type slowStore struct {
	blobstore.Store
	delay time.Duration
}

func (s slowStore) Write(ctx context.Context, tenantID, key string, r io.Reader) error {
	time.Sleep(s.delay)
	return s.Store.Write(ctx, tenantID, key, r)
}

// failTerminalStatus is an engine's bus with one thing broken: publishing a
// terminal JobStatus fails. Everything else, including the ack path, still
// works — which is exactly the window the ordering rule protects.
type failTerminalStatus struct {
	bus.Bus
	mu      sync.Mutex
	refusal bool
}

func (f *failTerminalStatus) Publish(ctx context.Context, subject string, msg proto.Message) error {
	if s, ok := msg.(*dholev1.JobStatus); ok {
		switch s.GetPhase() {
		case dholev1.Phase_PHASE_SUCCEEDED, dholev1.Phase_PHASE_FAILED, dholev1.Phase_PHASE_CANCELLED:
			f.mu.Lock()
			f.refusal = true
			f.mu.Unlock()
			return errors.New("bus: publish refused by test")
		case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
		}
	}
	return f.Bus.Publish(ctx, subject, msg)
}

func (f *failTerminalStatus) refused() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refusal
}

// identifiedExecutor is the process executor that names its environment, as a
// container backend does by resolving its image to a digest. It stands in for
// a backend this test cannot start.
type identifiedExecutor struct{ *process.Executor }

func (identifiedExecutor) EnvironmentIdentity() (string, error) {
	return "sha256:image-under-test", nil
}

// TestAnEngineAnnouncesTheEnvironmentItRunsStepsIn is the engine's half of
// ADR 0021. The control plane cannot see the environment a step runs in — on a
// distributed deployment it is on another machine — so if it is not on the
// registration there is no honest way for the plane to obtain it at all, and
// nothing anywhere gets cached.
func TestAnEngineAnnouncesTheEnvironmentItRunsStepsIn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-identified",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: identifiedExecutor{process.New()},
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	reg := receive(ctx, t, registrations, "registration")
	require.Equal(t, "sha256:image-under-test", reg.GetEnvironmentIdentity())
}

// TestAnEngineWithNoStableEnvironmentAnnouncesNone: absent, never invented. A
// host process runs against whatever the host carries, and an engine that
// hashed its results against a made-up constant would have them served to a
// later run on a machine carrying something else — which is the one wrong
// answer a cache must never give. Its tier caches nothing instead.
func TestAnEngineWithNoStableEnvironmentAnnouncesNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The premise: the backend really does refuse to name an environment.
	_, err := process.New().EnvironmentIdentity()
	require.ErrorIs(t, err, executor.ErrNoStableIdentity)

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-anonymous",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	reg := receive(ctx, t, registrations, "registration")
	require.Empty(t, reg.GetEnvironmentIdentity(),
		"an engine with nothing reproducible to name says nothing rather than something")
}

// recordingExecutor remembers the Spec it was asked to acquire, so a test can
// assert what the engine derived from a dispatch rather than only what the
// step printed.
type recordingExecutor struct {
	*process.Executor
	mu    sync.Mutex
	specs []executor.Spec
}

func (r *recordingExecutor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	return r.Executor.Acquire(ctx, spec)
}

func (r *recordingExecutor) acquired() []executor.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]executor.Spec(nil), r.specs...)
}

// TestTheImageAPipelineNamedReachesTheSandboxSpec. The image a step runs in
// was the executor's to decide: one pod template per Kubernetes engine, so
// every step on that engine ran the same image whatever the pipeline wanted,
// and an acceptance pipeline needing a toolchain had to carry a Dockerfile's
// text inside a step. Step.image only means anything if the engine hands it to
// Acquire, which is the one hop this asserts.
func TestTheImageAPipelineNamedReachesTheSandboxSpec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-image", "build")
	exec := &recordingExecutor{Executor: process.New()}

	h.start(ctx, t, engine.Config{
		EngineID: "engine-image",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: exec,
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-image", "build", "echo", "hi")
	d.Step.Image = "ghcr.io/dhole/toolchain@sha256:" + strings.Repeat("a", 64)
	h.publishDispatch(ctx, t, d)

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, statuses).GetPhase())
	specs := exec.acquired()
	require.Len(t, specs, 1)
	require.Equal(t, d.GetStep().GetImage(), specs[0].Image,
		"the engine must acquire the sandbox the pipeline asked for, not the one its backend defaults to")
}
