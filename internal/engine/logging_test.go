package engine_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// A deployment found this the hard way: the engine ran steps, failed every one
// of them, and printed nothing but its own banner. The reason a step failed —
// there, a missing /bin/sh in a distroless image — was reachable only by
// decoding a protobuf out of the run event table.
func TestTheEngineLogsWhyAStepFailed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log := captureSlog(t)

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-log", "boom")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-log",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	// A program that is not there, which is the shape of the deployment bug.
	h.publishDispatch(ctx, t, newDispatch("run-log", "boom", "/nonexistent/dhole-no-such-program"))

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase())

	out := eventually(t, log, "step finished")
	require.Contains(t, out, "run-log", "the log must name the run")
	require.Contains(t, out, "boom", "the log must name the step")
	require.Contains(t, out, "dhole-no-such-program",
		"the log must carry the reason, not just the fact of a failure")
}

// The other half: a working engine must be distinguishable from an idle one.
func TestTheEngineLogsAStepItAccepts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log := captureSlog(t)

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-ok", "hello")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-ok",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	h.publishDispatch(ctx, t, newDispatch("run-ok", "hello", "echo", "hi"))

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase())

	out := eventually(t, log, "step accepted")
	require.Contains(t, out, "engine-ok")
	require.Contains(t, out, "run-ok")
}

// captureSlog redirects the default logger for the duration of one test and
// returns the buffer it writes to. Concurrent-safe because the engine logs
// from its pump goroutine while the test reads.
func captureSlog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// eventually waits for want to appear in the log. The terminal status is
// published before logOutcome returns on some paths, so the assertion cannot
// assume the line is already there when the status arrives.
func eventually(t *testing.T, buf *syncBuffer, want string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		out := buf.String()
		if strings.Contains(out, want) {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("the engine never logged %q; it logged:\n%s", want, out)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
