package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/azrtydxb/dhole/internal/bus"
)

// slowStreamBus fails SubscribePull until the caller has tried enough times to
// have passed any fixed deadline, then succeeds — which is what an engine sees
// when the control plane is slow to create the dispatch stream.
type slowStreamBus struct {
	attempts  atomic.Int64
	succeedAt int64
}

func (b *slowStreamBus) Publish(context.Context, string, proto.Message) error { return nil }
func (b *slowStreamBus) Request(context.Context, string, proto.Message, proto.Message) error {
	return nil
}
func (b *slowStreamBus) SubscribeEphemeral(context.Context, string, func([]byte)) (func(), error) {
	return func() {}, nil
}
func (b *slowStreamBus) SubscribePull(context.Context, string, string, string) (bus.Subscription, error) {
	if b.attempts.Add(1) < b.succeedAt {
		return nil, errors.New("nats: stream not found")
	}
	// A nil Subscription with a nil error is enough: subscribe's contract here
	// is "keep trying until the bus stops refusing", and what it hands back
	// afterwards is the bus's business.
	return nil, nil
}

// TestSubscribeKeepsTryingWhileTheStreamIsMissing is the bug a real cluster
// found.
//
// An engine binds a consumer on a stream only the control plane may create, so
// it may well start first. subscribe retried for sixty seconds and then gave
// up — and the comment above it said the retry existed so that "start-up
// ordering is not load-bearing", which is precisely what it became. On the
// cluster, NATS crash-looped for ninety seconds; the trusted engine passed its
// deadline, and then sat there REGISTERED, heartbeating and reported `ready`
// with no consumer at all. Every step dispatched to its tier waited for a
// lease to expire and was re-dispatched, forever.
func TestSubscribeKeepsTryingWhileTheStreamIsMissing(t *testing.T) {
	// More attempts than the old sixty-second deadline allowed at its own
	// retry interval, so a fixed deadline cannot pass this.
	const attemptsNeeded = 200

	b := &slowStreamBus{succeedAt: attemptsNeeded}
	a := &Agent{cfg: Config{Tier: "trusted", Bus: b, SubscribeBackoff: time.Microsecond}}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := a.subscribe(ctx, "DISPATCH_trusted", "hash", "job.dispatch.trusted.hash")
	require.NoError(t, err, "subscribe gave up while the stream was still on its way")
	require.GreaterOrEqual(t, b.attempts.Load(), int64(attemptsNeeded),
		"subscribe stopped retrying early")
}

// TestSubscribeStopsWhenTheEngineIsShuttingDown: retrying must not outlive a
// drain, or a stopping engine hangs instead of exiting.
func TestSubscribeStopsWhenTheEngineIsShuttingDown(t *testing.T) {
	b := &slowStreamBus{succeedAt: 1 << 30}
	a := &Agent{cfg: Config{Tier: "trusted", Bus: b, SubscribeBackoff: time.Millisecond}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()

	_, err := a.subscribe(ctx, "DISPATCH_trusted", "hash", "job.dispatch.trusted.hash")
	require.ErrorIs(t, err, context.Canceled)
}

// TestSubscribeHasNoDeadline guards the fix in the only way that is fast.
//
// The difference between "retries for a minute" and "retries until stopped"
// takes a minute to observe, so a behavioural test for it would be a
// minute-long test nobody runs. This reads the source instead — the same
// technique this repository already uses for properties that cannot be
// observed cheaply — and fails if a deadline reappears in subscribe.
func TestSubscribeHasNoDeadline(t *testing.T) {
	src, err := os.ReadFile("agent.go")
	require.NoError(t, err)

	fn := string(src)
	start := strings.Index(fn, "func (a *Agent) subscribe(")
	require.Positive(t, start, "subscribe is not in agent.go any more; move this guard with it")
	end := strings.Index(fn[start:], "\n}\n")
	require.Positive(t, end, "could not find the end of subscribe")
	body := fn[start : start+end]

	for _, banned := range []string{"deadline", "Deadline", "WithTimeout", "time.Now().Add("} {
		require.NotContains(t, body, banned,
			"subscribe has a deadline again: an engine that gives up binding still "+
				"registers, still heartbeats, still reports ready — and takes no work, "+
				"forever. That is the bug a real cluster found.")
	}
}
