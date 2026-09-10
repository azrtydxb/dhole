package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestConnectWaitsForABusThatIsNotUpYet is the bug a real deployment found.
//
// An engine and its bus start together. On Kubernetes the bus was still
// crash-looping when the engines came up, and every engine exited on its first
// failed dial with "no route to host" — leaving the fleet to Kubernetes'
// restart backoff, which grows to five minutes. An engine that gives up
// instantly is an engine that is absent for minutes after any bus restart.
func TestConnectWaitsForABusThatIsNotUpYet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts int
	// A dial that fails twice and then succeeds, which is what a bus coming up
	// looks like from an engine's side.
	dial := func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errors.New("dial tcp: connect: no route to host")
		}
		return nil
	}

	require.NoError(t, connectWithRetry(ctx, dial, 10*time.Millisecond))
	require.Equal(t, 3, attempts, "the engine must keep trying while the bus comes up")
}

// TestConnectStopsWhenTheContextIsDone: retrying must not outlive a shutdown,
// or a drained engine hangs instead of exiting.
func TestConnectStopsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := connectWithRetry(ctx, func(context.Context) error {
		return errors.New("still down")
	}, 10*time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
}
