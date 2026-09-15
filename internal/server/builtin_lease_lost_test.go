package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// A step the plane hosts stops when its lease is taken away (ADR 0031).
//
// The commit already refused the RESULT of a builtin step whose lease had been
// swept: a plane partitioned past its TTL has had the step given to another
// plane, and its verdict must not decide a step somebody else is running. But
// the body ran to the end first. For an llm step that is a model call nobody
// will record; for an agent step it is tool calls — runs started, gates
// decided, operations applied — made a second time beside the plane that now
// holds the step.

// renewal decides what every Renew through it answers, and says when the
// first one was refused.
type renewal struct {
	lease.Manager
	mu      sync.Mutex
	answer  func(n int) error
	calls   int
	refused chan struct{}
	once    sync.Once
}

func newRenewal(m lease.Manager, answer func(n int) error) *renewal {
	return &renewal{Manager: m, answer: answer, refused: make(chan struct{})}
}

func (r *renewal) Renew(ctx context.Context, t lease.Token) error {
	r.mu.Lock()
	r.calls++
	n := r.calls
	r.mu.Unlock()
	if err := r.answer(n); err != nil {
		r.once.Do(func() { close(r.refused) })
		return err
	}
	return r.Manager.Renew(ctx, t)
}

// watchedBody is an effect whose end is observable: it reports the error the
// body returned the moment it returns.
func watchedBody(e *effect) (func(context.Context, builtinJob, uint32) error, chan error) {
	ended := make(chan error, 1)
	return func(ctx context.Context, job builtinJob, attempt uint32) error {
		_, err := e.do(ctx, job, attempt)
		ended <- err
		return err
	}, ended
}

func runHosted(
	ctx context.Context, b *builtins, job builtinJob, body func(context.Context, builtinJob, uint32) error,
) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := b.attempt(ctx, job, func(ctx context.Context, job builtinJob, attempt uint32) ([]*dholev1.OutputRef, error) {
			return nil, body(ctx, job, attempt)
		})
		done <- err
	}()
	return done
}

func TestAHostedStepWhoseRenewalIsRefusedStopsItsBody(t *testing.T) {
	ctx := testCtx(t)
	h := newHostedHarness(t)
	refuse := make(chan struct{})
	renew := newRenewal(h.leases(ctx, t), func(int) error {
		select {
		case <-refuse:
			return lease.ErrFenced
		default:
			return nil
		}
	})
	const ttl = 300 * time.Millisecond
	plane := h.plane(renew, ttl)

	e := newEffect()
	body, ended := watchedBody(e)
	done := runHosted(ctx, plane, take(ctx, t, plane, "run-1", 1), body)
	<-e.entered // the dispatch is recorded and the body is running

	close(refuse) // the lease was swept while this plane was partitioned
	<-renew.refused
	select {
	case err := <-ended:
		require.ErrorIs(t, err, context.Canceled, "the body ended for some other reason")
	case <-time.After(10 * time.Second):
		close(e.release)
		t.Fatal("a plane whose renewal was refused ran the step body on: " +
			"its model call or agent tool calls happen beside the plane that holds the step now")
	}
	require.NoError(t, <-done, "losing the lease is not a failure to record")

	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched))
	require.Zero(t, h.count(ctx, t, "run-1", runstore.StepSucceeded),
		"a body stopped because its lease was lost recorded a verdict")
	require.Zero(t, h.count(ctx, t, "run-1", runstore.StepFailed),
		"a body stopped because its lease was lost failed a step another plane holds")
}

func TestAHostedStepWhoseRenewalLapsesForATTLStopsItsBody(t *testing.T) {
	const ttl = 300 * time.Millisecond

	t.Run("no renewal succeeds for a whole TTL", func(t *testing.T) {
		ctx := testCtx(t)
		h := newHostedHarness(t)
		// Unreachable, not refused: the plane cannot tell whether the lease
		// was swept, and after a TTL without a renewal the KV has expired it.
		renew := newRenewal(h.leases(ctx, t), func(int) error { return errors.New("lease bucket unreachable") })
		plane := h.plane(renew, ttl)

		e := newEffect()
		body, ended := watchedBody(e)
		done := runHosted(ctx, plane, take(ctx, t, plane, "run-1", 1), body)
		<-e.entered

		select {
		case err := <-ended:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			close(e.release)
			t.Fatal("a plane that could not renew its lease for a whole TTL ran the step body on")
		}
		require.NoError(t, <-done)
		require.Zero(t, h.count(ctx, t, "run-1", runstore.StepSucceeded))
		require.Zero(t, h.count(ctx, t, "run-1", runstore.StepFailed))
	})

	t.Run("one failed renewal inside the TTL is not a lost lease", func(t *testing.T) {
		ctx := testCtx(t)
		h := newHostedHarness(t)
		renew := newRenewal(h.leases(ctx, t), func(n int) error {
			if n == 1 {
				return errors.New("a slow round trip")
			}
			return nil
		})
		plane := h.plane(renew, ttl)

		e := newEffect()
		body, ended := watchedBody(e)
		done := runHosted(ctx, plane, take(ctx, t, plane, "run-1", 1), body)
		<-e.entered
		<-renew.refused

		// Several TTLs of healthy renewals after the one that failed.
		time.Sleep(4 * ttl)
		select {
		case err := <-ended:
			t.Fatalf("a step whose lease was still renewed had its body stopped: %v", err)
		default:
		}
		close(e.release)
		require.NoError(t, <-ended)
		require.NoError(t, <-done)
		require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))
	})
}
