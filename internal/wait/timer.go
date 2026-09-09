package wait

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// PollInterval is how often the control plane looks for timers that have come
// due. A wait is therefore accurate to about a second, which is the trade the
// table buys: a wait that survives a restart is worth more than one that fires
// to the millisecond and dies with its process.
const PollInterval = time.Second

// Resumer advances a run. *scheduler.Scheduler is the implementation; the
// interface exists so this package does not depend on the whole of scheduling
// to say "this run has something to do now".
type Resumer interface {
	Advance(ctx context.Context, tenantID, runID string) error
}

// Runner is the poll. It holds no run state — it cannot, there is nowhere to
// put it — and can be stopped and started at will: everything it needs is in
// the timer table.
type Runner struct {
	timers   *Timers
	resume   Resumer
	interval time.Duration
	onError  func(error)
}

// RunnerOption tunes a Runner.
type RunnerOption func(*Runner)

// WithPollInterval overrides PollInterval. Tests use it to poll faster than a
// person can wait; a deployment has no reason to.
func WithPollInterval(d time.Duration) RunnerOption {
	return func(r *Runner) {
		if d > 0 {
			r.interval = d
		}
	}
}

// WithErrorHandler is called with every error the poll swallows. Without one
// they are invisible: the timer stays outstanding and is retried, which is
// correct, but says nothing about a database that has been unreachable for an
// hour.
func WithErrorHandler(fn func(error)) RunnerOption {
	return func(r *Runner) { r.onError = fn }
}

// NewRunner builds the poll over a timer table and whatever advances runs.
func NewRunner(timers *Timers, resume Resumer, opts ...RunnerOption) *Runner {
	r := &Runner{timers: timers, resume: resume, interval: PollInterval}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Tick fires everything due at now and advances the runs it woke, returning
// how many timers fired. Time is a parameter rather than a reading of the
// clock so that a test can be at a due time exactly instead of sleeping past
// it approximately.
//
// An Advance that fails does NOT put the timer back: the wait has already
// ended in the log, durably, and any later advance of that run — another
// status, the next recovery sweep — picks it up from there. Re-firing would
// be the catch-up bug in another costume.
func (r *Runner) Tick(ctx context.Context, now time.Time) (int, error) {
	if r.timers == nil || r.resume == nil {
		return 0, errors.New("wait: a runner needs a timer table and something to resume")
	}
	due, err := r.timers.Due(ctx, now)
	if err != nil {
		return 0, err
	}
	fired := 0
	for _, d := range due {
		if err := r.resume.Advance(ctx, d.TenantID, d.RunID); err != nil {
			return fired, fmt.Errorf("wait: resuming %s/%s: %w", d.RunID, d.StepID, err)
		}
		fired++
	}
	return fired, nil
}

// Run polls until ctx is done, returning ctx's error. It is the control
// plane's clock for everything that waits.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := r.Tick(ctx, time.Now().UTC()); err != nil {
				if r.onError != nil && ctx.Err() == nil {
					r.onError(err)
				}
			}
		}
	}
}
