package effects

import "time"

// MaxBackoff caps the wait between retries. Doubling forever turns a step that
// is failing into a step that is effectively abandoned without anyone saying
// so, and — with a large enough attempt count — overflows the duration into a
// negative number, which would retry instantly rather than never.
const MaxBackoff = 5 * time.Minute

// Backoff is how long to wait before dispatching the given attempt's
// successor, given the policy the step's effect class implies.
//
// It grows with each failure: a step that failed because the far end was
// overloaded must not be retried at the rate that overloaded it. attempt is
// the number of attempts made so far, so the first retry waits Policy.Backoff.
func Backoff(p Policy, attempt uint32) time.Duration {
	if p.Backoff <= 0 {
		return 0
	}
	wait := p.Backoff
	for range attempt - 1 {
		wait *= 2
		if wait >= MaxBackoff {
			return MaxBackoff
		}
	}
	if wait > MaxBackoff {
		return MaxBackoff
	}
	return wait
}

// AllowsRetry reports whether a step that has been dispatched `attempts` times
// may be dispatched again. It is the one place the count is compared, so a
// caller cannot get the off-by-one wrong in its own favour.
func (p Policy) AllowsRetry(attempts uint32) bool {
	return p.MaxAttempts > 1 && attempts < uint32(p.MaxAttempts) //nolint:gosec // MaxAttempts is a small constant
}
