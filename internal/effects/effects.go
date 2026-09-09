// Package effects derives retry behaviour from a step's declared effect class.
//
// The effect class is the only thing in the system that can decide whether
// repeating a step is safe, because nothing else knows what happens outside
// the process when it runs (ADR 0002):
//
//   - PURE has no effect anyone else can see. It may be repeated freely, and
//     its result may be cached.
//   - IDEMPOTENT has an external effect, but repeating it with the SAME
//     idempotency key is safe, because the far end recognises the repeat and
//     declines to act twice. So it is retried, and never without that key.
//   - AT_MOST_ONCE may never be repeated automatically. A failure waits for a
//     human, because the alternative is charging a card twice and finding out
//     from the customer.
//
// Everything else — an unset class, a class from a newer schema this binary
// does not know — takes the at-most-once policy. A step that has promised
// nothing has not promised it is safe to repeat, and the cost of being wrong
// is asymmetric: a conservative policy costs an operator a click, a permissive
// one costs somebody money.
package effects

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Policy is what a step's effect class implies about repeating it.
//
// MaxAttempts counts dispatches, not retries: 1 means the step is dispatched
// once and never automatically again. Backoff is the delay before the FIRST
// retry; each later one waits longer (see Backoff).
type Policy struct {
	MaxAttempts            int
	Backoff                time.Duration
	RequiresIdempotencyKey bool
	RequiresExclusiveLease bool
}

// The waits between retries. They are the first delay, not the whole schedule:
// a step that keeps failing waits longer each time, up to MaxBackoff.
const (
	// pureBackoff is short because retrying a pure step costs only the work.
	pureBackoff = 5 * time.Second
	// idempotentBackoff is longer because the far end is a system that may be
	// failing precisely because it is being asked too often.
	idempotentBackoff = 15 * time.Second
)

// RetryPolicy is the effect class table, and the class is its only input. Not
// the plugin, not the step's name, not whether it failed before: anything else
// would be a guess about behaviour, and the pipeline author is the one who
// knows (ADR 0002).
//
// A nil step takes the conservative policy for the same reason an undeclared
// class does.
func RetryPolicy(step *dholev1.Step) Policy {
	switch step.GetEffectClass() {
	case dholev1.EffectClass_EFFECT_CLASS_PURE:
		return Policy{
			MaxAttempts: 3,
			Backoff:     pureBackoff,
		}
	case dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT:
		return Policy{
			MaxAttempts: 3,
			Backoff:     idempotentBackoff,
			// Without the key the retry is a second request rather than the
			// same one, and the class's whole promise evaporates.
			RequiresIdempotencyKey: true,
		}
	case dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED:
		return atMostOnce()
	default:
		// A class this binary does not recognise: a newer control plane's
		// schema, most likely. Unknown is not permission.
		return atMostOnce()
	}
}

// atMostOnce is the most conservative policy there is, and the default for
// everything that has not earned a weaker one: one attempt, an exclusive lease
// held while it runs, and a key on any replay a human later authorises.
func atMostOnce() Policy {
	return Policy{
		MaxAttempts:            1,
		Backoff:                0,
		RequiresIdempotencyKey: true,
		RequiresExclusiveLease: true,
	}
}

// IdempotencyKey is the value a step presents to the far end so that a repeat
// is recognised as a repeat.
//
// It is derived from the run and the step and NOTHING ELSE. In particular it
// does not vary with the attempt, even though the attempt is a parameter: a
// key that changed between attempts would describe every retry as a fresh
// request, which is exactly what the key exists to prevent. The parameter is
// accepted so that callers pass what they have, and so that this comment sits
// where somebody would otherwise "fix" the unused argument by folding it into
// the hash.
func IdempotencyKey(runID, stepID string, attempt uint32) string {
	_ = attempt // deliberately not hashed; see above.

	h := sha256.New()
	// Length-prefixed, so ("run", "1step") and ("run1", "step") cannot hash to
	// the same key and let one step's repeat suppress another's request.
	// hash.Hash never returns an error from Write, which is why the value is
	// discarded here rather than plumbed into a signature nobody could act on.
	_, _ = fmt.Fprintf(h, "dhole.idempotency.v1|%d:%s|%d:%s", len(runID), runID, len(stepID), stepID)
	return "dhole-" + hex.EncodeToString(h.Sum(nil))
}
