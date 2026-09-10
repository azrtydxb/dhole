// Package gate is the step type that WAITS. A step carrying its plugin ref is
// never sent to an engine: reaching it arms a durable timer, and the run
// resumes when that timer comes due (ADR 0003).
//
// It exists because arming a gate used to be somebody else's job. Whoever
// started a run scheduled the wait in a transaction of its own, and the
// scheduler decided readiness in another. Sequence numbers are allocated
// inside a transaction and VISIBILITY is not, so a gate could hold a lower
// sequence than the STEP_DISPATCHED of the very step it gated — the log read
// "gated, then dispatched anyway", and the wait had not happened at all. Both
// acceptance pipelines worked around it by arming the gate behind a
// five-second predecessor, which is a race made unlikely rather than closed.
//
// Two decisions make it closed rather than unlikely.
//
// GATEDNESS IS A FACT ABOUT THE DEFINITION, not about the log. The scheduler
// knows this step waits because its plugin ref says so, and the pinned
// revision said so before the run existed. Nothing can commit late enough to
// be missed, because there is nothing to miss.
//
// THE ARMING RUNS IN THE TRANSACTION THAT WOULD HAVE DISPATCHED. The gate
// event and the timer row are written by the same act that concluded the step
// was ready, so a plane that dies between the two has done neither.
package gate

import (
	"context"
	"errors"
	"fmt"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/wait"
)

// PluginRef is what a gate node carries in a pipeline. Like the loop and
// generator nodes it is a well-known builtin rather than a plugin the registry
// resolves: a wait is served by the control plane's own timer table, and there
// is no engine anywhere that could be asked to perform one.
const PluginRef = "builtin:wait"

// AliasPluginRef is the other name a gate goes by.
//
// Both names arm through THIS package, in the transaction that decided the
// step was ready. A second implementation that armed out of band would
// reintroduce the bug 0023 closes — a wait recorded after the dispatch it was
// meant to prevent, and therefore skipped — under a different plugin_ref.
const AliasPluginRef = "builtin:timer"

// IsGate reports whether a plugin_ref names a step that waits rather than runs.
func IsGate(ref string) bool { return ref == PluginRef || ref == AliasPluginRef }

// ConfigDuration is the step config key naming how long the gate waits, as a
// Go duration ("30s", "72h"). It is a duration rather than an instant because
// a wait is relative to REACHING the gate: an absolute time in a definition
// would already have passed the second time the pipeline ran.
const ConfigDuration = "duration"

// The refusals. A gate that cannot say how long it waits is refused where the
// mistake was made, rather than defaulting to something and being discovered
// as a pipeline that either never waited or never woke up.
var (
	// ErrNoDuration: the step carries no duration at all.
	ErrNoDuration = errors.New("gate: a wait step must declare " + ConfigDuration)

	// ErrBadDuration: the duration is not a duration, or is not positive.
	// Zero is not "no wait configured yet" and a negative one is not a wait.
	ErrBadDuration = errors.New("gate: the declared wait is not a positive duration")
)

// Timers is the durable timer table, narrowed to the one thing a gate does
// with it: arm a wait inside a transaction somebody else owns. *wait.Timers
// satisfies it.
type Timers interface {
	ArmInTx(ctx context.Context, tx runstore.Tx, tenantID, runID, stepID string, at time.Time) error
}

var _ Timers = (*wait.Timers)(nil)

// Gate arms the wait a gate step declares. It is safe for concurrent use.
type Gate struct {
	timers Timers
	now    func() time.Time
}

// Options are the pieces a Gate cannot invent.
type Options struct {
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// New builds a Gate over the deployment's timer table.
func New(timers Timers, opts Options) (*Gate, error) {
	if timers == nil {
		return nil, errors.New("gate: a timer table is required; " +
			"a gate with nowhere to record its wait is a step that never resumes")
	}
	g := &Gate{timers: timers, now: opts.Now}
	if g.now == nil {
		g.now = time.Now
	}
	return g, nil
}

// Arm records that a step waits, in the caller's transaction, and reports
// whether the step is a gate at all.
//
// A step that is not a gate is answered false and NOTHING is written: the
// caller goes on to dispatch it exactly as it did before this existed. That is
// the whole of the seam — a gate is diverted, everything else is untouched.
func (g *Gate) Arm(
	ctx context.Context, tx runstore.Tx, tenantID, runID string, step *dholev1.Step,
) (bool, error) {
	if !IsGate(step.GetPluginRef()) {
		return false, nil
	}
	d, err := Duration(step)
	if err != nil {
		return false, err
	}
	if err := g.timers.ArmInTx(ctx, tx, tenantID, runID, step.GetId(), g.now().UTC().Add(d)); err != nil {
		return false, err
	}
	return true, nil
}

// Duration is how long a gate step waits, read from its config.
//
// It is exported because a pipeline is validated long before it is run, and a
// gate whose duration is unreadable should be refused by an editor rather than
// discovered by a run that stops on it.
func Duration(step *dholev1.Step) (time.Duration, error) {
	raw, ok := step.GetConfig()[ConfigDuration]
	if !ok || raw == "" {
		return 0, fmt.Errorf("%w: step %q", ErrNoDuration, step.GetId())
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: step %q declares %q", ErrBadDuration, step.GetId(), raw)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%w: step %q declares %q", ErrBadDuration, step.GetId(), raw)
	}
	return d, nil
}
