package scheduler

import (
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dynamic"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// realised is one fragment a generator emitted, read back out of the run log.
//
// It is the fragment ITSELF and not a list of ids: the run's graph is rebuilt
// from it, and a summary would leave the scheduler guessing at the steps'
// ports, edges and effect classes — the very things a dispatch is derived
// from.
type realised struct {
	// generator is the step the fragment was spliced below.
	generator string
	// fragment is the pipeline the generator emitted, as recorded.
	fragment *dholev1.Pipeline
}

// foldRealised decodes one GENERATOR_FRAGMENT_REALISED event.
func foldRealised(runID string, e runstore.Event) (realised, error) {
	rec, err := dynamic.UnmarshalRecord(e.Payload)
	if err != nil {
		return realised{}, fmt.Errorf("scheduler: run %q step %q: %w", runID, e.StepID, err)
	}
	fragment, err := rec.Fragment()
	if err != nil {
		return realised{}, fmt.Errorf("scheduler: run %q step %q: %w", runID, e.StepID, err)
	}
	return realised{generator: e.StepID, fragment: fragment}, nil
}

// spliceRealised returns the graph this run ACTUALLY has: the pinned
// definition with every fragment its generators realised put back where it
// was realised, in the order the log recorded them.
//
// This is ADR 0003 for the one step type that can invent work. A generator
// decides at runtime what there is to do — a matrix from an API, one step per
// file it found — and it answers differently the next time it is asked. So the
// fragment is recorded when it is realised and REREAD here, never regenerated:
// this scheduler holds no generator and cannot ask one. Without this fold a
// restarted plane would derive the authored graph, find the generator already
// succeeded, see nothing ready, and COMPLETE the run while the steps it
// realised had never run — green, and wrong.
//
// A fragment that cannot be put back is an error rather than a fallback. The
// alternative is running the authored pipeline under the id of a run that had
// a different graph, and reporting the result as that run.
func spliceRealised(
	runID string, pipeline *dholev1.Pipeline, fragments []realised,
) (*dholev1.Pipeline, error) {
	for _, r := range fragments {
		spliced, err := dynamic.Splice(pipeline, r.generator, r.fragment)
		if err != nil {
			return nil, fmt.Errorf(
				"scheduler: run %q realised a fragment at %q that no longer splices: %w",
				runID, r.generator, err)
		}
		pipeline = spliced
	}
	return pipeline, nil
}
