package scheduler

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/taint"
)

// THE FACTS A DISPATCH DECISION IS MADE ON THAT ONLY THE RUN AND THE FLEET KNOW
// (ADR 0031).
//
// ADR 0015's rules read `input.tainted`, `input.taint_sources` and
// `input.engine_capabilities`. None was set at dispatch, so the rules held there
// by never firing. Both are derived here, per decision, from what the scheduler
// already has — the run's log and the fleet — and stored nowhere else: a second
// record of a step's taint would be a second thing to replay and to get out of
// step with the first (ADR 0003).

// taintSources is every trigger whose data reaches step, sorted.
//
// A step's inputs are the run's values bound to its free ports — the ports no
// edge feeds, which is what a trigger binds (ADR 0007) — and the outputs of the
// steps that feed its other ports. Taint is carried conservatively, as
// taint.Propagate carries it: anything tainted a step reads taints everything
// it produces, however many hops downstream. A step on whose behalf a
// sanitisation gate recorded TAINT_SANITISED passes on its inputs' sources less
// the ones that record cleared — the record, and not the gate's presence in the
// graph, is what clears (taint.Gate).
func taintSources(p *dholev1.Pipeline, step *dholev1.Step, state *runState) []string {
	feeds := map[string][]string{} // to-step -> from-steps
	fed := map[string]bool{}       // "step\x00port" fed by an edge
	for _, e := range p.GetEdges() {
		feeds[e.GetToStep()] = append(feeds[e.GetToStep()], e.GetFromStep())
		fed[e.GetToStep()+"\x00"+e.GetToPort()] = true
	}

	memo := map[string]map[string]struct{}{}
	var reaching func(s *dholev1.Step) map[string]struct{}
	reaching = func(s *dholev1.Step) map[string]struct{} {
		if got, ok := memo[s.GetId()]; ok {
			return got
		}
		// Set before recursing: the graph is acyclic by dag.Build, and this
		// keeps a malformed one from recursing forever rather than relying on it.
		sources := map[string]struct{}{}
		memo[s.GetId()] = sources
		for _, port := range s.GetInputs() {
			if fed[s.GetId()+"\x00"+port.GetName()] {
				continue
			}
			for _, src := range taint.Sources(state.inputs[port.GetName()]) {
				sources[src] = struct{}{}
			}
		}
		for _, from := range feeds[s.GetId()] {
			upstream := stepByID(p, from)
			if upstream == nil {
				continue
			}
			for src := range reaching(upstream) {
				if !slices.Contains(state.sanitised[from], src) {
					sources[src] = struct{}{}
				}
			}
		}
		return sources
	}

	set := reaching(step)
	out := make([]string, 0, len(set))
	for src := range set {
		out = append(out, src)
	}
	sort.Strings(out)
	return out
}

// reachableCapabilities is every capability advertised by an engine that could
// take step, in enum order.
//
// Every one of them, and not the one that will: a dispatch goes onto the tier's
// work queue, and whichever matching engine fetches first runs it, so the only
// honest answer to "which engine does this reach" is all of them. A rule
// keeping untrusted data off privileged engines therefore refuses a step if ANY
// engine it could reach is privileged. A step the plane hosts itself reaches no
// engine at all.
func (s *Scheduler) reachableCapabilities(
	ctx context.Context, tenantID string, step *dholev1.Step,
) ([]dholev1.Capability, error) {
	if strings.HasPrefix(step.GetPluginRef(), builtinScheme) {
		return nil, nil
	}
	instances, err := s.fleet.Instances(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("scheduler: listing engines for %s: %w", tenantID, err)
	}
	seen := map[dholev1.Capability]bool{}
	var out []dholev1.Capability
	for _, e := range Match(s.requirements(step), instances) {
		for _, c := range e.Capabilities {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}
