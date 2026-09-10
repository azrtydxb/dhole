package dag

import (
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// FileInputs resolves the input ports a step reads from files the DEFINITION
// carries (ADR 0023), as the same InputRef an edge produces.
//
// It is here, and shared, because two callers must agree exactly. The
// scheduler builds the dispatch an engine materialises inputs from, and the
// planner builds the cache key that decides whether that dispatch happens at
// all; a file that reached one and not the other would be a step keyed on
// bytes it does not receive, or served from a cache of bytes it does. The two
// keys have to be the same expression over the same values or the cache is
// wrong rather than merely cold.
//
// A binding naming a path the definition does not carry is an error rather
// than a skipped input: an engine would run the step with a port that has
// nothing on it, which is a failure a long way from the definition that caused
// it. api.Validate reports the same fault as a diagnostic before a run starts.
func FileInputs(p *dholev1.Pipeline, step *dholev1.Step) ([]*dholev1.InputRef, error) {
	bindings := step.GetFileInputs()
	if len(bindings) == 0 {
		return nil, nil
	}
	byPath := make(map[string]*dholev1.File, len(p.GetFiles()))
	for _, f := range p.GetFiles() {
		byPath[f.GetPath()] = f
	}

	out := make([]*dholev1.InputRef, 0, len(bindings))
	for _, in := range bindings {
		f, ok := byPath[in.GetPath()]
		if !ok {
			return nil, fmt.Errorf(
				"step %q reads file %q on port %q, and the definition carries no such file",
				step.GetId(), in.GetPath(), in.GetPort())
		}
		// The digest is the whole point: it is what a revision pins and what
		// the cache key is folded over, so a file with none is refused rather
		// than contributing nothing to the key.
		if f.GetDigest().GetHex() == "" {
			return nil, fmt.Errorf("file %q has no digest: a revision pins the bytes it runs", in.GetPath())
		}
		out = append(out, &dholev1.InputRef{Port: in.GetPort(), Digest: f.GetDigest()})
	}
	return out, nil
}
