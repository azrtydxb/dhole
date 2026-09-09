// Package dag derives a pipeline's execution order from the edges between its
// typed ports. Nothing else declares dependencies: a step reaches its
// predecessor's data only by connecting a port to it, so the graph cannot
// drift from what the steps actually consume (ADR 0001). The levels this
// package computes are what lets independent steps run at the same time
// instead of in authoring order.
package dag

import (
	"fmt"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Graph is the derived dependency graph of one pipeline: which steps must
// finish before which others may start. It is immutable once built.
type Graph struct {
	// stepIDs in sorted order, so every traversal is deterministic.
	stepIDs []string
	// dependents maps a step to the steps that consume its outputs.
	dependents map[string][]string
	// indegree counts the distinct predecessors of each step.
	indegree map[string]int
}

// Build derives the graph from the pipeline's edges. It fails rather than
// returning a partial graph when an edge names a step the pipeline does not
// define, or when the edges form a cycle — a cyclic pipeline has no valid
// execution order at all, so there is nothing useful to hand a scheduler.
func Build(p *dholev1.Pipeline) (*Graph, error) {
	if p == nil {
		return nil, fmt.Errorf("dag: pipeline is nil")
	}

	g := &Graph{
		dependents: make(map[string][]string, len(p.GetSteps())),
		indegree:   make(map[string]int, len(p.GetSteps())),
	}

	known := make(map[string]bool, len(p.GetSteps()))
	for _, s := range p.GetSteps() {
		id := s.GetId()
		if id == "" {
			return nil, fmt.Errorf("dag: pipeline %q has a step with an empty id", p.GetId())
		}
		if known[id] {
			return nil, fmt.Errorf("dag: pipeline %q defines step %q more than once", p.GetId(), id)
		}
		known[id] = true
		g.stepIDs = append(g.stepIDs, id)
		g.indegree[id] = 0
	}
	sort.Strings(g.stepIDs)

	// Two edges between the same pair of steps (different ports) are one
	// dependency; counting them twice would leave the indegree unsatisfiable.
	seen := make(map[[2]string]bool, len(p.GetEdges()))
	for _, e := range p.GetEdges() {
		from, to := e.GetFromStep(), e.GetToStep()
		if !known[from] {
			return nil, fmt.Errorf("dag: edge %s.%s -> %s.%s names unknown step %q",
				from, e.GetFromPort(), to, e.GetToPort(), from)
		}
		if !known[to] {
			return nil, fmt.Errorf("dag: edge %s.%s -> %s.%s names unknown step %q",
				from, e.GetFromPort(), to, e.GetToPort(), to)
		}
		pair := [2]string{from, to}
		if seen[pair] {
			continue
		}
		seen[pair] = true
		g.dependents[from] = append(g.dependents[from], to)
		g.indegree[to]++
	}
	for id := range g.dependents {
		sort.Strings(g.dependents[id])
	}

	if path := g.findCycle(); path != nil {
		return nil, fmt.Errorf("dag: pipeline %q has a cycle: %s", p.GetId(), strings.Join(path, " -> "))
	}
	return g, nil
}

// Dependents returns the steps that consume stepID's outputs, sorted. A step
// with no dependents, or one the graph does not know, yields nothing.
func (g *Graph) Dependents(stepID string) []string {
	d := g.dependents[stepID]
	out := make([]string, len(d))
	copy(out, d)
	return out
}

// TopoLevels groups the steps into waves by Kahn's algorithm: every step in a
// level depends only on steps in earlier levels, so a scheduler may dispatch a
// whole level at once. Ids are sorted within a level so the output is stable
// across runs and comparable in tests.
func (g *Graph) TopoLevels() [][]string {
	remaining := make(map[string]int, len(g.indegree))
	for id, n := range g.indegree {
		remaining[id] = n
	}

	var level []string
	for _, id := range g.stepIDs {
		if remaining[id] == 0 {
			level = append(level, id)
		}
	}

	var levels [][]string
	for len(level) > 0 {
		levels = append(levels, level)
		var next []string
		for _, id := range level {
			delete(remaining, id)
			for _, d := range g.dependents[id] {
				remaining[d]--
				if remaining[d] == 0 {
					next = append(next, d)
				}
			}
		}
		sort.Strings(next)
		level = next
	}
	return levels
}

// DFS colours: white is unvisited, grey is on the current path, black is
// finished. Meeting a grey node is a back edge, which is a cycle.
const (
	white = 0
	grey  = 1
	black = 2
)

// findCycle returns one cycle as the path of step ids that closes it, first
// node repeated at the end, or nil when the graph is acyclic. Naming the path
// is the point: "there is a cycle" leaves the operator to find it by hand.
func (g *Graph) findCycle() []string {
	colour := make(map[string]int, len(g.stepIDs))
	var path []string

	var visit func(id string) []string
	visit = func(id string) []string {
		colour[id] = grey
		path = append(path, id)
		for _, d := range g.dependents[id] {
			switch colour[d] {
			case grey:
				// The cycle is the tail of the current path from d onwards.
				for i, p := range path {
					if p == d {
						return append(append([]string{}, path[i:]...), d)
					}
				}
			case white:
				if c := visit(d); c != nil {
					return c
				}
			}
		}
		colour[id] = black
		path = path[:len(path)-1]
		return nil
	}

	for _, id := range g.stepIDs {
		if colour[id] == white {
			if c := visit(id); c != nil {
				return c
			}
		}
	}
	return nil
}
