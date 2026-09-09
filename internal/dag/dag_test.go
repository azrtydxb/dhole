package dag_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/dag"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// blobPort is a minimal well-typed port; these DAG tests care about shape,
// not about types, which typecheck_test.go covers.
func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}}},
	}
}

func step(id string) *dholev1.Step {
	return &dholev1.Step{
		Id:      id,
		Inputs:  []*dholev1.Port{blobPort("in")},
		Outputs: []*dholev1.Port{blobPort("out")},
	}
}

func edge(fromStep, toStep string) *dholev1.Edge {
	return &dholev1.Edge{FromStep: fromStep, FromPort: "out", ToStep: toStep, ToPort: "in"}
}

// TestDAGDerivedFromPortsRunsIndependentStepsConcurrently is the reason the
// DAG exists: b and c share a dependency but not each other, so they must
// land on the same level and be schedulable at the same time. A level list
// that serialised them would still be a valid topological order and would
// still pass a naive ordering assertion — this pins the concurrency.
func TestDAGDerivedFromPortsRunsIndependentStepsConcurrently(t *testing.T) {
	p := &dholev1.Pipeline{
		Id:    "p",
		Steps: []*dholev1.Step{step("a"), step("b"), step("c"), step("d")},
		Edges: []*dholev1.Edge{
			edge("a", "b"),
			edge("a", "c"),
			edge("b", "d"),
			edge("c", "d"),
		},
	}

	g, err := dag.Build(p)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"a"}, {"b", "c"}, {"d"}}, g.TopoLevels())
	require.Equal(t, []string{"b", "c"}, g.Dependents("a"))
	require.Equal(t, []string{"d"}, g.Dependents("b"))
	require.Empty(t, g.Dependents("d"))
}

// TestDAGRejectsCycle requires the error to name the cycle in order: an
// operator staring at a rejected pipeline needs the path, not the fact.
func TestDAGRejectsCycle(t *testing.T) {
	p := &dholev1.Pipeline{
		Id:    "p",
		Steps: []*dholev1.Step{step("a"), step("b")},
		Edges: []*dholev1.Edge{edge("a", "b"), edge("b", "a")},
	}

	g, err := dag.Build(p)
	require.Error(t, err)
	require.Nil(t, g)
	require.Contains(t, err.Error(), "cycle: a -> b -> a")
}

// TestTopoLevelsAreDeterministic pins the sort inside a level. Two roots
// whose dependents arrive out of order would otherwise produce a level in
// discovery order, which makes run plans differ between two builds of the
// same pipeline and makes any downstream assertion on levels flaky.
func TestTopoLevelsAreDeterministic(t *testing.T) {
	p := &dholev1.Pipeline{
		Id:    "p",
		Steps: []*dholev1.Step{step("a"), step("b"), step("c"), step("z")},
		Edges: []*dholev1.Edge{edge("a", "z"), edge("b", "c")},
	}

	g, err := dag.Build(p)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"a", "b"}, {"c", "z"}}, g.TopoLevels())
}
