package dag_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/dag"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

func structuredPort(name, schemaID string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{SchemaId: schemaID}},
		},
	}
}

// TestValidateRejectsIncompatiblePortTypes is the check that makes a bad edge
// a save-time error rather than a run-time failure: a blob never carries a
// structured value, so connecting one to the other cannot be made to work at
// dispatch. The diagnostic must name both ends — an operator fixing an edge
// needs to know which two ports to look at.
func TestValidateRejectsIncompatiblePortTypes(t *testing.T) {
	p := &dholev1.Pipeline{
		Id: "p",
		Steps: []*dholev1.Step{
			{Id: "a", Outputs: []*dholev1.Port{blobPort("out")}},
			{Id: "b", Inputs: []*dholev1.Port{structuredPort("in", "https://example.test/thing.json")}},
		},
		Edges: []*dholev1.Edge{edge("a", "b")},
	}

	diags := dag.TypeCheck(p)
	require.Len(t, diags, 1)
	require.Contains(t, diags[0].Message, "a.out")
	require.Contains(t, diags[0].Message, "b.in")
	require.Equal(t, "b", diags[0].StepID)
	require.Equal(t, "in", diags[0].PortName)
}

// TestTypeCheckAcceptsMatchingStructuredSchemas pins that compatibility is
// decided by the schema id, not merely by both ends being structured.
func TestTypeCheckAcceptsMatchingStructuredSchemas(t *testing.T) {
	const id = "https://example.test/thing.json"
	p := &dholev1.Pipeline{
		Id: "p",
		Steps: []*dholev1.Step{
			{Id: "a", Outputs: []*dholev1.Port{structuredPort("out", id)}},
			{Id: "b", Inputs: []*dholev1.Port{structuredPort("in", id)}},
		},
		Edges: []*dholev1.Edge{edge("a", "b")},
	}
	require.Empty(t, dag.TypeCheck(p))

	p.Steps[1].Inputs = []*dholev1.Port{structuredPort("in", "https://example.test/other.json")}
	diags := dag.TypeCheck(p)
	require.Len(t, diags, 1)
	require.Contains(t, diags[0].Message, "https://example.test/other.json")
}

// TestTypeCheckReportsMissingStepsAndPorts: an edge that dangles is not a
// type error but it is still unrunnable, and it is reported the same way so
// the editor has one list to draw.
func TestTypeCheckReportsMissingStepsAndPorts(t *testing.T) {
	p := &dholev1.Pipeline{
		Id: "p",
		Steps: []*dholev1.Step{
			{Id: "a", Outputs: []*dholev1.Port{blobPort("out")}},
			{Id: "b", Inputs: []*dholev1.Port{blobPort("in")}},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "ghost", FromPort: "out", ToStep: "b", ToPort: "in"},
			{FromStep: "a", FromPort: "nope", ToStep: "b", ToPort: "in"},
			{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "nope"},
		},
	}

	diags := dag.TypeCheck(p)
	require.Len(t, diags, 3)
	require.Contains(t, diags[0].Message, "ghost")
	require.Contains(t, diags[1].Message, "a.nope")
	require.Contains(t, diags[2].Message, "b.nope")
	require.Equal(t, "b", diags[2].StepID)
	require.Equal(t, "nope", diags[2].PortName)
}

// TestTypeCheckRejectsUntypedPorts: a port whose type was never set is not
// "compatible with anything", it is unfinished. Two untyped ports at either
// end of an edge must still be reported, or a half-authored pipeline saves
// clean and fails at dispatch instead.
func TestTypeCheckRejectsUntypedPorts(t *testing.T) {
	p := &dholev1.Pipeline{
		Id: "p",
		Steps: []*dholev1.Step{
			{Id: "a", Outputs: []*dholev1.Port{{Name: "out"}}},
			{Id: "b", Inputs: []*dholev1.Port{{Name: "in"}}},
		},
		Edges: []*dholev1.Edge{edge("a", "b")},
	}

	diags := dag.TypeCheck(p)
	require.Len(t, diags, 1)
	require.Contains(t, diags[0].Message, "untyped")
}
