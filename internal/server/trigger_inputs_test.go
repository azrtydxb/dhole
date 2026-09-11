package server

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// inputPipeline is one free structured port, which is what a pipeline's
// declared inputs ARE: a port no edge feeds (ADR 0007).
func inputPipeline(schema string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: "bound",
		Steps: []*dholev1.Step{{
			Id: "work",
			Inputs: []*dholev1.Port{{
				Name: "event",
				Type: &dholev1.PortType{Kind: &dholev1.PortType_Structured{
					Structured: &dholev1.StructType{SchemaId: "dhole:test/event", Schema: schema},
				}},
			}},
		}},
	}
}

// TestATriggerThatBoundInputsAndSuppliedNoneIsRefusedRatherThanStartingARun is
// the silent case. ValidateInputs checks the values that ARE there and says
// nothing about the ones that are not, so a binding that evaluated to nothing
// would have passed every check on the fire path and started a run with bare
// ports — which fails inside a step, long after the event that should have
// filled them is gone.
func TestATriggerThatBoundInputsAndSuppliedNoneIsRefusedRatherThanStartingARun(t *testing.T) {
	spec := TriggerSpec{ID: "hook", Kind: "http", PipelineID: "bound",
		InputMapping: map[string]string{"event": "ref"}}

	err := checkTriggerInputs(spec, inputPipeline(`{"type":"string"}`), nil)
	if err == nil {
		t.Fatal("a trigger that bound an input and supplied none started a run anyway")
	}
	// The reason has to name the trigger: it is logged against the trigger's
	// id, and that is where somebody asking why their webhook does nothing
	// is already looking.
	if !strings.Contains(err.Error(), "hook") {
		t.Errorf("the refusal does not say which trigger: %s", err)
	}
}

// TestATriggerThatBindsNothingIsNotRefusedForSupplyingNothing keeps the
// refusal to the mistake it is about. A trigger with no binding at all is a
// pipeline started on a boundary and nothing else, which is legitimate — and a
// check that could not tell the two apart would break every such schedule.
func TestATriggerThatBindsNothingIsNotRefusedForSupplyingNothing(t *testing.T) {
	spec := TriggerSpec{ID: "nightly", Kind: "schedule", PipelineID: "bound"}
	if err := checkTriggerInputs(spec, inputPipeline(`{"type":"string"}`), nil); err != nil {
		t.Fatalf("a trigger that binds nothing was refused for supplying nothing: %v", err)
	}
}

// TestATriggerValueThePortsSchemaRefusesNeverReachesARun is the wrong-type
// half, at the seam itself: the check is against the pipeline the run will
// pin, not against the one the trigger was configured with a month ago.
func TestATriggerValueThePortsSchemaRefusesNeverReachesARun(t *testing.T) {
	spec := TriggerSpec{ID: "every-second", Kind: "schedule", PipelineID: "bound",
		InputMapping: map[string]string{"event": "scheduled_for"}}
	inputs := map[string]*structpb.Value{"event": structpb.NewStringValue("2026-01-01T00:00:00Z")}

	err := checkTriggerInputs(spec, inputPipeline(`{"type":"object"}`), inputs)
	if err == nil {
		t.Fatal("a string reached a port declaring an object")
	}
	if !strings.Contains(err.Error(), "event") {
		t.Errorf("the refusal does not say which input: %s", err)
	}
}
