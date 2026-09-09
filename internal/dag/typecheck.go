package dag

import (
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Diagnostic is one problem found in a pipeline, addressed to the port it is
// about so an editor can put a marker on that port. Line and Col locate the
// problem in the authored source when the caller knows the mapping from step
// and port to source position; they are zero for a pipeline that arrived
// already decoded, which carries no positions.
type Diagnostic struct {
	StepID   string
	PortName string
	Message  string
	Line     int
	Col      int
}

// TypeCheck reports every edge that cannot carry data, in edge order: a
// dangling reference to a step or a port, or two ends whose types do not
// match. It returns findings rather than the first error because the editor
// draws all of them at once, and it never stops early for the same reason.
//
// The type system is the PortType oneof itself: a blob and a structured value
// are unrelated, and two structured values are compatible only when they name
// the same schema id. Comparing schema ids rather than schema bodies is what
// keeps the check cheap enough to run on every keystroke.
func TypeCheck(p *dholev1.Pipeline) []Diagnostic {
	if p == nil {
		return nil
	}

	steps := make(map[string]*dholev1.Step, len(p.GetSteps()))
	for _, s := range p.GetSteps() {
		steps[s.GetId()] = s
	}

	var diags []Diagnostic
	for _, e := range p.GetEdges() {
		from, ok := steps[e.GetFromStep()]
		if !ok {
			diags = append(diags, Diagnostic{
				StepID:   e.GetFromStep(),
				PortName: e.GetFromPort(),
				Message: fmt.Sprintf("edge source step %q is not defined in pipeline %q",
					e.GetFromStep(), p.GetId()),
			})
			continue
		}
		to, ok := steps[e.GetToStep()]
		if !ok {
			diags = append(diags, Diagnostic{
				StepID:   e.GetToStep(),
				PortName: e.GetToPort(),
				Message: fmt.Sprintf("edge target step %q is not defined in pipeline %q",
					e.GetToStep(), p.GetId()),
			})
			continue
		}

		out := findPort(from.GetOutputs(), e.GetFromPort())
		if out == nil {
			diags = append(diags, Diagnostic{
				StepID:   from.GetId(),
				PortName: e.GetFromPort(),
				Message: fmt.Sprintf("step %q has no output port %s.%s",
					from.GetId(), from.GetId(), e.GetFromPort()),
			})
			continue
		}
		in := findPort(to.GetInputs(), e.GetToPort())
		if in == nil {
			diags = append(diags, Diagnostic{
				StepID:   to.GetId(),
				PortName: e.GetToPort(),
				Message: fmt.Sprintf("step %q has no input port %s.%s",
					to.GetId(), to.GetId(), e.GetToPort()),
			})
			continue
		}

		if why := incompatible(out.GetType(), in.GetType()); why != "" {
			// The diagnostic is attached to the input port: that is the end
			// the author usually has to change, and it keeps one marker per
			// edge even when an output feeds several inputs.
			diags = append(diags, Diagnostic{
				StepID:   to.GetId(),
				PortName: in.GetName(),
				Message: fmt.Sprintf("cannot connect %s.%s to %s.%s: %s",
					from.GetId(), out.GetName(), to.GetId(), in.GetName(), why),
			})
		}
	}
	return diags
}

// untyped names the state of a port whose oneof arm was never set.
const untyped = "an untyped port"

func findPort(ports []*dholev1.Port, name string) *dholev1.Port {
	for _, p := range ports {
		if p.GetName() == name {
			return p
		}
	}
	return nil
}

// incompatible returns why the two port types cannot be connected, or the
// empty string when they can.
func incompatible(from, to *dholev1.PortType) string {
	fromKind, toKind := describe(from), describe(to)
	if fromKind != toKind {
		return fmt.Sprintf("%s is not %s", fromKind, toKind)
	}

	fs, fromStructured := from.GetKind().(*dholev1.PortType_Structured)
	ts, toStructured := to.GetKind().(*dholev1.PortType_Structured)
	switch {
	case fromStructured && toStructured:
		if fs.Structured.GetSchemaId() != ts.Structured.GetSchemaId() {
			return fmt.Sprintf("schema %q is not schema %q",
				fs.Structured.GetSchemaId(), ts.Structured.GetSchemaId())
		}
		return ""
	case fromKind == untyped:
		// Equal kinds, but an unfinished port has no type to agree on.
		return "both ports are untyped"
	default:
		// Both blobs. Blobs are opaque; the media type is advisory and is
		// deliberately not type-checked.
		return ""
	}
}

// describe names a port type's oneof arm for a message, including the case of
// a port whose type was never set.
func describe(t *dholev1.PortType) string {
	switch t.GetKind().(type) {
	case *dholev1.PortType_Blob:
		return "blob"
	case *dholev1.PortType_Structured:
		return "structured"
	default:
		return untyped
	}
}
