package trigger

import (
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/azrtydxb/dhole/internal/taint"
)

// The taint marker: the boundary half of ADR 0015.
//
// Data entering from an untrusted trigger is tainted AT THE BOUNDARY, the mark
// propagates through typed ports, and tainted data reaching an effectful step
// or a privileged engine is blocked unless a sanitisation gate clears it. This
// file is the first of those three sentences and nothing more: the trigger is
// the only place that knows a value came off a public endpoint rather than out
// of another step, so the mark has to be applied here or there is nothing
// downstream to propagate.
//
// A mark is a WRAPPER rather than a side-table because a trigger hands the
// scheduler `map[string]*structpb.Value` and nothing else. Anything kept
// beside the value is lost the moment the value is stored, replayed or passed
// through a port — which is exactly when the check needs it. The wrapper is a
// struct with one reserved field:
//
//	{"$dhole.taint": {"source": "git:github:pushes", "value": <the value>}}
//
// Everything that reads a value must therefore read it through
// UntaintedValue; ValidateInputs does.
//
// # Task 51 reconciled this
//
// The implementation moved to `internal/taint`, which owns the rest of ADR
// 0015 — propagation on step completion, the policy check, the sanitisation
// gate. These four names stay because Task 41's triggers and their tests read
// naturally through them, and they now DELEGATE: one implementation of the
// wrapper, so the shape a fired run already carries and the shape the taint
// package looks for cannot drift apart. Nothing here clears a taint; that is
// taint.Gate's alone.
const (
	// TaintField is the reserved struct field a mark lives under. The `$`
	// keeps it out of the way of any JSON Schema a port declares.
	TaintField = taint.Field
)

// MarkTainted wraps v as untrusted data admitted by source, which names the
// trigger that let it in — "git:github:pushes" — so that a refusal downstream
// can say where the value came from.
//
// Marking an already-marked value is a no-op: the mark records the boundary it
// crossed, and it crossed one.
func MarkTainted(v *structpb.Value, source string) *structpb.Value {
	return taint.Mark(v, source)
}

// IsTainted reports whether v carries a taint mark — at its top level or
// anywhere inside it, since a webhook body is a document and the untrusted
// part is rarely the outermost value.
func IsTainted(v *structpb.Value) bool { return taint.IsTainted(v) }

// TaintSource returns the trigger that admitted v, or "" if v is not tainted.
func TaintSource(v *structpb.Value) string { return taint.Source(v) }

// UntaintedValue returns the value a mark carries, or v unchanged if it is not
// marked. It reads THROUGH the mark rather than removing it: nothing here
// clears a taint, which is the sanitisation gate's job (ADR 0015).
func UntaintedValue(v *structpb.Value) *structpb.Value { return taint.Value(v) }
