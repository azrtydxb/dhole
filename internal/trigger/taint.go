package trigger

import (
	"google.golang.org/protobuf/types/known/structpb"
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
// # What Task 51 has to reconcile
//
// Task 51 owns `internal/taint` and the rest of ADR 0015 — propagation on step
// completion, the policy check, the sanitisation gate. It should move these
// four functions there under its own names (`taint.Mark`, `taint.IsTainted`)
// and leave this file delegating, or delete this file and update the three
// triggers. What it must NOT do is invent a second representation: the
// wrapper above is what a fired run's inputs already carry, so a taint package
// that looks for a different shape will read every webhook payload as clean.
const (
	// TaintField is the reserved struct field a mark lives under. The `$`
	// keeps it out of the way of any JSON Schema a port declares.
	TaintField = "$dhole.taint"

	taintSourceField = "source"
	taintValueField  = "value"
)

// MarkTainted wraps v as untrusted data admitted by source, which names the
// trigger that let it in — "git:github:pushes" — so that a refusal downstream
// can say where the value came from.
//
// Marking an already-marked value is a no-op: the mark records the boundary it
// crossed, and it crossed one.
func MarkTainted(v *structpb.Value, source string) *structpb.Value {
	if v == nil {
		v = structpb.NewNullValue()
	}
	if IsTainted(v) {
		return v
	}
	return structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		TaintField: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			taintSourceField: structpb.NewStringValue(source),
			taintValueField:  v,
		}}),
	}})
}

// IsTainted reports whether v carries a taint mark.
func IsTainted(v *structpb.Value) bool { return taintMark(v) != nil }

// TaintSource returns the trigger that admitted v, or "" if v is not tainted.
func TaintSource(v *structpb.Value) string {
	mark := taintMark(v)
	if mark == nil {
		return ""
	}
	return mark.GetFields()[taintSourceField].GetStringValue()
}

// UntaintedValue returns the value a mark carries, or v unchanged if it is not
// marked. It reads THROUGH the mark rather than removing it: nothing here
// clears a taint, which is the sanitisation gate's job (ADR 0015).
func UntaintedValue(v *structpb.Value) *structpb.Value {
	mark := taintMark(v)
	if mark == nil {
		return v
	}
	return mark.GetFields()[taintValueField]
}

// taintMark returns the mark's inner struct, or nil.
//
// The shape is checked exactly — one field, named, carrying a source and a
// value — so that a payload which happens to contain a similarly named key
// cannot claim to be tainted, and, more importantly, so that an attacker
// cannot forge the marker's ABSENCE by sending a struct that looks like one.
func taintMark(v *structpb.Value) *structpb.Struct {
	fields := v.GetStructValue().GetFields()
	if len(fields) != 1 {
		return nil
	}
	mark := fields[TaintField].GetStructValue()
	if mark == nil {
		return nil
	}
	if _, ok := mark.GetFields()[taintValueField]; !ok {
		return nil
	}
	if _, ok := mark.GetFields()[taintSourceField]; !ok {
		return nil
	}
	return mark
}
