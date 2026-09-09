// Package taint implements ADR 0015's second half: data entering from an
// untrusted trigger is marked AT THE BOUNDARY, the mark propagates through
// typed ports, and tainted data reaching an effectful step or a privileged
// engine is refused unless an explicit sanitisation gate clears it.
//
// # The representation is not negotiable
//
// A mark on a structured value is a WRAPPER, not metadata beside the value:
//
//	{"$dhole.taint": {"source": "git:github:pushes", "value": <the value>}}
//
// A trigger hands the scheduler `map[string]*structpb.Value` and nothing else,
// so anything kept alongside a value is lost the moment that value is stored
// and replayed — which is exactly when the check needs it. Task 41's triggers
// have been writing this shape since before this package existed, and every
// run already fired carries it; a package that looked for a different shape
// would read all of them as clean, which is the one failure taint tracking
// exists to prevent. A taint that can be lost is worse than no taint at all,
// because the system then reports as trusted data that never was.
//
// `internal/trigger/taint.go` now delegates to this package, so there is one
// implementation of the shape and no second copy to drift from it.
package taint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/azrtydxb/dhole/internal/runstore"
)

const (
	// Field is the reserved struct field a mark lives under. The `$` keeps it
	// out of the way of any JSON Schema a port declares.
	Field = "$dhole.taint"

	sourceField = "source"
	valueField  = "value"
)

// EventSanitised is the run-log event a gate writes when it clears a mark.
// The value is stored verbatim and is therefore a persistence contract.
const EventSanitised runstore.EventType = "TAINT_SANITISED"

// Mark wraps v as untrusted data admitted by source, which names the trigger
// that let it in — "git:github:pushes" — so a refusal downstream can say where
// the value came from.
//
// Marking an already-marked value is a NO-OP and the FIRST source survives.
// That is a deliberate choice over nesting a second wrapper: the first
// boundary is the one nearest the attacker and the one an incident asks about,
// a second crossing cannot make untrusted data more untrusted, and an onion of
// wrappers is a shape nothing downstream — including the schema check at the
// port — can read.
func Mark(v *structpb.Value, source string) *structpb.Value {
	if v == nil {
		v = structpb.NewNullValue()
	}
	// Deliberately the TOP-LEVEL mark, not IsTainted: a payload that merely
	// contains a tainted field has not itself crossed a boundary, and must
	// still be markable when it crosses one.
	if mark(v) != nil {
		return v
	}
	return structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		Field: structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			sourceField: structpb.NewStringValue(source),
			valueField:  v,
		}}),
	}})
}

// IsTainted reports whether v carries a taint mark anywhere within it.
//
// The search is RECURSIVE because a webhook body is a document: the untrusted
// part is a commit message three levels down, not the top-level value. A
// check that looked only at the top level would read `{"commits": [<marked>]}`
// as clean, which is the obvious wrong implementation.
func IsTainted(v *structpb.Value) bool {
	found := false
	walk(v, func(string) bool { found = true; return false })
	return found
}

// Source returns the trigger that admitted v, or "" if v carries no mark at
// its top level. Use Sources for a value whose taint may be nested.
func Source(v *structpb.Value) string {
	m := mark(v)
	if m == nil {
		return ""
	}
	return m.GetFields()[sourceField].GetStringValue()
}

// Sources returns every distinct trigger whose data is anywhere inside v,
// sorted, so a refusal can name all of them and read the same way twice.
func Sources(v *structpb.Value) []string {
	seen := map[string]struct{}{}
	walk(v, func(s string) bool { seen[s] = struct{}{}; return true })
	return sorted(seen)
}

// Value returns the value a mark carries, or v unchanged if it carries none.
// It reads THROUGH the mark rather than removing it: nothing here clears a
// taint, which is Gate's job alone.
func Value(v *structpb.Value) *structpb.Value {
	m := mark(v)
	if m == nil {
		return v
	}
	return m.GetFields()[valueField]
}

// Untainted returns v with every mark inside it removed. It is unexported
// behaviour in effect: only Gate.Sanitise calls it, because removing a mark
// anywhere else is laundering.
func untainted(v *structpb.Value) *structpb.Value {
	if m := mark(v); m != nil {
		return untainted(m.GetFields()[valueField])
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_StructValue:
		fields := make(map[string]*structpb.Value, len(k.StructValue.GetFields()))
		for name, f := range k.StructValue.GetFields() {
			fields[name] = untainted(f)
		}
		return structpb.NewStructValue(&structpb.Struct{Fields: fields})
	case *structpb.Value_ListValue:
		values := make([]*structpb.Value, 0, len(k.ListValue.GetValues()))
		for _, item := range k.ListValue.GetValues() {
			values = append(values, untainted(item))
		}
		return structpb.NewListValue(&structpb.ListValue{Values: values})
	default:
		return v
	}
}

// walk calls visit for every mark inside v, deepest included, stopping early
// when visit returns false.
func walk(v *structpb.Value, visit func(source string) bool) bool {
	if m := mark(v); m != nil {
		if !visit(m.GetFields()[sourceField].GetStringValue()) {
			return false
		}
		return walk(m.GetFields()[valueField], visit)
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_StructValue:
		for _, f := range k.StructValue.GetFields() {
			if !walk(f, visit) {
				return false
			}
		}
	case *structpb.Value_ListValue:
		for _, item := range k.ListValue.GetValues() {
			if !walk(item, visit) {
				return false
			}
		}
	}
	return true
}

// mark returns the mark's inner struct at v's TOP level, or nil.
//
// The shape is checked exactly — one field, named, carrying a source and a
// value — so that a payload which happens to contain a similarly named key
// cannot claim to be tainted, and, more importantly, so that an attacker
// cannot forge the marker's ABSENCE by sending a struct that looks like one.
func mark(v *structpb.Value) *structpb.Struct {
	fields := v.GetStructValue().GetFields()
	if len(fields) != 1 {
		return nil
	}
	m := fields[Field].GetStructValue()
	if m == nil {
		return nil
	}
	if _, ok := m.GetFields()[valueField]; !ok {
		return nil
	}
	if _, ok := m.GetFields()[sourceField]; !ok {
		return nil
	}
	return m
}

// Gate is the sanitisation step type: the ONLY thing in this system that
// clears a mark.
//
// A gate declares the fields it is allowed to clear, which is a configuration
// question — "this node exists to validate the ref" — and clears them only
// when Sanitise is called, which is an act somebody is answerable for. A gate
// sitting in a graph has cleared nothing; if presence were enough, drawing the
// node next to the deploy step would launder the payload.
type Gate struct {
	// StepID names the gate in the pipeline, and is what the audit record
	// points at.
	StepID string
	// Fields are the input names this gate may clear. Anything else is
	// refused, so a gate written for the ref cannot quietly clear the body.
	Fields []string
}

// Sanitisation is one invocation of a gate: whose authority, which fields, over
// which inputs.
type Sanitisation struct {
	TenantID  string
	RunID     string
	Principal string
	Fields    []string
	Inputs    map[string]*structpb.Value
}

// Record is what the gate wrote to the run log: who cleared what, and which
// taint they cleared. Sources is kept because "was that clearance reasonable"
// is unanswerable without knowing what the data was.
type Record struct {
	Gate      string   `json:"gate"`
	Principal string   `json:"principal"`
	Fields    []string `json:"fields"`
	Sources   []string `json:"sources"`
}

// Appender is the slice of the run store a gate needs. runstore.Store and
// runstore.Tx both satisfy it, so a gate can write its record inside the same
// transaction as the step event that ran it.
type Appender interface {
	Append(ctx context.Context, tenantID string, e runstore.Event) error
}

// ErrNoRecord is returned when a gate is asked to clear a taint with nowhere
// to record it. Clearing is refused rather than performed silently: an
// unrecorded clearance is indistinguishable afterwards from a taint that was
// never applied.
var ErrNoRecord = errors.New("taint: a gate needs a run log to record what it cleared")

// Sanitise clears the marks on exactly the named fields, on the named
// principal's authority, records what it did in the run log, and returns a NEW
// input map. The caller's map is untouched: clearing must not reach back into
// what a parallel branch is still reading, or the record of what arrived.
//
// Every refusal below leaves the mark exactly where it was.
func (g Gate) Sanitise(
	ctx context.Context, log Appender, s Sanitisation,
) (map[string]*structpb.Value, Record, error) {
	if log == nil {
		return nil, Record{}, ErrNoRecord
	}
	if s.TenantID == "" {
		return nil, Record{}, runstore.ErrTenantRequired
	}
	if s.RunID == "" {
		return nil, Record{}, errors.New("taint: a sanitisation belongs to a run")
	}
	if s.Principal == "" {
		return nil, Record{}, errors.New(
			"taint: a gate clears a taint on somebody's authority; no principal was named")
	}
	if len(s.Fields) == 0 {
		return nil, Record{}, fmt.Errorf(
			"taint: gate %q was asked to clear nothing; sanitisation names the fields it clears",
			g.StepID)
	}

	declared := map[string]struct{}{}
	for _, f := range g.Fields {
		declared[f] = struct{}{}
	}

	cleared := make(map[string]*structpb.Value, len(s.Inputs))
	for name, v := range s.Inputs {
		cleared[name] = v
	}

	sources := map[string]struct{}{}
	fields := append([]string(nil), s.Fields...)
	sort.Strings(fields)
	for _, name := range fields {
		if _, ok := declared[name]; !ok {
			return nil, Record{}, fmt.Errorf(
				"taint: gate %q does not declare input %q; a gate clears only what it was written for",
				g.StepID, name)
		}
		v, ok := s.Inputs[name]
		if !ok {
			return nil, Record{}, fmt.Errorf(
				"taint: gate %q was asked to clear input %q, which was not supplied", g.StepID, name)
		}
		if !IsTainted(v) {
			return nil, Record{}, fmt.Errorf(
				"taint: gate %q was asked to clear input %q, which carries no taint", g.StepID, name)
		}
		for _, src := range Sources(v) {
			sources[src] = struct{}{}
		}
		cleared[name] = untainted(v)
	}

	record := Record{
		Gate:      g.StepID,
		Principal: s.Principal,
		Fields:    fields,
		Sources:   sorted(sources),
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, Record{}, fmt.Errorf("taint: encode sanitisation record: %w", err)
	}
	// Sequence 0: the log allocates the position itself.
	if err := log.Append(ctx, s.TenantID, runstore.Event{
		RunID:   s.RunID,
		StepID:  g.StepID,
		Type:    EventSanitised,
		Payload: payload,
		At:      time.Now().UTC(),
	}); err != nil {
		return nil, Record{}, fmt.Errorf("taint: record sanitisation: %w", err)
	}
	return cleared, record, nil
}

// MarshalRecord encodes a sanitisation record.
func MarshalRecord(r Record) ([]byte, error) { return json.Marshal(r) }

// UnmarshalRecord decodes a sanitisation record from a run event's payload.
func UnmarshalRecord(b []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, fmt.Errorf("taint: decode %s payload: %w", EventSanitised, err)
	}
	return r, nil
}

func sorted(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
