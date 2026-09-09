package taint

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// refTaintField is the field number a mark occupies on an OutputRef.
//
// An OutputRef has no room for metadata and its schema is a public contract
// that cannot be changed under a concurrent task, so the mark rides as a
// protobuf UNKNOWN field: a length-delimited source string at a number the
// schema does not use. That is not a trick, it is the one place in the
// encoding that survives everything the mark has to survive — proto.Marshal
// into the cache, a gRPC hop to an engine, a row in the run log and back —
// because unknown fields are preserved verbatim by every protobuf runtime.
// Keeping it beside the ref in a Go map would lose it at the first of those.
//
// The number is far above anything `dhole.v1` will additively allocate and
// below protobuf's own reserved 19000-19999 range; `engine.proto` should
// reserve it when OutputRef next changes.
const refTaintField protowire.Number = 15015

// MarkRef marks o as carrying untrusted data admitted by source and returns
// it, so a ref can be marked where it is constructed. Marking twice with the
// same source is a no-op; a second, different source is added, because an
// output derived from two boundaries came from both.
func MarkRef(o *dholev1.OutputRef, source string) *dholev1.OutputRef {
	if o == nil {
		return nil
	}
	for _, existing := range RefSources(o) {
		if existing == source {
			return o
		}
	}
	raw := protowire.AppendTag(nil, refTaintField, protowire.BytesType)
	raw = protowire.AppendString(raw, source)
	m := o.ProtoReflect()
	m.SetUnknown(append(append(protoreflect.RawFields(nil), m.GetUnknown()...), raw...))
	return o
}

// RefTainted reports whether o carries a mark.
func RefTainted(o *dholev1.OutputRef) bool { return len(RefSources(o)) > 0 }

// RefSources returns the triggers whose data o is derived from, in the order
// they were applied.
func RefSources(o *dholev1.OutputRef) []string {
	if o == nil {
		return nil
	}
	var sources []string
	rest := []byte(o.ProtoReflect().GetUnknown())
	for len(rest) > 0 {
		number, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return sources
		}
		rest = rest[n:]
		if number == refTaintField && typ == protowire.BytesType {
			s, n := protowire.ConsumeString(rest)
			if n < 0 {
				return sources
			}
			rest = rest[n:]
			sources = append(sources, s)
			continue
		}
		n = protowire.ConsumeFieldValue(number, typ, rest)
		if n < 0 {
			return sources
		}
		rest = rest[n:]
	}
	return sources
}

// Propagate carries the marks on a step's inputs onto its outputs. The
// scheduler calls it on every step completion, on whatever the engine
// reported.
//
// It is CONSERVATIVE — any tainted input taints every output — and it only
// ever ADDS. Both halves are load-bearing. Requiring agreement between inputs
// would call the ordinary case clean: a step reading one trusted config file
// and one webhook body produces a result derived from the webhook body.
// Removing a mark here would make every step a sanitisation gate, since an
// engine could report outputs it had simply declared clean; clearing is
// Gate.Sanitise's alone, and an engine's silence about taint is not a
// clearance.
func Propagate(in []*dholev1.OutputRef, out []*dholev1.OutputRef) {
	var sources []string
	seen := map[string]struct{}{}
	for _, ref := range in {
		for _, s := range RefSources(ref) {
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			sources = append(sources, s)
		}
	}
	if len(sources) == 0 {
		return
	}
	for _, o := range out {
		for _, s := range sources {
			MarkRef(o, s)
		}
	}
}

// Dispatch is what a taint check is asked about: the step about to run, the
// engine it would run on, and the data it would be given — as structured
// inputs, as output refs from upstream steps, or both.
type Dispatch struct {
	// TenantID scopes the decision and its audit row. There is no unscoped
	// taint check, and an empty one is refused rather than read as a
	// wildcard.
	TenantID string
	// Tier is the trust tier the taint policy is keyed on. Empty means the
	// strictest one this package knows, taint.Tier.
	Tier string
	// Subject names the step, and is what a denial reports.
	Subject string
	// EffectClass is the class the step operates under.
	EffectClass dholev1.EffectClass
	// EngineCapabilities are the capabilities the candidate engine advertises.
	EngineCapabilities []dholev1.Capability
	// Inputs are the step's structured inputs, as a trigger supplied them.
	Inputs map[string]*structpb.Value
	// InputRefs are the outputs of upstream steps feeding this one.
	InputRefs []*dholev1.OutputRef
}

// The decision itself is Checker.Check, in policy.go: whether tainted data may
// reach a dispatch is a trust-tier decision, and ADR 0012 puts every one of
// those in internal/policy, evaluated from CEL and audited. Four rules in Go
// here were four rules an operator could not see, audit or change.
