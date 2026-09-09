package api

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// settableProperties are the step fields set_property may change, named by
// their protobuf field names. A step's name is Rename's business and its id is
// its identity, so neither is here.
var settableProperties = []string{"effect_class", "lease_scope", "plugin_ref"}

// Apply applies one operation to a pipeline and returns the result, the diff,
// and the operation that undoes it.
//
// It is a pure function over a COPY. The pipeline handed in is never touched,
// which is what lets the caller keep holding an immutable revision it read out
// of the store while an edit is computed from it: a revision an edit could
// reach in place would stop being the thing a run pinned.
//
// The inverse is exact, not best-effort. Every operation here is refused
// unless its inverse would restore precisely the pipeline that went in —
// which is why removing a connected step is refused rather than cascading,
// and why an edge that does not exist cannot be removed. An approximate
// inverse would make undo a lie in exactly the cases a user is most likely to
// reach for it.
func Apply(
	p *dholev1.Pipeline, op *dholev1.Operation,
) (*dholev1.Pipeline, *dholev1.Diff, *dholev1.Operation, error) {
	if p == nil {
		return nil, nil, nil, errors.New("apply: no pipeline given")
	}
	if op == nil || op.GetKind() == nil {
		return nil, nil, nil, errors.New("apply: no operation given")
	}

	next, ok := proto.Clone(p).(*dholev1.Pipeline)
	if !ok {
		return nil, nil, nil, errors.New("apply: cloned pipeline is not a pipeline")
	}

	var (
		change  *dholev1.Change
		inverse *dholev1.Operation
		err     error
	)
	switch kind := op.GetKind().(type) {
	case *dholev1.Operation_AddStep:
		change, inverse, err = applyAddStep(next, kind.AddStep)
	case *dholev1.Operation_RemoveStep:
		change, inverse, err = applyRemoveStep(next, kind.RemoveStep)
	case *dholev1.Operation_Connect:
		change, inverse, err = applyConnect(next, kind.Connect)
	case *dholev1.Operation_RemoveEdge:
		change, inverse, err = applyRemoveEdge(next, kind.RemoveEdge)
	case *dholev1.Operation_SetProperty:
		change, inverse, err = applySetProperty(next, kind.SetProperty)
	case *dholev1.Operation_SetStepConfig:
		change, inverse, err = applySetStepConfig(next, kind.SetStepConfig)
	case *dholev1.Operation_Rename:
		change, inverse, err = applyRename(next, kind.Rename)
	default:
		// Unreachable while the switch covers the oneof, and a compile-time
		// reminder to extend it — with its inverse — when a kind is added.
		err = fmt.Errorf("apply: unsupported operation %T", kind)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return next, &dholev1.Diff{Changes: []*dholev1.Change{change}}, inverse, nil
}

// applyAddStep introduces a step. Its inverse is the removal of that step.
func applyAddStep(p *dholev1.Pipeline, op *dholev1.AddStep) (*dholev1.Change, *dholev1.Operation, error) {
	step := op.GetStep()
	if step.GetId() == "" {
		return nil, nil, errors.New("add_step: a step id is required")
	}
	if findStep(p, step.GetId()) != nil {
		return nil, nil, fmt.Errorf("add_step: step %q already exists", step.GetId())
	}
	added, ok := proto.Clone(step).(*dholev1.Step)
	if !ok {
		return nil, nil, errors.New("add_step: cloned step is not a step")
	}
	p.Steps = append(p.Steps, added)

	return &dholev1.Change{
			Kind:    dholev1.ChangeKind_CHANGE_KIND_ADDED,
			StepId:  step.GetId(),
			Summary: fmt.Sprintf("added step %q", step.GetId()),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_RemoveStep{
			RemoveStep: &dholev1.RemoveStep{StepId: step.GetId()},
		}}, nil
}

// applyRemoveStep takes a step out. Its inverse is adding back exactly the
// step that was removed, which is only expressible while the step carries no
// edges — so a connected step is refused rather than cascaded.
func applyRemoveStep(p *dholev1.Pipeline, op *dholev1.RemoveStep) (*dholev1.Change, *dholev1.Operation, error) {
	id := op.GetStepId()
	step := findStep(p, id)
	if step == nil {
		return nil, nil, fmt.Errorf("remove_step: no step %q in this pipeline", id)
	}
	for _, e := range p.GetEdges() {
		if e.GetFromStep() == id || e.GetToStep() == id {
			return nil, nil, fmt.Errorf(
				"remove_step: step %q is still connected by %s; remove its edges first", id, edgeText(e))
		}
	}
	removed, ok := proto.Clone(step).(*dholev1.Step)
	if !ok {
		return nil, nil, errors.New("remove_step: cloned step is not a step")
	}
	p.Steps = slices.DeleteFunc(p.GetSteps(), func(s *dholev1.Step) bool { return s.GetId() == id })

	return &dholev1.Change{
			Kind:    dholev1.ChangeKind_CHANGE_KIND_REMOVED,
			StepId:  id,
			Summary: fmt.Sprintf("removed step %q", id),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_AddStep{
			AddStep: &dholev1.AddStep{Step: removed},
		}}, nil
}

// applyConnect adds an edge. Its inverse is removing that edge.
func applyConnect(p *dholev1.Pipeline, op *dholev1.Connect) (*dholev1.Change, *dholev1.Operation, error) {
	edge := op.GetEdge()
	if err := checkEdgeEndpoints(p, "connect", edge); err != nil {
		return nil, nil, err
	}
	if findEdge(p, edge) != nil {
		return nil, nil, fmt.Errorf("connect: edge %s already exists", edgeText(edge))
	}
	added, ok := proto.Clone(edge).(*dholev1.Edge)
	if !ok {
		return nil, nil, errors.New("connect: cloned edge is not an edge")
	}
	p.Edges = append(p.Edges, added)

	return &dholev1.Change{
			Kind:    dholev1.ChangeKind_CHANGE_KIND_ADDED,
			Edge:    added,
			Summary: fmt.Sprintf("connected %s", edgeText(edge)),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_RemoveEdge{
			RemoveEdge: &dholev1.RemoveEdge{Edge: added},
		}}, nil
}

// applyRemoveEdge deletes an edge. Its inverse is reconnecting it.
//
// An edge that is not there is refused. The alternative — treating it as a
// no-op — would return an inverse that CREATES an edge the pipeline never
// had, so undoing a removal that removed nothing would add something. That is
// the one case where a best-effort inverse is actively wrong, so the
// operation is refused instead.
func applyRemoveEdge(p *dholev1.Pipeline, op *dholev1.RemoveEdge) (*dholev1.Change, *dholev1.Operation, error) {
	edge := op.GetEdge()
	if edge == nil {
		return nil, nil, errors.New("remove_edge: an edge is required")
	}
	existing := findEdge(p, edge)
	if existing == nil {
		return nil, nil, fmt.Errorf("remove_edge: no such edge %s in this pipeline", edgeText(edge))
	}
	removed, ok := proto.Clone(existing).(*dholev1.Edge)
	if !ok {
		return nil, nil, errors.New("remove_edge: cloned edge is not an edge")
	}
	p.Edges = slices.DeleteFunc(p.GetEdges(), func(e *dholev1.Edge) bool { return sameEdge(e, edge) })

	return &dholev1.Change{
			Kind:    dholev1.ChangeKind_CHANGE_KIND_REMOVED,
			Edge:    removed,
			Summary: fmt.Sprintf("disconnected %s", edgeText(removed)),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_Connect{
			Connect: &dholev1.Connect{Edge: removed},
		}}, nil
}

// applySetProperty changes one scalar property. Its inverse sets the property
// back to the text form of the value it held, which for an enum is the
// declared name — including the UNSPECIFIED one, so "it was never set" round
// trips as precisely as any other value.
func applySetProperty(p *dholev1.Pipeline, op *dholev1.SetProperty) (*dholev1.Change, *dholev1.Operation, error) {
	step := findStep(p, op.GetStepId())
	if step == nil {
		return nil, nil, fmt.Errorf("set_property: no step %q in this pipeline", op.GetStepId())
	}

	var old string
	switch op.GetProperty() {
	case "plugin_ref":
		old = step.GetPluginRef()
		step.PluginRef = op.GetValue()
	case "effect_class":
		value, ok := dholev1.EffectClass_value[op.GetValue()]
		if !ok {
			return nil, nil, fmt.Errorf(
				"set_property: %q is not an effect_class; expected one of %s",
				op.GetValue(), enumNames(dholev1.EffectClass_name))
		}
		old = dholev1.EffectClass_name[int32(step.GetEffectClass())]
		step.EffectClass = dholev1.EffectClass(value)
	case "lease_scope":
		value, ok := dholev1.LeaseScope_value[op.GetValue()]
		if !ok {
			return nil, nil, fmt.Errorf(
				"set_property: %q is not a lease_scope; expected one of %s",
				op.GetValue(), enumNames(dholev1.LeaseScope_name))
		}
		old = dholev1.LeaseScope_name[int32(step.GetLeaseScope())]
		step.LeaseScope = dholev1.LeaseScope(value)
	default:
		return nil, nil, fmt.Errorf("set_property: unknown property %q on step %q; settable properties are %s",
			op.GetProperty(), op.GetStepId(), strings.Join(settableProperties, ", "))
	}

	return &dholev1.Change{
			Kind:   dholev1.ChangeKind_CHANGE_KIND_CHANGED,
			StepId: op.GetStepId(),
			Summary: fmt.Sprintf("set %s of step %q from %q to %q",
				op.GetProperty(), op.GetStepId(), old, op.GetValue()),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_SetProperty{
			SetProperty: &dholev1.SetProperty{
				StepId: op.GetStepId(), Property: op.GetProperty(), Value: old,
			},
		}}, nil
}

// applySetStepConfig sets or removes one value a step passes to its plugin.
//
// The inverse is exact in all three directions: setting a key that was absent
// inverts to removing it, overwriting one inverts to restoring the value it
// held, and removing one inverts to setting that value back. Removing a key
// that is not there is refused, exactly as removing an edge that does not
// exist is — the "inverse" of a removal that removed nothing would SET a value
// the step never had, so undoing it would add something.
func applySetStepConfig(p *dholev1.Pipeline, op *dholev1.SetStepConfig) (*dholev1.Change, *dholev1.Operation, error) {
	step := findStep(p, op.GetStepId())
	if step == nil {
		return nil, nil, fmt.Errorf("set_step_config: no step %q in this pipeline", op.GetStepId())
	}
	key := op.GetKey()
	if key == "" {
		return nil, nil, errors.New("set_step_config: a key is required")
	}
	old, had := step.GetConfig()[key]

	inverse := &dholev1.SetStepConfig{StepId: op.GetStepId(), Key: key}
	if had {
		inverse.Value = old
	} else {
		inverse.Remove = true
	}

	var (
		kind    dholev1.ChangeKind
		summary string
	)
	switch {
	case op.GetRemove():
		if !had {
			return nil, nil, fmt.Errorf(
				"set_step_config: step %q has no config value %q to remove", op.GetStepId(), key)
		}
		delete(step.Config, key)
		// An empty map and no map at all are the same definition to a reader
		// and DIFFERENT bytes to the content hash, so the last key removed
		// leaves the field unset — which is what the step that never had one
		// encodes as, and therefore what the inverse must land back on.
		if len(step.GetConfig()) == 0 {
			step.Config = nil
		}
		kind = dholev1.ChangeKind_CHANGE_KIND_REMOVED
		summary = fmt.Sprintf("removed config %q of step %q, which was %q", key, op.GetStepId(), old)
	default:
		if step.GetConfig() == nil {
			step.Config = map[string]string{}
		}
		step.Config[key] = op.GetValue()
		kind = dholev1.ChangeKind_CHANGE_KIND_CHANGED
		if !had {
			kind = dholev1.ChangeKind_CHANGE_KIND_ADDED
		}
		summary = fmt.Sprintf("set config %q of step %q from %q to %q",
			key, op.GetStepId(), old, op.GetValue())
	}

	return &dholev1.Change{Kind: kind, StepId: op.GetStepId(), Summary: summary},
		&dholev1.Operation{Kind: &dholev1.Operation_SetStepConfig{SetStepConfig: inverse}}, nil
}

// applyRename changes a step's display name. Its inverse restores the old one,
// the empty name included: a name is a label, the id is the identity.
func applyRename(p *dholev1.Pipeline, op *dholev1.Rename) (*dholev1.Change, *dholev1.Operation, error) {
	step := findStep(p, op.GetStepId())
	if step == nil {
		return nil, nil, fmt.Errorf("rename: no step %q in this pipeline", op.GetStepId())
	}
	old := step.GetName()
	step.Name = op.GetName()

	return &dholev1.Change{
			Kind:    dholev1.ChangeKind_CHANGE_KIND_CHANGED,
			StepId:  op.GetStepId(),
			Summary: fmt.Sprintf("renamed step %q from %q to %q", op.GetStepId(), old, op.GetName()),
		},
		&dholev1.Operation{Kind: &dholev1.Operation_Rename{
			Rename: &dholev1.Rename{StepId: op.GetStepId(), Name: old},
		}}, nil
}

// checkEdgeEndpoints refuses an edge naming a step or a port that is not
// there. Silently ignoring one turns a typo into a pipeline nobody meant.
func checkEdgeEndpoints(p *dholev1.Pipeline, op string, edge *dholev1.Edge) error {
	if edge == nil {
		return fmt.Errorf("%s: an edge is required", op)
	}
	from := findStep(p, edge.GetFromStep())
	if from == nil {
		return fmt.Errorf("%s: no step %q in this pipeline", op, edge.GetFromStep())
	}
	if findPort(from.GetOutputs(), edge.GetFromPort()) == nil {
		return fmt.Errorf("%s: step %q has no output port %q", op, edge.GetFromStep(), edge.GetFromPort())
	}
	to := findStep(p, edge.GetToStep())
	if to == nil {
		return fmt.Errorf("%s: no step %q in this pipeline", op, edge.GetToStep())
	}
	if findPort(to.GetInputs(), edge.GetToPort()) == nil {
		return fmt.Errorf("%s: step %q has no input port %q", op, edge.GetToStep(), edge.GetToPort())
	}
	return nil
}

// findStep returns the step with that id, or nil.
func findStep(p *dholev1.Pipeline, id string) *dholev1.Step {
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	return nil
}

// findPort returns the port with that name, or nil.
func findPort(ports []*dholev1.Port, name string) *dholev1.Port {
	for _, port := range ports {
		if port.GetName() == name {
			return port
		}
	}
	return nil
}

// findEdge returns the stored edge equal to want, or nil.
func findEdge(p *dholev1.Pipeline, want *dholev1.Edge) *dholev1.Edge {
	for _, e := range p.GetEdges() {
		if sameEdge(e, want) {
			return e
		}
	}
	return nil
}

// sameEdge compares the four fields that are an edge's identity.
func sameEdge(a, b *dholev1.Edge) bool {
	return a.GetFromStep() == b.GetFromStep() && a.GetFromPort() == b.GetFromPort() &&
		a.GetToStep() == b.GetToStep() && a.GetToPort() == b.GetToPort()
}

// edgeText names an edge the way a diff and an error both show it.
func edgeText(e *dholev1.Edge) string {
	return fmt.Sprintf("%q.%s -> %q.%s", e.GetFromStep(), e.GetFromPort(), e.GetToStep(), e.GetToPort())
}

// enumNames lists an enum's declared names, sorted, for an error a human can
// act on without opening the schema.
func enumNames(names map[int32]string) string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, n)
	}
	slices.Sort(out)
	return strings.Join(out, ", ")
}
