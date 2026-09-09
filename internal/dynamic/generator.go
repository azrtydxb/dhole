// Package dynamic is the step type that decides at RUNTIME what work there
// is: a generator emits a pipeline fragment — a matrix from an API, one step
// per file it found — and that fragment is spliced into the run.
//
// Four decisions here carry the weight.
//
// THE REALISED FRAGMENT IS RECORDED IN THE RUN LOG. This is not an
// optimisation, it is ADR 0003. A run is replayed from its event log; if the
// fragment is not in that log, a replay re-runs the generator, and a generator
// that lists a directory or asks an API answers differently the second time.
// The replayed run would then be a DIFFERENT run from the one that happened,
// which is the one thing an event-sourced run may never be. So Realise reads
// the log first and only asks the generator when the log has nothing to say.
//
// THE AUTHORED INTERFACE IS NOT NEGOTIABLE. The fragment is grafted BELOW the
// generator: its entry steps consume the generator's declared output ports,
// and the parent's own edges out of the generator are left exactly as they
// were. The authored graph was type-checked against those ports, so a
// generator that could change its own interface at runtime would invalidate
// a check that already passed — and there would be no editor open to show the
// diagnostic.
//
// THE SPLICED GRAPH IS RE-VALIDATED, WITH THE SAME CODE THAT VALIDATES AN
// AUTHORED ONE. dag.Build for the cycle and dag.TypeCheck for the ports. A
// generator is the one place a cycle or a mistyped wire can arrive after the
// authored graph was checked, and a second implementation of either check
// here would eventually disagree with the first.
//
// EXPANSION IS BOUNDED PER RUN. A generator may emit a fragment containing
// another generator, which is genuinely useful and would otherwise never
// stop. The bound is one ceiling for the whole RUN, counted from the run's own
// event log, rather than one per generator: a per-generator ceiling stops
// being a bound the moment generators nest — ten emitting ten is a hundred —
// and a counter that lives in the log cannot be lifted by a nested generator
// and survives a restart, because it IS the log.
package dynamic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// PluginRef is what a generator node carries in a pipeline. Like the loop
// node it is a well-known builtin rather than a plugin the registry resolves:
// the control plane splices the fragment itself, because the expansion bound
// has to be enforced somewhere no tenant-supplied code can reach.
const PluginRef = "builtin:generator"

// The run-log events a generator writes. The values are stored verbatim and
// are therefore a persistence contract: add new ones, never rename these.
const (
	// EventFragmentRealised carries the fragment a generator emitted, in
	// full. This event IS the determinism guarantee: a replay rebuilds the
	// graph from it instead of asking the generator again.
	EventFragmentRealised runstore.EventType = "GENERATOR_FRAGMENT_REALISED"

	// EventCeilingReached is the expansion bound doing its job. Its reason
	// always contains "expansion ceiling reached", so a run view has one
	// string to look for.
	EventCeilingReached runstore.EventType = "GENERATOR_CEILING_REACHED"

	// EventFailed is a generator that stopped on something other than its
	// bound: it errored, or the fragment it emitted could not be spliced.
	EventFailed runstore.EventType = "GENERATOR_FAILED"
)

// ceilingReason is the exact phrase every ceiling event carries. It is
// asserted on by tests and read by the run view, so it lives in one place.
const ceilingReason = "expansion ceiling reached"

// The refusals. Each is a distinct thing that went wrong and a distinct thing
// to do about it.
var (
	// ErrDuplicateStepID: the fragment reuses an id the parent already
	// defines. Overwriting silently would replace a step somebody authored
	// with one a generator invented, with nothing on screen to say so.
	ErrDuplicateStepID = errors.New("dynamic: the fragment reuses a step id")

	// ErrNoSuchStep: the fragment was spliced at a step the parent does not
	// define. The fragment would attach to nothing and its entry steps would
	// become roots the scheduler dispatches immediately.
	ErrNoSuchStep = errors.New("dynamic: no such step to splice at")

	// ErrEmptyFragment: the generator emitted no steps. That may be correct —
	// a matrix with no rows — but it is said OUT LOUD, because splicing
	// nothing and returning the parent unchanged makes an empty matrix
	// indistinguishable from a generator that died before it spoke.
	ErrEmptyFragment = errors.New("dynamic: the fragment has no steps")

	// ErrNotADAG: the spliced graph has a cycle, or an edge naming a step
	// nothing defines. dag.Build's own words are carried through.
	ErrNotADAG = errors.New("dynamic: the spliced graph is not a DAG")

	// ErrPortMismatch: the fragment does not fit where it is spliced. See
	// Rejection, which carries the diagnostics an editor draws.
	ErrPortMismatch = errors.New("dynamic: the fragment does not fit where it is spliced")

	// ErrExpansionCeiling: this run has realised as many fragments as it is
	// allowed to. It is the last line of defence against a generator that
	// emits a generator, not the design.
	ErrExpansionCeiling = errors.New("dynamic: " + ceilingReason)

	// ErrUnbounded: a generator was configured without a positive expansion
	// ceiling. Zero is not "no limit configured yet" and a negative number is
	// not a limit; both are refused where the mistake was made rather than
	// discovered at three in the morning.
	ErrUnbounded = errors.New(
		"dynamic: a bounded generator needs a positive maximum expansion count")
)

// Rejection is a splice refused because the fragment does not fit, carrying
// the same port-addressed diagnostics an editor draws on the canvas. It is a
// type rather than a string because there is usually more than one and each
// belongs to a particular port.
type Rejection struct {
	// At is the generator the fragment was being spliced at.
	At string
	// Diagnostics is every reason it does not fit, in graph order.
	Diagnostics []dag.Diagnostic
}

func (r *Rejection) Error() string {
	reasons := make([]string, 0, len(r.Diagnostics))
	for _, d := range r.Diagnostics {
		reasons = append(reasons, fmt.Sprintf("%s.%s: %s", d.StepID, d.PortName, d.Message))
	}
	return fmt.Sprintf("%s: at %q: %s", ErrPortMismatch.Error(), r.At, strings.Join(reasons, "; "))
}

// Unwrap makes errors.Is(err, ErrPortMismatch) true, so a caller that only
// wants to know which KIND of refusal it was does not have to type-assert.
func (r *Rejection) Unwrap() error { return ErrPortMismatch }

// Splice grafts a realised fragment into a parent pipeline below the
// generator step named by at, and returns the new pipeline. The parent is
// never modified.
//
// The graft is by PORT NAME: an entry step of the fragment — one with no
// incoming edge inside the fragment — is connected from the generator output
// port that shares its input port's name. A fragment entry that declares no
// inputs is left a root, which is correct: the generator has already finished
// by the time its fragment exists, so there is no ordering left to enforce.
//
// Everything that can be refused is refused here, before anything is
// scheduled: a colliding id, an anchor that does not exist, an empty
// fragment, a cycle, and ports that do not line up.
func Splice(parent *dholev1.Pipeline, at string, fragment *dholev1.Pipeline) (*dholev1.Pipeline, error) {
	if parent == nil {
		return nil, errors.New("dynamic: splice into a nil pipeline")
	}
	anchor := findStep(parent, at)
	if anchor == nil {
		return nil, fmt.Errorf("%w: pipeline %q has no step %q",
			ErrNoSuchStep, parent.GetId(), at)
	}
	if fragment == nil || len(fragment.GetSteps()) == 0 {
		return nil, fmt.Errorf("%w: generator %q emitted a fragment with no steps",
			ErrEmptyFragment, at)
	}

	// Ids first. dag.Build would refuse a duplicate too, but its message is
	// about a pipeline defining a step twice; the operator needs to be told
	// that a GENERATOR reused an id somebody else authored.
	known := make(map[string]bool, len(parent.GetSteps()))
	for _, s := range parent.GetSteps() {
		known[s.GetId()] = true
	}
	for _, s := range fragment.GetSteps() {
		id := s.GetId()
		if id == "" {
			return nil, fmt.Errorf("%w: generator %q emitted a step with an empty id",
				ErrDuplicateStepID, at)
		}
		if known[id] {
			return nil, fmt.Errorf(
				"%w: generator %q emitted step %q, which pipeline %q already defines",
				ErrDuplicateStepID, at, id, parent.GetId())
		}
		known[id] = true
	}

	spliced, ok := proto.Clone(parent).(*dholev1.Pipeline)
	if !ok {
		return nil, errors.New("dynamic: cloning the parent pipeline did not yield a pipeline")
	}
	for _, s := range fragment.GetSteps() {
		cloned, ok := proto.Clone(s).(*dholev1.Step)
		if !ok {
			return nil, errors.New("dynamic: cloning a fragment step did not yield a step")
		}
		spliced.Steps = append(spliced.Steps, cloned)
	}
	for _, e := range fragment.GetEdges() {
		cloned, ok := proto.Clone(e).(*dholev1.Edge)
		if !ok {
			return nil, errors.New("dynamic: cloning a fragment edge did not yield an edge")
		}
		spliced.Edges = append(spliced.Edges, cloned)
	}

	// The graft: every entry step's declared inputs, wired from the
	// generator's declared outputs of the same name.
	var diags []dag.Diagnostic
	for _, s := range fragment.GetSteps() {
		if hasIncoming(fragment, s.GetId()) {
			continue
		}
		for _, in := range s.GetInputs() {
			if findPort(anchor.GetOutputs(), in.GetName()) == nil {
				diags = append(diags, dag.Diagnostic{
					StepID:   s.GetId(),
					PortName: in.GetName(),
					Message: fmt.Sprintf(
						"fragment step %q consumes port %q, which generator %q does not produce",
						s.GetId(), in.GetName(), at),
				})
				continue
			}
			spliced.Edges = append(spliced.Edges, &dholev1.Edge{
				FromStep: at, FromPort: in.GetName(),
				ToStep: s.GetId(), ToPort: in.GetName(),
			})
		}
	}

	// The same two checks an authored graph gets, run again over the graph
	// that will actually be scheduled.
	if _, err := dag.Build(spliced); err != nil {
		return nil, fmt.Errorf("%w: splicing at %q: %s", ErrNotADAG, at, err.Error())
	}
	diags = append(diags, dag.TypeCheck(spliced)...)
	if len(diags) > 0 {
		return nil, &Rejection{At: at, Diagnostics: diags}
	}
	return spliced, nil
}

func findStep(p *dholev1.Pipeline, id string) *dholev1.Step {
	if id == "" {
		return nil
	}
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	return nil
}

func findPort(ports []*dholev1.Port, name string) *dholev1.Port {
	for _, p := range ports {
		if p.GetName() == name {
			return p
		}
	}
	return nil
}

// hasIncoming reports whether the fragment itself feeds this step. One that
// nothing feeds is an entry, and an entry is what gets wired to the generator.
func hasIncoming(fragment *dholev1.Pipeline, id string) bool {
	for _, e := range fragment.GetEdges() {
		if e.GetToStep() == id {
			return true
		}
	}
	return false
}

// --- the step type ------------------------------------------------------

// Input is what a generator is asked. It carries the run it belongs to and
// the graph as it stands, so a generator can look at what it is expanding.
type Input struct {
	TenantID string
	RunID    string
	StepID   string
	Parent   *dholev1.Pipeline
}

// Emit produces the fragment. It is injected rather than executed here
// because running a generator step is the executor's job; this package owns
// the recording and the bound, which is what must not be re-implementable by
// whatever produces the fragment.
type Emit func(ctx context.Context, in Input) (*dholev1.Pipeline, error)

// Options is everything a generator needs.
type Options struct {
	Store    runstore.Store
	TenantID string
	Emit     Emit
	// MaxExpansions is how many fragments ONE RUN may realise in total,
	// across every generator in it and every generator those emit. Positive,
	// always.
	MaxExpansions int
	Now           func() time.Time
}

// Generator is a configured, bounded generator step type. It is safe for
// concurrent use; it holds no per-run state, because the run's state is its
// event log.
type Generator struct {
	store    runstore.Store
	tenantID string
	emit     Emit
	max      int
	now      func() time.Time
}

// New validates the options and builds a Generator.
func New(opts Options) (*Generator, error) {
	if opts.TenantID == "" {
		return nil, fmt.Errorf("dynamic: %w", runstore.ErrTenantRequired)
	}
	if opts.Store == nil {
		return nil, errors.New(
			"dynamic: a run store is required; a fragment nobody recorded cannot be replayed")
	}
	if opts.Emit == nil {
		return nil, errors.New("dynamic: a generator with nothing to emit is not a generator")
	}
	if opts.MaxExpansions <= 0 {
		return nil, fmt.Errorf("%w: %d is not a ceiling", ErrUnbounded, opts.MaxExpansions)
	}
	g := &Generator{
		store:    opts.Store,
		tenantID: opts.TenantID,
		emit:     opts.Emit,
		max:      opts.MaxExpansions,
		now:      opts.Now,
	}
	if g.now == nil {
		g.now = time.Now
	}
	return g, nil
}

// Realise returns the run's graph with this generator's fragment spliced in.
//
// It reads the run log FIRST. A fragment already recorded for this step is
// decoded and used, and the generator is not asked again — which is what
// makes a replay the run that happened rather than a new one that happens to
// start the same way. Only when the log has nothing does it ask, record what
// it got, and splice.
func (g *Generator) Realise(
	ctx context.Context, runID, stepID string, parent *dholev1.Pipeline,
) (*dholev1.Pipeline, error) {
	if runID == "" || stepID == "" {
		return nil, errors.New("dynamic: a run and a step are required")
	}
	if parent == nil {
		return nil, errors.New("dynamic: a generator expands a pipeline; none was given")
	}

	events, err := g.store.Replay(ctx, g.tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("dynamic: replay %s: %w", runID, err)
	}

	// What this run has already realised, and whether this step is among it.
	expansions := 0
	var recorded *runstore.Event
	for i, e := range events {
		if e.Type != EventFragmentRealised {
			continue
		}
		expansions++
		if e.StepID == stepID {
			recorded = &events[i]
		}
	}

	// The replay path. It is checked BEFORE the ceiling on purpose: a
	// fragment that is already in the log has already been paid for, and
	// refusing it because the budget is now spent would make a restart of
	// this run diverge from the run that happened.
	if recorded != nil {
		rec, err := UnmarshalRecord(recorded.Payload)
		if err != nil {
			return nil, err
		}
		fragment, err := rec.Fragment()
		if err != nil {
			return nil, err
		}
		return Splice(parent, stepID, fragment)
	}

	if expansions >= g.max {
		reason := fmt.Sprintf(
			"%s: run %q has realised %d of its %d permitted fragments, so generator %q was not expanded",
			ceilingReason, runID, expansions, g.max, stepID)
		if err := g.record(ctx, runID, stepID, EventCeilingReached, Record{
			Generator: stepID, Reason: reason,
		}); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %s", ErrExpansionCeiling, reason)
	}

	fragment, err := g.emit(ctx, Input{
		TenantID: g.tenantID, RunID: runID, StepID: stepID, Parent: parent,
	})
	if err != nil {
		return nil, g.fail(ctx, runID, stepID,
			fmt.Sprintf("generator %q failed: %v", stepID, err), err)
	}

	spliced, err := Splice(parent, stepID, fragment)
	if err != nil {
		// Deliberately NOT recorded as a realised fragment: nothing ran, and
		// a fragment in the log is a promise that a replay will rebuild this
		// graph from it.
		return nil, g.fail(ctx, runID, stepID,
			fmt.Sprintf("generator %q emitted a fragment that cannot be spliced: %v", stepID, err),
			err)
	}

	encoded, err := proto.Marshal(fragment)
	if err != nil {
		return nil, fmt.Errorf("dynamic: encode fragment of %q: %w", stepID, err)
	}
	ids := make([]string, 0, len(fragment.GetSteps()))
	for _, s := range fragment.GetSteps() {
		ids = append(ids, s.GetId())
	}
	if err := g.record(ctx, runID, stepID, EventFragmentRealised, Record{
		Generator: stepID, Steps: ids, Encoded: encoded,
	}); err != nil {
		return nil, err
	}
	return spliced, nil
}

// Record is the payload of every event this package writes. One shape with
// one reader beats three that drift.
type Record struct {
	// Generator is the step that emitted, or failed to emit, the fragment.
	Generator string `json:"generator"`
	// Steps is the fragment's step ids, in emitted order — enough for a run
	// view to draw the realised steps without decoding the fragment.
	Steps []string `json:"steps,omitempty"`
	// Reason explains a ceiling or a failure.
	Reason string `json:"reason,omitempty"`
	// Encoded is the fragment itself, on the wire. The ids above are a
	// convenience; THIS is what a replay rebuilds the graph from, so it is
	// the whole message rather than a summary of it.
	Encoded []byte `json:"fragment,omitempty"`
}

// Fragment decodes the recorded pipeline.
func (r Record) Fragment() (*dholev1.Pipeline, error) {
	if len(r.Encoded) == 0 {
		return nil, fmt.Errorf("%w: the log has no fragment for generator %q",
			ErrEmptyFragment, r.Generator)
	}
	p := &dholev1.Pipeline{}
	if err := proto.Unmarshal(r.Encoded, p); err != nil {
		return nil, fmt.Errorf("dynamic: decode the fragment of %q: %w", r.Generator, err)
	}
	return p, nil
}

// UnmarshalRecord decodes any of this package's event payloads.
func UnmarshalRecord(b []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, fmt.Errorf("dynamic: decode generator payload: %w", err)
	}
	return r, nil
}

// fail records a generator that stopped on something other than its bound.
// The two are deliberately different events: a failure shown as a ceiling
// sends the operator to raise a limit that was never the problem.
func (g *Generator) fail(
	ctx context.Context, runID, stepID, reason string, cause error,
) error {
	if err := g.record(ctx, runID, stepID, EventFailed, Record{
		Generator: stepID, Reason: reason,
	}); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// record appends one event. Sequence is left ZERO: the log allocates its own
// position, and a caller that computes one reintroduces the collision the
// store's design removed.
func (g *Generator) record(
	ctx context.Context, runID, stepID string, kind runstore.EventType, rec Record,
) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("dynamic: encode %s payload: %w", kind, err)
	}
	if err := g.store.Append(ctx, g.tenantID, runstore.Event{
		RunID:   runID,
		StepID:  stepID,
		Type:    kind,
		Payload: payload,
		At:      g.now().UTC(),
	}); err != nil {
		return fmt.Errorf("dynamic: record %s for %s/%s: %w", kind, runID, stepID, err)
	}
	return nil
}
