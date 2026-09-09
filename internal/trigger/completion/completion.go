// Package completion is the trigger that fires when another pipeline's run
// finishes.
//
// It is what makes a chain of pipelines expressible without one giant graph:
// build finishes, publish starts, and neither definition mentions the other's
// steps. To the downstream pipeline this is one more way its inputs arrived
// (ADR 0007).
//
// Two rules are load-bearing:
//
// A run that FAILED is not a run that completed. The trigger fires on
// RUN_COMPLETED and on nothing else — default-deny, so an event type added
// later cannot quietly start meaning "success" — because a downstream pipeline
// fired off a failed upstream is how a broken build gets published.
//
// A completion trigger must not be able to fire itself, directly or around a
// loop. The graph is acyclic by decision, and iteration is a bounded loop node
// (ADR 0015); a pipeline that starts itself on completion is not one run
// looping, it is an unbounded number of runs, with no iteration budget
// anywhere. New refuses the direct case on its own, and refuses the indirect
// one — a to b to c to a — when the triggers share a Registry, which is how a
// control plane holding all of a tenant's triggers wires them up.
package completion

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/trigger"
)

// Kind is what this trigger reports itself as.
const Kind = "completion"

// The fields of a completion event, which is all a binding may draw on.
const (
	FieldUpstreamRunID      = "upstream_run_id"
	FieldUpstreamPipelineID = "upstream_pipeline_id"
	FieldOutcome            = "outcome"
	FieldCompletedAt        = "completed_at"
	FieldTriggerID          = "trigger_id"
	FieldKind               = "kind"
)

// Event is one upstream run reaching its end. It is deliberately the run event
// log's own vocabulary — the type is a runstore.EventType — so that whoever
// feeds this trigger passes on what was recorded rather than its own reading
// of it.
type Event struct {
	TenantID   string
	PipelineID string
	RunID      string
	Type       runstore.EventType
	At         time.Time
}

// Outcome is what one observed event did. Reason is filled whenever Fired is
// false: a downstream pipeline that did not start is a thing somebody will
// have to explain.
type Outcome struct {
	Fired  bool
	Reason string
}

// Config configures a completion trigger.
type Config struct {
	ID       string
	TenantID string
	// UpstreamPipelineID is the pipeline whose completion is watched.
	UpstreamPipelineID string
	// Binding names the pipeline this trigger STARTS, and maps its inputs to
	// the completion event's fields.
	Binding trigger.Binding
	// Pipeline is the downstream definition, checked against the binding.
	Pipeline *dholev1.Pipeline

	// Source is what Start consumes. Observe is the same trigger driven one
	// event at a time, and is what a control plane with its own subscription
	// calls.
	Source <-chan Event
	// Registry, when set, holds every completion trigger wired in one
	// control plane and is what makes a cycle through several pipelines
	// refusable at configuration time.
	Registry *Registry
	// OnError is called with every error Start swallows.
	OnError func(error)
}

// Trigger is one configured completion watch.
type Trigger struct {
	id       string
	tenantID string
	upstream string
	binding  trigger.Binding
	pipeline *dholev1.Pipeline
	source   <-chan Event
	onError  func(error)
}

// Compile-time proof that this is a trigger.
var _ trigger.Trigger = (*Trigger)(nil)

// New validates cfg and returns the trigger it describes.
func New(cfg Config) (*Trigger, error) {
	if cfg.TenantID == "" {
		return nil, fmt.Errorf("completion trigger %q: %w", cfg.ID, trigger.ErrTenantRequired)
	}
	if cfg.ID == "" {
		return nil, errors.New("completion trigger: an id is required")
	}
	if cfg.UpstreamPipelineID == "" {
		return nil, fmt.Errorf(
			"completion trigger %q: an upstream pipeline is required; a trigger watching "+
				"every run in the tenant is not a completion trigger", cfg.ID)
	}
	if err := trigger.ValidateBinding(cfg.Pipeline, cfg.Binding); err != nil {
		return nil, fmt.Errorf("completion trigger %q: %w", cfg.ID, err)
	}
	if err := validateSources(cfg.Binding); err != nil {
		return nil, fmt.Errorf("completion trigger %q: %w", cfg.ID, err)
	}
	if cfg.UpstreamPipelineID == cfg.Binding.PipelineID {
		return nil, fmt.Errorf(
			"completion trigger %q: pipeline %q would start itself on completion, which is "+
				"an unbounded chain of runs and not a loop anything bounds; iteration is a "+
				"bounded loop node inside a pipeline",
			cfg.ID, cfg.Binding.PipelineID)
	}
	if cfg.Registry != nil {
		if err := cfg.Registry.add(
			cfg.TenantID, cfg.ID, cfg.UpstreamPipelineID, cfg.Binding.PipelineID); err != nil {
			return nil, fmt.Errorf("completion trigger %q: %w", cfg.ID, err)
		}
	}

	return &Trigger{
		id:       cfg.ID,
		tenantID: cfg.TenantID,
		upstream: cfg.UpstreamPipelineID,
		binding:  cfg.Binding,
		pipeline: cfg.Pipeline,
		source:   cfg.Source,
		onError:  cfg.OnError,
	}, nil
}

// validateSources rejects a binding drawing on a field a completion event does
// not carry.
func validateSources(b trigger.Binding) error {
	known := eventFields("", Event{})
	for input, field := range b.InputMapping {
		if _, ok := known[field]; !ok {
			return fmt.Errorf(
				"binding fills input %q from %q, which a completion event does not carry "+
					"(it carries: %s)", input, field, strings.Join(fieldNames(), ", "))
		}
	}
	return nil
}

// Kind implements trigger.Trigger.
func (t *Trigger) Kind() string { return Kind }

// Start consumes completions until ctx is done and returns ctx's error.
func (t *Trigger) Start(ctx context.Context, sink trigger.Sink) error {
	if sink == nil {
		return fmt.Errorf("completion trigger %q: a sink is required", t.id)
	}
	if t.source == nil {
		return fmt.Errorf(
			"completion trigger %q: a source is required; a trigger with nothing to consume "+
				"is a watch that silently never fires", t.id)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-t.source:
			if !ok {
				return ctx.Err()
			}
			if _, err := t.Observe(ctx, sink, ev); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if t.onError != nil {
					t.onError(err)
				}
			}
		}
	}
}

// Observe offers one run event to the trigger, and is the whole trigger:
// Start is a loop around it.
//
// Everything it refuses, it refuses with a reason. The order is deliberate —
// scope first, then subject, then outcome — so that the reason names the first
// thing that did not match rather than the last.
func (t *Trigger) Observe(ctx context.Context, sink trigger.Sink, ev Event) (Outcome, error) {
	if sink == nil {
		return Outcome{}, fmt.Errorf("completion trigger %q: a sink is required", t.id)
	}
	switch {
	case ev.TenantID != t.tenantID:
		return Outcome{Reason: fmt.Sprintf(
			"the run belongs to tenant %q and this trigger to %q", ev.TenantID, t.tenantID)}, nil
	case ev.PipelineID != t.upstream:
		return Outcome{Reason: fmt.Sprintf(
			"the run belongs to pipeline %q and this trigger watches %q",
			ev.PipelineID, t.upstream)}, nil
	case ev.PipelineID == t.binding.PipelineID:
		// Unreachable through New, which refuses the configuration. It is
		// here because the consequence — a pipeline starting itself, forever
		// — is bad enough to be worth refusing twice.
		return Outcome{Reason: fmt.Sprintf(
			"pipeline %q would start itself", ev.PipelineID)}, nil
	case ev.Type != runstore.RunCompleted:
		return Outcome{Reason: fmt.Sprintf(
			"run %q ended with %s, and only %s starts a downstream pipeline",
			ev.RunID, ev.Type, runstore.RunCompleted)}, nil
	}

	inputs, err := t.inputs(ev)
	if err != nil {
		return Outcome{}, fmt.Errorf("completion trigger %q: %w", t.id, err)
	}
	if err := trigger.ValidateInputs(t.pipeline, inputs); err != nil {
		return Outcome{}, fmt.Errorf("completion trigger %q: %w", t.id, err)
	}
	if err := sink.Fire(ctx, t.tenantID, t.binding.PipelineID, inputs); err != nil {
		return Outcome{}, fmt.Errorf("completion trigger %q: firing on run %q: %w",
			t.id, ev.RunID, err)
	}
	return Outcome{Fired: true}, nil
}

// inputs turns one completion into the downstream pipeline's declared inputs.
func (t *Trigger) inputs(ev Event) (map[string]*structpb.Value, error) {
	fields := eventFields(t.id, ev)
	out := make(map[string]*structpb.Value, len(t.binding.InputMapping))
	for _, input := range sortedKeys(t.binding.InputMapping) {
		field := t.binding.InputMapping[input]
		v, ok := fields[field]
		if !ok {
			return nil, fmt.Errorf("input %q reads unknown event field %q", input, field)
		}
		out[input] = structpb.NewStringValue(v)
	}
	return out, nil
}

func eventFields(id string, ev Event) map[string]string {
	return map[string]string{
		FieldUpstreamRunID:      ev.RunID,
		FieldUpstreamPipelineID: ev.PipelineID,
		FieldOutcome:            string(ev.Type),
		FieldCompletedAt:        ev.At.UTC().Format(time.RFC3339Nano),
		FieldTriggerID:          id,
		FieldKind:               Kind,
	}
}

func fieldNames() []string {
	out := make([]string, 0, 6)
	for name := range eventFields("", Event{}) {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Registry holds the completion edges wired in one control plane, so that a
// cycle spanning several triggers can be refused when the last one is added.
//
// The direct case — a pipeline watching itself — needs no registry and is
// refused by New unconditionally. The case that actually reaches production is
// a to b to c to a, wired by three people who each added one perfectly
// sensible trigger, and nothing local to any of them can see it.
//
// The graph is per TENANT: two tenants using the same pipeline ids are not
// each other's cycle.
type Registry struct {
	mu    sync.Mutex
	edges map[string]map[string][]string
}

// NewRegistry returns an empty registry. It is safe for concurrent use.
func NewRegistry() *Registry {
	return &Registry{edges: map[string]map[string][]string{}}
}

// add records that finishing upstream starts downstream, refusing the edge if
// it closes a cycle.
func (r *Registry) add(tenantID, triggerID, upstream, downstream string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.edges == nil {
		r.edges = map[string]map[string][]string{}
	}
	tenant := r.edges[tenantID]
	if tenant == nil {
		tenant = map[string][]string{}
		r.edges[tenantID] = tenant
	}

	// The new edge closes a cycle exactly when the downstream pipeline can
	// already reach the upstream one.
	if path, found := reach(tenant, downstream, upstream); found {
		return fmt.Errorf(
			"watching %q to start %q closes a completion cycle (%s), and a cycle of "+
				"pipelines starting each other never stops; trigger %q was not wired",
			upstream, downstream,
			strings.Join(append([]string{upstream, downstream}, path...), " -> "), triggerID)
	}
	tenant[upstream] = append(tenant[upstream], downstream)
	return nil
}

// reach walks the edges depth-first, returning the path from -> to.
func reach(edges map[string][]string, from, to string) ([]string, bool) {
	seen := map[string]bool{}
	var walk func(at string) ([]string, bool)
	walk = func(at string) ([]string, bool) {
		if seen[at] {
			return nil, false
		}
		seen[at] = true
		for _, next := range edges[at] {
			if next == to {
				return []string{next}, true
			}
			if path, found := walk(next); found {
				return append([]string{next}, path...), true
			}
		}
		return nil, false
	}
	if from == to {
		return nil, true
	}
	return walk(from)
}
