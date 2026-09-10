// Package loop is ADR 0015's answer to the one shape a DAG cannot hold: an
// agent that thinks, acts, observes and goes round again.
//
// The answer is NOT to allow a cycle. A cycle in the top-level graph would
// cost the cache its key derivation and the scheduler its topological order,
// and both are load-bearing (ADR 0001, ADR 0003). Iteration is instead a NODE
// containing a subgraph, with a maximum iteration count and an exit condition
// — so the graph above it stays acyclic and analysable, the subgraph is
// validated on its own, and the realised-run view unrolls the container into
// the iterations that actually happened.
//
// Three decisions here carry the weight.
//
// A loop is bounded AT CONFIGURATION. MaxIterations of zero is not "no limit
// configured yet" and a negative one is not a limit: both are the unbounded
// loop this package exists to prevent, and both are refused where the mistake
// was made rather than discovered at three in the morning by whoever is on
// call.
//
// An exit condition that ERRORS stops the loop. internal/policy fails closed
// for the same reason: an expression nobody can evaluate has not said "keep
// going". Carrying on would turn a typo in a condition into a loop that runs
// to its ceiling every time, and the ceiling is the last line of defence, not
// the design.
//
// The iteration budget is shared ACROSS A NEST. A per-loop ceiling stops being
// a bound the moment loops nest: ten around ten is a hundred body iterations
// and three levels is a thousand. The budget is therefore one counter, carried
// on the context, seeded by the outermost loop and spent by every loop inside
// it — so the worst case of a nest is the seed, never the product of the
// ceilings. A nested loop cannot lift it; it can only spend from it.
package loop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/cel-go/cel"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// PluginRef is what a loop node's step carries in a pipeline. It is a
// well-known builtin rather than a plugin the registry resolves: the control
// plane runs the iteration itself, because the bound has to be enforced
// somewhere no tenant-supplied code can reach.
const PluginRef = "builtin:loop"

// The run-log events a loop writes. The values are stored verbatim and are
// therefore a persistence contract: add new ones, never rename these.
const (
	// EventIterationStarted opens one pass. One per iteration, each under its
	// own unrolled step id, which is what lets the run view expand the
	// container into what ran.
	EventIterationStarted runstore.EventType = "LOOP_ITERATION_STARTED"

	// EventIterationFinished closes one pass and carries the state the body
	// produced, so a replay can show what the exit condition was asked about.
	EventIterationFinished runstore.EventType = "LOOP_ITERATION_FINISHED"

	// EventExited is the good ending: the condition held.
	EventExited runstore.EventType = "LOOP_EXITED"

	// EventCeilingReached is the bound doing its job. Its reason always
	// contains "iteration ceiling reached", whether the loop ran out of its
	// own iterations or the nest ran out of shared budget, so a run view has
	// one string to look for.
	EventCeilingReached runstore.EventType = "LOOP_CEILING_REACHED"

	// EventFailed is a loop that stopped on something other than its bound —
	// a body that failed, or a condition that could not be evaluated.
	EventFailed runstore.EventType = "LOOP_FAILED"
)

// CeilingReason is the exact phrase every ceiling event carries. It is
// asserted on by tests, read by the run view, and written by whoever stops a
// loop — the plane's own controller since ADR 0022 — so it lives in one place.
const CeilingReason = "iteration ceiling reached"

// The refusals. Each is a distinct thing that went wrong and a distinct thing
// to do about it.
var (
	// ErrUnbounded: a loop was configured without a positive ceiling. This is
	// the failure the whole package exists to prevent, so it is refused at
	// construction and there is no runtime path that reaches it.
	ErrUnbounded = errors.New(
		"loop: a bounded loop needs a positive maximum iteration count")

	// ErrIterationCeiling: the loop ran its full allowance without its exit
	// condition holding, or the nest it belongs to ran out of shared budget.
	ErrIterationCeiling = errors.New("loop: " + CeilingReason)

	// ErrExitCondition: the exit condition does not compile, does not answer
	// yes or no, or could not be evaluated against this iteration's state.
	// All three stop the loop.
	ErrExitCondition = errors.New("loop: the exit condition could not be answered")

	// ErrSubgraphInvalid: the loop's body is not a valid pipeline in its own
	// right. It is validated INDEPENDENTLY of the graph containing the loop,
	// which is what keeps a cycle inside a body from being a cycle nobody
	// checked.
	ErrSubgraphInvalid = errors.New("loop: the loop's subgraph is not a valid pipeline")
)

// The CEL variables an exit condition may read. This is a versioned public
// contract: pipeline authors write against these names, so keys are added,
// never renamed.
const (
	varIteration = "iteration"
	varMax       = "max"
	varState     = "state"
)

// costLimit bounds one evaluation of an exit condition, for the reason
// internal/policy bounds a rule: CEL always terminates, but nested
// comprehensions can multiply into work no run should wait for. A limit of a
// million is far above any real condition.
const costLimit = 1_000_000

// Node is the loop as a pipeline author writes it: a body, a ceiling, and the
// question asked after each pass.
type Node struct {
	// Subgraph is the body. It is a Pipeline because it IS one — validated,
	// scheduled and cached exactly like the graph above it.
	Subgraph *dholev1.Pipeline
	// MaxIterations is the hard ceiling. Positive, always.
	MaxIterations int
	// ExitCondition is a CEL expression over `iteration`, `max` and `state`,
	// evaluated after each pass. True ends the loop.
	ExitCondition string
}

// Iteration is one pass, as the body sees it.
type Iteration struct {
	// Number is 1-based.
	Number int
	// StepID is the UNROLLED id for this pass — "retry#2" — so the events the
	// body writes land under the pass they belong to and the run view can
	// expand the container.
	StepID string
	// State is what the previous pass returned, which is what the exit
	// condition was asked about.
	State map[string]any
}

// Body runs the subgraph once and returns the state the exit condition is
// evaluated against. It is injected rather than executed here because running
// a subgraph is the scheduler's job, and a loop that reached into the
// scheduler would make the bound depend on the thing being bounded.
type Body func(ctx context.Context, it Iteration) (map[string]any, error)

// Options is everything a loop needs that is not the author's Node.
type Options struct {
	Store    runstore.Store
	TenantID string
	Body     Body
	// TotalIterationBudget seeds the counter shared across a NEST of loops.
	// Zero means MaxIterations, which is exactly right for a loop that is not
	// nested and deliberately tight for one that is: an author who nests must
	// say how much the whole nest may spend.
	TotalIterationBudget int
	Now                  func() time.Time
}

// Loop is a configured, bounded loop node. It is safe for concurrent use.
type Loop struct {
	node     Node
	store    runstore.Store
	tenantID string
	body     Body
	exit     *Condition
	budget   int
	now      func() time.Time
}

// New validates the node and builds a Loop.
//
// Everything that can be refused is refused here: the ceiling, the subgraph
// and the exit condition. A loop whose condition only fails to compile at
// runtime is a loop that reaches its ceiling in production for every author
// who typed it.
func New(node Node, opts Options) (*Loop, error) {
	if opts.TenantID == "" {
		return nil, fmt.Errorf("loop: %w", runstore.ErrTenantRequired)
	}
	if opts.Store == nil {
		return nil, errors.New(
			"loop: a run store is required; iterations nobody recorded cannot be unrolled")
	}
	if opts.Body == nil {
		return nil, errors.New("loop: a loop with no body is an empty ceiling")
	}
	if node.MaxIterations <= 0 {
		return nil, fmt.Errorf("%w: %d is not a ceiling", ErrUnbounded, node.MaxIterations)
	}
	if node.Subgraph == nil {
		return nil, fmt.Errorf("%w: no subgraph", ErrSubgraphInvalid)
	}
	// The body is validated on its OWN, as a pipeline. dag.Build's refusal is
	// carried through verbatim so the author is told what it found.
	if _, err := dag.Build(node.Subgraph); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrSubgraphInvalid, err.Error())
	}
	condition, err := NewCondition(node.ExitCondition)
	if err != nil {
		return nil, err
	}

	budget := opts.TotalIterationBudget
	if budget <= 0 {
		budget = node.MaxIterations
	}
	l := &Loop{
		node:     node,
		store:    opts.Store,
		tenantID: opts.TenantID,
		body:     opts.Body,
		exit:     condition,
		budget:   budget,
		now:      opts.Now,
	}
	if l.now == nil {
		l.now = time.Now
	}
	return l, nil
}

// Condition is a loop's exit expression, compiled once and asked after each
// pass. It is a type rather than a bare cel.Program because a spliced loop
// (ADR 0022) compiles the same expression on every controller of the run, and
// two places that each planned their own program would eventually disagree
// about what a condition means.
type Condition struct {
	expr    string
	program cel.Program
}

// NewCondition compiles an exit condition, refusing anything that does not
// parse or does not answer yes or no.
//
// A loop whose condition only fails to compile at runtime is a loop that
// reaches its ceiling in production for every author who typed it.
func NewCondition(expr string) (*Condition, error) {
	if expr == "" {
		return nil, fmt.Errorf(
			"%w: a loop with no exit condition can only ever end at its ceiling", ErrExitCondition)
	}
	env, err := cel.NewEnv(
		cel.Variable(varIteration, cel.IntType),
		cel.Variable(varMax, cel.IntType),
		// A map rather than a struct, for internal/policy's reason: a
		// condition naming a key the body never set is an EVALUATION error,
		// which stops the loop, instead of being silently rewritten to a zero
		// value that reads as "not done yet".
		cel.Variable(varState, cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return nil, fmt.Errorf("loop: build cel environment: %w", err)
	}
	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("%w: %q does not compile: %s", ErrExitCondition, expr, iss.Err())
	}
	// dyn is accepted because a value read out of `state` has no static type;
	// it is checked again on the value at evaluation, where a non-bool stops
	// the loop.
	if out := ast.OutputType(); !out.IsExactType(cel.BoolType) && !out.IsExactType(cel.DynType) {
		return nil, fmt.Errorf("%w: %q returns %s, not bool", ErrExitCondition, expr, out)
	}
	program, err := env.Program(ast, cel.CostLimit(costLimit))
	if err != nil {
		return nil, fmt.Errorf("%w: %q cannot be planned: %w", ErrExitCondition, expr, err)
	}
	return &Condition{expr: expr, program: program}, nil
}

// Expression is the author's own text, which is what a spliced controller
// carries forward to the next iteration.
func (c *Condition) Expression() string { return c.expr }

// Holds evaluates the condition, failing closed. An expression that errors or
// answers with something that is not a bool has not said "keep going".
func (c *Condition) Holds(
	ctx context.Context, iteration, ceiling int, state map[string]any,
) (bool, error) {
	out, _, err := c.program.ContextEval(ctx, map[string]any{
		varIteration: iteration,
		varMax:       ceiling,
		varState:     celState(state),
	})
	if err != nil {
		return false, fmt.Errorf("%w: %q at iteration %d: %v", ErrExitCondition,
			c.expr, iteration, err)
	}
	done, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("%w: %q returned %T at iteration %d, not a bool",
			ErrExitCondition, c.expr, out.Value(), iteration)
	}
	return done, nil
}

// Result is what one run of the loop did.
type Result struct {
	// Iterations is how many passes the body actually ran.
	Iterations int
	// Exited is true only when the condition held. A loop stopped by its
	// ceiling or by an error did not exit — it was stopped.
	Exited bool
	// State is the last state the body produced.
	State map[string]any
}

// Record is the payload of every event this package writes. One shape with
// one reader beats four that drift.
type Record struct {
	Loop      string         `json:"loop"`
	Iteration int            `json:"iteration"`
	Of        int            `json:"of"`
	StepID    string         `json:"step_id,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	State     map[string]any `json:"state,omitempty"`
}

// UnmarshalIteration decodes any of this package's event payloads.
func UnmarshalIteration(b []byte) (Record, error) {
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, fmt.Errorf("loop: decode iteration payload: %w", err)
	}
	return r, nil
}

// Run iterates the body until the condition holds, the ceiling is reached, the
// nest's budget is spent, or something fails — and records every one of those
// endings in the run log.
//
// The context it passes the body carries the nest's shared budget, so a loop
// started from inside this one spends from the same counter.
func (l *Loop) Run(
	ctx context.Context, runID, stepID string, state map[string]any,
) (Result, error) {
	if runID == "" || stepID == "" {
		return Result{}, errors.New("loop: a run and a step are required")
	}
	// A nested loop finds the ancestor's budget and shares it. Only the
	// outermost seeds one, which is what makes the nest's worst case the seed
	// rather than the product of the ceilings.
	budget, ctx := budgetFor(ctx, l.budget)

	res := Result{State: state}
	for n := 1; n <= l.node.MaxIterations; n++ {
		if !budget.spend() {
			return res, l.ceiling(ctx, runID, stepID, res.Iterations, fmt.Sprintf(
				"%s: the nest's total iteration budget of %d is spent, so loop %q stopped after %d "+
					"of its %d iterations",
				CeilingReason, budget.seed, stepID, res.Iterations, l.node.MaxIterations))
		}

		it := Iteration{Number: n, StepID: unrolled(stepID, n), State: res.State}
		if err := l.record(ctx, runID, it.StepID, EventIterationStarted, Record{
			Loop: stepID, Iteration: n, Of: l.node.MaxIterations, StepID: it.StepID,
		}); err != nil {
			return res, err
		}

		next, err := l.body(ctx, it)
		res.Iterations = n
		if err != nil {
			return res, l.fail(ctx, runID, stepID, n, fmt.Sprintf(
				"loop %q: iteration %d failed: %v", stepID, n, err), err)
		}
		res.State = next

		if err := l.record(ctx, runID, it.StepID, EventIterationFinished, Record{
			Loop: stepID, Iteration: n, Of: l.node.MaxIterations, StepID: it.StepID, State: next,
		}); err != nil {
			return res, err
		}

		done, err := l.done(ctx, n, next)
		if err != nil {
			return res, l.fail(ctx, runID, stepID, n, err.Error(), err)
		}
		if done {
			res.Exited = true
			return res, l.record(ctx, runID, stepID, EventExited, Record{
				Loop: stepID, Iteration: n, Of: l.node.MaxIterations, State: next,
				Reason: fmt.Sprintf("loop %q exited after %d of %d iterations",
					stepID, n, l.node.MaxIterations),
			})
		}
	}

	return res, l.ceiling(ctx, runID, stepID, res.Iterations, fmt.Sprintf(
		"%s: loop %q ran its maximum of %d iterations without its exit condition holding",
		CeilingReason, stepID, l.node.MaxIterations))
}

// done evaluates the exit condition for one pass.
func (l *Loop) done(ctx context.Context, n int, state map[string]any) (bool, error) {
	return l.exit.Holds(ctx, n, l.node.MaxIterations, state)
}

// celState is the activation's `state`. A nil map is an EMPTY map, not a
// missing variable: a condition reading a key of it must fail on the key, not
// on the variable, so the message names what the author got wrong.
func celState(state map[string]any) map[string]any {
	if state == nil {
		return map[string]any{}
	}
	return state
}

// unrolled is the step id of one pass. The suffix is what a run view groups
// on; three events that all say "retry" cannot be expanded into three
// iterations.
func unrolled(stepID string, n int) string {
	return fmt.Sprintf("%s#%d", stepID, n)
}

// ceiling records the bound doing its job and returns the refusal.
func (l *Loop) ceiling(ctx context.Context, runID, stepID string, n int, reason string) error {
	rec := Record{Loop: stepID, Iteration: n, Of: l.node.MaxIterations, Reason: reason}
	if err := l.record(ctx, runID, stepID, EventCeilingReached, rec); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrIterationCeiling, reason)
}

// fail records a loop that stopped on something other than its bound. The two
// are deliberately different events: a run view that showed a failed body as a
// ceiling would send the operator to raise a limit that was never the problem.
func (l *Loop) fail(
	ctx context.Context, runID, stepID string, n int, reason string, cause error,
) error {
	rec := Record{Loop: stepID, Iteration: n, Of: l.node.MaxIterations, Reason: reason}
	if err := l.record(ctx, runID, stepID, EventFailed, rec); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// record appends one event. Sequence is left ZERO: the log allocates its own
// position, and a caller that computes one reintroduces the collision the
// store's design removed.
func (l *Loop) record(
	ctx context.Context, runID, stepID string, kind runstore.EventType, rec Record,
) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("loop: encode %s payload: %w", kind, err)
	}
	if err := l.store.Append(ctx, l.tenantID, runstore.Event{
		RunID:   runID,
		StepID:  stepID,
		Type:    kind,
		Payload: payload,
		At:      l.now().UTC(),
	}); err != nil {
		return fmt.Errorf("loop: record %s for %s/%s: %w", kind, runID, stepID, err)
	}
	return nil
}

// --- the nest's shared budget -------------------------------------------

// Budget is the iteration allowance of a whole NEST of loops: one counter,
// spent by every loop under the one that seeded it.
//
// It exists because a per-loop ceiling is not a bound once loops nest. Ten
// around ten is a hundred body iterations, three levels is a thousand, and
// every one of those is a step dispatched to an engine. Sharing the counter
// makes the nest's worst case the seed.
type Budget struct {
	seed int

	mu        sync.Mutex
	remaining int
}

// NewBudget returns a budget of n iterations. A non-positive n is the
// unbounded case this package refuses, and is clamped to one rather than to
// infinity.
func NewBudget(n int) *Budget {
	if n <= 0 {
		n = 1
	}
	return &Budget{seed: n, remaining: n}
}

// Remaining is what the nest has left to spend.
func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining
}

// spend takes one iteration, reporting whether there was one to take.
func (b *Budget) spend() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining <= 0 {
		return false
	}
	b.remaining--
	return true
}

type budgetKey struct{}

// WithBudget puts a budget on ctx, so every loop started under it — however
// deeply nested — spends from that one counter.
func WithBudget(ctx context.Context, b *Budget) context.Context {
	return context.WithValue(ctx, budgetKey{}, b)
}

// BudgetFrom returns the budget ctx carries, if any.
func BudgetFrom(ctx context.Context) (*Budget, bool) {
	b, ok := ctx.Value(budgetKey{}).(*Budget)
	return b, ok
}

// budgetFor returns the budget this run must spend from, seeding one only when
// there is no ancestor. A nested loop CANNOT lift the nest's allowance — it
// finds the counter and spends from it — which is the whole point.
func budgetFor(ctx context.Context, seed int) (*Budget, context.Context) {
	if b, ok := BudgetFrom(ctx); ok {
		return b, ctx
	}
	b := NewBudget(seed)
	return b, WithBudget(ctx, b)
}
