// Package agent is the agent step type: a model given a bounded loop and a
// closed set of things it is allowed to do.
//
// ADR 0015's third rule is the whole package: an agent step may only invoke
// steps explicitly granted to it, and may never invoke an `at-most-once` step
// without passing an approval gate. An agent that can call anything is an
// arbitrary-code-execution primitive wearing a friendly name — and that is
// true before anyone mentions prompt injection. Once the agent is reading a
// webhook body, "the model decided to" and "an attacker decided to" are the
// same sentence.
//
// Four decisions here carry the weight.
//
// The action-space check is at INVOCATION, not at tool-list construction. The
// tool list is what the agent was OFFERED; what has to be refused is what it
// ASKED FOR, and a model that invents a plausible tool name it was never given
// is not a hypothetical — it is a Tuesday. Every path to running a step goes
// through Invoke, and Invoke asks ActionSpace.Check every time.
//
// Taint is consulted BEFORE anything effectful runs. internal/taint answers
// exactly this question, and its answer is a policy.Decision so that a taint
// refusal reads and audits like every other refusal (ADR 0012) rather than
// being a second vocabulary for "no". A pure action may still read untrusted
// data — parsing and reshaping a webhook body is what should happen to it —
// and everything else may not, an unspecified effect class included.
//
// An at-most-once action is ROUTED to Task 20's gate, not executed and then
// reported. The gate is the same one a person's approval queue reads, so an
// agent asking to deploy appears where a human asking to deploy appears; a
// separate agent-approval path would be a second queue nobody watches.
//
// The loop is bounded by MaxSteps, refused at configuration when it is not
// positive. A model that always calls a tool never stops on its own, and the
// SDK's own default of eight is a number nobody chose for this pipeline.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/taint"
	"github.com/azrtydxb/go-ai-sdk/agent"
	"github.com/azrtydxb/go-ai-sdk/ai"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// PluginRef is what an agent node's step carries in a pipeline. Like a loop,
// it is a well-known builtin: the action space has to be enforced somewhere no
// tenant-supplied code can reach.
const PluginRef = "builtin:agent"

// The refusals. Each is a distinct thing that was refused and a distinct thing
// to do about it, so none of them collapses into a generic error.
var (
	// ErrOutsideActionSpace: the agent asked for a step it was not granted.
	// The message names both the step and the grant.
	ErrOutsideActionSpace = errors.New("agent: step outside the granted action space")

	// ErrApprovalRequired: an at-most-once action was requested. The gate has
	// been asked; nothing has run.
	ErrApprovalRequired = errors.New("agent: an at-most-once step needs an approval")

	// ErrTainted: the agent is acting on untrusted data and asked for an
	// effectful step. The message names the trigger the data came from.
	ErrTainted = errors.New("agent: an effectful step may not act on untrusted data")

	// ErrUnbounded: an agent was configured without a positive step ceiling.
	ErrUnbounded = errors.New("agent: an agent loop needs a positive step ceiling")

	// ErrNoGate: an at-most-once step was granted with nothing to route its
	// approval to. Refused at construction, because discovering it at the
	// moment of a deploy is discovering it too late.
	ErrNoGate = errors.New("agent: an at-most-once grant needs an approval gate")
)

// Invocation is one request to run a granted step.
type Invocation struct {
	// RunID and StepID are the AGENT's own position in the run — the gate's
	// event and any refusal belong to the agent step, not to the action it
	// wanted.
	RunID  string
	StepID string
	// Action is the granted step being asked for.
	Action string
	// Args are the model's arguments, passed through to the invoker.
	Args json.RawMessage
	// Inputs are structured values this particular call would carry, on top
	// of the agent's own. They are checked for taint like everything else.
	Inputs map[string]*structpb.Value
}

// Invoker actually runs a granted step. It is injected because running a step
// is the scheduler's job, and an agent that reached into the scheduler would
// make the action space depend on the thing it is bounding.
type Invoker interface {
	Invoke(ctx context.Context, inv Invocation) (json.RawMessage, error)
}

// Gate is Task 20's approval step, narrowed to the one thing this package
// asks of it. *approval.Step satisfies it directly.
type Gate interface {
	Request(ctx context.Context, runID, stepID, prompt string) error
}

// Config is the pipeline author's half of an agent step.
type Config struct {
	// GrantedSteps is the closed set of steps this agent may invoke.
	GrantedSteps []string
	// MaxSteps bounds the model's tool-calling loop. Positive, always.
	MaxSteps int
	// Catalogue defines the steps that exist, so a grant can be resolved to
	// an effect class.
	Catalogue []*dholev1.Step
	// Instructions is the system prompt.
	Instructions string
}

// Options is everything else the step needs.
type Options struct {
	Model   provider.LanguageModel
	Invoker Invoker
	Gate    Gate
	Store   runstore.Store
	// TenantID scopes everything. There is no unscoped agent.
	TenantID string
	// Inputs are the structured values the agent itself is acting on — the
	// webhook body it was asked to triage. They are what makes every
	// invocation it attempts tainted or not.
	Inputs map[string]*structpb.Value
	// EngineCapabilities are the capabilities of the engine an action would
	// run on. A privileged engine refuses tainted data whatever the class.
	EngineCapabilities []dholev1.Capability
}

// Step is the agent step type. It is safe for concurrent use.
//
// GrantedSteps and MaxSteps are exported because they are the two facts anyone
// auditing an agent asks for, and reading them should not require the
// ActionSpace behind them.
type Step struct {
	GrantedSteps []string
	MaxSteps     int

	actions      *ActionSpace
	instructions string
	model        provider.LanguageModel
	invoker      Invoker
	gate         Gate
	tenantID     string
	inputs       map[string]*structpb.Value
	capabilities []dholev1.Capability
}

// New validates the configuration and builds a Step.
//
// Everything that can be refused is refused here: the tenant, the ceiling, the
// grants, and an at-most-once grant with no gate to route it to.
func New(cfg Config, opts Options) (*Step, error) {
	if opts.TenantID == "" {
		return nil, fmt.Errorf("agent: %w", runstore.ErrTenantRequired)
	}
	if opts.Model == nil {
		return nil, errors.New("agent: a language model is required")
	}
	if opts.Invoker == nil {
		return nil, errors.New("agent: an invoker is required; an agent that cannot act is a prompt")
	}
	if cfg.MaxSteps <= 0 {
		return nil, fmt.Errorf("%w: %d is not a ceiling", ErrUnbounded, cfg.MaxSteps)
	}
	actions, err := NewActionSpace(cfg.GrantedSteps, cfg.Catalogue)
	if err != nil {
		return nil, err
	}
	if opts.Gate == nil {
		for _, name := range actions.Names() {
			if step, _ := actions.Action(name); needsApproval(step) {
				return nil, fmt.Errorf("%w: %q is at-most-once", ErrNoGate, name)
			}
		}
	}

	return &Step{
		GrantedSteps: actions.Names(),
		MaxSteps:     cfg.MaxSteps,
		actions:      actions,
		instructions: cfg.Instructions,
		model:        opts.Model,
		invoker:      opts.Invoker,
		gate:         opts.Gate,
		tenantID:     opts.TenantID,
		inputs:       opts.Inputs,
		capabilities: opts.EngineCapabilities,
	}, nil
}

// Actions is the agent's action space, for anything that needs to render or
// audit it.
func (s *Step) Actions() *ActionSpace { return s.actions }

// needsApproval reports whether an action may never run unasked. An
// UNSPECIFIED effect class counts, because the cost of guessing wrong the
// other way is an agent-triggered at-most-once action nobody approved.
func needsApproval(step *dholev1.Step) bool {
	switch step.GetEffectClass() {
	case dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED:
		return true
	default:
		return false
	}
}

// Invoke is the one path to running a granted step, and therefore the one
// place the three rules are enforced. Every refusal below leaves the invoker
// untouched: nothing ran.
func (s *Step) Invoke(ctx context.Context, inv Invocation) (json.RawMessage, error) {
	if inv.RunID == "" || inv.StepID == "" {
		return nil, errors.New("agent: a run and a step are required")
	}
	if strings.TrimSpace(inv.Action) == "" {
		return nil, s.actions.Check("")
	}

	// 1. The grant. Asked HERE, on every call, not once when the tool list was
	// built — a name the model invented reaches this and nothing else.
	if err := s.actions.Check(inv.Action); err != nil {
		return nil, err
	}
	action, _ := s.actions.Action(inv.Action)

	// 2. The taint, before anything effectful. This is the check that makes a
	// webhook body unable to become a deploy.
	if err := s.checkTaint(inv, action); err != nil {
		return nil, err
	}

	// 3. The approval gate. Routed to, not reported after.
	if needsApproval(action) {
		return nil, s.requestApproval(ctx, inv)
	}

	return s.invoker.Invoke(ctx, inv)
}

// checkTaint asks internal/taint whether untrusted data may reach this action,
// over the agent's own inputs AND the call's. Both matter: the agent is acting
// on what it read, and a refusal that only looked at the call's own arguments
// would miss the webhook body entirely — which is the only interesting case.
func (s *Step) checkTaint(inv Invocation, action *dholev1.Step) error {
	inputs := make(map[string]*structpb.Value, len(s.inputs)+len(inv.Inputs))
	for k, v := range s.inputs {
		inputs[k] = v
	}
	for k, v := range inv.Inputs {
		inputs[k] = v
	}

	decision := taint.Check(taint.Dispatch{
		Subject:            inv.Action,
		EffectClass:        action.GetEffectClass(),
		EngineCapabilities: s.capabilities,
		Inputs:             inputs,
	})
	if decision.Allow {
		return nil
	}
	return fmt.Errorf("%w: agent %q: %s [%s]",
		ErrTainted, inv.StepID, decision.Reason, decision.Rule)
}

// requestApproval routes the call to Task 20's gate and returns without
// running anything. The gate's event is the same STEP_AWAITING_APPROVAL a
// person's queue reads, appended against the AGENT's step: the run is now
// waiting for somebody, exactly as it would be for a human-authored gate.
func (s *Step) requestApproval(ctx context.Context, inv Invocation) error {
	if s.gate == nil {
		return fmt.Errorf("%w: %q", ErrNoGate, inv.Action)
	}
	prompt := fmt.Sprintf(
		"agent step %q wants to invoke the at-most-once step %q; approve?", inv.StepID, inv.Action)
	if err := s.gate.Request(ctx, inv.RunID, inv.StepID, prompt); err != nil {
		return fmt.Errorf("agent: requesting approval for %q: %w", inv.Action, err)
	}
	return fmt.Errorf("%w: %q is at-most-once; the run is waiting for a decision",
		ErrApprovalRequired, inv.Action)
}

// AsTool exposes ONE granted step to a model.
//
// It refuses a step outside the action space through the same Check the
// invocation uses, so a tool for an ungranted step cannot be built — and if
// one somehow were, its Execute would still be refused.
func (s *Step) AsTool(action string) (ai.Tool, error) {
	return s.asTool(action, "", "", newGuard())
}

func (s *Step) asTool(action, runID, stepID string, g *guard) (ai.Tool, error) {
	if err := s.actions.Check(action); err != nil {
		return nil, err
	}
	def, _ := s.actions.Action(action)
	name := def.GetName()
	if name == "" {
		name = def.GetId()
	}
	tool := ai.Tool(&actionTool{
		step:   s,
		action: action,
		desc: fmt.Sprintf("invoke the %q step (%s)", name,
			strings.TrimPrefix(def.GetEffectClass().String(), "EFFECT_CLASS_")),
		runID:  runID,
		stepID: stepID,
		guard:  g,
	})
	if needsApproval(def) {
		// The SDK's own approval hook, so an at-most-once call is stopped
		// BEFORE Execute rather than inside it. Invoke refuses it as well;
		// this is the outer of two nets, not the only one.
		tool = ai.RequireApproval(tool)
	}
	return tool, nil
}

// Tools is the action space as the model sees it: granted steps and nothing
// else.
func (s *Step) Tools() ([]ai.Tool, error) {
	return s.toolsFor("", "", newGuard())
}

func (s *Step) toolsFor(runID, stepID string, g *guard) ([]ai.Tool, error) {
	names := s.actions.Names()
	tools := make([]ai.Tool, 0, len(names))
	for _, name := range names {
		tool, err := s.asTool(name, runID, stepID, g)
		if err != nil {
			return nil, err
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

// Run drives the model's bounded tool-calling loop.
//
// It returns the SDK's result on success and this package's own refusal when
// the agent tried something it may not do — including when the SDK's loop was
// perfectly happy to carry on. That distinction is the reason for the guard:
// a tool error is handed back to the model as a result and the loop continues,
// so an agent refused a deploy would try something else and the run would end
// reporting success.
func (s *Step) Run(
	ctx context.Context, runID, stepID, prompt string,
) (*ai.GenerateTextResult, error) {
	if runID == "" || stepID == "" {
		return nil, errors.New("agent: a run and a step are required")
	}
	g := newGuard()
	tools, err := s.toolsFor(runID, stepID, g)
	if err != nil {
		return nil, err
	}

	a := &agent.Agent{
		Model:        s.model,
		Instructions: s.instructions,
		Tools:        tools,
		// The ceiling. Not the SDK's default of eight, which is a number
		// nobody chose for this pipeline.
		MaxSteps: s.MaxSteps,
		StopWhen: func([]ai.Step) bool { return g.err() != nil },
		ApproveToolCall: func(
			ctx context.Context, req ai.ApprovalRequest,
		) (ai.ApprovalDecision, bool) {
			refusal := s.approve(ctx, runID, stepID, req.Call.Name)
			g.record(refusal)
			return ai.ApprovalDecision{
				ToolCallID: req.Call.ID, Approved: false, Reason: refusal.Error(),
			}, true
		},
		PrepareOpts: func(opts *ai.GenerateTextOpts) {
			// The SDK's transport retry is off, for ADR 0002's reason: the
			// effect class is the one thing that decides whether repeating a
			// call is safe, and a retry budget hidden in a library is a
			// second, invisible answer.
			opts.MaxRetries = new(int)
		},
	}

	result, genErr := a.Generate(ctx, agent.RunOpts{Prompt: prompt})
	// The guard first: a refusal recorded by a tool is what actually happened,
	// and the SDK's own error (or lack of one) is a description of a loop that
	// carried on around it.
	if refusal := g.err(); refusal != nil {
		return result, refusal
	}
	if genErr != nil {
		// A name the model invented never reached a tool, so it never reached
		// Invoke. It is answered by the SAME check, so the refusal an operator
		// reads is identical either way.
		var noSuch *ai.NoSuchToolError
		if errors.As(genErr, &noSuch) {
			if err := s.actions.Check(noSuch.ToolName); err != nil {
				return nil, err
			}
		}
		return nil, fmt.Errorf("agent: %s/%s: %w", runID, stepID, genErr)
	}
	return result, nil
}

// approve is the ApproveToolCall hook: it routes to the gate and refuses. It
// never approves — an approval is a person's act, and a hook that could say
// yes would be an agent approving itself.
func (s *Step) approve(ctx context.Context, runID, stepID, action string) error {
	if err := s.actions.Check(action); err != nil {
		return err
	}
	return s.requestApproval(ctx, Invocation{RunID: runID, StepID: stepID, Action: action})
}

// guard carries the first refusal out of the SDK's loop.
//
// It holds the FIRST because that is the one that explains the run: everything
// after it happened in a loop that should already have stopped.
type guard struct {
	mu    sync.Mutex
	first error
}

func newGuard() *guard { return &guard{} }

func (g *guard) record(err error) {
	if err == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.first == nil {
		g.first = err
	}
}

func (g *guard) err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.first
}
