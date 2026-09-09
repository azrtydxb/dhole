package policy

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
)

// costLimit bounds one rule's evaluation.
//
// CEL is not Turing-complete: it has no recursion and no unbounded loop, so
// every expression terminates (ADR 0019). What it does have is comprehensions,
// and nesting a few of those over modest lists multiplies into work no
// dispatcher should wait for. The cost limit turns that from a stalled
// dispatch into a bounded evaluation error, which — like every other error
// here — denies. A limit of a million is far above any real policy: the rules
// in this repository cost tens.
const costLimit = 1_000_000

// The variable a rule reads, and the keys it holds. This is a versioned public
// contract: tenant-authored policies depend on it, so keys are added, never
// renamed or removed (ADR 0019).
const (
	inputVar = "input"

	keyTier         = "tier"
	keyTenantID     = "tenant_id"
	keySubject      = "subject"
	keyCapabilities = "capabilities"
	keyEffectClass  = "effect_class"
	keyPluginRef    = "plugin_ref"
	keySigned       = "signed"
	keyUpstream     = "upstream"
)

// newEnv builds the CEL environment every policy is compiled in.
//
// `input` is declared as a map rather than a struct so that a rule naming a
// field that does not exist is an evaluation error — which fails closed —
// instead of being silently rewritten to a zero value. Its keys are exactly
// the ones inputMap writes, one per Input field.
func newEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.Variable(inputVar, cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return nil, fmt.Errorf("policy: build cel environment: %w", err)
	}
	return env, nil
}

// inputMap is the activation one Input presents to a rule. Enums are exposed
// by their short name — PRIVILEGED, not CAPABILITY_PRIVILEGED — because that
// is what a policy author writes and what the proto prefix adds nothing to.
func inputMap(in Input) map[string]any {
	caps := make([]string, 0, len(in.Capabilities))
	for _, c := range in.Capabilities {
		caps = append(caps, strings.TrimPrefix(c.String(), "CAPABILITY_"))
	}
	return map[string]any{
		keyTier:         in.Tier,
		keyTenantID:     in.TenantID,
		keySubject:      in.Subject,
		keyCapabilities: caps,
		keyEffectClass:  strings.TrimPrefix(in.EffectClass.String(), "EFFECT_CLASS_"),
		keyPluginRef:    in.PluginRef,
		keySigned:       in.Signed,
		keyUpstream:     in.Upstream,
	}
}

// Validate compiles every rule of a tier policy without evaluating it. A rule
// that does not parse, or that answers with something other than a bool, is a
// configuration error: caught here it is a message to whoever wrote the policy,
// caught at evaluation it would be an outage shaped like a deny.
func Validate(p TierPolicy) error {
	env, err := newEnv()
	if err != nil {
		return err
	}
	_, err = compile(env, p)
	return err
}

// compiledRule is one rule with its program ready to run.
type compiledRule struct {
	id      string
	reason  string
	program cel.Program
}

// compile turns a tier's rules into programs, in order.
func compile(env *cel.Env, p TierPolicy) ([]compiledRule, error) {
	out := make([]compiledRule, 0, len(p.Rules))
	for _, r := range p.Rules {
		if r.ID == "" {
			return nil, fmt.Errorf("policy: rule with expression %q has no id", r.Expression)
		}
		ast, iss := env.Compile(r.Expression)
		if iss != nil && iss.Err() != nil {
			return nil, fmt.Errorf("policy: rule %q does not compile: %w", r.ID, iss.Err())
		}
		// A rule that answers with something other than yes or no is a
		// configuration error, and this is where it is caught. The type is
		// dyn whenever the answer comes out of the input map — CEL cannot
		// know a map value's type — so dyn is accepted here and checked
		// again on the value at evaluation, where a non-bool denies.
		if out := ast.OutputType(); !out.IsExactType(cel.BoolType) && !out.IsExactType(cel.DynType) {
			return nil, fmt.Errorf("policy: rule %q must return bool, not %s", r.ID, out)
		}
		program, err := env.Program(ast, cel.CostLimit(costLimit))
		if err != nil {
			return nil, fmt.Errorf("policy: rule %q cannot be planned: %w", r.ID, err)
		}
		out = append(out, compiledRule{id: r.ID, reason: r.Reason, program: program})
	}
	return out, nil
}

// cacheKey identifies a compiled rule set. The revision is half of it: keyed on
// the tier alone, an edited policy would keep being evaluated with the program
// compiled from the one it replaced.
type cacheKey struct {
	tier     string
	revision string
}

// CELEngine is the Engine implementation. It is safe for concurrent use: the
// dispatch path evaluates policy from many goroutines at once.
type CELEngine struct {
	env    *cel.Env
	source Source
	audit  Auditor

	mu    sync.RWMutex
	cache map[cacheKey][]compiledRule
}

// Compile-time proof the CEL implementation is the Engine its callers consume.
var _ Engine = (*CELEngine)(nil)

// New returns an engine reading policy from src and recording every decision
// through aud.
func New(src Source, aud Auditor) (*CELEngine, error) {
	if src == nil {
		return nil, fmt.Errorf("policy: nil source")
	}
	if aud == nil {
		return nil, fmt.Errorf("policy: nil auditor")
	}
	env, err := newEnv()
	if err != nil {
		return nil, err
	}
	return &CELEngine{
		env:    env,
		source: src,
		audit:  aud,
		cache:  make(map[cacheKey][]compiledRule),
	}, nil
}

// Evaluate decides one input and records the decision.
//
// Every path but two returns a Decision and a nil error: a missing policy, a
// policy that will not compile and a rule that errors are all denials with a
// reason, not errors the caller has to remember to treat as denials. The two
// exceptions are an empty tenant — a caller bug — and an audit write that
// fails, because a decision nobody can answer for later must not take effect.
func (e *CELEngine) Evaluate(ctx context.Context, in Input) (Decision, error) {
	if in.TenantID == "" {
		return Decision{}, ErrTenantRequired
	}

	decision := e.decide(ctx, in)
	if err := e.audit.Record(ctx, AuditRecord{
		TenantID:  in.TenantID,
		Tier:      in.Tier,
		Subject:   in.Subject,
		Rule:      decision.Rule,
		Reason:    decision.Reason,
		PluginRef: in.PluginRef,
		Allow:     decision.Allow,
	}); err != nil {
		return Decision{
			Rule:   decision.Rule,
			Reason: "policy error: the decision could not be audited",
		}, fmt.Errorf("policy: record audit: %w", err)
	}
	return decision, nil
}

// decide is the evaluation itself, without the audit write.
func (e *CELEngine) decide(ctx context.Context, in Input) Decision {
	rules, err := e.programs(ctx, in)
	if err != nil {
		return Decision{Reason: "policy error: " + err.Error()}
	}
	if len(rules) == 0 {
		return Decision{Reason: fmt.Sprintf(
			"no policy configured for tier %q: denied", in.Tier)}
	}

	activation := map[string]any{inputVar: inputMap(in)}
	for _, r := range rules {
		out, _, err := r.program.ContextEval(ctx, activation)
		if err != nil {
			// A rule that cannot be evaluated has not permitted anything.
			return Decision{
				Rule:   r.id,
				Reason: fmt.Sprintf("policy error: rule %q: %v", r.id, err),
			}
		}
		permitted, ok := boolValue(out)
		if !ok {
			return Decision{
				Rule:   r.id,
				Reason: fmt.Sprintf("policy error: rule %q did not return a bool", r.id),
			}
		}
		if !permitted {
			reason := r.reason
			if reason == "" {
				reason = fmt.Sprintf("rule %q denied", r.id)
			}
			return Decision{Rule: r.id, Reason: reason}
		}
	}
	return Decision{
		Allow:  true,
		Reason: fmt.Sprintf("every rule of tier %q permitted this", in.Tier),
	}
}

// boolValue reads a rule's answer, refusing anything that is not a bool. The
// compile-time check makes this unreachable for a rule this package compiled;
// it stays because an Engine that trusted a non-bool would be admitting on the
// strength of a truthy value.
func boolValue(v ref.Val) (bool, bool) {
	b, ok := v.Value().(bool)
	return b, ok
}

// programs returns the tier's compiled rules, compiling and caching them on
// first use of that (tier, revision).
func (e *CELEngine) programs(ctx context.Context, in Input) ([]compiledRule, error) {
	p, ok, err := e.source.Policy(ctx, in.TenantID, in.Tier)
	if err != nil {
		return nil, err
	}
	if !ok || len(p.Rules) == 0 {
		return nil, nil
	}

	key := cacheKey{tier: in.Tier, revision: p.Revision}
	e.mu.RLock()
	cached, hit := e.cache[key]
	e.mu.RUnlock()
	if hit {
		return cached, nil
	}

	compiled, err := compile(e.env, p)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	e.cache[key] = compiled
	e.mu.Unlock()
	return compiled, nil
}
