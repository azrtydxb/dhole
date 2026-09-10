// Package policy is the one place a trust-tier decision is made and the one
// place it is recorded (ADR 0012). The scheduler, the registry, the dispatcher
// and the secret resolver all ask the same question here, so "why was this
// allowed" has a single answer from a single log rather than four subtly
// divergent notions of "trusted".
//
// Policies are written in CEL (ADR 0019): data rather than code, so a tenant
// can supply its own without a rebuild, and non-Turing-complete, so evaluation
// always terminates. The set of variables a rule may read is a versioned
// public contract — see inputMap in cel.go.
//
// Everything here fails closed. A tier with no policy, a policy that will not
// compile, a rule that errors at evaluation, an audit write that fails: each
// one denies. Permission is something a rule has to grant, never something the
// absence of an answer leaves behind.
package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
)

// ErrTenantRequired is returned for an empty tenant. Every decision and every
// audit row carries a tenant scope; an empty one is a bug in the caller, never
// a wildcard.
var ErrTenantRequired = errors.New("tenant scope required")

// Input is the subject of one decision: who is asking, under which tier, and
// what the thing being decided declares about itself. The fields come from a
// plugin manifest and the step that references it, which is what makes effect
// classes and capabilities enforceable rather than advisory (ADR 0012).
type Input struct {
	// Tier is the trust tier the decision is keyed on.
	Tier string
	// TenantID scopes both the decision and its audit row.
	TenantID string
	// Subject names what is being decided — a step, a plugin, a secret.
	Subject string
	// Capabilities are the capabilities the plugin manifest declares.
	Capabilities []dholev1.Capability
	// EffectClass is the effect class the step operates under.
	EffectClass dholev1.EffectClass
	// PluginRef is the reference of the plugin involved.
	PluginRef string
	// Signed reports whether the plugin carries a verified signature.
	Signed bool
	// Upstream is the registry or source the artifact came from.
	Upstream string

	// The taint keys (ADR 0015). They are here because a taint refusal IS a
	// trust-tier decision: "may this untrusted data reach this step, on this
	// engine" is the same question as every other one asked here, and keeping
	// it in Go would make it the one decision an operator cannot read, audit
	// or change.

	// Tainted reports whether any input reaching the subject carries a taint
	// mark. It is the key a rule guards on, so that a rule about untrusted
	// data says nothing about clean data.
	Tainted bool
	// TaintSources are the distinct triggers that admitted the tainted data,
	// sorted, so a rule may name one and a reason may list them.
	TaintSources []string
	// EngineCapabilities are the capabilities the engine the work would run
	// on advertises. They are NOT the step's own: a privileged ENGINE is a
	// host-level foothold whatever the step asked for.
	EngineCapabilities []dholev1.Capability

	// The principal keys (ADR 0025). Taint follows the CREDENTIAL as well as
	// the value: an agent step acts through the contract as a principal of
	// its tenant, and its token is marked untrusted, so a rule can refuse an
	// at-most-once effect or an unsigned plugin to an agent while allowing it
	// to a person — without every call having to carry provenance.
	//
	// They were added because there was no expression that told the two
	// apart. A rule naming a key the input map does not hold is an evaluation
	// error, which denies, so before these existed an agent and a person were
	// admitted together or refused together and the ADR's own example could
	// not be written.

	// PrincipalKind is the kind of principal asking — identity.Kind's value,
	// "user", "service" or "agent" — or empty when the decision is about a
	// definition rather than a call.
	PrincipalKind string
	// PrincipalUntrusted reports whether the credential itself is untrusted.
	// It is separate from Tainted, which is about the DATA: an agent that has
	// read nothing at all still holds an untrusted token.
	PrincipalUntrusted bool
}

// Decision is the answer, plus enough of the reasoning to answer for it later.
// Rule names the rule that decided; Reason is what that rule says about it.
type Decision struct {
	Allow  bool
	Rule   string
	Reason string
}

// Engine is the evaluation point every caller consults.
//
// Evaluate returns an error only for a caller bug (no tenant) or a failure to
// record the decision. A policy that is missing, broken or errors is not an
// error to the caller: it is a deny, and the reason says so.
type Engine interface {
	Evaluate(ctx context.Context, in Input) (Decision, error)
}

// Rule is one CEL expression with an identity. The expression must evaluate to
// a bool; a rule that returns false denies, and its ID is what the decision and
// the audit row name.
type Rule struct {
	ID         string
	Expression string
	// Reason is what a denial by this rule reports to the caller.
	Reason string
}

// TierPolicy is the ordered rule set for one trust tier, plus the revision it
// belongs to. The revision is part of the compiled-program cache key: without
// it, an edited policy would go on being evaluated with the program compiled
// from the policy it replaced.
type TierPolicy struct {
	Revision string
	Rules    []Rule
}

// Source supplies the policy in force for a tenant's tier. The second result
// reports whether a policy exists at all; false denies.
//
// It is an interface because a hosted deployment reads tenant-authored policy
// from the store while a single-tenant one reads it from configuration
// (ADR 0014, ADR 0019).
type Source interface {
	Policy(ctx context.Context, tenantID, tier string) (TierPolicy, bool, error)
}

// StaticSource is an in-memory Source: the policy a single-tenant deployment
// configures, and what tests drive the engine with. Set compiles every rule
// before accepting it, so a policy that cannot work is rejected where it is
// written rather than denying everything at evaluation time.
type StaticSource struct {
	mu    sync.RWMutex
	tiers map[string]TierPolicy
}

// NewStaticSource returns an empty source. Every tier denies until one is set.
func NewStaticSource() *StaticSource {
	return &StaticSource{tiers: make(map[string]TierPolicy)}
}

// Set installs the policy for a tier, rejecting one that does not compile or
// whose rules do not answer yes or no.
func (s *StaticSource) Set(tier string, p TierPolicy) error {
	if err := Validate(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tiers[tier] = p
	return nil
}

// Policy returns the tier's policy. StaticSource is not tenant-partitioned:
// it is the deployment-wide configuration every tenant is judged by.
func (s *StaticSource) Policy(_ context.Context, tenantID, tier string) (TierPolicy, bool, error) {
	if tenantID == "" {
		return TierPolicy{}, false, ErrTenantRequired
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.tiers[tier]
	return p, ok, nil
}

// DeniedError is what a guarded operation returns when policy refuses it. It
// carries the decision so the caller can report which rule refused and why.
type DeniedError struct {
	Decision Decision
}

func (e *DeniedError) Error() string {
	if e.Decision.Rule == "" {
		return "policy denied: " + e.Decision.Reason
	}
	return fmt.Sprintf("policy denied by rule %q: %s", e.Decision.Rule, e.Decision.Reason)
}

// TierResolver says which trust tier a tenant's work is judged under.
type TierResolver func(tenantID string) string

// FixedTier is the resolver of a deployment with one tier for everyone.
func FixedTier(tier string) TierResolver {
	return func(string) string { return tier }
}

// SaveGuard is the definition-save half of ADR 0012: a plugin's declared
// capabilities and effect class are refused when the pipeline is saved, rather
// than discovered when it runs.
//
// It wraps a defstore.Store rather than living inside it, so the definition
// store keeps knowing nothing about policy and a deployment without policy
// simply does not wrap it.
type SaveGuard struct {
	defstore.Store
	engine Engine
	tier   TierResolver
}

// Compile-time proof the guard is substitutable for the store it wraps.
var _ defstore.Store = (*SaveGuard)(nil)

// NewSaveGuard returns inner, guarded by engine.
func NewSaveGuard(engine Engine, inner defstore.Store, tier TierResolver) *SaveGuard {
	return &SaveGuard{Store: inner, engine: engine, tier: tier}
}

// Save evaluates every step of p before the definition is stored, and refuses
// the whole save if any step is denied. A pipeline is saved whole or not at
// all: storing the permitted half of it would leave a definition nobody wrote.
func (g *SaveGuard) Save(
	ctx context.Context, tenantID string, p *dholev1.Pipeline, author string,
) (defstore.Revision, error) {
	if tenantID == "" {
		return defstore.Revision{}, ErrTenantRequired
	}
	if p == nil {
		return defstore.Revision{}, errors.New("policy: save definition: nil pipeline")
	}
	for _, step := range p.GetSteps() {
		decision, err := g.engine.Evaluate(ctx, Input{
			Tier:         g.tier(tenantID),
			TenantID:     tenantID,
			Subject:      "step:" + step.GetId(),
			Capabilities: step.GetCapabilities(),
			EffectClass:  step.GetEffectClass(),
			PluginRef:    step.GetPluginRef(),
		})
		if err != nil {
			return defstore.Revision{}, err
		}
		if !decision.Allow {
			return defstore.Revision{}, &DeniedError{Decision: decision}
		}
	}
	return g.Store.Save(ctx, tenantID, p, author)
}
