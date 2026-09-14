package secrets

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrUndeclaredCapability is what a step reports when it declares a secret and
// not CAPABILITY_SECRETS (ADR 0027). The capability is what routes its dispatch
// to an engine able to redeem, and what policy sees in input.capabilities; a
// step handed a secret without it would be invisible to both.
var ErrUndeclaredCapability = errors.New("secrets: a step declaring secrets must declare CAPABILITY_SECRETS")

// Scope is what one issuance is FOR: a tenant's step, in one run, on one
// attempt. Every field is required. A handle minted for no attempt is a handle
// a retry could inherit, and one minted for no tenant is the unscoped record
// this system has none of.
type Scope struct {
	TenantID string
	RunID    string
	StepID   string
	Attempt  uint32
}

// StepIssuer turns the secrets a step DECLARES into the SecretRefs its dispatch
// carries (ADR 0027).
//
// The values come from a Source the operator configured for steps, which is
// deliberately not the plane's own (ADR 0024's, for model credentials): a key
// an operator gave the plane for its model calls is not thereby given to every
// pipeline author in the tenant.
type StepIssuer struct {
	broker *Broker
	source Source
}

// NewStepIssuer returns an issuer minting through b from src. A nil src is a
// plane that holds no step secrets: a step declaring one is refused naming it.
func NewStepIssuer(b *Broker, src Source) *StepIssuer {
	return &StepIssuer{broker: b, source: src}
}

// Check reports whether every secret the step declares can be issued for this
// tenant, without issuing anything. It is what the scheduler asks BEFORE it
// claims a lease or a slot, so a step that can never be dispatched takes
// neither.
//
// The error names the secret, or the declaration at fault, and never a value:
// it is written into the run's log.
func (i *StepIssuer) Check(ctx context.Context, tenantID string, step *dholev1.Step) error {
	if len(step.GetSecrets()) == 0 {
		return nil
	}
	if tenantID == "" {
		return errors.New("secrets: a tenant is required to resolve a step's secrets")
	}
	if err := ValidateDeclarations(step); err != nil {
		return err
	}
	for _, decl := range step.GetSecrets() {
		if _, err := i.value(ctx, tenantID, decl.GetName()); err != nil {
			return err
		}
	}
	return nil
}

// Issue mints one single-use handle per declaration, for this scope, valid for
// ttl, bound to the environment variable the step named — which is what an
// engine binds the redeemed value to.
func (i *StepIssuer) Issue(
	ctx context.Context, scope Scope, step *dholev1.Step, ttl time.Duration,
) ([]*dholev1.SecretRef, error) {
	if len(step.GetSecrets()) == 0 {
		return nil, nil
	}
	switch {
	case scope.TenantID == "", scope.RunID == "", scope.StepID == "", scope.Attempt == 0:
		return nil, fmt.Errorf("secrets: issuing for step %q needs a tenant, a run, a step and an attempt; got %+v",
			step.GetId(), scope)
	case ttl <= 0:
		return nil, errors.New("secrets: a step secret needs a positive expiry")
	case i == nil || i.broker == nil:
		return nil, errors.New("secrets: this control plane has no broker to issue step secrets through")
	}
	if err := ValidateDeclarations(step); err != nil {
		return nil, err
	}
	refs := make([]*dholev1.SecretRef, 0, len(step.GetSecrets()))
	for _, decl := range step.GetSecrets() {
		value, err := i.value(ctx, scope.TenantID, decl.GetName())
		if err != nil {
			return nil, err
		}
		ref, err := i.broker.IssueFor(scope, decl.GetEnv(), value, ttl)
		if err != nil {
			return nil, fmt.Errorf("secrets: issuing a handle for the secret named %q: %w", decl.GetName(), err)
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// Revoke forgets the unspent handles issued for one attempt, when it ends
// (ADR 0030).
func (i *StepIssuer) Revoke(_ context.Context, scope Scope) {
	if i == nil || i.broker == nil {
		return
	}
	i.broker.RevokeAttempt(scope)
}

// RevokeRun forgets the unspent handles of every attempt of one run.
func (i *StepIssuer) RevokeRun(_ context.Context, tenantID, runID string) {
	if i == nil || i.broker == nil {
		return
	}
	i.broker.RevokeRun(tenantID, runID)
}

// Discard forgets handles issued for a dispatch that never committed.
func (i *StepIssuer) Discard(_ context.Context, refs []*dholev1.SecretRef) {
	if i == nil || i.broker == nil || len(refs) == 0 {
		return
	}
	handles := make([]string, 0, len(refs))
	for _, ref := range refs {
		handles = append(handles, ref.GetHandle())
	}
	i.broker.RevokeHandles(handles...)
}

// value reads one secret, naming it in every failure.
func (i *StepIssuer) value(ctx context.Context, tenantID, name string) (string, error) {
	if i == nil || i.source == nil {
		return "", fmt.Errorf("%w: %q — this control plane holds no step secrets (dhole serve --secret)",
			ErrNoSecret, name)
	}
	value, err := i.source.Value(ctx, tenantID, name)
	if err != nil {
		return "", fmt.Errorf("secrets: the secret named %q: %w", name, err)
	}
	return value, nil
}

// ValidateDeclarations refuses a declaration that would bind a value nowhere,
// or somewhere the step did not mean, and a step declaring secrets without
// CAPABILITY_SECRETS.
//
// It is exported so that the API's Validate reports a definition's secret
// declarations in the words the scheduler refuses to dispatch them in — one
// wording, not two that drift (ADR 0028).
func ValidateDeclarations(step *dholev1.Step) error {
	if len(step.GetSecrets()) == 0 {
		return nil
	}
	if !slices.Contains(step.GetCapabilities(), dholev1.Capability_CAPABILITY_SECRETS) {
		return fmt.Errorf("%w: step %q declares %d secret(s)", ErrUndeclaredCapability,
			step.GetId(), len(step.GetSecrets()))
	}
	seen := map[string]bool{}
	for n, decl := range step.GetSecrets() {
		switch {
		case decl.GetName() == "":
			return fmt.Errorf("secrets: step %q secret #%d names no secret", step.GetId(), n+1)
		case !IsEnvName(decl.GetEnv()):
			return fmt.Errorf("secrets: step %q binds the secret named %q to %q, which is not an environment variable name",
				step.GetId(), decl.GetName(), decl.GetEnv())
		case seen[decl.GetEnv()]:
			return fmt.Errorf("secrets: step %q binds two secrets to %s", step.GetId(), decl.GetEnv())
		}
		seen[decl.GetEnv()] = true
	}
	return nil
}

// IsEnvName reports whether s is a portable shell variable name:
// [A-Za-z_][A-Za-z0-9_]*.
func IsEnvName(s string) bool {
	if s == "" {
		return false
	}
	for n, r := range s {
		switch {
		case r == '_', r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9' && n > 0:
		default:
			return false
		}
	}
	return true
}
