package secrets_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/secrets"
)

const stepValue = "correcthorsebatterystaple"

// secretStep is a step asking for one secret, with the capability that makes
// the ask visible to routing and to policy.
func secretStep(decl ...*dholev1.StepSecret) *dholev1.Step {
	return &dholev1.Step{
		Id:           "push",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
		Secrets:      decl,
	}
}

func harborSource() *secrets.MapSource {
	src := secrets.NewMapSource()
	src.Set("acme", "harbor-robot", stepValue)
	return src
}

var attemptOne = secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "push", Attempt: 1}

// TestAStepIssuerMintsOneRedeemableHandlePerDeclaration is the producer half:
// the ref is bound to the ENVIRONMENT VARIABLE the step named, because that is
// what the engine binds the redeemed value to, and it carries no value.
func TestAStepIssuerMintsOneRedeemableHandlePerDeclaration(t *testing.T) {
	broker := secrets.NewBroker()
	issuer := secrets.NewStepIssuer(broker, harborSource())
	step := secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"})

	require.NoError(t, issuer.Check(context.Background(), "acme", step))
	refs, err := issuer.Issue(context.Background(), attemptOne, step, time.Minute)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, "REGISTRY_PASSWORD", refs[0].GetName())
	require.NotContains(t, refs[0].String(), stepValue, "a SecretRef carries a handle, never the value")

	value, err := broker.Redeem(refs[0].GetHandle())
	require.NoError(t, err)
	require.Equal(t, stepValue, value)
}

// TestEachAttemptGetsItsOwnHandles is the scoping: a retry must not carry a
// handle an earlier attempt could still spend.
func TestEachAttemptGetsItsOwnHandles(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), harborSource())
	step := secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"})

	first, err := issuer.Issue(context.Background(), attemptOne, step, time.Minute)
	require.NoError(t, err)
	second := attemptOne
	second.Attempt = 2
	again, err := issuer.Issue(context.Background(), second, step, time.Minute)
	require.NoError(t, err)
	require.NotEqual(t, first[0].GetHandle(), again[0].GetHandle())
}

// TestASecretTheTenantDoesNotHoldIsNamedAndNothingIsIssued: another tenant's
// secret of the same name is not this tenant's.
func TestASecretTheTenantDoesNotHoldIsNamedAndNothingIsIssued(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), harborSource())
	step := secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"})

	err := issuer.Check(context.Background(), "globex", step)
	require.Error(t, err)
	require.True(t, errors.Is(err, secrets.ErrNoSecret), "got %v", err)
	require.Contains(t, err.Error(), "harbor-robot")

	scope := attemptOne
	scope.TenantID = "globex"
	_, err = issuer.Issue(context.Background(), scope, step, time.Minute)
	require.ErrorIs(t, err, secrets.ErrNoSecret)
}

// TestAPlaneWithNoStepSecretSourceRefusesByName is the honest default.
func TestAPlaneWithNoStepSecretSourceRefusesByName(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), nil)
	err := issuer.Check(context.Background(), "acme",
		secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"}))
	require.ErrorIs(t, err, secrets.ErrNoSecret)
	require.Contains(t, err.Error(), "harbor-robot")
}

// TestADeclarationWithoutTheSecretsCapabilityIsRefused: the capability is what
// routes the dispatch to an engine that can redeem and what policy sees. A step
// that got a secret without it would be invisible to both.
func TestADeclarationWithoutTheSecretsCapabilityIsRefused(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), harborSource())
	step := secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"})
	step.Capabilities = nil

	err := issuer.Check(context.Background(), "acme", step)
	require.ErrorIs(t, err, secrets.ErrUndeclaredCapability)
	require.Contains(t, err.Error(), "CAPABILITY_SECRETS")
}

// TestAMalformedDeclarationIsRefusedRatherThanBound covers the declarations
// that would bind a value somewhere the step did not mean, or nowhere.
func TestAMalformedDeclarationIsRefusedRatherThanBound(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), harborSource())
	for name, decl := range map[string][]*dholev1.StepSecret{
		"no name":         {{Env: "REGISTRY_PASSWORD"}},
		"no env":          {{Name: "harbor-robot"}},
		"not a variable":  {{Name: "harbor-robot", Env: "REGISTRY PASSWORD"}},
		"leading digit":   {{Name: "harbor-robot", Env: "1PASSWORD"}},
		"one env, twice":  {{Name: "harbor-robot", Env: "P"}, {Name: "harbor-robot", Env: "P"}},
		"nil declaration": {nil},
	} {
		err := issuer.Check(context.Background(), "acme", secretStep(decl...))
		require.Error(t, err, name)
		require.False(t, strings.Contains(err.Error(), stepValue), name)
	}
}

// TestAStepIssuerRefusesAnUnscopedIssue: no unscoped record, even while only
// one tenant exists, and no handle that belongs to no attempt.
func TestAStepIssuerRefusesAnUnscopedIssue(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), harborSource())
	step := secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"})
	for name, scope := range map[string]secrets.Scope{
		"tenant":  {RunID: "run-1", StepID: "push", Attempt: 1},
		"run":     {TenantID: "acme", StepID: "push", Attempt: 1},
		"step":    {TenantID: "acme", RunID: "run-1", Attempt: 1},
		"attempt": {TenantID: "acme", RunID: "run-1", StepID: "push"},
	} {
		_, err := issuer.Issue(context.Background(), scope, step, time.Minute)
		require.Error(t, err, "an issue with no %s was accepted", name)
	}
}

// TestAStepThatDeclaresNothingIsIssuedNothing: the common case costs nothing
// and needs no source at all.
func TestAStepThatDeclaresNothingIsIssuedNothing(t *testing.T) {
	issuer := secrets.NewStepIssuer(secrets.NewBroker(), nil)
	step := &dholev1.Step{Id: "build"}
	require.NoError(t, issuer.Check(context.Background(), "acme", step))
	refs, err := issuer.Issue(context.Background(), attemptOne, step, time.Minute)
	require.NoError(t, err)
	require.Empty(t, refs)
}
