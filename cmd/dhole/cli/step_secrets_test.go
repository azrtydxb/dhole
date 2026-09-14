package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/secrets"
)

// TestAStepSecretIsNamedForATenantAndItsValueIsRead is the shape of the flag:
// the tenant it belongs to, the name a step declares, and the environment
// variable its value is read from. The tenant is never implied, even with one
// tenant: a secret that belonged to whichever tenant asked would be the ambient
// credential this design refuses.
func TestAStepSecretIsNamedForATenantAndItsValueIsRead(t *testing.T) {
	t.Setenv("HARBOR_ROBOT_PASSWORD", "correcthorsebatterystaple")

	source, err := loadStepSecrets([]string{"default/harbor-robot=HARBOR_ROBOT_PASSWORD"})
	require.NoError(t, err)
	require.NotNil(t, source)

	value, err := source.Value(context.Background(), "default", "harbor-robot")
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)

	_, err = source.Value(context.Background(), "globex", "harbor-robot")
	require.ErrorIs(t, err, secrets.ErrNoSecret, "a secret configured for one tenant reached another")
}

// TestAStepSecretWithNoTenantOrAnUnsetVariableIsRefusedAtStartUp puts the
// failure where the operator is looking rather than in a run days later.
func TestAStepSecretWithNoTenantOrAnUnsetVariableIsRefusedAtStartUp(t *testing.T) {
	t.Setenv("HARBOR_ROBOT_PASSWORD", "correcthorsebatterystaple")
	for _, spec := range []string{
		"harbor-robot=HARBOR_ROBOT_PASSWORD",              // no tenant
		"/harbor-robot=HARBOR_ROBOT_PASSWORD",             // empty tenant
		"Not.A.Tenant/harbor-robot=HARBOR_ROBOT_PASSWORD", // invalid tenant
		"default/=HARBOR_ROBOT_PASSWORD",                  // no name
		"default/harbor-robot=",                           // no variable
		"default/harbor-robot",                            // no separator
		"default/harbor-robot=NOT_SET_ANYWHERE",           // unset variable
	} {
		_, err := loadStepSecrets([]string{spec})
		require.Error(t, err, "spec %q was accepted", spec)
		require.Contains(t, err.Error(), "--secret")
	}
}

// TestAStepSecretDeclaredTwiceIsRefused: the second would silently win.
func TestAStepSecretDeclaredTwiceIsRefused(t *testing.T) {
	t.Setenv("A", "one")
	t.Setenv("B", "two")
	_, err := loadStepSecrets([]string{"default/harbor-robot=A", "default/harbor-robot=B"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "harbor-robot")
}

// TestNoStepSecretsMeansStepsCanBeGivenNone keeps a model credential out of
// steps: the two flags feed two sources, so an operator who configured only a
// model key has not, by upgrading, handed it to every pipeline author.
func TestNoStepSecretsMeansStepsCanBeGivenNone(t *testing.T) {
	source, err := loadStepSecrets(nil)
	require.NoError(t, err)
	require.Nil(t, source)
}
