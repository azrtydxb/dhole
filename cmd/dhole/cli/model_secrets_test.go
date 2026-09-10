package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/server"
)

// TestAModelSecretIsNamedOnTheCommandLineAndItsValueIsRead is the shape of the
// flag: a secret NAME, and the environment variable its value is read from.
// The value itself is never an argument, because arguments are visible to
// every process on the box through the process table.
func TestAModelSecretIsNamedOnTheCommandLineAndItsValueIsRead(t *testing.T) {
	t.Setenv("MY_ANTHROPIC_KEY", "sk-ant-correcthorsebatterystaple")

	source, err := loadModelSecrets([]string{"ANTHROPIC_API_KEY=MY_ANTHROPIC_KEY"})
	require.NoError(t, err)
	require.NotNil(t, source)

	value, err := source.Value(context.Background(), server.DefaultTenant, "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	require.Equal(t, "sk-ant-correcthorsebatterystaple", value)
}

// TestAModelSecretWhoseEnvironmentVariableIsUnsetIsRefusedAtStartUp puts the
// failure where a person is looking. A plane that started with a credential it
// could not read would fail its first model call instead, minutes or days
// later, with a refusal that by design names nothing.
func TestAModelSecretWhoseEnvironmentVariableIsUnsetIsRefusedAtStartUp(t *testing.T) {
	_, err := loadModelSecrets([]string{"ANTHROPIC_API_KEY=NOT_SET_ANYWHERE"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "NOT_SET_ANYWHERE")
}

// TestAMalformedModelSecretIsRefusedRatherThanIgnored covers the typo. A
// silently ignored declaration is a plane that runs with no credential and
// says nothing about why.
func TestAMalformedModelSecretIsRefusedRatherThanIgnored(t *testing.T) {
	noVariable := "ANTHROPIC_API_KEY" + "="
	noName := "=" + "MY_KEY"
	noSeparator := "ANTHROPIC_API_KEY"
	for _, spec := range []string{noSeparator, noName, noVariable} {
		_, err := loadModelSecrets([]string{spec})
		require.Error(t, err, "spec %q was accepted", spec)
		require.Contains(t, err.Error(), "--model-secret")
	}
}

// TestNoModelSecretsMeansThePlaneCanRedeemNone is the default, and it stays
// honest: a step naming a credential on such a plane fails saying so, rather
// than the plane inventing one.
func TestNoModelSecretsMeansThePlaneCanRedeemNone(t *testing.T) {
	source, err := loadModelSecrets(nil)
	require.NoError(t, err)
	require.Nil(t, source)
}
