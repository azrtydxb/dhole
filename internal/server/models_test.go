package server_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/server"
)

// TestTheDefaultModelFactoryRefusesToInheritACredentialFromTheEnvironment is
// the trap this factory exists to avoid. The provider libraries default their
// API key to os.Getenv, so a factory that simply forwarded whatever it was
// given would silently run on the operator's ambient environment variable —
// the ambient credential ADR 0010 and ADR 0024 both refuse, reached by
// accident.
//
// The credential arrives redeemed or the call does not happen.
func TestTheDefaultModelFactoryRefusesToInheritACredentialFromTheEnvironment(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-ambient-key-that-must-not-be-used")

	_, err := server.DefaultModels(context.Background(), server.ModelRequest{
		TenantID: "t1", Provider: "anthropic", Model: "claude-sonnet-4-5",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "api_key_secret",
		"the refusal must say how to supply the credential")
	require.NotContains(t, err.Error(), "ambient-key-that-must-not-be-used",
		"a refusal must not echo a credential it found")
}

// TestTheDefaultModelFactoryBuildsAClientForEachProviderItKnows is the whole
// of what it does: name a provider, get a client for that model, built with the
// credential this call redeemed.
func TestTheDefaultModelFactoryBuildsAClientForEachProviderItKnows(t *testing.T) {
	for _, name := range []string{"anthropic", "openai"} {
		model, err := server.DefaultModels(context.Background(), server.ModelRequest{
			TenantID: "t1", Provider: name, Model: "some-model", APIKey: "sk-redeemed",
		})
		require.NoError(t, err)
		require.Equal(t, "some-model", model.ModelID())
	}
}

// TestTheDefaultModelFactoryRefusesAProviderItDoesNotKnowByName is the honest
// failure again: a deployment reaching its models through something this
// binary has never heard of supplies its own factory, and is told so.
func TestTheDefaultModelFactoryRefusesAProviderItDoesNotKnowByName(t *testing.T) {
	_, err := server.DefaultModels(context.Background(), server.ModelRequest{
		TenantID: "t1", Provider: "acme-gateway", Model: "some-model", APIKey: "sk-redeemed",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "acme-gateway")
	require.Contains(t, err.Error(), "server.Config.Models",
		"the refusal must name where a deployment supplies its own")
}
