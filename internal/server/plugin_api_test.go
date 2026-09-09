package server_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/catalog"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// deploySchema is a plugin author's declaration. Nothing in the control plane
// knows these field names; they exist only in the manifest.
const deploySchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.test/plugins/deploy/1.json",
  "type": "object",
  "properties": {"target": {"type": "string", "title": "target"}},
  "required": ["target"]
}`

// TestAPluginsDeclarationIsReadableThroughTheContract closes the gap Task 47
// worked around: the browser could not read catalog.Manifest.InputSchema at
// all, so the properties panel rendered whatever schema the DEFINITION carried
// inline on a step's port — a copy of the declaration, made when the step was
// authored, free to be stale, and absent for a step whose ports carry none.
//
// Through the served binary, because a handler built in-process proves the
// handler works and says nothing about whether it is mounted.
func TestAPluginsDeclarationIsReadableThroughTheContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)

	// Published straight into the plane's own catalog, because the contract
	// has no PublishPlugin: this is the plane's database, opened as a second
	// process would open it.
	db, err := runstore.OpenSQLite(filepath.Join(dir, "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := catalog.New(db, runstore.DialectSQLite)
	require.NoError(t, store.Publish(ctx, "default", catalog.Manifest{
		Namespace:    "acme",
		Name:         "deploy",
		Version:      "1.0.0",
		Digest:       &dholev1.Digest{Algo: "sha256", Hex: "aa"},
		Kind:         catalog.KindStep,
		EffectClass:  dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
		InputSchema:  []byte(deploySchema),
		OutputSchema: []byte(`{"$id":"https://example.test/plugins/deploy/out.json","type":"object"}`),
		EngineTypes:  []string{"process"},
	}))

	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	// No credential: refused like every other RPC.
	_, err = client.GetPlugin(ctx, connect.NewRequest(&dholev1.GetPluginRequest{
		PluginRef: "acme/deploy@1.0.0",
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	req := connect.NewRequest(&dholev1.GetPluginRequest{PluginRef: "acme/deploy@1.0.0"})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.GetPlugin(ctx, req)
	require.NoError(t, err)

	plugin := res.Msg.GetPlugin()
	require.Equal(t, "acme/deploy@1.0.0", plugin.GetRef())
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, plugin.GetEffectClass())
	require.Equal(t, []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK}, plugin.GetCapabilities())
	require.Equal(t, []string{"process"}, plugin.GetEngineTypes())
	require.JSONEq(t, deploySchema, plugin.GetInputSchema(),
		"the declaration came back changed, so it is not the plugin's own")

	// A reference nobody published, and one that does not parse: two
	// different problems with two different fixes.
	missing := connect.NewRequest(&dholev1.GetPluginRequest{PluginRef: "acme/nothing@1.0.0"})
	missing.Header().Set("Authorization", "Bearer "+token)
	_, err = client.GetPlugin(ctx, missing)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	malformed := connect.NewRequest(&dholev1.GetPluginRequest{PluginRef: "acme/deploy"})
	malformed.Header().Set("Authorization", "Bearer "+token)
	_, err = client.GetPlugin(ctx, malformed)
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}
