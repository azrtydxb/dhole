package server_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
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

// TestAPluginIsPublishedThroughTheContract closes the other half of the same
// gap: a declaration could be READ through GetPlugin and there was no
// supported way to put one there. Not the API, not the CLI, not the git
// mirror — catalog.Publish had no caller outside tests, which is why the
// Playwright suite's seeder grew a /plugin route that wrote the row itself.
//
// It belongs on the contract rather than in a CLI that writes the database,
// because a publish only a process with the plane's file open could perform is
// a capability the GUI and an agent cannot have (ADR 0013).
func TestAPluginIsPublishedThroughTheContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	manifest := &dholev1.Plugin{
		Namespace:    "acme",
		Name:         "publish",
		Version:      "1.0.0",
		Digest:       &dholev1.Digest{Algo: "sha256", Hex: "bb"},
		Kind:         "step",
		EffectClass:  dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
		InputSchema:  deploySchema,
		OutputSchema: `{"$id":"https://example.test/plugins/publish/out.json","type":"object"}`,
		EngineTypes:  []string{"process"},
	}

	// No credential: refused like every other RPC. A publish anybody could
	// make is a way to put a declaration in front of another tenant's steps.
	_, err := client.PublishPlugin(ctx, connect.NewRequest(&dholev1.PublishPluginRequest{
		Plugin: manifest,
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	published, err := publishPlugin(ctx, client, token, manifest)
	require.NoError(t, err)
	require.Equal(t, "acme/publish@1.0.0", published.GetPlugin().GetRef())

	// And it is readable through the contract's own read, out of the plane's
	// catalog rather than out of anything this test wrote.
	read := connect.NewRequest(&dholev1.GetPluginRequest{PluginRef: "acme/publish@1.0.0"})
	read.Header().Set("Authorization", "Bearer "+token)
	got, err := client.GetPlugin(ctx, read)
	require.NoError(t, err)
	require.Equal(t, deploySchema, got.Msg.GetPlugin().GetInputSchema(),
		"the declaration read back is not the one that was published")
	require.Equal(t, []string{"process"}, got.Msg.GetPlugin().GetEngineTypes())

	// Publishing the same bytes again is a no-op, because a deploy is retried.
	_, err = publishPlugin(ctx, client, token, manifest)
	require.NoError(t, err, "an identical republish was refused, so publishing cannot be retried")

	// Publishing DIFFERENT bytes under the same version is refused: a version
	// is immutable, and a mutable one would make every lockfile already
	// written point silently at different code.
	changed := proto.Clone(manifest).(*dholev1.Plugin)
	changed.Digest = &dholev1.Digest{Algo: "sha256", Hex: "cc"}
	_, err = publishPlugin(ctx, client, token, changed)
	require.Error(t, err, "a published version was overwritten")
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

// publishPlugin is one PublishPlugin with a credential on it.
func publishPlugin(
	ctx context.Context, client dholev1connect.PipelineServiceClient,
	token string, plugin *dholev1.Plugin,
) (*dholev1.PublishPluginResponse, error) {
	req := connect.NewRequest(&dholev1.PublishPluginRequest{Plugin: plugin})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.PublishPlugin(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}
