package cli_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// TestPluginPublishSendsTheManifestItWasRead is the property a publish command
// loses quietly: a file read into a request whose fields are dropped on the
// way looks exactly like a successful publish and leaves the catalog holding
// something else.
//
// It reads a FILE, because @file is how anybody publishing a real manifest
// will use this — a declaration with two JSON Schemas in it does not go on a
// command line.
func TestPluginPublishSendsTheManifestItWasRead(t *testing.T) {
	stub := &publishingStub{}
	url := startStubAPI(t, stub)

	path := filepath.Join(t.TempDir(), "deploy.json")
	require.NoError(t, os.WriteFile(path, []byte(publishableManifest), 0o600))

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"--server", url, "--token", goodToken, "plugin", "publish", "@" + path,
	}, out, errOut)
	require.Zero(t, code, "stderr: %s", errOut)

	sent := stub.received
	require.NotNil(t, sent, "the command reported success without calling PublishPlugin")
	require.Equal(t, "acme", sent.GetNamespace())
	require.Equal(t, "deploy", sent.GetName())
	require.Equal(t, "1.0.0", sent.GetVersion())
	require.Equal(t, "step", sent.GetKind())
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_PURE, sent.GetEffectClass())
	require.Equal(t, "sha256", sent.GetDigest().GetAlgo())
	require.JSONEq(t, `{"type":"object"}`, sent.GetInputSchema(),
		"the input schema did not survive the command that read it")
	require.Contains(t, out.String(), "acme/deploy@1.0.0",
		"a publish that says nothing about what it published is not a report")
}

// publishingStub records the manifest it was sent and answers with the ref the
// catalog would have addressed it by. Everything else refuses.
type publishingStub struct {
	dholev1connect.UnimplementedPipelineServiceHandler
	received *dholev1.Plugin
}

func (s *publishingStub) PublishPlugin(
	_ context.Context, req *connect.Request[dholev1.PublishPluginRequest],
) (*connect.Response[dholev1.PublishPluginResponse], error) {
	s.received = req.Msg.GetPlugin()
	published := s.received
	published.Ref = published.GetNamespace() + "/" + published.GetName() + "@" + published.GetVersion()
	return connect.NewResponse(&dholev1.PublishPluginResponse{Plugin: published}), nil
}
