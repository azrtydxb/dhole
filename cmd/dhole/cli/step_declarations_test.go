package cli_test

import (
	"bytes"
	"context"
	"errors"
	"regexp"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// applyingStub answers ApplyOperation by applying the operation with the API's
// own Apply to the definition it holds, so what the CLI prints is what the
// contract does rather than what a stub guessed.
type applyingStub struct {
	dholev1connect.UnimplementedPipelineServiceHandler
	mu       sync.Mutex
	pipeline *dholev1.Pipeline
}

func (s *applyingStub) ApplyOperation(
	_ context.Context, req *connect.Request[dholev1.ApplyOperationRequest],
) (*connect.Response[dholev1.ApplyOperationResponse], error) {
	if req.Header().Get("Authorization") == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no credential"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	next, diff, inverse, err := api.Apply(s.pipeline, req.Msg.GetOperation())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	s.pipeline = next
	return connect.NewResponse(&dholev1.ApplyOperationResponse{
		Revision: &dholev1.Revision{Id: "rev_next", PipelineId: next.GetId()},
		Diff:     diff, Inverse: inverse, Pipeline: next,
	}), nil
}

// TestPipelineApplyDeclaresASecretOnAnExistingStep is the kw finding as a
// person at a terminal meets it: one `pipeline apply` binds a secret on a step
// that already exists, one more declares the capability it needs, and the undo
// line each prints puts the step back exactly as it was (ADR 0028).
func TestPipelineApplyDeclaresASecretOnAnExistingStep(t *testing.T) {
	original := &dholev1.Pipeline{Id: "ci", Steps: []*dholev1.Step{{
		Id: "image", Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
	}}}
	stub := &applyingStub{pipeline: proto.Clone(original).(*dholev1.Pipeline)}
	url := startStubAPI(t, stub)
	undoLine := regexp.MustCompile(`undo with: --operation '(.*)'`)

	apply := func(t *testing.T, operation string) string {
		t.Helper()
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		code := cli.Main([]string{
			"--server", url, "--token", goodToken,
			"pipeline", "apply", "ci", "--base", "rev_1", "--operation", operation,
		}, out, errOut)
		require.Zero(t, code, "stderr: %s", errOut)
		return out.String()
	}

	secret := apply(t, `{"setStepSecret":{"stepId":"image","env":"NEXUS_PASSWORD","name":"nexus-push"}}`)
	require.Contains(t, secret, `bound NEXUS_PASSWORD of step "image" to secret "nexus-push"`)
	capability := apply(t, `{"setStepCapability":{"stepId":"image","capability":"CAPABILITY_SECRETS"}}`)
	require.Contains(t, capability, `declared CAPABILITY_SECRETS on step "image"`)

	landed := stub.pipeline.GetSteps()[0].GetSecrets()
	require.Len(t, landed, 1, "the secret did not land on the existing step")
	require.True(t, proto.Equal(&dholev1.StepSecret{Name: "nexus-push", Env: "NEXUS_PASSWORD"}, landed[0]),
		"the existing step carries %v", landed[0])

	// Undo in reverse order, using exactly the text the CLI printed.
	for _, printed := range []string{capability, secret} {
		match := undoLine.FindStringSubmatch(printed)
		require.Len(t, match, 2, "no undo line in: %s", printed)
		apply(t, match[1])
	}
	require.True(t, proto.Equal(original, stub.pipeline),
		"the printed undo lines did not restore the step:\nwant %v\ngot  %v", original, stub.pipeline)
}
