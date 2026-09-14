package cli_test

import (
	"bytes"
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// TestRunApproveRefusesToSendADecisionWithoutAReason: approve and deny alike.
// The server refuses a reason-less decision too, but the command must not even
// send one — a person typing a decision is the one who knows why, and the flag
// they forgot is named right there rather than in a round trip's error.
func TestRunApproveRefusesToSendADecisionWithoutAReason(t *testing.T) {
	for _, extra := range [][]string{nil, {"--deny"}, {"--reason", "   "}, {"--deny", "--reason", ""}} {
		stub := &decidingStub{}
		url := startStubAPI(t, stub)

		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		args := append([]string{"--server", url, "--token", goodToken, "run", "approve", "run_1", "deploy"}, extra...)
		code := cli.Main(args, out, errOut)

		require.NotZero(t, code, "%v exited zero", extra)
		require.Contains(t, errOut.String(), "--reason", "%v: the refusal must name the missing flag", extra)
		require.Nil(t, stub.received, "%v: a decision with no reason was sent to the server", extra)
	}
}

// TestRunApproveSendsAndPrintsTheReason: the reason reaches the request, and
// the line a person reads back names it beside the verdict and the approver.
func TestRunApproveSendsAndPrintsTheReason(t *testing.T) {
	for _, deny := range []bool{false, true} {
		stub := &decidingStub{}
		url := startStubAPI(t, stub)
		const said = "the change freeze runs until Monday"

		args := []string{"--server", url, "--token", goodToken, "run", "approve", "run_1", "deploy", "--reason", said}
		if deny {
			args = append(args, "--deny")
		}
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		require.Zero(t, cli.Main(args, out, errOut), "stderr: %s", errOut)

		require.NotNil(t, stub.received, "the command reported success without calling DecideApproval")
		require.Equal(t, said, stub.received.GetReason(), "the reason did not reach the request")
		require.Equal(t, !deny, stub.received.GetApproved())
		require.Contains(t, out.String(), said, "the decision was printed without its reason")
		require.Contains(t, out.String(), "tester")
	}
}

// decidingStub records the decision it was sent and answers as the server
// would: the approver is the credential's, the reason the one recorded.
type decidingStub struct {
	dholev1connect.UnimplementedPipelineServiceHandler
	received *dholev1.DecideApprovalRequest
}

func (s *decidingStub) DecideApproval(
	_ context.Context, req *connect.Request[dholev1.DecideApprovalRequest],
) (*connect.Response[dholev1.DecideApprovalResponse], error) {
	s.received = req.Msg
	return connect.NewResponse(&dholev1.DecideApprovalResponse{
		RunId: req.Msg.GetRunId(), StepId: req.Msg.GetStepId(), Approver: "tester",
		Approved: req.Msg.GetApproved(), Reason: req.Msg.GetReason(),
	}), nil
}
