package agent_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/steps/agent"
)

// TestAnAgentsDecisionCarriesItsReason: an agent deciding a gate goes through
// the same contract a person does, and the contract requires a reason. An
// invoker that dropped the model's reason on the way would have every agent
// decision refused — or, worse, recorded without the only sentence that says
// why an agent released an at-most-once effect.
func TestAnAgentsDecisionCarriesItsReason(t *testing.T) {
	stub := &decisionRecorder{}
	_, handler := dholev1connect.NewPipelineServiceHandler(stub)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	invoker, err := agent.NewContractInvoker(agent.ContractInvokerConfig{
		HTTPClient: srv.Client(), BaseURL: srv.URL, Credential: "agent-token",
	})
	require.NoError(t, err)

	const said = "the failing check is the flaky one tracked in CI-77"
	out, err := invoker.Invoke(context.Background(), agent.Invocation{
		Action: agent.ActionDecideApproval,
		Args:   json.RawMessage(`{"run_id":"run-1","step_id":"deploy","approved":true,"reason":"` + said + `"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, stub.received, "the invoker never called DecideApproval")
	require.Equal(t, said, stub.received.GetReason(), "the agent's reason did not reach the request")

	var answer struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(out, &answer))
	require.Equal(t, said, answer.Reason, "the model is not told the reason that was recorded")
}

type decisionRecorder struct {
	dholev1connect.UnimplementedPipelineServiceHandler
	received *dholev1.DecideApprovalRequest
}

func (d *decisionRecorder) DecideApproval(
	_ context.Context, req *connect.Request[dholev1.DecideApprovalRequest],
) (*connect.Response[dholev1.DecideApprovalResponse], error) {
	d.received = req.Msg
	return connect.NewResponse(&dholev1.DecideApprovalResponse{
		RunId: req.Msg.GetRunId(), StepId: req.Msg.GetStepId(), Approver: "agent",
		Approved: req.Msg.GetApproved(), Reason: req.Msg.GetReason(),
	}), nil
}
