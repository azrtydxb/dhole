package api_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/approval"
)

// TestDecideApprovalRefusesADecisionWithNoReason: approve and deny alike. A
// caller that sends no reason — including one written before the field
// existed — is told what is missing, and nothing is recorded: a reason-less
// record of an at-most-once decision is a boolean and a name.
func TestDecideApprovalRefusesADecisionWithNoReason(t *testing.T) {
	ctx := context.Background()
	h := newGateHarness(ctx, t)

	for _, approved := range []bool{true, false} {
		for _, reason := range []string{"", "  \t "} {
			_, err := h.client.DecideApproval(ctx, authed(&dholev1.DecideApprovalRequest{
				RunId: gateRun, StepId: gateStep, Approved: approved, Reason: reason,
			}, tokenAlice))
			require.Error(t, err, "approved=%v with reason %q was accepted", approved, reason)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err),
				"approved=%v with reason %q: %v", approved, reason, err)
			require.Contains(t, err.Error(), "reason",
				"the refusal must say what is missing, not just report a code")
		}
	}

	events, err := h.runs.Replay(ctx, tenantA, gateRun)
	require.NoError(t, err)
	for _, e := range events {
		require.NotEqual(t, approval.StepApprovalDecided, e.Type, "a decision with no reason was recorded")
	}
}

// TestDecideApprovalRecordsAndReturnsTheReason: the reason is on the decision
// event beside the approver the credential resolved to, and the response
// carries it back as it was recorded.
func TestDecideApprovalRecordsAndReturnsTheReason(t *testing.T) {
	for _, approved := range []bool{true, false} {
		ctx := context.Background()
		h := newGateHarness(ctx, t)
		const said = "the scan finding is a false positive, see SEC-114"

		res, err := h.client.DecideApproval(ctx, authed(&dholev1.DecideApprovalRequest{
			RunId: gateRun, StepId: gateStep, Approved: approved, Reason: "  " + said + "\n",
		}, tokenAlice))
		require.NoError(t, err)
		require.Equal(t, "alice", res.Msg.GetApprover())
		require.Equal(t, approved, res.Msg.GetApproved())
		require.Equal(t, said, res.Msg.GetReason(), "the response does not carry the recorded reason")

		events, err := h.runs.Replay(ctx, tenantA, gateRun)
		require.NoError(t, err)
		var found bool
		for _, e := range events {
			if e.Type != approval.StepApprovalDecided {
				continue
			}
			found = true
			decision, err := approval.UnmarshalDecision(e.Payload)
			require.NoError(t, err)
			require.Equal(t, "alice", decision.Approver)
			require.Equal(t, said, decision.Reason,
				"approved=%v: the reason is not on the decision event", approved)
		}
		require.True(t, found, "no decision event was recorded")
	}
}

const (
	gateRun  = "run-gated"
	gateStep = "deploy"
)

// gateHarness is the API over a real run store with one gate armed in it.
type gateHarness struct {
	client dholev1connect.PipelineServiceClient
	runs   runstore.Store
}

func newGateHarness(ctx context.Context, t *testing.T) *gateHarness {
	t.Helper()

	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	adv := &recordingAdvancer{}
	gate, err := approval.New(approval.Config{
		Store: runs, TenantID: tenantA, Approvers: knownApprovers{}, Resume: adv,
	})
	require.NoError(t, err)
	require.NoError(t, gate.Request(ctx, gateRun, gateStep, "deploy to production?"))

	srv, err := api.NewServer(api.Config{
		Definitions:  sqliteDefs(t),
		Auth:         fakeAuth{},
		Runs:         runs,
		Advancer:     adv,
		Approvers:    knownApprovers{},
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	return &gateHarness{
		client: dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL),
		runs:   runs,
	}
}

// knownApprovers says every subject of tenant-a is a principal of it. Who may
// approve is not what these tests are about; the approval package's own tests
// hold that line against a real identity store.
type knownApprovers struct{}

func (knownApprovers) PrincipalCredential(
	_ context.Context, tenantID, subject string,
) (identity.StoredPrincipal, error) {
	if tenantID != tenantA {
		return identity.StoredPrincipal{}, identity.ErrNotFound
	}
	return identity.StoredPrincipal{TenantID: tenantID, Subject: subject, Kind: identity.PrincipalUser}, nil
}
