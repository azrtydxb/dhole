package server_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/approval"
)

// The tenant every embedded plane serves, and the person who decides the gate.
const (
	approvalTenant = "default"
	releaseManager = "release-manager"
)

// gatedPipeline is one approval gate and nothing else.
//
// The step asks for NETWORK, which the embedded plane's process executor does
// not advertise, so no engine matches it and the scheduler records
// STEP_UNSCHEDULABLE instead of dispatching it. That is deliberate: the gate
// is armed by whatever runs the step, and until step types are wired into
// `dhole serve` this test has to arm it itself — which it cannot do before the
// run exists, and the run starts advancing the moment StartRun returns. An
// unmatchable step leaves the run open and undispatched for as long as the
// test needs, without inventing a pause the plane does not have.
func gatedPipeline(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: id,
		Steps: []*dholev1.Step{{
			Id:           "approve",
			Name:         "release approval",
			PluginRef:    "builtin:approval",
			EffectClass:  dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
		}},
	}
}

// planeStore is the plane's own database, opened once as a second process
// would open it — and opened ONCE on purpose. Every open re-runs the
// migrations, and migration 0022 backfills a principal for every token
// subject that has none; a test that reopened the file after minting its
// token would be exercising that backfill and not the mint, and would stay
// green with the mint's own fix removed. Observed while mutation-testing this
// change.
type planeStore struct {
	db    *sql.DB
	store runstore.Store
}

func openPlane(t *testing.T, dir string) planeStore {
	t.Helper()
	path := filepath.Join(dir, "dhole.db")
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return planeStore{db: db, store: store}
}

// issueTokenTheSupportedWay mints a credential exactly as `dhole token issue`
// does: identity.Local.IssueToken against the plane's own database, and
// nothing else. No CreateUser, because the CLI has none to offer — a person
// with a fresh binary has this path and no other.
func (p planeStore) issueTokenTheSupportedWay(ctx context.Context, t *testing.T, subject string) string {
	t.Helper()
	local := identity.NewLocal(identity.NewSQLStoreWithDialect(p.db, runstore.DialectSQLite))
	token, err := local.IssueToken(ctx, identity.Principal{
		TenantID: approvalTenant,
		Subject:  subject,
		Kind:     identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)
	return token
}

// armTheGate appends the STEP_AWAITING_APPROVAL that stops the scheduler
// moving past the step. Arming is the run's job, not a client's — there is no
// RPC for it and there should not be — so the test does what the step type
// will do once `dhole serve` dispatches it.
func (p planeStore) armTheGate(ctx context.Context, t *testing.T, runID, stepID string) {
	t.Helper()
	gate, err := approval.New(approval.Config{
		Store:     p.store,
		TenantID:  approvalTenant,
		Approvers: identity.NewSQLStoreWithDialect(p.db, runstore.DialectSQLite),
		// Arming releases nothing: the run is stopping here, not resuming.
		Resume: noResume{},
	})
	require.NoError(t, err)
	require.NoError(t, gate.Request(ctx, runID, stepID, "ship it?"))
}

// noResume advances nothing: arming a gate releases no run.
type noResume struct{}

func (noResume) Advance(context.Context, string, string) error { return nil }

// startGatedRun creates a pipeline, has it approved by somebody other than its
// author and starts it — all through the served contract, because a run
// started through a back door proves nothing about the contract that has to
// release it.
func startGatedRun(
	ctx context.Context, t *testing.T,
	client dholev1connect.PipelineServiceClient, bootstrap, approverToken, id string,
) string {
	t.Helper()

	create := connect.NewRequest(&dholev1.CreatePipelineRequest{
		PipelineId: id, Pipeline: gatedPipeline(id),
	})
	create.Header().Set("Authorization", "Bearer "+bootstrap)
	created, err := client.CreatePipeline(ctx, create)
	require.NoError(t, err)
	revision := created.Msg.GetRevision().GetId()

	// By the approver, not the author: a revision's author may not approve it.
	approve := connect.NewRequest(&dholev1.ApproveRevisionRequest{RevisionId: revision})
	approve.Header().Set("Authorization", "Bearer "+approverToken)
	_, err = client.ApproveRevision(ctx, approve)
	require.NoError(t, err, "the token the supported path minted was refused by ApproveRevision")

	start := connect.NewRequest(&dholev1.StartRunRequest{PipelineId: id, RevisionId: revision})
	start.Header().Set("Authorization", "Bearer "+bootstrap)
	started, err := client.StartRun(ctx, start)
	require.NoError(t, err)
	require.NotEmpty(t, started.Msg.GetRunId())
	return started.Msg.GetRunId()
}

// decideApproval is one DecideApproval with a credential on it.
func decideApproval(
	ctx context.Context, client dholev1connect.PipelineServiceClient, token string,
	msg *dholev1.DecideApprovalRequest,
) (*dholev1.DecideApprovalResponse, error) {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.DecideApproval(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// watchUntilTerminal follows the run over the wire and returns its whole log.
// WatchRun ends the stream on the run's terminal event, so a run that never
// ends fails this test by the context deadline rather than by hanging forever.
func watchUntilTerminal(
	ctx context.Context, t *testing.T,
	client dholev1connect.PipelineServiceClient, token, runID string,
) []*dholev1.WatchRunResponse {
	t.Helper()
	req := connect.NewRequest(&dholev1.WatchRunRequest{RunId: runID})
	req.Header().Set("Authorization", "Bearer "+token)
	stream, err := client.WatchRun(ctx, req)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	var events []*dholev1.WatchRunResponse
	for stream.Receive() {
		events = append(events, stream.Msg())
	}
	require.NoError(t, stream.Err(), "the run stream ended in an error; %d events seen", len(events))
	return events
}

// TestAnApprovalGateIsDecidedThroughTheServedContractAndReleasesTheRun is the
// gap ADR 0013 could not have: a client could create a pipeline, get its
// revision approved and start a run through the contract, and then had no way
// at all to release the run it had started. The gate was decidable only
// in-process, through internal/steps/approval's Go API — so the GUI, the CLI
// and an agent could each begin something none of them could finish.
//
// It goes through the shipping binary and a real Connect client, because a
// handler built in-process proves the handler works and says nothing about
// whether anything serves it.
func TestAnApprovalGateIsDecidedThroughTheServedContractAndReleasesTheRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	client := apiClient(t, srv)
	bootstrap := srv.BootstrapToken()
	plane := openPlane(t, dir)
	approverToken := plane.issueTokenTheSupportedWay(ctx, t, releaseManager)

	runID := startGatedRun(ctx, t, client, bootstrap, approverToken, "gated")
	plane.armTheGate(ctx, t, runID, "approve")

	// No credential: refused like every other RPC. A gate anybody could open
	// is not a gate.
	_, err := client.DecideApproval(ctx, connect.NewRequest(&dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "approve", Approved: true,
	}))
	require.Error(t, err, "the API decided an approval for a call that carried no credential")
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	decided, err := decideApproval(ctx, client, approverToken, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "approve", Approved: true,
	})
	require.NoError(t, err, "the gate could not be decided through the contract")

	// The approver is the credential's subject and was never in the request:
	// an approval attributed to whoever the caller named records something
	// that did not happen.
	require.Equal(t, releaseManager, decided.GetApprover())
	require.True(t, decided.GetApproved())

	// And the decision RELEASED the run, rather than only being recorded: the
	// step the gate held reaches its verdict and the run ends.
	events := watchUntilTerminal(ctx, t, client, bootstrap, runID)
	require.NotEmpty(t, events)

	var sawDecision, sawSucceeded bool
	for _, e := range events {
		switch e.GetType() {
		case string(approval.StepApprovalDecided):
			sawDecision = true
			decision, err := approval.UnmarshalDecision(e.GetPayload())
			require.NoError(t, err)
			require.Equal(t, releaseManager, decision.Approver,
				"the log names an approver nobody authenticated")
			require.True(t, decision.Approved)
		case string(runstore.StepSucceeded):
			sawSucceeded = e.GetStepId() == "approve"
		case string(runstore.StepDispatched):
			require.NotEqual(t, "approve", e.GetStepId(),
				"the approval gate was dispatched to an engine")
		}
	}
	require.True(t, sawDecision, "the decision made over the wire is not in the run's log")
	require.True(t, sawSucceeded, "the gate was decided and the step behind it never succeeded")
	require.Equal(t, string(runstore.RunCompleted), events[len(events)-1].GetType(),
		"the run did not end after its only gate was approved")

	// The double-click. A second decision is refused rather than answered
	// "done": the two decisions may disagree, and the second decider would
	// otherwise be told theirs took effect.
	_, err = decideApproval(ctx, client, approverToken, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "approve", Approved: false,
	})
	require.Error(t, err, "a gate with a standing decision was decided a second time")
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// TestATokenFromTheSupportedMintingPathIsAcceptedAsAnApprover is the identity
// half, and it is the sharper of the two: a credential's identity and an
// approver's identity lived in different tables. `dhole token issue` writes
// `tokens`; approval.Decide reads `principals`. So a token minted the only way
// a fresh binary offers authenticated every API call and was then refused —
// "is not a principal of tenant" — by the one subsystem it needed, and the
// operator had no way to act on a refusal about an identity they had just
// created.
//
// Through the wire, with a token and no CreateUser anywhere in sight.
func TestATokenFromTheSupportedMintingPathIsAcceptedAsAnApprover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	client := apiClient(t, srv)
	plane := openPlane(t, dir)
	approverToken := plane.issueTokenTheSupportedWay(ctx, t, releaseManager)

	runID := startGatedRun(ctx, t, client, srv.BootstrapToken(), approverToken, "minted")
	plane.armTheGate(ctx, t, runID, "approve")

	decided, err := decideApproval(ctx, client, approverToken, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "approve", Approved: true,
	})
	require.NoError(t, err,
		"a token from the supported minting path was refused by the approval subsystem")
	require.Equal(t, releaseManager, decided.GetApprover())
}

// TestDecidingAGateNobodyOpenedIsRefused: without it, any caller could mark
// any step of any run succeeded by naming it.
//
// The step it names is an ORDINARY one, and that is the whole test. It used to
// name the gate of a gated pipeline and rely on nothing having armed it yet —
// which held only while `dhole serve` ran no step types at all. Now that the
// plane arms its own gates, that pipeline's gate is legitimately open by the
// time the call lands, and the test was asserting a race rather than the rule
// in its own comment.
func TestDecidingAGateNobodyOpenedIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	client := apiClient(t, srv)
	approverToken := openPlane(t, dir).issueTokenTheSupportedWay(ctx, t, releaseManager)

	runID := startUngatedRun(ctx, t, client, srv.BootstrapToken(), approverToken, "no-gate-here")

	_, err := decideApproval(ctx, client, approverToken, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "work", Approved: true,
	})
	require.Error(t, err, "a step nobody asked an approval for was marked approved")
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// startUngatedRun starts a run of a pipeline with no approval gate in it, so
// the step named above is one no approval was ever requested for.
func startUngatedRun(
	ctx context.Context, t *testing.T,
	client dholev1connect.PipelineServiceClient, bootstrap, approverToken, id string,
) string {
	t.Helper()

	pipeline := &dholev1.Pipeline{
		Id: id,
		Steps: []*dholev1.Step{{
			Id:          "work",
			Name:        "ordinary work",
			PluginRef:   `command:{"args":["/bin/sh","-c","true"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		}},
	}

	create := connect.NewRequest(&dholev1.CreatePipelineRequest{PipelineId: id, Pipeline: pipeline})
	create.Header().Set("Authorization", "Bearer "+bootstrap)
	created, err := client.CreatePipeline(ctx, create)
	require.NoError(t, err)
	revision := created.Msg.GetRevision().GetId()

	approve := connect.NewRequest(&dholev1.ApproveRevisionRequest{RevisionId: revision})
	approve.Header().Set("Authorization", "Bearer "+approverToken)
	_, err = client.ApproveRevision(ctx, approve)
	require.NoError(t, err)

	start := connect.NewRequest(&dholev1.StartRunRequest{PipelineId: id, RevisionId: revision})
	start.Header().Set("Authorization", "Bearer "+bootstrap)
	started, err := client.StartRun(ctx, start)
	require.NoError(t, err)
	return started.Msg.GetRunId()
}
