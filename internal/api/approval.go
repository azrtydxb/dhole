package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/steps/approval"
)

// Approvers answers "is this subject a principal of this tenant", which is
// what an approval is verified against before it is recorded.
//
// It is identity.Store narrowed to the one question this file asks, and
// *identity.SQLStore satisfies it directly. Narrow, because the API has no
// business being able to write a credential.
type Approvers interface {
	PrincipalCredential(ctx context.Context, tenantID, subject string) (identity.StoredPrincipal, error)
}

// resumeNothing advances no run.
//
// approval.New requires a Resumer because a gate that opens and does not
// release the run is worse than one that never opened. A plane configured
// with no Advancer re-advances its open runs on its own tick out of the
// store's index, so the decision still takes effect — later, rather than in
// this call. Refusing to serve the RPC at all in that configuration would be
// worse: the gate would be undecidable through the contract for the sake of
// a nil field.
type resumeNothing struct{}

func (resumeNothing) Advance(context.Context, string, string) error { return nil }

// DecideApproval answers an approval gate a run is waiting at, as the
// principal this call's credential authenticated.
//
// It exists because a human gate was decidable only in-process, through
// internal/steps/approval's Go API. Every client of this contract — the GUI,
// the CLI, an agent — could create a pipeline, approve its revision and start
// a run through the contract, and then had no way whatever to release the run
// it had started. A capability the API cannot express is the ADR 0013 failure
// this service exists to refuse.
//
// The approver is never taken from the request. It is the credential's
// subject, for the same reason ApproveRevision's is: an approval attributed
// to whoever the caller named records something that did not happen, and
// "who approved this" is the only question this trail is ever asked.
func (s *Server) DecideApproval(
	ctx context.Context, req *connect.Request[dholev1.DecideApprovalRequest],
) (*connect.Response[dholev1.DecideApprovalResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.runs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a run store"))
	}
	if s.approvers == nil {
		// An approval whose approver was not verified is not an approval, so
		// this server says it cannot serve the call rather than recording an
		// unverified one.
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a principal store, and an unverified approver is not an approver"))
	}
	runID, stepID := req.Msg.GetRunId(), req.Msg.GetStepId()
	if runID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a run_id is required"))
	}
	if stepID == "" {
		// A run may hold more than one gate, so a decision that named no step
		// would be a decision about whichever one happened to be found first.
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a step_id is required"))
	}

	// Built per call, with the tenant from the credential: an approval is an
	// act by a principal OF a tenant, and the same subject in another tenant
	// is a different person. A gate bound to one tenant at start-up would
	// serve every caller as that tenant.
	resume := Advancer(resumeNothing{})
	if s.adv != nil {
		resume = s.adv
	}
	gate, err := approval.New(approval.Config{
		Store:     s.runs,
		TenantID:  p.TenantID,
		Approvers: s.approvers,
		Resume:    resume,
		Now:       s.now,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: decide approval: %w", err))
	}

	if err := gate.Decide(ctx, runID, stepID, p.Subject, req.Msg.GetApproved()); err != nil {
		return nil, decisionError(err)
	}
	return connect.NewResponse(&dholev1.DecideApprovalResponse{
		RunId:    runID,
		StepId:   stepID,
		Approver: p.Subject,
		Approved: req.Msg.GetApproved(),
	}), nil
}

// decisionError maps a refusal from the gate onto a Connect code.
//
// Each of these is a different thing the caller has to do about it, so none
// of them collapses into a generic failure: the double-click needs to be told
// what already stands, and a decision on a gate nobody opened needs to be
// told the gate is not there — an "OK" to either would report a decision that
// never took effect.
func decisionError(err error) error {
	switch {
	case errors.Is(err, approval.ErrUnknownApprover):
		return connect.NewError(connect.CodePermissionDenied,
			fmt.Errorf("api: decide approval: %w", err))
	case errors.Is(err, approval.ErrAlreadyDecided):
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("api: decide approval: %w", err))
	case errors.Is(err, approval.ErrNotAwaiting):
		return connect.NewError(connect.CodeFailedPrecondition,
			fmt.Errorf("api: decide approval: %w", err))
	case errors.Is(err, approval.ErrApproverRequired):
		return connect.NewError(connect.CodeUnauthenticated,
			fmt.Errorf("api: decide approval: %w", err))
	default:
		return connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: decide approval: %w", err))
	}
}
