package server

import (
	"context"
	"log/slog"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// acceptResponder is the part of the plane's bus connection acceptance is
// served on.
type acceptResponder interface {
	RespondQueue(
		ctx context.Context, subject, queue string, fn func([]byte) (proto.Message, error),
	) (func(), error)
}

// acceptFunc decides one acceptance request: Scheduler.Accept.
type acceptFunc func(ctx context.Context, st *dholev1.JobStatus) (dholev1.Acceptance, error)

// serveAcceptanceOn answers acceptance requests on plane with accept until the
// returned function is called.
//
// In the queue group bus.AcceptQueue, so each request reaches ONE plane. An
// answer is a lease Renew; served by a plain subscription, every plane on the
// bus renewed the lease and replied for every question an engine asked, and
// the engine read one of the replies (ADR 0033). A plane from before this, still
// subscribed plainly, answers as well during an upgrade — the engine takes the
// first reply, and both are the same compare against the same lease.
//
// Every reply is sent, UNSPECIFIED with the reason included: an engine waiting
// on an at-most-once step retries on it, and one with nothing to retry on
// would wait out its whole timeout for a plane that had already decided it
// could not decide.
func serveAcceptanceOn(
	startCtx, runCtx context.Context, plane acceptResponder, accept acceptFunc, log *slog.Logger,
) (func(), error) {
	return plane.RespondQueue(startCtx, bus.SubjectAcceptWildcard(), bus.AcceptQueue, func(raw []byte) (proto.Message, error) {
		st := &dholev1.JobStatus{}
		if err := proto.Unmarshal(raw, st); err != nil {
			return &dholev1.AcceptReply{Error: "undecodable acceptance request: " + err.Error()}, nil
		}
		ctx, cancel := context.WithTimeout(runCtx, acceptanceTimeout)
		defer cancel()
		verdict, err := accept(ctx, st)
		if err != nil {
			log.Error("answering an acceptance request",
				"run", st.GetRunId(), "step", st.GetStepId(), "error", err)
			return &dholev1.AcceptReply{Error: err.Error()}, nil
		}
		return &dholev1.AcceptReply{Acceptance: verdict}, nil
	})
}
