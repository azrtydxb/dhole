package api

// Multiplayer editing: two halves of one problem, and neither works alone.
//
// THE FIRST HALF IS THE EDIT. Every operation carries the base_revision it was
// made against, and the head moves by compare-and-set, so two planes can never
// both accept an edit against the same base (ADR 0013, and internal/defstore's
// SQLHeads). That check is correct and, on its own, useless to two people
// editing one pipeline: it refuses the second person unconditionally, even
// when their edit touched nothing the first person's did. So an edit whose
// base has moved is REBASED when the two edits are disjoint, and refused when
// they are not. The refusal carries the revision the pipeline is actually at,
// because a client told only "you are stale" can do nothing but re-read blind,
// whereas one handed the newer revision can rebase onto it and show its user
// what changed.
//
// THE SECOND HALF IS PRESENCE. Knowing about a conflict after making it is
// strictly worse than seeing that somebody else is in the step you are about
// to change. Presence is who is here NOW: it lives on an ephemeral bus subject
// and in the memory of the streams that are open, and nothing about it is ever
// stored. A stored presence is a person who appears to be editing a pipeline
// they closed last Tuesday.
//
// TENANCY IS THE SUBJECT, NOT THE HANDLER. Pipeline ids are scoped per tenant,
// so two tenants holding a "deploy" pipeline is ordinary rather than
// adversarial — and a presence subject keyed by pipeline id alone would put
// both tenants' editors on one string. The tenant is therefore IN the subject,
// taken from the credential, exactly as stream.go establishes the right to a
// run's log subject before it opens one.
//
// EXPIRY IS NOT AN OPTIMISATION. A browser that is killed, a laptop that
// sleeps, a proxy that drops a connection without an RST: in none of these
// does the plane holding that editor's stream get to run another line of code
// on their behalf, so nothing publishes their departure. Every live session
// therefore refreshes its own announcement, and one that stops being refreshed
// is reported gone by everyone watching. The clean case — a closed tab, whose
// stream ends — announces itself immediately rather than waiting out the TTL.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
)

const (
	// DefaultPresenceTTL is how long an announcement stands without being
	// refreshed. Three heartbeats fit inside it, so a single lost message
	// does not evict a live editor.
	DefaultPresenceTTL = 9 * time.Second

	// subscribeTimeout bounds establishing the subscription, which is a round
	// trip to the bus rather than something that waits on a person.
	subscribeTimeout = 10 * time.Second

	// PresenceBuffer is how many events are held for one watcher before they
	// are dropped. Presence is best-effort by construction: a slow client
	// loses a cursor position, and the next announcement — or the expiry —
	// puts it right. Blocking the bus's delivery goroutine instead would
	// stall every other subscriber in the process behind one browser.
	PresenceBuffer = 128
)

// PresenceBus is the ephemeral subject presence travels on, narrowed to the
// two calls this file makes. bus.Bus satisfies it.
//
// Nothing here may make presence durable, and the interface says so: there is
// no pull consumer, no stream and no acknowledgement. A presence with a
// delivery guarantee would be a log of who was where, which is a different and
// much more sensitive thing than a cursor.
type PresenceBus interface {
	Publish(ctx context.Context, subject string, msg proto.Message) error
	SubscribeEphemeral(ctx context.Context, subject string, fn func([]byte)) (func(), error)
}

// presenceSubject is where one pipeline's editors announce themselves.
//
// It is built here rather than in internal/bus because it is not part of the
// engine wire contract: no engine subscribes to it, and docs/wire-contract.md
// — which that package mirrors exactly — does not name it. It carries the
// tenant for the reason every subject in this system does: there is no
// unscoped subject, even while only one tenant exists.
func presenceSubject(tenantID, pipelineID string) string {
	return "presence." + tenantID + "." + pipelineID
}

// presenceRecord is one editor as a watcher currently holds them.
type presenceRecord struct {
	event *dholev1.PresenceEvent
	seen  time.Time
}

// WatchPresence streams who else is editing one pipeline.
//
// The caller's own session is not streamed back to it: an editor draws its own
// cursor with its own pointer, and echoing it would put a second one on the
// canvas a frame behind the first.
func (s *Server) WatchPresence(
	ctx context.Context,
	req *connect.Request[dholev1.WatchPresenceRequest],
	stream *connect.ServerStream[dholev1.WatchPresenceResponse],
) error {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return err
	}
	if s.presence == nil {
		return connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a presence subject"))
	}
	pipelineID := req.Msg.GetPipelineId()
	if pipelineID == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}
	// The right to the subject is established HERE, from the definition store,
	// before anything is subscribed — the same order stream.go uses, and for
	// the same reason: the subject itself proves nothing about who may read
	// it. A pipeline of another tenant is indistinguishable from one that does
	// not exist.
	if err := s.mustRead(ctx, p.TenantID, pipelineID); err != nil {
		return err
	}

	subject := presenceSubject(p.TenantID, pipelineID)
	session := req.Msg.GetSessionId()
	ttl := s.presenceTTL
	beat := ttl / 3

	incoming := make(chan *dholev1.PresenceEvent, PresenceBuffer)
	// A DEADLINE on the subscribe, and only on the subscribe: this stream has
	// none of its own — an editor may sit in a pipeline all afternoon — and
	// the bus refuses to confirm a subscription against a context that can
	// never expire, because a confirmation that cannot time out hangs the
	// caller when the server does not answer. The subscription itself outlives
	// this context; it ends with the stream.
	subscribeCtx, subscribed := context.WithTimeout(ctx, subscribeTimeout)
	defer subscribed()
	unsubscribe, err := s.presence.SubscribeEphemeral(subscribeCtx, subject, func(data []byte) {
		event := &dholev1.PresenceEvent{}
		if err := proto.Unmarshal(data, event); err != nil {
			return
		}
		select {
		case incoming <- event:
		default:
			// Dropped deliberately; see PresenceBuffer.
		}
	})
	if err != nil {
		return connect.NewError(connect.CodeUnavailable,
			fmt.Errorf("api: subscribing to presence: %w", err))
	}
	defer unsubscribe()

	// The caller's own announcement goes out HERE, immediately after the
	// subscription exists and before anything else can happen. Leaving it to a
	// first UpdatePresence would race the subscription: the subject is
	// ephemeral, so an announcement published a moment too early reaches
	// whoever was already listening and is then never refreshed by anybody —
	// the editor appears for one beat and expires.
	var own *dholev1.PresenceEvent
	if session != "" {
		own = &dholev1.PresenceEvent{
			Principal: p.Subject,
			Selection: req.Msg.GetSelection(),
			Cursor:    req.Msg.GetCursor(),
			SessionId: session,
		}
		if err := s.presence.Publish(ctx, subject, own); err != nil {
			return connect.NewError(connect.CodeUnavailable,
				fmt.Errorf("api: announcing presence: %w", err))
		}
	}

	// Everyone this watcher currently draws, and the announcement it last saw
	// from them. It lives in this stream and nowhere else: two watchers of the
	// same pipeline hold two of these, and when the last stream closes there
	// is nothing left anywhere that remembers anyone was here.
	others := map[string]presenceRecord{}
	// own, above, is what this stream keeps refreshing. It is updated from the
	// subject like anyone else's announcement, because UpdatePresence
	// publishes rather than writing anything down — so a change made against
	// another control plane refreshes here just the same.

	ticker := time.NewTicker(beat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// The clean departure. It is best-effort by definition — if this
			// process is what died, the expiry above is what covers it.
			s.announceDeparture(ctx, subject, session, p.Subject)
			return nil

		case event := <-incoming:
			if event.GetSessionId() == "" {
				continue
			}
			if event.GetSessionId() == session {
				if event.GetGone() {
					own = nil
					continue
				}
				own = event
				continue
			}
			if event.GetGone() {
				delete(others, event.GetSessionId())
			} else {
				others[event.GetSessionId()] = presenceRecord{event: event, seen: s.now()}
			}
			if err := stream.Send(&dholev1.WatchPresenceResponse{Event: event}); err != nil {
				return err
			}

		case <-ticker.C:
			if own != nil {
				if err := s.presence.Publish(ctx, subject, own); err != nil {
					return connect.NewError(connect.CodeUnavailable,
						fmt.Errorf("api: refreshing presence: %w", err))
				}
			}
			for id, record := range others {
				if s.now().Sub(record.seen) <= ttl {
					continue
				}
				delete(others, id)
				// Reported gone with the same handle it was shown under, so a
				// client can drop exactly what it drew.
				if err := stream.Send(&dholev1.WatchPresenceResponse{
					Event: &dholev1.PresenceEvent{
						Principal: record.event.GetPrincipal(),
						SessionId: id,
						Gone:      true,
					},
				}); err != nil {
					return err
				}
			}
		}
	}
}

// UpdatePresence announces this editor's selection and cursor to the others.
//
// The principal is the credential's and is never taken from the request: an
// editor may say where its pointer is, and may not say who it is.
func (s *Server) UpdatePresence(
	ctx context.Context, req *connect.Request[dholev1.UpdatePresenceRequest],
) (*connect.Response[dholev1.UpdatePresenceResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.presence == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a presence subject"))
	}
	pipelineID, session := req.Msg.GetPipelineId(), req.Msg.GetSessionId()
	if pipelineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}
	if session == "" {
		// Without one there is nothing to expire and nothing to tie a
		// departure to, so the announcement could never be withdrawn.
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("api: a session_id is required; an announcement that cannot be withdrawn never expires"))
	}
	if err := s.mustRead(ctx, p.TenantID, pipelineID); err != nil {
		return nil, err
	}

	// Exactly the fields a recipient needs to draw a cursor. The tenant, the
	// kind of credential and everything else identity.Principal holds stay
	// here.
	event := &dholev1.PresenceEvent{
		Principal: p.Subject,
		Selection: req.Msg.GetSelection(),
		Cursor:    req.Msg.GetCursor(),
		SessionId: session,
		Gone:      req.Msg.GetGone(),
	}
	if err := s.presence.Publish(ctx, presenceSubject(p.TenantID, pipelineID), event); err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("api: announcing presence: %w", err))
	}
	return connect.NewResponse(&dholev1.UpdatePresenceResponse{}), nil
}

// announceDeparture publishes one session's exit on a context of its own,
// because the caller's is already cancelled — which is exactly why this is
// being sent.
func (s *Server) announceDeparture(ctx context.Context, subject, session, principal string) {
	if session == "" || s.presence == nil {
		return
	}
	out, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_ = s.presence.Publish(out, subject, &dholev1.PresenceEvent{
		Principal: principal, SessionId: session, Gone: true,
	})
}

// mustRead refuses a pipeline the caller's tenant does not hold, with the same
// answer GetPipeline gives: a caller able to tell "not yours" from "not there"
// would learn which pipelines exist elsewhere.
func (s *Server) mustRead(ctx context.Context, tenantID, pipelineID string) error {
	revisions, err := s.defs.Revisions(ctx, tenantID, pipelineID)
	if err != nil {
		return storeError("list revisions", err)
	}
	if len(revisions) == 0 {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("api: no pipeline %q", pipelineID))
	}
	return nil
}

// --- concurrent edits -----------------------------------------------------

// rebaseOnto decides what happens to an edit whose base the head has moved
// away from, and returns the revision the edit should actually be applied to.
//
// Disjoint edits merge: the operation is applied to the head instead of to the
// base it was written against, and both people keep their change. Overlapping
// ones are refused, because there is no answer to "we both set this property"
// that does not tell one of the two that their change landed when it did not.
func (s *Server) rebaseOnto(
	ctx context.Context, tenantID, pipelineID, base, head string, op *dholev1.Operation,
) (string, error) {
	before, err := s.defs.Get(ctx, tenantID, pipelineID, base)
	if err != nil {
		// A base this store has never held is not a conflict; it is an edit
		// against something that does not exist.
		return "", storeError("get pipeline", err)
	}
	after, err := s.defs.Get(ctx, tenantID, pipelineID, head)
	if err != nil {
		return "", storeError("get pipeline", err)
	}

	touched := touchedBy(op)
	for key := range changedBetween(before, after) {
		if touched[key] {
			return "", s.conflict(ctx, tenantID, head, fmt.Sprintf(
				"pipeline %q moved to revision %s, and that change touches %s as well",
				pipelineID, head, describe(key)))
		}
	}
	return head, nil
}

// conflict is the refusal, with the newer revision ATTACHED rather than only
// named in the sentence. A client that has it can rebase; a client that has to
// parse an error message can only re-read and hope.
func (s *Server) conflict(ctx context.Context, tenantID, head, why string) error {
	err := connect.NewError(connect.CodeAborted, fmt.Errorf("api: revision conflict: %s", why))
	rev, readErr := s.defs.Revision(ctx, tenantID, head)
	if readErr != nil {
		// The conflict is real whether or not the revision can be read back;
		// losing the detail is better than turning it into an internal error.
		return err
	}
	detail, detailErr := connect.NewErrorDetail(wireRevision(rev))
	if detailErr != nil {
		return err
	}
	err.AddDetail(detail)
	return err
}

// touchedBy is what one operation claims, in the vocabulary changedBetween
// answers in.
//
// A "life" key is the step's EXISTENCE and a "step" key is anything about it.
// The distinction is what lets one person wire a step up while another renames
// it — those are independent — while wiring up a step somebody else removed is
// still a conflict rather than an edit that fails confusingly later.
func touchedBy(op *dholev1.Operation) map[string]bool {
	touched := map[string]bool{}
	switch kind := op.GetKind().(type) {
	case *dholev1.Operation_AddStep:
		id := kind.AddStep.GetStep().GetId()
		touched["life:"+id], touched["step:"+id] = true, true
	case *dholev1.Operation_RemoveStep:
		id := kind.RemoveStep.GetStepId()
		touched["life:"+id], touched["step:"+id] = true, true
	case *dholev1.Operation_Rename:
		touched["step:"+kind.Rename.GetStepId()] = true
	case *dholev1.Operation_SetProperty:
		touched["step:"+kind.SetProperty.GetStepId()] = true
	case *dholev1.Operation_SetStepConfig:
		touched["step:"+kind.SetStepConfig.GetStepId()] = true
	case *dholev1.Operation_Connect:
		edge := kind.Connect.GetEdge()
		touched["edge:"+edgeKey(edge)] = true
		touched["life:"+edge.GetFromStep()], touched["life:"+edge.GetToStep()] = true, true
	case *dholev1.Operation_RemoveEdge:
		edge := kind.RemoveEdge.GetEdge()
		touched["edge:"+edgeKey(edge)] = true
		touched["life:"+edge.GetFromStep()], touched["life:"+edge.GetToStep()] = true, true
	default:
		// An operation kind added to the oneof and not listed here claims
		// nothing, which would silently merge everything. Claim the whole
		// pipeline instead: an operation nobody has classified conflicts with
		// any concurrent change, and that is the safe direction.
		touched["pipeline"] = true
	}
	return touched
}

// changedBetween is what somebody else's edits did, in the same vocabulary.
//
// It is computed from the two DEFINITIONS rather than from the operations
// between them, because the operations are not kept: a revision is content, and
// the content is what a rebase has to be correct against.
func changedBetween(before, after *dholev1.Pipeline) map[string]bool {
	changed := map[string]bool{"pipeline": true}

	steps := map[string]*dholev1.Step{}
	for _, step := range before.GetSteps() {
		steps[step.GetId()] = step
	}
	for _, step := range after.GetSteps() {
		old, existed := steps[step.GetId()]
		switch {
		case !existed:
			changed["life:"+step.GetId()], changed["step:"+step.GetId()] = true, true
		case !proto.Equal(old, step):
			changed["step:"+step.GetId()] = true
		}
		delete(steps, step.GetId())
	}
	// Whatever is left was removed.
	for id := range steps {
		changed["life:"+id], changed["step:"+id] = true, true
	}

	edges := map[string]bool{}
	for _, edge := range before.GetEdges() {
		edges[edgeKey(edge)] = true
	}
	for _, edge := range after.GetEdges() {
		if edges[edgeKey(edge)] {
			delete(edges, edgeKey(edge))
			continue
		}
		changed["edge:"+edgeKey(edge)] = true
	}
	for key := range edges {
		changed["edge:"+key] = true
	}
	return changed
}

// edgeKey names one edge unambiguously.
func edgeKey(edge *dholev1.Edge) string {
	return edge.GetFromStep() + "|" + edge.GetFromPort() + "|" +
		edge.GetToStep() + "|" + edge.GetToPort()
}

// describe turns a key back into something a person reads in a refusal.
func describe(key string) string {
	switch {
	case key == "pipeline":
		return "this pipeline"
	case len(key) > 5 && key[:5] == "step:":
		return "step " + key[5:]
	case len(key) > 5 && key[:5] == "life:":
		return "step " + key[5:]
	case len(key) > 5 && key[:5] == "edge:":
		return "the edge " + key[5:]
	default:
		return key
	}
}

// headMoved says whether a store failure was the compare-and-set losing.
func headMoved(err error) bool {
	return errors.Is(err, defstore.ErrHeadMoved)
}
