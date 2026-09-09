// Package api serves the one contract the GUI, the CLI and agents all use.
//
// There are no privileged endpoints here. The React canvas is one client of
// PipelineService among the CLI and an agent working alongside the user, and
// an editor-only affordance would be a bug rather than a roadmap item (ADR
// 0013). That is why editing is operation-level rather than document-level:
// every edit is an Operation, and every applied Operation returns the diff it
// produced and the Operation that undoes it, so undo is a property of the API
// instead of a feature each client reimplements.
//
// Every RPC is authenticated and tenant-scoped. The tenant comes from the
// credential and never from the request: there is no unscoped query in this
// system, even while only one tenant exists.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// DefaultPollInterval is how often WatchRun re-reads the run log while it has
// nothing new to send.
const DefaultPollInterval = 250 * time.Millisecond

// RevisionLister is the listing query ListRevisions needs.
//
// It is a separate, optional interface because defstore.Store does not offer
// one yet: it can fetch a revision, the active revision and the definition
// behind either, but it cannot enumerate a pipeline's history. A store that
// cannot list makes ListRevisions answer CodeUnimplemented rather than an
// empty list, because "no revisions" is a lie about a pipeline that has many.
type RevisionLister interface {
	// Revisions returns the pipeline's revisions, oldest first.
	Revisions(ctx context.Context, tenantID, pipelineID string) ([]defstore.Revision, error)
}

// Advancer is the scheduler, narrowed to what StartRun needs. A run that is
// recorded and never advanced never starts.
type Advancer interface {
	Advance(ctx context.Context, tenantID, runID string) error
}

// Heads records each pipeline's current editing head, which is what an edit's
// base_revision is compared against.
//
// It is an interface, and its only implementation here keeps the heads in
// memory, because the definition store has no notion of a head: revisions are
// content-addressed and carry no parent, so "is this base the latest?" cannot
// be answered from the rows as they stand. In one control-plane process this
// is a real optimistic-concurrency check; across several it is not, and it
// must be replaced by a stored head column before the control plane is run
// horizontally. A head this store has not seen is accepted once, seeding it.
type Heads interface {
	// Head returns the pipeline's head revision, and whether one is known.
	Head(ctx context.Context, tenantID, pipelineID string) (string, bool, error)
	// SetHead records a new head.
	SetHead(ctx context.Context, tenantID, pipelineID, revisionID string) error
}

// MemoryHeads is the in-process Heads. Keys carry the tenant, so two tenants
// editing pipelines of the same id never see each other's head.
type MemoryHeads struct {
	mu    sync.Mutex
	heads map[string]string
}

// NewMemoryHeads returns an empty head table.
func NewMemoryHeads() *MemoryHeads {
	return &MemoryHeads{heads: map[string]string{}}
}

// Head returns the recorded head, if there is one.
func (m *MemoryHeads) Head(_ context.Context, tenantID, pipelineID string) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.heads[tenantID+"/"+pipelineID]
	return id, ok, nil
}

// SetHead records the head.
func (m *MemoryHeads) SetHead(_ context.Context, tenantID, pipelineID, revisionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heads[tenantID+"/"+pipelineID] = revisionID
	return nil
}

// Config is everything the API server needs.
type Config struct {
	// Definitions is the canonical store of pipeline definitions.
	Definitions defstore.Store
	// Auth turns a bearer credential into a Principal.
	Auth identity.Provider
	// Runs is the run event log WatchRun follows and StartRun appends to.
	Runs runstore.Store
	// Advancer is handed each new run. Optional: without one, a run is
	// recorded and waits for whatever else advances it.
	Advancer Advancer
	// Heads tracks the editing head per pipeline. Defaults to MemoryHeads.
	Heads Heads
	// Cache answers whether a step's work has been done before. Plan reads
	// it and never writes it.
	Cache CacheReader
	// Fleet is the live engine registry a plan matches steps against.
	Fleet Fleet
	// Environment is where steps would run: the engine kind and the digest
	// every cache key is computed against. An executor.Executor satisfies it.
	Environment Environment
	// Catalog resolves the plugin a step names, so Validate can report a
	// step whose plugin is unpublished or whose declaration disagrees with
	// it. Optional: without one, Validate checks only the definition.
	Catalog StepResolver
	// OS and Arch are the platform steps are planned for, in Go's
	// GOOS/GOARCH vocabulary, and must match the scheduler's. Empty means
	// the deployment does not care.
	OS   string
	Arch string
	// PollInterval overrides DefaultPollInterval.
	PollInterval time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Server implements PipelineService.
type Server struct {
	defs  defstore.Store
	auth  identity.Provider
	runs  runstore.Store
	adv   Advancer
	heads Heads
	cache CacheReader
	fleet Fleet
	env   Environment
	cat   StepResolver
	os    string
	arch  string
	poll  time.Duration
	now   func() time.Time
}

// Compile-time proof that the server serves the whole generated contract: a
// missing RPC fails the build rather than the GUI.
var _ dholev1connect.PipelineServiceHandler = (*Server)(nil)

// NewServer validates the configuration and builds the API server.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Definitions == nil {
		return nil, errors.New("api: a definition store is required")
	}
	if cfg.Auth == nil {
		// A server with no way to authenticate would serve every caller,
		// which is worse than a server that does not start.
		return nil, errors.New("api: an authentication provider is required")
	}

	s := &Server{
		defs:  cfg.Definitions,
		auth:  cfg.Auth,
		runs:  cfg.Runs,
		adv:   cfg.Advancer,
		heads: cfg.Heads,
		cache: cfg.Cache,
		fleet: cfg.Fleet,
		env:   cfg.Environment,
		cat:   cfg.Catalog,
		os:    cfg.OS,
		arch:  cfg.Arch,
		poll:  cfg.PollInterval,
		now:   cfg.Now,
	}
	if s.heads == nil {
		s.heads = NewMemoryHeads()
	}
	if s.poll <= 0 {
		s.poll = DefaultPollInterval
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// Handler mounts the service on its generated route and returns it.
func (s *Server) Handler(opts ...connect.HandlerOption) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(dholev1connect.NewPipelineServiceHandler(s, opts...))
	return mux
}

// GetPipeline reads one revision of one pipeline.
func (s *Server) GetPipeline(
	ctx context.Context, req *connect.Request[dholev1.GetPipelineRequest],
) (*connect.Response[dholev1.GetPipelineResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	pipelineID := req.Msg.GetPipelineId()
	if pipelineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}

	revisionID := req.Msg.GetRevisionId()
	if revisionID == "" {
		revisionID, err = s.headRevision(ctx, p.TenantID, pipelineID)
		if err != nil {
			return nil, err
		}
	}

	definition, err := s.defs.Get(ctx, p.TenantID, pipelineID, revisionID)
	if err != nil {
		return nil, storeError("get pipeline", err)
	}
	rev, err := s.defs.Revision(ctx, p.TenantID, revisionID)
	if err != nil {
		return nil, storeError("get revision", err)
	}
	return connect.NewResponse(&dholev1.GetPipelineResponse{
		Pipeline: definition,
		Revision: wireRevision(rev),
	}), nil
}

// ApplyOperation applies one edit and returns the new revision, the diff and
// the inverse.
func (s *Server) ApplyOperation(
	ctx context.Context, req *connect.Request[dholev1.ApplyOperationRequest],
) (*connect.Response[dholev1.ApplyOperationResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	pipelineID, base := req.Msg.GetPipelineId(), req.Msg.GetBaseRevision()
	if pipelineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}
	if base == "" {
		// An edit with no base cannot conflict, which means it silently
		// overwrites whatever someone else wrote in the meantime.
		return nil, connect.NewError(connect.CodeInvalidArgument,
			errors.New("api: a base_revision is required; an edit that cannot conflict overwrites silently"))
	}

	head, known, err := s.heads.Head(ctx, p.TenantID, pipelineID)
	if err != nil {
		return nil, storeError("read head", err)
	}
	if known && head != base {
		return nil, connect.NewError(connect.CodeAborted, fmt.Errorf(
			"api: revision conflict: pipeline %q is at revision %s, not the %s this edit was made against",
			pipelineID, head, base))
	}

	// The definition is read out of the store and edited on a COPY: the
	// revision the caller based its edit on is immutable, and a run that
	// pinned it must read back exactly what it pinned however long it lasts.
	current, err := s.defs.Get(ctx, p.TenantID, pipelineID, base)
	if err != nil {
		return nil, storeError("get pipeline", err)
	}

	next, diff, inverse, err := Apply(current, req.Msg.GetOperation())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	rev, err := s.defs.Save(ctx, p.TenantID, next, p.Subject)
	if err != nil {
		return nil, storeError("save revision", err)
	}
	if err := s.heads.SetHead(ctx, p.TenantID, pipelineID, rev.ID); err != nil {
		return nil, storeError("record head", err)
	}

	return connect.NewResponse(&dholev1.ApplyOperationResponse{
		Revision: wireRevision(rev),
		Diff:     diff,
		Inverse:  inverse,
		Pipeline: next,
	}), nil
}

// ListRevisions returns a pipeline's revision history.
func (s *Server) ListRevisions(
	ctx context.Context, req *connect.Request[dholev1.ListRevisionsRequest],
) (*connect.Response[dholev1.ListRevisionsResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if req.Msg.GetPipelineId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}
	lister, ok := s.defs.(RevisionLister)
	if !ok {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New(
			"api: the configured definition store cannot list revisions; "+
				"defstore.Store has no history query yet, and an empty list here would be a lie"))
	}
	revs, err := lister.Revisions(ctx, p.TenantID, req.Msg.GetPipelineId())
	if err != nil {
		return nil, storeError("list revisions", err)
	}
	out := make([]*dholev1.Revision, 0, len(revs))
	for _, rev := range revs {
		out = append(out, wireRevision(rev))
	}
	return connect.NewResponse(&dholev1.ListRevisionsResponse{Revisions: out}), nil
}

// ApproveRevision promotes a revision to active, recording the authenticated
// caller as the approver. The approver is never taken from the request: an
// approval attributed to whoever the caller named records something that did
// not happen.
func (s *Server) ApproveRevision(
	ctx context.Context, req *connect.Request[dholev1.ApproveRevisionRequest],
) (*connect.Response[dholev1.ApproveRevisionResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	revisionID := req.Msg.GetRevisionId()
	if revisionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a revision_id is required"))
	}
	if err := s.defs.Approve(ctx, p.TenantID, revisionID, p.Subject); err != nil {
		return nil, storeError("approve revision", err)
	}
	rev, err := s.defs.Revision(ctx, p.TenantID, revisionID)
	if err != nil {
		return nil, storeError("get revision", err)
	}
	return connect.NewResponse(&dholev1.ApproveRevisionResponse{Revision: wireRevision(rev)}), nil
}

// StartRun starts a run of an approved revision.
func (s *Server) StartRun(
	ctx context.Context, req *connect.Request[dholev1.StartRunRequest],
) (*connect.Response[dholev1.StartRunResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.runs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a run store"))
	}
	pipelineID := req.Msg.GetPipelineId()
	if pipelineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}

	rev, err := s.runnableRevision(ctx, p.TenantID, pipelineID, req.Msg.GetRevisionId())
	if err != nil {
		return nil, err
	}

	runID, err := newRunID()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: pipelineID, RevisionID: rev.ID,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	sequence, err := s.runs.LastSequence(ctx, p.TenantID)
	if err != nil {
		return nil, storeError("read run log", err)
	}
	if err := s.runs.Append(ctx, p.TenantID, runstore.Event{
		RunID:    runID,
		Sequence: sequence + 1,
		Type:     runstore.RunCreated,
		Payload:  payload,
		At:       s.now().UTC(),
	}); err != nil {
		return nil, storeError("record run", err)
	}

	if s.adv != nil {
		if err := s.adv.Advance(ctx, p.TenantID, runID); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: advance run: %w", err))
		}
	}
	return connect.NewResponse(&dholev1.StartRunResponse{RunId: runID, RevisionId: rev.ID}), nil
}

// WatchRun streams a run's event log from the beginning and then follows it,
// so a client that connects late misses nothing. The log is the run's only
// position (ADR 0003); this endpoint invents no state of its own.
func (s *Server) WatchRun(
	ctx context.Context,
	req *connect.Request[dholev1.WatchRunRequest],
	stream *connect.ServerStream[dholev1.WatchRunResponse],
) error {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return err
	}
	if s.runs == nil {
		return connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a run store"))
	}
	runID := req.Msg.GetRunId()
	if runID == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("api: a run_id is required"))
	}

	sent := 0
	for {
		events, err := s.runs.Replay(ctx, p.TenantID, runID)
		if err != nil {
			return storeError("replay run", err)
		}
		if sent == 0 && len(events) == 0 {
			// A run of another tenant is indistinguishable from one that
			// does not exist.
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("api: no run %q", runID))
		}
		for _, e := range events[sent:] {
			if err := stream.Send(wireEvent(e)); err != nil {
				return err
			}
			sent++
			if terminal(e.Type) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.poll):
		}
	}
}

// headRevision is the revision an unqualified read resolves to: the editing
// head if one is known, and otherwise the approved revision.
func (s *Server) headRevision(ctx context.Context, tenantID, pipelineID string) (string, error) {
	head, known, err := s.heads.Head(ctx, tenantID, pipelineID)
	if err != nil {
		return "", storeError("read head", err)
	}
	if known {
		return head, nil
	}
	active, err := s.defs.Active(ctx, tenantID, pipelineID)
	if err != nil {
		return "", storeError("get active revision", err)
	}
	return active.ID, nil
}

// runnableRevision resolves the revision a run will pin and refuses one that
// has not been approved. Approval stands in for the review a forge would have
// given (ADR 0008); a draft that could run would make it decorative.
func (s *Server) runnableRevision(
	ctx context.Context, tenantID, pipelineID, revisionID string,
) (defstore.Revision, error) {
	if revisionID == "" {
		rev, err := s.defs.Active(ctx, tenantID, pipelineID)
		if err != nil {
			return defstore.Revision{}, storeError("get active revision", err)
		}
		return rev, nil
	}
	rev, err := s.defs.Revision(ctx, tenantID, revisionID)
	if err != nil {
		return defstore.Revision{}, storeError("get revision", err)
	}
	if rev.PipelineID != pipelineID {
		return defstore.Revision{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"api: revision %s belongs to pipeline %q, not %q", revisionID, rev.PipelineID, pipelineID))
	}
	if rev.State != defstore.StateActive {
		return defstore.Revision{}, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"api: revision %s is %s, and only an active revision may run", revisionID, rev.State))
	}
	return rev, nil
}

// terminal says whether an event ends the run, and so the stream.
func terminal(t runstore.EventType) bool {
	return t == runstore.RunCompleted || t == scheduler.RunFailed
}

// wireRevision is the store's revision as the wire carries it.
func wireRevision(rev defstore.Revision) *dholev1.Revision {
	return &dholev1.Revision{
		Id:          rev.ID,
		PipelineId:  rev.PipelineID,
		ContentHash: rev.ContentHash,
		State:       string(rev.State),
		Lockfile:    rev.Lockfile,
		Author:      rev.Author,
		Approver:    rev.Approver,
	}
}

// wireEvent is one run event as the wire carries it.
func wireEvent(e runstore.Event) *dholev1.WatchRunResponse {
	return &dholev1.WatchRunResponse{
		RunId:      e.RunID,
		StepId:     e.StepID,
		Attempt:    e.Attempt,
		Sequence:   e.Sequence,
		Type:       string(e.Type),
		Payload:    e.Payload,
		AtUnixNano: e.At.UTC().UnixNano(),
	}
}

// storeError maps a store failure onto a Connect code.
//
// A record belonging to another tenant comes back as ErrNotFound and stays
// CodeNotFound here: a caller who could tell "not yours" from "not there"
// would learn which pipelines exist elsewhere.
func storeError(what string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, defstore.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("api: %s: not found", what))
	case errors.Is(err, defstore.ErrNoActiveRevision):
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("api: %s: %w", what, err))
	case errors.Is(err, defstore.ErrSelfApproval):
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("api: %s: %w", what, err))
	default:
		return connect.NewError(connect.CodeInternal, fmt.Errorf("api: %s: %w", what, err))
	}
}

// newRunID mints a run identifier. The bytes come from crypto/rand and
// nowhere else: two runs sharing an id would share an event log.
func newRunID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("api: generate run id: %w", err)
	}
	return "run_" + hex.EncodeToString(buf), nil
}
