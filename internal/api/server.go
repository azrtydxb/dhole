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
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// DefaultPollInterval is how often WatchRun re-reads the run log while it has
// nothing new to send.
const DefaultPollInterval = 250 * time.Millisecond

// Advancer is the scheduler, narrowed to what StartRun needs. A run that is
// recorded and never advanced never starts.
type Advancer interface {
	Advance(ctx context.Context, tenantID, runID string) error
}

// Quotas is the tenant admission check, narrowed to the one question the
// run-creating path asks. *tenancy.Enforcer satisfies it, and so does a fake
// in a test.
//
// It is declared here rather than imported from the scheduler for the same
// reason scheduler.Quotas is declared there: admission is asked wherever the
// thing being admitted happens, and the dependency runs one way — this package
// knows tenancy, and tenancy knows neither this one nor the scheduler.
//
// Only AdmitRun. `max_concurrent_steps` is the scheduler's question, asked
// before every dispatch against the in-flight count the budgets bucket holds;
// this is `max_runs_per_day`, and until it was asked here it was a column
// nobody read.
type Quotas interface {
	// AdmitRun decides whether a run may start and COUNTS it if it may, so
	// the limit is applied to the same number the invoice is made from. It
	// is idempotent on the run id.
	AdmitRun(ctx context.Context, tenantID, runID string) (tenancy.Decision, error)
}

// Heads records each pipeline's current editing head, which is what an edit's
// base_revision is compared against.
//
// The move is a COMPARE-and-set rather than a write. Reading the head and
// then writing the new one is two statements with a window between them, and
// two control planes in that window both find the base current and both
// accept — which is exactly the check base_revision exists to be. Naming the
// head the writer read closes it: the plane that lost writes nothing and is
// told so.
//
// A pipeline with no recorded head is seeded by a move from the empty string,
// and that move loses to whoever seeded it first for the same reason.
type Heads interface {
	// Head returns the pipeline's head revision, and whether one is known.
	Head(ctx context.Context, tenantID, pipelineID string) (string, bool, error)
	// CompareAndSetHead moves the head from `from` to `to`, and does nothing
	// if the head is not `from`. An empty `from` means "no head yet".
	// Returns defstore.ErrHeadMoved when the head was not `from`.
	CompareAndSetHead(ctx context.Context, tenantID, pipelineID, from, to string) error
}

// MemoryHeads is the in-process Heads, and it is a TEST DOUBLE rather than a
// deployment option: it is a real optimistic-concurrency check for one process
// and no check whatever between two, which is the gap defstore.SQLHeads
// closes. A control plane serving real traffic passes the stored one.
//
// Keys carry the tenant, so two tenants editing pipelines of the same id never
// see each other's head.
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

// CompareAndSetHead moves the head only when it is where the caller left it.
func (m *MemoryHeads) CompareAndSetHead(_ context.Context, tenantID, pipelineID, from, to string) error {
	if to == "" {
		return errors.New("api: set head: a revision id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := tenantID + "/" + pipelineID
	current, known := m.heads[key]
	if (known && current != from) || (!known && from != "") {
		return fmt.Errorf("%w: pipeline %q is no longer at %s",
			defstore.ErrHeadMoved, pipelineID, from)
	}
	m.heads[key] = to
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
	// Approvers is the principal table an approval gate's decider is
	// verified against. Optional: without one, DecideApproval says so rather
	// than recording an approval by nobody in particular.
	Approvers Approvers
	// Advancer is handed each new run. Optional: without one, a run is
	// recorded and waits for whatever else advances it.
	Advancer Advancer
	// Quotas admits a run against the tenant's daily limit before it exists.
	// Optional: without one, StartRun enforces no limit and writes no
	// RUN_STARTED row for the ledger to bill from — which is what every
	// deployment did until it was passed one.
	Quotas Quotas
	// Heads tracks the editing head per pipeline. Defaults to MemoryHeads,
	// which is a check within this process only: a deployment running more
	// than one control plane MUST pass defstore.NewHeads.
	Heads Heads
	// Cache answers whether a step's work has been done before. Plan reads
	// it and never writes it.
	Cache CacheReader
	// Fleet is the live engine registry a plan matches steps against, and
	// that EngineService lists.
	Fleet Fleet
	// Drain stops new work reaching one engine. Optional: without one,
	// DrainEngine says so rather than pretending.
	Drain Drainer
	// Control reaches an engine holding a step, which is what makes a
	// cancellation stop the work rather than only the bookkeeping. Optional,
	// with the same rule.
	Control EngineControl
	// Tier is the trust tier steps would be dispatched to, and must match the
	// scheduler's. It is what a plan resolves the environment identity of:
	// every cache key is hashed against the identity that tier's engines
	// announced (ADR 0021), so a plan computed against another tier — or
	// against this plane's own executor, as it once was — reports hits and
	// misses the run would not see.
	Tier string
	// Catalog resolves the plugin a step names, so Validate can report a
	// step whose plugin is unpublished or whose declaration disagrees with
	// it. Optional: without one, Validate checks only the definition.
	Catalog StepResolver
	// CatalogWriter records a published declaration, which is what
	// PublishPlugin does and the only supported way anything reaches the
	// catalog. Optional: without one, PublishPlugin says so rather than
	// accepting a declaration it drops.
	CatalogWriter PluginPublisher
	// OS and Arch are the platform steps are planned for, in Go's
	// GOOS/GOARCH vocabulary, and must match the scheduler's. Empty means
	// the deployment does not care.
	OS   string
	Arch string
	// LiveLogs is the ephemeral log subject the run view tails while a step
	// is running. Optional: without one the log stream serves only the
	// authoritative copy, and says so rather than showing an empty tail.
	LiveLogs LiveLogs
	// LogArchive is the authoritative log the run view falls back to once a
	// step has finished. Optional, with the same rule.
	LogArchive LogArchive
	// Presence is the ephemeral subject multiplayer editors announce
	// themselves on. Optional: without one, WatchPresence says so rather
	// than serving an editor who appears to be alone.
	Presence PresenceBus
	// PresenceTTL overrides DefaultPresenceTTL: how long an announcement
	// stands without being refreshed.
	PresenceTTL time.Duration
	// PollInterval overrides DefaultPollInterval.
	PollInterval time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Server implements PipelineService.
type Server struct {
	defs defstore.Store
	auth identity.Provider
	runs runstore.Store
	// approvers verifies the decider of an approval gate. See approval.go.
	approvers Approvers
	adv       Advancer
	quotas    Quotas
	heads     Heads
	cache     CacheReader
	fleet     Fleet
	drain     Drainer
	// control is the one inbound path to an engine. See control.go.
	control   EngineControl
	tier      string
	cat       StepResolver
	catWriter PluginPublisher
	// live and archive are the two copies of a step's log: the ephemeral
	// subject and the durable object. See stream.go for why both exist.
	live    LiveLogs
	archive LogArchive
	os      string
	arch    string
	// presence is the ephemeral subject editors announce on, and presenceTTL
	// is how long one announcement stands unrefreshed. See presence.go.
	presence    PresenceBus
	presenceTTL time.Duration
	poll        time.Duration
	now         func() time.Time
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
		defs:        cfg.Definitions,
		auth:        cfg.Auth,
		runs:        cfg.Runs,
		approvers:   cfg.Approvers,
		adv:         cfg.Advancer,
		quotas:      cfg.Quotas,
		heads:       cfg.Heads,
		cache:       cfg.Cache,
		fleet:       cfg.Fleet,
		drain:       cfg.Drain,
		control:     cfg.Control,
		tier:        cfg.Tier,
		cat:         cfg.Catalog,
		catWriter:   cfg.CatalogWriter,
		live:        cfg.LiveLogs,
		archive:     cfg.LogArchive,
		os:          cfg.OS,
		arch:        cfg.Arch,
		presence:    cfg.Presence,
		presenceTTL: cfg.PresenceTTL,
		poll:        cfg.PollInterval,
		now:         cfg.Now,
	}
	if s.presenceTTL <= 0 {
		s.presenceTTL = DefaultPresenceTTL
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
	// The fleet, on the same mux and the same credential. A service declared
	// in the contract and served from somewhere else would be a second API.
	mux.Handle(dholev1connect.NewEngineServiceHandler(s, opts...))
	// The two SSE streams a browser holds open. They are plain HTTP because
	// EventSource speaks neither Connect nor gRPC, and they authenticate
	// through the same Server.principal every RPC above uses (stream.go).
	s.registerStreams(mux)
	return mux
}

// CreatePipeline creates a pipeline and writes its first revision.
//
// It exists because nothing else in this service can write one: ApplyOperation
// requires a base_revision, and that requirement is load-bearing rather than
// incidental — an edit that cannot conflict silently overwrites someone
// else's. So creation is its own act, and the alternative of letting an empty
// base mean "create" is refused: a client that lost its base would then read
// as a client starting fresh, in precisely the situation where the two must
// not be confused.
//
// The id is taken; a second create of the same id is refused rather than
// returning the existing pipeline or overwriting it. The refusal is the head
// seeding, which is one statement and therefore holds between control planes
// as well as within one.
func (s *Server) CreatePipeline(
	ctx context.Context, req *connect.Request[dholev1.CreatePipelineRequest],
) (*connect.Response[dholev1.CreatePipelineResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	pipelineID := req.Msg.GetPipelineId()
	if pipelineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a pipeline_id is required"))
	}

	// The friendly refusal. It is a read and therefore racy on its own, which
	// is why the head seeding below is the one that actually decides.
	existing, err := s.defs.Revisions(ctx, p.TenantID, pipelineID)
	if err != nil {
		return nil, storeError("list revisions", err)
	}
	if len(existing) > 0 {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"api: pipeline %q already exists, with %d revision(s)", pipelineID, len(existing)))
	}

	// The definition is built here rather than taken as given: the id is
	// pipeline_id and the tenant is the credential's, because there is no
	// unscoped record in this system and a caller does not get to name the
	// tenant it writes into.
	definition, ok := proto.Clone(req.Msg.GetPipeline()).(*dholev1.Pipeline)
	if !ok || definition == nil {
		definition = &dholev1.Pipeline{}
	}
	definition.Id = pipelineID
	definition.Tenant = &dholev1.Tenant{Id: p.TenantID}

	rev, err := s.defs.Save(ctx, p.TenantID, definition, p.Subject)
	if err != nil {
		return nil, storeError("save revision", err)
	}
	// Seeding the head is what makes the create atomic: two callers racing on
	// the same id both save a revision — they are content-addressed and
	// harmless — and exactly one of them seeds the head. The other is told the
	// id is taken.
	if err := s.heads.CompareAndSetHead(ctx, p.TenantID, pipelineID, "", rev.ID); err != nil {
		if errors.Is(err, defstore.ErrHeadMoved) {
			return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
				"api: pipeline %q already exists", pipelineID))
		}
		return nil, storeError("record head", err)
	}

	return connect.NewResponse(&dholev1.CreatePipelineResponse{
		Pipeline: definition,
		Revision: wireRevision(rev),
	}), nil
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
	// The revision this edit will actually be applied to. It is the caller's
	// base whenever the head is still there, and the head when somebody else
	// has moved it and their edit touched nothing this one touches: two people
	// editing different parts of one pipeline both keep their change, which is
	// the whole of multiplayer editing (presence.go). Overlapping edits are
	// refused there, with the newer revision attached.
	applyTo := base
	if known && head != base {
		applyTo, err = s.rebaseOnto(ctx, p.TenantID, pipelineID, base, head, req.Msg.GetOperation())
		if err != nil {
			return nil, err
		}
	}

	// The definition is read out of the store and edited on a COPY: the
	// revision the caller based its edit on is immutable, and a run that
	// pinned it must read back exactly what it pinned however long it lasts.
	current, err := s.defs.Get(ctx, p.TenantID, pipelineID, applyTo)
	if err != nil {
		return nil, storeError("get pipeline", err)
	}

	next, diff, inverse, err := Apply(current, req.Msg.GetOperation())
	if err != nil {
		if applyTo != base {
			// The edit was disjoint on paper and does not apply in fact.
			// That is a conflict rather than a bad request: the operation was
			// well formed against the revision its author was looking at.
			return nil, s.conflict(ctx, p.TenantID, applyTo, fmt.Sprintf(
				"pipeline %q moved to revision %s, where this edit no longer applies: %v",
				pipelineID, applyTo, err))
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	rev, err := s.defs.Save(ctx, p.TenantID, next, p.Subject)
	if err != nil {
		return nil, storeError("save revision", err)
	}
	// The head moves from exactly the revision this edit was read against.
	// Another plane that moved it in the meantime wins, and this caller is
	// told to rebase rather than having its edit silently overwrite one it
	// never saw — the check within a process the read above makes, held
	// between processes too.
	from := ""
	if known {
		from = applyTo
	}
	if err := s.heads.CompareAndSetHead(ctx, p.TenantID, pipelineID, from, rev.ID); err != nil {
		if headMoved(err) {
			// Somebody moved the head between the read above and this write.
			// The refusal carries wherever it is NOW rather than where this
			// caller last looked, because that is the revision they have to
			// rebase onto.
			current, _, headErr := s.heads.Head(ctx, p.TenantID, pipelineID)
			if headErr != nil {
				return nil, storeError("read head", headErr)
			}
			return nil, s.conflict(ctx, p.TenantID, current, fmt.Sprintf(
				"pipeline %q moved to revision %s while this edit was being applied",
				pipelineID, current))
		}
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
	revs, err := s.defs.Revisions(ctx, p.TenantID, req.Msg.GetPipelineId())
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

	// Admission comes BEFORE the first event, so a refused run leaves nothing
	// behind: no RUN_CREATED for a client to watch, no revision pinned to a
	// run that never was. The id is minted first only because admission is
	// idempotent on it — a re-admission of a run already counted passes, which
	// is what stops a restart killing work the tenant was already charged for.
	if err := s.admitRun(ctx, p.TenantID, runID); err != nil {
		return nil, err
	}

	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: pipelineID, RevisionID: rev.ID,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.runs.Append(ctx, p.TenantID, runstore.Event{
		RunID: runID,
		// Sequence 0: the store allocates it in the same transaction as the
		// write. Reading LastSequence here reads a high-water mark another
		// caller is about to write, and the loser of that race is discarded
		// silently by the idempotent insert.
		Type:    runstore.RunCreated,
		Payload: payload,
		At:      s.now().UTC(),
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

// admitRun asks the tenant's quota whether one more run may start today, and
// counts it when it may.
//
// A refusal is RESOURCE_EXHAUSTED and carries the enforcer's own reason, which
// names the quota, the limit and what has already been used. A generic error
// here would leave the person whose run stopped with nothing to act on and no
// way to tell "you are over your limit", which they can fix, from "the store
// is broken", which they cannot.
func (s *Server) admitRun(ctx context.Context, tenantID, runID string) error {
	if s.quotas == nil {
		return nil
	}
	decision, err := s.quotas.AdmitRun(ctx, tenantID, runID)
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("api: admit run: %w", err))
	}
	if !decision.Allowed {
		return connect.NewError(connect.CodeResourceExhausted, fmt.Errorf("api: %w", decision.Err()))
	}
	return nil
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
	return t == runstore.RunCompleted || t == scheduler.RunFailed || t == runstore.RunCancelled
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
