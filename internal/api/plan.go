package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/catalog"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// Fleet is the live engine registry, narrowed to the one question planning
// asks of it. registry.Registry satisfies it, and so does a list in a test,
// which is the point: matching must be answerable without a bus.
type Fleet interface {
	Instances(ctx context.Context, tenantID string) ([]registry.Instance, error)
}

// CacheReader is the step cache, narrowed to lookup ALONE.
//
// The narrowing is the safeguard. Plan is a question, and a question that
// recorded an entry would change the answer to the next one; a type that
// cannot record cannot record by accident.
type CacheReader interface {
	Lookup(ctx context.Context, tenantID string, key *dholev1.Digest) ([]*dholev1.OutputRef, bool, error)
}

// StepResolver is the catalog, narrowed to the two questions the API asks of
// it: what does this step resolve to, and what does this plugin declare.
// catalog.Store satisfies it.
type StepResolver interface {
	// ResolveStep resolves a step against the plugin it names, applying the
	// effect-class defaulting rule and reporting a widening override.
	ResolveStep(ctx context.Context, tenantID string, step *dholev1.Step) (catalog.Entry, error)
	// Resolve returns what one published reference declares.
	Resolve(ctx context.Context, tenantID, ref string) (catalog.Entry, error)
}

// PluginPublisher is the catalog, narrowed to the one thing publishing needs.
// catalog.Store satisfies it.
//
// It is a second interface rather than a method on StepResolver because the
// two are read and write, and the reader is what Plan and Validate take: a
// resolver that could also publish would let a dry run write to the catalog,
// which is the property Plan exists to not have.
type PluginPublisher interface {
	// Publish records a type. It is idempotent for a byte-identical manifest
	// and returns catalog.ErrVersionExists for one that differs.
	Publish(ctx context.Context, tenantID string, m catalog.Manifest) error
}

// Plan is a dry run: what would execute, in what order, what would be served
// from cache, and which kind of engine each step would land on.
//
// It has NO side effects, and that is the whole endpoint rather than a
// quality of it. Plan is what a person runs to find out what would happen; if
// it dispatched a step, wrote a cache entry or claimed a lease, then asking
// the question would change the answer to it. So nothing here publishes,
// appends to a run log, or records anything: the cache is read through a type
// that cannot write, and the scheduler is consulted through Match, which is
// pure.
func (s *Server) Plan(
	ctx context.Context, req *connect.Request[dholev1.PlanRequest],
) (*connect.Response[dholev1.PlanResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	// Each of these would make the plan a confident lie rather than an
	// incomplete answer: no cache reads as "everything would run", no fleet
	// as "nothing lands anywhere". Refusing says which piece is missing.
	switch {
	case s.cache == nil:
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a cache, and a plan that "+
				"reported every step as a miss would read as a plan"))
	case s.fleet == nil:
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without an engine registry"))
	case s.tier == "":
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a dispatch tier, and a plan "+
				"cannot say what an unknown tier would cache"))
	}

	pipeline, revisionID, err := s.pinned(ctx, p.TenantID, req.Msg.GetPipelineId(), req.Msg.GetRevisionId())
	if err != nil {
		return nil, err
	}

	// A cyclic or malformed pipeline has no execution order at all, so there
	// is no partial plan worth returning: half a plan is the half a reader
	// would believe.
	graph, err := dag.Build(pipeline)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	rev, err := s.defs.Revision(ctx, p.TenantID, revisionID)
	if err != nil {
		return nil, storeError("get revision", err)
	}
	engines, err := s.fleet.Instances(ctx, p.TenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: reading the engine fleet: %w", err))
	}
	// The identity comes from the same place the scheduler takes it from: the
	// tier these steps would be dispatched to, as its engines announced it
	// (ADR 0021). A plan computed against this plane's own executor answered a
	// different question from the one the run would ask — and on a distributed
	// plane, which has no executor at all, it answered none.
	//
	// A tier with no agreed identity is not an error: cache.Eligible then
	// reports it per step, in its own words, exactly as a run would find it.
	tierIdentity, _ := registry.TierEnvironmentIdentity(engines, s.tier)

	steps := make([]*dholev1.PlannedStep, 0, len(pipeline.GetSteps()))
	byID := stepsByID(pipeline)
	// known holds the output digests a step would have, port by port, but
	// only for steps whose outputs are already recorded. A step that would
	// really run produces bytes nobody has seen, so everything downstream of
	// it has inputs no key can be computed from.
	known := make(map[string]map[string]*dholev1.Digest, len(pipeline.GetSteps()))

	for _, level := range graph.TopoLevels() {
		for _, id := range level {
			step := byID[id]
			planned := &dholev1.PlannedStep{StepId: id}

			// The kind comes from the instance that MATCHED, not from this
			// plane's own executor: on a heterogeneous fleet those are two
			// different answers, and the local one is a guess dressed as a
			// fact. An instance that advertised no engine type names none,
			// rather than being credited with this plane's.
			if matched := scheduler.Match(s.requirements(step), engines); len(matched) > 0 {
				planned.EngineKind = kindOf(matched[0])
			}

			// Per step, because a step that names its own image runs in that
			// image rather than in the engine's. A plan computed against the
			// tier's digest for every step would report a hit for a step whose
			// key the run will not even compute the same way.
			envIdentity := cache.StepEnvironment(step, tierIdentity)
			cacheable, reason := cache.Eligible(step, leaseScopeOf(step.GetLeaseScope()), envIdentity)
			planned.NonCacheableReason = reason
			if cacheable {
				outputs, hit, err := s.lookup(ctx, p.TenantID, pipeline, step, envIdentity, rev.Lockfile, known)
				if err != nil {
					return nil, err
				}
				planned.CacheHit = hit
				if hit {
					known[id] = digestsByPort(outputs)
				}
			}
			steps = append(steps, planned)
		}
	}
	return connect.NewResponse(&dholev1.PlanResponse{Steps: steps}), nil
}

// kindOf is the executor backend an instance would run a step on: the first
// kind it advertises. An engine offering several is matched on all of them
// and takes the step on the first it named.
func kindOf(e registry.Instance) string {
	if len(e.EngineTypes) == 0 {
		return ""
	}
	return e.EngineTypes[0]
}

// lookup answers whether one step's work is already recorded.
//
// It can only ask when every input digest is known, which is to say when every
// upstream step was itself a hit. That is not a limitation to work around: a
// step whose predecessor would really run consumes bytes that do not exist
// yet, and a key computed over anything else would either miss forever or —
// far worse — collide with another step's work.
func (s *Server) lookup(
	ctx context.Context,
	tenantID string,
	pipeline *dholev1.Pipeline,
	step *dholev1.Step,
	envIdentity string,
	lockfile map[string]string,
	known map[string]map[string]*dholev1.Digest,
) ([]*dholev1.OutputRef, bool, error) {
	inputs, resolved := inputDigests(pipeline, step, known)
	if !resolved {
		return nil, false, nil
	}
	key, err := cache.Key(step, envIdentity, inputs, lockfile)
	if err != nil {
		// Eligible already agreed the step is cacheable, so this is the two
		// disagreeing rather than an ordinary refusal: report no hit rather
		// than failing the whole plan.
		if errors.Is(err, cache.ErrNotCacheable) {
			return nil, false, nil
		}
		return nil, false, connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: computing the cache key for step %q: %w", step.GetId(), err))
	}
	outputs, hit, err := s.cache.Lookup(ctx, tenantID, key)
	if err != nil {
		return nil, false, connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: cache lookup for step %q: %w", step.GetId(), err))
	}
	return outputs, hit, nil
}

// inputDigests collects the digests feeding a step's input ports, and says
// whether all of them are known. An edge from a step that would really run
// makes the answer false for the whole step.
func inputDigests(
	pipeline *dholev1.Pipeline, step *dholev1.Step, known map[string]map[string]*dholev1.Digest,
) ([]*dholev1.Digest, bool) {
	var inputs []*dholev1.Digest
	// The files the definition carries come first, and they are always known:
	// a revision pins their bytes, so nothing has to run to resolve them.
	files, err := dag.FileInputs(pipeline, step)
	if err != nil {
		// A binding that names no file leaves the key short of an input, and a
		// short key collides with the same step reading nothing. Validate
		// reports this as a diagnostic; here it simply means no key.
		return nil, false
	}
	for _, f := range files {
		inputs = append(inputs, f.GetDigest())
	}
	for _, e := range pipeline.GetEdges() {
		if e.GetToStep() != step.GetId() {
			continue
		}
		outputs, ok := known[e.GetFromStep()]
		if !ok {
			return nil, false
		}
		d, ok := outputs[e.GetFromPort()]
		if !ok {
			return nil, false
		}
		inputs = append(inputs, d)
	}
	return inputs, true
}

// digestsByPort indexes a cache entry's outputs so the next step can find the
// one its edge names.
func digestsByPort(outputs []*dholev1.OutputRef) map[string]*dholev1.Digest {
	out := make(map[string]*dholev1.Digest, len(outputs))
	for _, o := range outputs {
		out[o.GetPort()] = o.GetDigest()
	}
	return out
}

// stepsByID indexes a pipeline's steps.
func stepsByID(pipeline *dholev1.Pipeline) map[string]*dholev1.Step {
	out := make(map[string]*dholev1.Step, len(pipeline.GetSteps()))
	for _, step := range pipeline.GetSteps() {
		out[step.GetId()] = step
	}
	return out
}

// requirements are what a step needs of the place it runs. It must agree with
// the scheduler's own: a plan computed against wider requirements than the
// dispatch uses promises engines that will never take the work.
func (s *Server) requirements(step *dholev1.Step) executor.Requirements {
	return executor.Requirements{
		OS:           s.os,
		Arch:         s.arch,
		Capabilities: step.GetCapabilities(),
		// Straight off the step, in both callers, so the placement the planner
		// reports and the placement the dispatcher performs are the same
		// computation over the same field.
		EngineType: step.GetEngineType(),
	}
}

// leaseScopeOf maps the scope a step declares on the wire to the one
// cacheability is decided against.
//
// An unspecified scope is the step scope, the only default that is safe:
// every other scope deliberately reuses a previous occupant's state, which no
// cache key can see, so a definition that simply did not say must not land in
// one.
func leaseScopeOf(scope dholev1.LeaseScope) executor.LeaseScope {
	switch scope {
	case dholev1.LeaseScope_LEASE_SCOPE_JOB:
		return executor.LeaseJob
	case dholev1.LeaseScope_LEASE_SCOPE_PIPELINE:
		return executor.LeasePipeline
	case dholev1.LeaseScope_LEASE_SCOPE_POOL:
		return executor.LeasePool
	case dholev1.LeaseScope_LEASE_SCOPE_SERVICE:
		return executor.LeaseService
	case dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED, dholev1.LeaseScope_LEASE_SCOPE_STEP:
		return executor.LeaseStep
	default:
		return executor.LeaseStep
	}
}
