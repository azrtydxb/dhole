package scheduler_test

import (
	"bytes"
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/obs"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// testEnvIdentity is the environment every step in this file is hashed
// against. It is a constant and not "" on purpose: an empty identity makes
// cache.Eligible refuse every step, and a cache test over that would pass by
// never caching anything.
//
// It reaches the scheduler the only way it can reach one in production: the
// engines of the tier announce it, and the plane reads it off the fleet
// (ADR 0021). It is not configuration, and there is no longer anywhere to
// inject it.
const testEnvIdentity = "sha256:env"

// mutableFleet is the engine registry with the membership in the test's hands,
// so a case can do what a rolling upgrade does: change what the tier's engines
// say about their environment while runs are going through it.
type mutableFleet struct {
	mu        sync.Mutex
	instances []registry.Instance
}

func (f *mutableFleet) Instances(context.Context, string) ([]registry.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.instances), nil
}

func (f *mutableFleet) set(instances ...registry.Instance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.instances = instances
}

// engineIn is one ready engine in a tier, naming the environment it runs steps
// in — or naming none, which is what a host-process engine honestly reports.
func engineIn(id, tier, identity string) registry.Instance {
	e := readyEngine(id)
	e.Tier = tier
	e.EnvironmentIdentity = identity
	return e
}

// countingCache is the REAL cache with a tally. Nothing about the lookup or
// the record is faked — the SQL, the key encoding and the stored bytes are the
// production ones — because a fake here would be free to agree with whatever
// the scheduler did.
type countingCache struct {
	inner *cache.Cache

	mu      sync.Mutex
	lookups int
	records int
}

func (c *countingCache) Lookup(
	ctx context.Context, tenantID string, key *dholev1.Digest,
) ([]*dholev1.OutputRef, bool, error) {
	c.mu.Lock()
	c.lookups++
	c.mu.Unlock()
	return c.inner.Lookup(ctx, tenantID, key)
}

func (c *countingCache) Record(
	ctx context.Context, tenantID string, key *dholev1.Digest, outs []*dholev1.OutputRef,
) error {
	c.mu.Lock()
	c.records++
	c.mu.Unlock()
	return c.inner.Record(ctx, tenantID, key, outs)
}

func (c *countingCache) counts() (lookups, records int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lookups, c.records
}

// mutableRevisions is the lockfile the run's revision pinned, changeable
// between runs so a test can move a plugin under the cache's feet.
type mutableRevisions struct {
	mu       sync.Mutex
	lockfile map[string]string
}

func (r *mutableRevisions) Revision(_ context.Context, tenantID, revisionID string) (defstore.Revision, error) {
	if tenantID == "" {
		return defstore.Revision{}, runstore.ErrTenantRequired
	}
	if revisionID != testRevision {
		return defstore.Revision{}, defstore.ErrNotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	pinned := make(map[string]string, len(r.lockfile))
	for ref, digest := range r.lockfile {
		pinned[ref] = digest
	}
	return defstore.Revision{ID: revisionID, PipelineID: testPipeline, Lockfile: pinned}, nil
}

func (r *mutableRevisions) set(ref, digest string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lockfile[ref] = digest
}

// cacheHarness is a scheduler with the cache actually wired, over a real
// SQLite store, a real CAS on disk and the real KV lease manager.
type cacheHarness struct {
	store  runstore.Store
	db     *sql.DB
	blobs  cas.Store
	bus    *recordingBus
	outbox *outbox.Outbox
	cache  *countingCache
	leases lease.Manager
	revs   *mutableRevisions
	sched  *scheduler.Scheduler
	fleet  *mutableFleet
}

func newCacheHarness(ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline) *cacheHarness {
	t.Helper()
	return newCacheHarnessWithFleet(ctx, t, pipeline,
		&mutableFleet{instances: []registry.Instance{engineIn("e1", testTier, testEnvIdentity)}})
}

func newCacheHarnessWithFleet(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, fleet *mutableFleet,
) *cacheHarness {
	t.Helper()
	return newCacheHarnessLogging(ctx, t, pipeline, fleet, nil)
}

// newCacheHarnessLogging is the same with the scheduler's log in the test's
// hands, so a case can assert what an operator is actually told about a fleet
// that cannot be cached against. That log is the only answer to "why did my
// runs get slower", so how often it says it is a behaviour, not a detail.
func newCacheHarnessLogging(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, fleet *mutableFleet,
	log *slog.Logger,
) *cacheHarness {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "run.db")

	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// The same database as the run log, reached through database/sql: cache
	// entries and blob references are collected with the runs that pin them,
	// so they cannot live somewhere else (ADR 0009).
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	blobs := cas.NewFilesystem(filepath.Join(dir, "cas"))
	refs := &cas.GC{Store: blobs, Runs: store, DB: db, Dialect: runstore.DialectSQLite}

	srv, err := bus.StartEmbedded(filepath.Join(dir, "nats"))
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	entries := &countingCache{inner: cache.New(db, runstore.DialectSQLite)}
	revs := &mutableRevisions{lockfile: map[string]string{"oci://tool": "sha256:tool-v1"}}

	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: staticDefs{pipeline: pipeline},
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		Cache:       entries,
		Revisions:   revs,
		BlobRefs:    refs,
		Log:         log,
	})
	require.NoError(t, err)

	return &cacheHarness{
		store: store, db: db, blobs: blobs, bus: recorder, outbox: ob,
		cache: entries, leases: leases, revs: revs, sched: sched, fleet: fleet,
	}
}

// seed writes the RUN_CREATED event one run starts from.
func (h *cacheHarness) seed(ctx context.Context, t *testing.T, runID string) {
	t.Helper()
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline,
		RevisionID: testRevision,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID:   runID,
		Type:    runstore.RunCreated,
		Payload: payload,
		At:      time.Now().UTC(),
	}))
}

// dispatchedSteps drains the outbox and reports which steps of one run
// actually reached the bus.
func (h *cacheHarness) dispatchedSteps(ctx context.Context, t *testing.T, runID string) []string {
	t.Helper()
	for {
		n, err := h.outbox.Drain(ctx)
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	var ids []string
	for _, d := range h.bus.dispatches(t) {
		if d.GetRunId() == runID {
			ids = append(ids, d.GetStepId())
		}
	}
	return ids
}

// finish reports a step succeeded exactly as an engine would, with real bytes
// in the content-addressed store behind the digest it reports. Cached outputs
// are only reusable if the blobs are still there, so a fabricated digest would
// make every later hit fail for a reason this test never meant to create.
func (h *cacheHarness) finish(ctx context.Context, t *testing.T, runID, stepID, content string) {
	t.Helper()
	digest, err := h.blobs.Put(ctx, testTenant, bytes.NewReader([]byte(content)))
	require.NoError(t, err)

	for _, d := range h.bus.dispatches(t) {
		if d.GetRunId() != runID || d.GetStepId() != stepID {
			continue
		}
		require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
			RunId:      d.GetRunId(),
			StepId:     d.GetStepId(),
			Attempt:    d.GetAttempt(),
			FenceToken: d.GetFenceToken(),
			Phase:      dholev1.Phase_PHASE_SUCCEEDED,
			Outputs:    []*dholev1.OutputRef{{Port: "out", Digest: digest}},
		}))
		return
	}
	t.Fatalf("step %q of run %q was never dispatched", stepID, runID)
}

// referencedBy is the set of blobs the collector believes one run needs. It
// reads the reference index directly because that index IS the contract
// between a run and the collector: cas.GC reads exactly this table to decide
// what may be deleted.
func (h *cacheHarness) referencedBy(ctx context.Context, t *testing.T, runID string) []string {
	t.Helper()
	rows, err := h.db.QueryContext(ctx,
		`SELECT digest FROM blob_refs WHERE tenant_id = ? AND run_id = ?`, testTenant, runID)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var digests []string
	for rows.Next() {
		var digest string
		require.NoError(t, rows.Scan(&digest))
		digests = append(digests, digest)
	}
	require.NoError(t, rows.Err())
	return digests
}

// events is one run's whole log.
func (h *cacheHarness) events(ctx context.Context, t *testing.T, runID string) []runstore.Event {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, runID)
	require.NoError(t, err)
	return events
}

// chain is two pure steps wired a.out -> b.in.
func chain() *dholev1.Pipeline {
	step := func(id string, in bool) *dholev1.Step {
		s := &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   "oci://tool",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{{Name: "out"}},
		}
		if in {
			s.Inputs = []*dholev1.Port{{Name: "in"}}
		}
		return s
	}
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps:  []*dholev1.Step{step("a", false), step("b", true)},
		Edges: []*dholev1.Edge{
			{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "in"},
		},
	}
}

// runChainForReal takes one run of the chain from nothing to completion with
// an engine reporting both steps, and returns the run id.
func (h *cacheHarness) runChainForReal(ctx context.Context, t *testing.T, runID string) string {
	t.Helper()
	h.seed(ctx, t, runID)
	require.NoError(t, h.sched.Advance(ctx, testTenant, runID))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, runID))
	h.finish(ctx, t, runID, "a", "bytes from a")
	require.Equal(t, []string{"a", "b"}, h.dispatchedSteps(ctx, t, runID))
	h.finish(ctx, t, runID, "b", "bytes from b")
	return runID
}

// TestARecordedStepIsServedInsteadOfDispatched is the round trip: what one run
// recorded, the next one reuses without an engine ever seeing the step.
func TestARecordedStepIsServedInsteadOfDispatched(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	first := h.runChainForReal(ctx, t, "run-real")
	_, recorded := h.cache.counts()
	require.Equal(t, 2, recorded, "both pure steps of the first run record what they produced")

	h.seed(ctx, t, "run-cached")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-cached"))

	require.Empty(t, h.dispatchedSteps(ctx, t, "run-cached"),
		"every step of the second run was recorded by the first: none may reach an engine")

	// It finished, and it finished with the first run's outputs.
	second := h.events(ctx, t, "run-cached")
	require.Equal(t, eventKinds(h.events(ctx, t, first)), eventKinds(second),
		"a cached run writes the same events, for the same steps, in the same order")
	require.Equal(t, statusOf(t, h.events(ctx, t, first), "b").GetOutputs(),
		statusOf(t, second, "b").GetOutputs())

	// The blobs it reused are pinned to THIS run. Without that, the run holds
	// nothing: the bytes it displays belong to a run that will age out of the
	// retention window, and the first collection after that empties a run that
	// still looks complete (ADR 0009).
	require.ElementsMatch(t,
		[]string{digestText(statusOf(t, second, "a")), digestText(statusOf(t, second, "b"))},
		h.referencedBy(ctx, t, "run-cached"),
		"a cache hit pins the blobs it handed back, or the collector may take them")

	// And it recorded nothing new: an entry that rewrote itself on every reuse
	// would keep refreshing the timestamp its retention is measured from.
	_, afterCachedRun := h.cache.counts()
	require.Equal(t, recorded, afterCachedRun,
		"a step served from the cache must not re-record the entry it was served from")
}

// TestACacheHitIsReportedAsOneInTheStepDurationMetric: the whole point of the
// cache_hit label on dhole_step_duration_seconds is to separate a build that
// got slower from one that merely ran cold. Until a hit could be served, the
// label had exactly one value everywhere in the system — the engine's constant
// false — and separated nothing.
func TestACacheHitIsReportedAsOneInTheStepDurationMetric(t *testing.T) {
	ctx := testContext(t)
	reader := sdkmetric.NewManualReader()
	shutdown, err := obs.Init(ctx, obs.Config{ServiceName: "dhole-test", MetricReader: reader})
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	h := newCacheHarness(ctx, t, chain())
	h.runChainForReal(ctx, t, "run-real")
	h.seed(ctx, t, "run-cached")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-cached"))
	require.Empty(t, h.dispatchedSteps(ctx, t, "run-cached"))

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &collected))

	hits := 0
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != obs.MetricStepDurationSeconds {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			for _, point := range histogram.DataPoints {
				if value, found := point.Attributes.Value(attribute.Key(obs.AttrCacheHit)); found &&
					value.AsString() == "true" {
					hits += int(point.Count)
					require.Equal(t, testTenant, attrString(point.Attributes, obs.AttrTenant))
				}
			}
		}
	}
	require.Equal(t, 2, hits,
		"both steps of the cached run were served from the cache and must be counted as such")
}

func attrString(set attribute.Set, key string) string {
	value, _ := set.Value(attribute.Key(key))
	return value.AsString()
}

// TestAStepReportedAgainstACacheHitDoesNotRewriteItsOwnEntry covers the guard
// that keeps an entry from refreshing itself forever.
//
// The state it builds — a dispatch marked as a cache hit, followed by a status
// for that same attempt — cannot arise from the scheduler as it stands, because
// a hit writes the dispatch and the success in ONE transaction and no engine
// ever sees the step. It is expressible in the log all the same, which is what
// a replay authorised by a person (ADR 0002) would have to write, and the
// consequence of recording there is not a wasted write: it moves the entry's
// created_at forward on every reuse, so an entry that is only ever REUSED never
// ages out of the retention window it is collected by (ADR 0009).
func TestAStepReportedAgainstACacheHitDoesNotRewriteItsOwnEntry(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	h.runChainForReal(ctx, t, "run-real")
	_, recorded := h.cache.counts()

	// A run whose step a the log says was served from the cache.
	h.seed(ctx, t, "run-replayed")
	token, err := h.leases.Claim(ctx, testTenant, "run-replayed", "a", 1, time.Minute)
	require.NoError(t, err)
	payload, err := scheduler.MarshalDispatched(scheduler.Dispatched{
		Attempt: 1, Fence: token.Fence, Cacheable: true, CacheHit: true,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: "run-replayed", StepID: "a", Attempt: 1,
		Type: runstore.StepDispatched, Payload: payload, At: time.Now().UTC(),
	}))

	digest, err := h.blobs.Put(ctx, testTenant, bytes.NewReader([]byte("bytes from a")))
	require.NoError(t, err)
	require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId:      "run-replayed",
		StepId:     "a",
		Attempt:    1,
		FenceToken: scheduler.EncodeFence(testTenant, token),
		Phase:      dholev1.Phase_PHASE_SUCCEEDED,
		Outputs:    []*dholev1.OutputRef{{Port: "out", Digest: digest}},
	}))

	_, after := h.cache.counts()
	require.Equal(t, recorded, after,
		"a step the log says was served from the cache must not re-record the entry it was served from")
}

// TestAStepWhoseLeaseCarriesStateIsNeverServedFromTheCache is the refusal that
// keeps the cache honest. A pool lease reuses a sandbox a previous occupant
// left state in, and that state is an input no key can see — so an entry
// existing under the step's key must NOT be enough to skip it.
//
// The entry here is planted deliberately, because nothing in the system would
// ever write one: cache.Key does not look at the lease scope, so a scheduler
// that computed a key and trusted it would serve this hit. cache.Eligible is
// the only thing standing between the two.
func TestAStepWhoseLeaseCarriesStateIsNeverServedFromTheCache(t *testing.T) {
	ctx := testContext(t)
	pipeline := chain()
	pipeline.GetSteps()[0].LeaseScope = dholev1.LeaseScope_LEASE_SCOPE_POOL
	h := newCacheHarness(ctx, t, pipeline)

	// An entry under exactly the key step a would be looked up with.
	planted, err := h.blobs.Put(ctx, testTenant, bytes.NewReader([]byte("stale")))
	require.NoError(t, err)
	key, err := cache.Key(pipeline.GetSteps()[0], testEnvIdentity, nil,
		map[string]string{"oci://tool": "sha256:tool-v1"})
	require.NoError(t, err)
	require.NoError(t, h.cache.Record(ctx, testTenant, key, []*dholev1.OutputRef{
		{Port: "out", Digest: planted},
	}))

	h.seed(ctx, t, "run-pooled")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-pooled"))

	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-pooled"),
		"a pool-leased step carries state no key can see, so it runs however much is recorded for it")
	require.False(t, dispatchOf(t, h.events(ctx, t, "run-pooled"), "a").CacheHit)
}

// TestAnEntryWhoseBlobsAreGoneIsNotServed is the other half of ADR 0009's
// refcount reclamation. An entry outliving its bytes is a state the collector
// deliberately tolerates — it drops the entry before the blob precisely so the
// pair can only fail in this direction — and the cost of trusting one is not a
// slow run but a step that "succeeds" with a digest nothing can read, taking
// everything downstream of it with it.
func TestAnEntryWhoseBlobsAreGoneIsNotServed(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	h.runChainForReal(ctx, t, "run-before")

	// The bytes step a produced, collected out from under the entry naming
	// them.
	deleter, ok := h.blobs.(cas.Deleter)
	require.True(t, ok)
	digest := statusOf(t, h.events(ctx, t, "run-before"), "a").GetOutputs()[0].GetDigest()
	require.NoError(t, deleter.Delete(ctx, testTenant, digest))

	h.seed(ctx, t, "run-after")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-after"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-after"),
		"the recorded outputs no longer exist, so the step has to be run for real")
}

// TestAMovedLockfilePinIsAMiss: the key is folded over the pinned plugin
// digests, so upgrading a plugin invalidates every entry it produced. A key
// that ignored the lockfile would serve the OLD plugin's output as the new
// one's, which is the class of wrong answer a cache must never give.
func TestAMovedLockfilePinIsAMiss(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	h.runChainForReal(ctx, t, "run-before")

	// The same definition, the same inputs, the same environment — and a
	// plugin that has moved underneath them.
	h.revs.set("oci://tool", "sha256:tool-v2")

	h.seed(ctx, t, "run-after")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-after"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-after"),
		"the entry was recorded under the old pin and must not answer for the new one")
}

// TestACacheNeedsItsLockfileAndItsReferences refuses a half-wired cache. This
// is the shape the defect took: everything built, one end not connected, and
// nothing that failed to say so.
func TestACacheNeedsItsLockfileAndItsReferences(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	base := func() scheduler.Config {
		return scheduler.Config{
			Store: h.store, Outbox: h.outbox, Leases: noLeases{},
			Fleet: staticFleet{}, Definitions: staticDefs{pipeline: chain()},
			Tier: testTier,
		}
	}

	cfg := base()
	cfg.Cache = h.cache
	_, err := scheduler.New(cfg)
	require.Error(t, err, "a cache with no lockfile resolver keys entries on nothing")

	cfg = base()
	cfg.Cache, cfg.Revisions = h.cache, h.revs
	_, err = scheduler.New(cfg)
	require.Error(t, err, "a cache with no reference recorder builds runs on collectable blobs")

	cfg = base()
	cfg.Revisions = h.revs
	_, err = scheduler.New(cfg)
	require.Error(t, err, "a revision store passed without a cache is a cache that was never wired")
}

// noLeases is a lease manager for the cases that never get as far as claiming
// one.
type noLeases struct{}

func (noLeases) Claim(context.Context, string, string, string, uint32, time.Duration) (lease.Token, error) {
	panic("noLeases: not reached")
}
func (noLeases) Renew(context.Context, lease.Token) error       { panic("noLeases: not reached") }
func (noLeases) Validate(context.Context, lease.Token) error    { panic("noLeases: not reached") }
func (noLeases) Expire(context.Context) ([]lease.Orphan, error) { panic("noLeases: not reached") }

// eventKinds reduces a log to what a reader replaying it sees happen.
func eventKinds(events []runstore.Event) []string {
	kinds := make([]string, 0, len(events))
	for _, e := range events {
		kinds = append(kinds, string(e.Type)+"/"+e.StepID)
	}
	return kinds
}

func statusOf(t *testing.T, events []runstore.Event, stepID string) *dholev1.JobStatus {
	t.Helper()
	for _, e := range events {
		if e.Type == runstore.StepSucceeded && e.StepID == stepID {
			status := &dholev1.JobStatus{}
			require.NoError(t, proto.Unmarshal(e.Payload, status))
			return status
		}
	}
	t.Fatalf("step %q never succeeded", stepID)
	return nil
}

// digestText renders a step's single output digest the way the reference index
// stores it.
func digestText(status *dholev1.JobStatus) string {
	d := status.GetOutputs()[0].GetDigest()
	return d.GetAlgo() + ":" + d.GetHex()
}

func dispatchOf(t *testing.T, events []runstore.Event, stepID string) scheduler.Dispatched {
	t.Helper()
	for _, e := range events {
		if e.Type == runstore.StepDispatched && e.StepID == stepID {
			payload, err := scheduler.UnmarshalDispatched(e.Payload)
			require.NoError(t, err)
			return payload
		}
	}
	t.Fatalf("step %q was never dispatched", stepID)
	return scheduler.Dispatched{}
}

// TestTheEnvironmentACacheKeyIsHashedAgainstComesFromTheTiersEngines is the
// defect ADR 0021 was written for, in the smallest form that shows it.
//
// The plane used to take the identity from an executor of its own, which in
// every shipped configuration was a host process reporting that it has none —
// so cache.Eligible refused every step, `cache_entries` stayed empty, and two
// runs of a pure pipeline on a live cluster re-executed everything. Here the
// identity arrives the only way it can: on the registrations of the engines in
// the tier this scheduler dispatches to.
func TestTheEnvironmentACacheKeyIsHashedAgainstComesFromTheTiersEngines(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())

	h.runChainForReal(ctx, t, "run-real")
	lookups, records := h.cache.counts()
	require.Positive(t, records, "an identity that reached the plane makes the steps cacheable")
	require.Positive(t, lookups)

	// The entry really is under the key the tier's identity produces: computed
	// here from the same inputs, independently of anything the scheduler did.
	key, err := cache.Key(chain().GetSteps()[0], testEnvIdentity, nil,
		map[string]string{"oci://tool": "sha256:tool-v1"})
	require.NoError(t, err)
	outputs, hit, err := h.cache.Lookup(ctx, testTenant, key)
	require.NoError(t, err)
	require.True(t, hit, "step a was recorded under the identity its tier's engines announced")
	require.Equal(t, digestText(statusOf(t, h.events(ctx, t, "run-real"), "a")),
		outputs[0].GetDigest().GetAlgo()+":"+outputs[0].GetDigest().GetHex())

	// And the next run is served from it.
	h.seed(ctx, t, "run-cached")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-cached"))
	require.Empty(t, h.dispatchedSteps(ctx, t, "run-cached"))
}

// TestALookupAndARecordUseTheSameKey is the sharpest edge in ADR 0021 and the
// one that would fail silently.
//
// The lookup happens while a step is being considered and the record happens
// when its status comes back — different calls, minutes apart, on either side
// of an engine running the step. If those two resolved the environment
// separately, or one of them held a copy from configuration, every step would
// be recorded under one key and looked up under another: nothing would ever
// hit, nothing would error, and the only symptom would be a cache that quietly
// did nothing. Which is exactly the symptom this whole change is about.
//
// So: run for real, then ask the cache — through the SAME expression the
// scheduler uses for a lookup — for what the record path wrote.
func TestALookupAndARecordUseTheSameKey(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())
	h.runChainForReal(ctx, t, "run-real")

	// Step b's key folds in the digest a produced, so this covers the harder
	// of the two: a key over resolved inputs, not just over the step.
	events := h.events(ctx, t, "run-real")
	inputDigest := statusOf(t, events, "a").GetOutputs()[0].GetDigest()
	key, err := cache.Key(chain().GetSteps()[1], testEnvIdentity,
		[]*dholev1.Digest{inputDigest}, map[string]string{"oci://tool": "sha256:tool-v1"})
	require.NoError(t, err)

	_, hit, err := h.cache.Lookup(ctx, testTenant, key)
	require.NoError(t, err)
	require.True(t, hit,
		"what the record path wrote must be findable at the key the lookup path computes")
}

// TestATierWhoseEnginesDisagreeAboutTheirEnvironmentCachesNothing: a tier is a
// set of interchangeable workers and the queue picks which one runs the step,
// so two engines advertising two different sandbox images mean the plane
// cannot say what a result was produced in. Recording it anyway would serve
// one image's output as the other's. This is a rollout in progress, and its
// cache is off until it finishes.
func TestATierWhoseEnginesDisagreeAboutTheirEnvironmentCachesNothing(t *testing.T) {
	ctx := testContext(t)
	fleet := &mutableFleet{instances: []registry.Instance{
		engineIn("e1", testTier, "sha256:image-v1"),
		engineIn("e2", testTier, "sha256:image-v2"),
	}}
	h := newCacheHarnessWithFleet(ctx, t, chain(), fleet)

	h.runChainForReal(ctx, t, "run-first")
	_, records := h.cache.counts()
	require.Zero(t, records, "a tier that cannot say what its steps ran in records nothing")

	// And the next run does the work again rather than reusing something that
	// was never written.
	h.seed(ctx, t, "run-second")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-second"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-second"))
	require.False(t, dispatchOf(t, h.events(ctx, t, "run-second"), "a").Cacheable,
		"and the run log says why, rather than leaving a slow run unexplained")

	// Finish the rollout, and the tier starts caching.
	fleet.set(engineIn("e1", testTier, "sha256:image-v2"), engineIn("e2", testTier, "sha256:image-v2"))
	h.finish(ctx, t, "run-second", "a", "bytes from a")
	_, afterRollout := h.cache.counts()
	require.Positive(t, afterRollout, "an agreed tier caches again")
}

// TestATierWhoseEngineNamesNoEnvironmentCachesNothing is the honest outcome
// for a host-process tier, and the pre-existing behaviour reached for the
// right reason: nothing about a host is reproducible, the engine says so, and
// its tier is cached against nothing rather than against a lie.
func TestATierWhoseEngineNamesNoEnvironmentCachesNothing(t *testing.T) {
	ctx := testContext(t)
	fleet := &mutableFleet{instances: []registry.Instance{engineIn("e1", testTier, "")}}
	h := newCacheHarnessWithFleet(ctx, t, chain(), fleet)

	h.runChainForReal(ctx, t, "run-first")
	lookups, records := h.cache.counts()
	require.Zero(t, records)
	require.Zero(t, lookups, "there is no key to look anything up by")

	h.seed(ctx, t, "run-second")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-second"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-second"))
	require.Equal(t, "no stable environment identity to hash the step against",
		dispatchOf(t, h.events(ctx, t, "run-second"), "a").CacheIneligibleReason)
}

// TestAStepIsNotServedFromAnEntryRecordedInADifferentEnvironment: the identity
// is in the key, so an engine fleet that moved to a new sandbox image does not
// answer the new image's questions with the old image's results. This is the
// property that makes taking the identity from the fleet safe — it is allowed
// to change, and a change is a miss rather than a wrong hit.
func TestAStepIsNotServedFromAnEntryRecordedInADifferentEnvironment(t *testing.T) {
	ctx := testContext(t)
	fleet := &mutableFleet{instances: []registry.Instance{engineIn("e1", testTier, "sha256:image-v1")}}
	h := newCacheHarnessWithFleet(ctx, t, chain(), fleet)

	h.runChainForReal(ctx, t, "run-on-v1")

	fleet.set(engineIn("e1", testTier, "sha256:image-v2"))
	h.seed(ctx, t, "run-on-v2")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-on-v2"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-on-v2"),
		"the recorded entry belongs to an environment this step will not run in")
}

// TestAnotherTiersEnvironmentIsNotThisTiersEnvironment: engines are matched to
// a tier by the subject their work is published on, so an engine in another
// tier has no say in what this one caches against — and, in particular, cannot
// disable this tier's cache by disagreeing with it.
func TestAnotherTiersEnvironmentIsNotThisTiersEnvironment(t *testing.T) {
	ctx := testContext(t)
	fleet := &mutableFleet{instances: []registry.Instance{
		engineIn("e1", testTier, testEnvIdentity),
		engineIn("e2", "some-other-tier", "sha256:something-else"),
	}}
	h := newCacheHarnessWithFleet(ctx, t, chain(), fleet)

	h.runChainForReal(ctx, t, "run-first")
	_, records := h.cache.counts()
	require.Equal(t, 2, records, "only this tier's engines answer for this tier's environment")
}

// engineOfKind is one ready engine in a tier that also says which executor
// kind it offers. The kind is not decoration here either: a step naming
// engine_type may only be placed on an engine that advertised that kind, and
// it is now also what decides which engines answer for the environment the
// step's cache key is hashed against.
func engineOfKind(id, tier, kind, identity string) registry.Instance {
	e := engineIn(id, tier, identity)
	e.EngineTypes = []string{kind}
	return e
}

// kindStep is a one-step pipeline whose step names the executor kind it needs,
// or names none when kind is "".
func kindStep(kind string) *dholev1.Pipeline {
	step := &dholev1.Step{
		Id:          "a",
		Name:        "a",
		PluginRef:   "oci://tool",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		Outputs:     []*dholev1.Port{{Name: "out"}},
		EngineType:  kind,
	}
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps:  []*dholev1.Step{step},
	}
}

// mixedTier is the fleet found on kw: one tier holding a Kubernetes engine and
// a VM engine, each naming its own environment honestly — a busybox image
// digest and a rootfs digest — and disagreeing with each other because they
// are not the same kind of thing.
func mixedTier() *mutableFleet {
	return &mutableFleet{instances: []registry.Instance{
		engineOfKind("k8s-1", testTier, "kubernetes", "sha256:busybox"),
		engineOfKind("vm-1", testTier, "vm", "sha256:rootfs"),
	}}
}

// TestAStepNamingAnEngineKindIsCachedAgainstThatKindsEnvironment is the defect
// found on kw: a tier holding a Kubernetes engine and a VM engine had its
// WHOLE cache turned off, because the identity was folded over every instance
// of the tier and the two disagreed. A step naming engine_type: vm can only
// ever land on the VM engine, whose identity is single and unambiguous, so
// there is nothing ambiguous to refuse.
func TestAStepNamingAnEngineKindIsCachedAgainstThatKindsEnvironment(t *testing.T) {
	ctx := testContext(t)
	pipeline := kindStep("vm")
	h := newCacheHarnessWithFleet(ctx, t, pipeline, mixedTier())

	h.seed(ctx, t, "run-first")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-first"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-first"))
	require.True(t, dispatchOf(t, h.events(ctx, t, "run-first"), "a").Cacheable,
		"the engines this step can actually reach agree about their environment")
	h.finish(ctx, t, "run-first", "a", "bytes from a")

	// The entry is under the VM engine's identity, computed here from the same
	// inputs and independently of anything the scheduler did. The Kubernetes
	// engine in the same tier has no say in it: the step cannot land there.
	key, err := cache.Key(pipeline.GetSteps()[0], "sha256:rootfs", nil,
		map[string]string{"oci://tool": "sha256:tool-v1"})
	require.NoError(t, err)
	_, hit, err := h.cache.Lookup(ctx, testTenant, key)
	require.NoError(t, err)
	require.True(t, hit, "the step was recorded against the environment it can only have run in")

	// And the next run is served from it rather than dispatched.
	h.seed(ctx, t, "run-second")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-second"))
	require.Empty(t, h.dispatchedSteps(ctx, t, "run-second"))
}

// TestAStepNamingNoEngineKindInAMixedTierIsNotCached is ADR 0021 still working
// and must keep working. A step that names no kind really can be handed to
// either engine, so the plane cannot say what its result was produced in, and
// recording it would serve a busybox result as a VM result. Refusing is the
// right answer, not the bug above.
func TestAStepNamingNoEngineKindInAMixedTierIsNotCached(t *testing.T) {
	ctx := testContext(t)
	h := newCacheHarnessWithFleet(ctx, t, kindStep(""), mixedTier())

	h.seed(ctx, t, "run-first")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-first"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-first"))
	require.Equal(t, "no stable environment identity to hash the step against",
		dispatchOf(t, h.events(ctx, t, "run-first"), "a").CacheIneligibleReason,
		"a step that could land on either engine has no environment to be keyed on")
	h.finish(ctx, t, "run-first", "a", "bytes from a")

	_, records := h.cache.counts()
	require.Zero(t, records, "nothing may be recorded for a step whose environment is undecided")
}

// TestTwoEnginesOfOneKindThatDisagreeCacheNothingForThatKind: narrowing the
// fold to the kind a step names must not narrow it to ONE ENGINE. Two VM
// engines mid-rollout, on two different rootfs digests, are the disagreement
// ADR 0021 refuses to cache through — the queue still picks which of them runs
// the step.
func TestTwoEnginesOfOneKindThatDisagreeCacheNothingForThatKind(t *testing.T) {
	ctx := testContext(t)
	fleet := &mutableFleet{instances: []registry.Instance{
		engineOfKind("vm-1", testTier, "vm", "sha256:rootfs-v1"),
		engineOfKind("vm-2", testTier, "vm", "sha256:rootfs-v2"),
	}}
	h := newCacheHarnessWithFleet(ctx, t, kindStep("vm"), fleet)

	h.seed(ctx, t, "run-first")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-first"))
	require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, "run-first"))
	require.False(t, dispatchOf(t, h.events(ctx, t, "run-first"), "a").Cacheable)
	h.finish(ctx, t, "run-first", "a", "bytes from a")

	_, records := h.cache.counts()
	require.Zero(t, records, "a half-finished rollout of one kind still caches nothing")
}

// warnRecorder keeps every warning the scheduler emitted, in order.
type warnRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (w *warnRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (w *warnRecorder) WithAttrs([]slog.Attr) slog.Handler       { return w }
func (w *warnRecorder) WithGroup(string) slog.Handler            { return w }

func (w *warnRecorder) Handle(_ context.Context, r slog.Record) error {
	line := r.Message
	r.Attrs(func(a slog.Attr) bool {
		line += " " + a.Key + "=" + a.Value.String()
		return true
	})
	w.mu.Lock()
	w.lines = append(w.lines, line)
	w.mu.Unlock()
	return nil
}

func (w *warnRecorder) recorded() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.lines)
}

// TestEachSetOfEnginesThatCannotBeCachedAgainstIsReportedOnceAndNamed. The
// report is latched so that resolving the identity for every ready step of
// every run does not bury the one occurrence that mattered. Once the state
// became per KIND, a single tier-wide latch had to alternate between the
// kinds' states and re-log both on every step — "report once" turning into
// "report every time", which is the same as not latching at all.
func TestEachSetOfEnginesThatCannotBeCachedAgainstIsReportedOnceAndNamed(t *testing.T) {
	ctx := testContext(t)
	// Both kinds are mid-rollout, so both are in conflict at the same time and
	// the two states have to coexist.
	fleet := &mutableFleet{instances: []registry.Instance{
		engineOfKind("k8s-1", testTier, "kubernetes", "sha256:busybox-v1"),
		engineOfKind("k8s-2", testTier, "kubernetes", "sha256:busybox-v2"),
		engineOfKind("vm-1", testTier, "vm", "sha256:rootfs-v1"),
		engineOfKind("vm-2", testTier, "vm", "sha256:rootfs-v2"),
	}}
	pipeline := kindStep("vm")
	k8sStep := proto.Clone(pipeline.GetSteps()[0]).(*dholev1.Step)
	k8sStep.Id, k8sStep.Name, k8sStep.EngineType = "k", "k", "kubernetes"
	pipeline.Steps = append(pipeline.Steps, k8sStep)

	warnings := &warnRecorder{}
	h := newCacheHarnessLogging(ctx, t, pipeline, fleet, slog.New(warnings))

	h.seed(ctx, t, "run-first")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-first"))
	require.ElementsMatch(t, []string{"a", "k"}, h.dispatchedSteps(ctx, t, "run-first"))

	// Said once per set, and each says WHICH engines disagreed: a kubernetes
	// digest and a vm digest listed together, as the tier-wide line did, read
	// as a fault when they are two different kinds of engine behaving
	// correctly.
	lines := warnings.recorded()
	require.Len(t, lines, 2, "one line per set of engines, not one per step")
	require.Contains(t, lines[0], "the vm engines of this tier disagree")
	require.Contains(t, lines[0], "sha256:rootfs-v1 sha256:rootfs-v2")
	require.Contains(t, lines[1], "the kubernetes engines of this tier disagree")
	require.Contains(t, lines[1], "sha256:busybox-v1 sha256:busybox-v2")

	// And nothing repeats while the state holds, however many steps resolve it.
	h.seed(ctx, t, "run-second")
	require.NoError(t, h.sched.Advance(ctx, testTenant, "run-second"))
	require.Len(t, warnings.recorded(), 2, "a state that has not changed is not said again")
}
