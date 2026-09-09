package api_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// The two tenants every test works with. Nothing in this package is ever
// asked for without one: a call carrying no tenant is a bug, not a wildcard.
const (
	tenantA = "tenant-a"
	tenantB = "tenant-b"

	tokenAlice = "tok-alice" // tenant-a, author of the fixtures
	tokenCarol = "tok-carol" // tenant-a, a second human who may approve
	tokenBob   = "tok-bob"   // tenant-b, and must see nothing of tenant-a's
	tokenGhost = "tok-ghost" // authenticates, but carries no tenant at all
)

// fakeAuth is a credential table. It deliberately does no tenant checking of
// its own: every tenant decision this package's tests observe has to have been
// made by the server, not by something underneath it.
type fakeAuth struct{}

func (fakeAuth) Authenticate(_ context.Context, credential string) (identity.Principal, error) {
	switch credential {
	case tokenAlice:
		return identity.Principal{Subject: "alice", TenantID: tenantA, Kind: identity.PrincipalUser}, nil
	case tokenCarol:
		return identity.Principal{Subject: "carol", TenantID: tenantA, Kind: identity.PrincipalUser}, nil
	case tokenBob:
		return identity.Principal{Subject: "bob", TenantID: tenantB, Kind: identity.PrincipalUser}, nil
	case tokenGhost:
		return identity.Principal{Subject: "ghost", Kind: identity.PrincipalService}, nil
	default:
		return identity.Principal{}, identity.ErrUnauthenticated
	}
}

// recordingDefs is a definition store that records the tenant of every call
// and enforces nothing.
//
// That is the whole point of it. A store that refused a cross-tenant read
// would answer the tenant question on the server's behalf, and a server that
// had stopped passing the principal's tenant would still look correct. Here it
// does not: the tenant the server passed is compared directly.
type recordingDefs struct {
	mu        sync.Mutex
	tenants   []string
	revisions map[string]defstore.Revision // revision id -> metadata
	pipelines map[string]*dholev1.Pipeline // revision id -> definition
	order     map[string][]string          // pipeline id -> revision ids, oldest first
	active    map[string]string            // pipeline id -> active revision id
}

func newRecordingDefs() *recordingDefs {
	return &recordingDefs{
		revisions: map[string]defstore.Revision{},
		pipelines: map[string]*dholev1.Pipeline{},
		order:     map[string][]string{},
		active:    map[string]string{},
	}
}

func (r *recordingDefs) note(tenantID string) {
	r.tenants = append(r.tenants, tenantID)
}

func (r *recordingDefs) seenTenants() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.tenants...)
}

func (r *recordingDefs) Save(
	_ context.Context, tenantID string, p *dholev1.Pipeline, author string,
) (defstore.Revision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	hash := defstore.ContentHash(p)
	id := "rev_" + hash
	if existing, ok := r.revisions[id]; ok {
		return existing, nil
	}
	rev := defstore.Revision{
		ID: id, PipelineID: p.GetId(), ContentHash: hash,
		State: defstore.StateDraft, Lockfile: map[string]string{}, Author: author,
	}
	r.revisions[id] = rev
	r.pipelines[id] = proto.Clone(p).(*dholev1.Pipeline)
	r.order[p.GetId()] = append(r.order[p.GetId()], id)
	return rev, nil
}

func (r *recordingDefs) Get(
	_ context.Context, tenantID, pipelineID, revisionID string,
) (*dholev1.Pipeline, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	p, ok := r.pipelines[revisionID]
	if !ok || p.GetId() != pipelineID {
		return nil, defstore.ErrNotFound
	}
	return proto.Clone(p).(*dholev1.Pipeline), nil
}

func (r *recordingDefs) Active(_ context.Context, tenantID, pipelineID string) (defstore.Revision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	id, ok := r.active[pipelineID]
	if !ok {
		return defstore.Revision{}, defstore.ErrNoActiveRevision
	}
	return r.revisions[id], nil
}

func (r *recordingDefs) Approve(_ context.Context, tenantID, revisionID, approver string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	rev, ok := r.revisions[revisionID]
	if !ok {
		return defstore.ErrNotFound
	}
	rev.State = defstore.StateActive
	rev.Approver = approver
	r.revisions[revisionID] = rev
	r.active[rev.PipelineID] = revisionID
	return nil
}

func (r *recordingDefs) Revision(_ context.Context, tenantID, revisionID string) (defstore.Revision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	rev, ok := r.revisions[revisionID]
	if !ok {
		return defstore.Revision{}, defstore.ErrNotFound
	}
	return rev, nil
}

// Revisions is the optional listing capability api.ListRevisions needs and
// defstore.Store does not yet offer.
func (r *recordingDefs) Revisions(
	_ context.Context, tenantID, pipelineID string,
) ([]defstore.Revision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.note(tenantID)
	out := make([]defstore.Revision, 0, len(r.order[pipelineID]))
	for _, id := range r.order[pipelineID] {
		out = append(out, r.revisions[id])
	}
	return out, nil
}

// recordingAdvancer stands in for the scheduler StartRun hands the new run to.
type recordingAdvancer struct {
	mu   sync.Mutex
	runs []string
}

func (a *recordingAdvancer) Advance(_ context.Context, tenantID, runID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs = append(a.runs, tenantID+"/"+runID)
	return nil
}

func (a *recordingAdvancer) seen() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.runs...)
}

// harness is one API server behind a real HTTP client.
type harness struct {
	client   dholev1connect.PipelineServiceClient
	defs     defstore.Store
	runs     runstore.Store
	advancer *recordingAdvancer
}

func newHarness(t *testing.T, defs defstore.Store) *harness {
	t.Helper()

	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	adv := &recordingAdvancer{}
	srv, err := api.NewServer(api.Config{
		Definitions:  defs,
		Auth:         fakeAuth{},
		Runs:         runs,
		Advancer:     adv,
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)

	mux := srv.Handler()
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	return &harness{
		client:   dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL),
		defs:     defs,
		runs:     runs,
		advancer: adv,
	}
}

// sqliteDefs is the real definition store on a throwaway database.
func sqliteDefs(t *testing.T) defstore.Store {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "definitions.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	// The fixtures name no plugin, so there is nothing to pin; the option
	// says so deliberately rather than leaving an unpinned save to chance.
	return defstore.New(db, defstore.WithoutPinning())
}

func newRealHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, sqliteDefs(t))
}

// authed stamps a credential on a request. Every call in this package carries
// one; the calls that do not are the tests that say so.
func authed[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "application/octet-stream"}},
		},
	}
}

// basePipeline is the fixture every operation is applied to. Step "c" is
// deliberately unnamed and unconnected: it is what rename, set_property and
// remove_step act on, and its empty name is what proves the inverse of a
// rename can restore "no name at all".
func basePipeline(tenantID string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     "pipe-1",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{
			{
				Id: "a", Name: "fetch",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Outputs:     []*dholev1.Port{blobPort("out")},
			},
			{
				Id: "b", Name: "build",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Inputs:      []*dholev1.Port{blobPort("in"), blobPort("aux")},
			},
			{
				Id:      "c",
				Outputs: []*dholev1.Port{blobPort("out")},
			},
		},
		Edges: []*dholev1.Edge{{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "in"}},
	}
}

// seed saves the fixture as tenantID's first revision and returns it.
func seed(t *testing.T, h *harness, tenantID string) (*dholev1.Pipeline, defstore.Revision) {
	t.Helper()
	p := basePipeline(tenantID)
	rev, err := h.defs.Save(context.Background(), tenantID, p, "alice")
	require.NoError(t, err)
	return p, rev
}

// operationFixtures is one representative operation per kind of the oneof,
// keyed by the proto field name. The tests that iterate every kind drive off
// the descriptor and look each kind up here, so an operation added to the
// oneof later fails this map rather than quietly escaping the coverage.
var operationFixtures = map[string]*dholev1.Operation{
	"add_step": {Kind: &dholev1.Operation_AddStep{AddStep: &dholev1.AddStep{
		Step: &dholev1.Step{Id: "d", Name: "publish"},
	}}},
	"remove_step": {Kind: &dholev1.Operation_RemoveStep{RemoveStep: &dholev1.RemoveStep{
		StepId: "c",
	}}},
	"connect": {Kind: &dholev1.Operation_Connect{Connect: &dholev1.Connect{
		Edge: &dholev1.Edge{FromStep: "c", FromPort: "out", ToStep: "b", ToPort: "aux"},
	}}},
	"remove_edge": {Kind: &dholev1.Operation_RemoveEdge{RemoveEdge: &dholev1.RemoveEdge{
		Edge: &dholev1.Edge{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "in"},
	}}},
	"set_property": {Kind: &dholev1.Operation_SetProperty{SetProperty: &dholev1.SetProperty{
		StepId: "c", Property: "effect_class", Value: "EFFECT_CLASS_PURE",
	}}},
	"rename": {Kind: &dholev1.Operation_Rename{Rename: &dholev1.Rename{
		StepId: "c", Name: "cache",
	}}},
}

// operationKinds is every field of the Operation oneof, read from the
// descriptor rather than a hand-written list.
func operationKinds(t *testing.T) []string {
	t.Helper()
	oneof := (&dholev1.Operation{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	require.NotNil(t, oneof, "Operation must carry a oneof named kind")
	names := make([]string, 0, oneof.Fields().Len())
	for i := range oneof.Fields().Len() {
		names = append(names, string(oneof.Fields().Get(i).Name()))
	}
	require.NotEmpty(t, names)
	return names
}

func fixtureFor(t *testing.T, kind string) *dholev1.Operation {
	t.Helper()
	op, ok := operationFixtures[kind]
	require.Truef(t, ok,
		"operation %q has no fixture: add one to operationFixtures so the new operation is covered here",
		kind)
	return proto.Clone(op).(*dholev1.Operation)
}

// TestOperationInverseRestoresRevision is the property the whole
// operation-level design rests on: undo is a property of the API, not a
// feature the GUI reimplements.
func TestOperationInverseRestoresRevision(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	applied, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "add_step"),
	}, tokenAlice))
	require.NoError(t, err)
	require.NotNil(t, applied.Msg.GetInverse(), "an operation with no inverse is an operation nobody can undo")

	undone, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: applied.Msg.GetRevision().GetId(),
		Operation:    applied.Msg.GetInverse(),
	}, tokenAlice))
	require.NoError(t, err)

	got, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: original.GetId(),
		RevisionId: undone.Msg.GetRevision().GetId(),
	}, tokenAlice))
	require.NoError(t, err)
	require.True(t, proto.Equal(original, got.Msg.GetPipeline()),
		"inverse did not restore the original:\nwant %v\ngot  %v", original, got.Msg.GetPipeline())

	// Revision identity is the content hash, so a true round trip lands back
	// on the revision it started from rather than on a look-alike.
	require.Equal(t, base.ID, undone.Msg.GetRevision().GetId())
}

// TestEveryOperationInverseRoundTrips is the same property for every kind of
// operation, not only the one the plan names.
func TestEveryOperationInverseRoundTrips(t *testing.T) {
	for _, kind := range operationKinds(t) {
		t.Run(kind, func(t *testing.T) {
			h := newRealHarness(t)
			original, base := seed(t, h, tenantA)
			ctx := context.Background()

			applied, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
				PipelineId:   original.GetId(),
				BaseRevision: base.ID,
				Operation:    fixtureFor(t, kind),
			}, tokenAlice))
			require.NoError(t, err)
			require.NotEqual(t, base.ID, applied.Msg.GetRevision().GetId(),
				"the operation changed nothing")
			require.NotNil(t, applied.Msg.GetInverse())

			undone, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
				PipelineId:   original.GetId(),
				BaseRevision: applied.Msg.GetRevision().GetId(),
				Operation:    applied.Msg.GetInverse(),
			}, tokenAlice))
			require.NoError(t, err)
			require.Equal(t, base.ID, undone.Msg.GetRevision().GetId(),
				"the inverse of %s did not land back on the original revision", kind)
		})
	}
}

// TestApplyOperationRejectsStaleVersion: two people editing at once is the
// normal case. The second edit against a superseded base is refused, not
// silently merged into a document that neither of them wrote.
func TestApplyOperationRejectsStaleVersion(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	_, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "rename"),
	}, tokenAlice))
	require.NoError(t, err)

	_, err = h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "add_step"),
	}, tokenCarol))
	require.Error(t, err)
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))
	require.Contains(t, err.Error(), "revision conflict")
}

// TestEveryOperationReturnsANonEmptyDiff: the diff is how a GUI paints the
// change and how an agent explains it. An operation that reports nothing is
// indistinguishable from one that did nothing.
func TestEveryOperationReturnsANonEmptyDiff(t *testing.T) {
	for _, kind := range operationKinds(t) {
		t.Run(kind, func(t *testing.T) {
			h := newRealHarness(t)
			original, base := seed(t, h, tenantA)

			applied, err := h.client.ApplyOperation(context.Background(),
				authed(&dholev1.ApplyOperationRequest{
					PipelineId:   original.GetId(),
					BaseRevision: base.ID,
					Operation:    fixtureFor(t, kind),
				}, tokenAlice))
			require.NoError(t, err)

			changes := applied.Msg.GetDiff().GetChanges()
			require.NotEmpty(t, changes, "%s reported no change", kind)
			for _, ch := range changes {
				require.NotEqual(t, dholev1.ChangeKind_CHANGE_KIND_UNSPECIFIED, ch.GetKind())
				require.NotEmpty(t, ch.GetSummary(), "a change with no summary names nothing")
				switch {
				case ch.GetEdge() != nil:
					require.Contains(t, ch.GetSummary(), ch.GetEdge().GetFromStep())
					require.Contains(t, ch.GetSummary(), ch.GetEdge().GetToStep())
				case ch.GetStepId() != "":
					require.Contains(t, ch.GetSummary(), ch.GetStepId())
				default:
					t.Fatalf("change names neither a step nor an edge: %v", ch)
				}
			}
		})
	}
}

// TestApplyOperationDoesNotMutateTheCallersPipeline proves the operation is
// applied to a copy: the message handed in is untouched, and so is the
// revision it came from.
func TestApplyOperationDoesNotMutateTheCallersPipeline(t *testing.T) {
	input := basePipeline(tenantA)
	before := proto.Clone(input).(*dholev1.Pipeline)

	next, diff, inverse, err := api.Apply(input, fixtureFor(t, "add_step"))
	require.NoError(t, err)
	require.NotNil(t, diff)
	require.NotNil(t, inverse)
	require.NotSame(t, input, next, "the operation was applied in place")
	require.True(t, proto.Equal(before, input), "Apply mutated the caller's pipeline")
	require.False(t, proto.Equal(before, next), "Apply changed nothing")
}

// TestApplyOperationDoesNotMutateTheStoredRevision is the same property one
// level up: an immutable revision that an edit can reach is not immutable.
func TestApplyOperationDoesNotMutateTheStoredRevision(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	_, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "add_step"),
	}, tokenAlice))
	require.NoError(t, err)

	stored, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: original.GetId(),
		RevisionId: base.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.True(t, proto.Equal(original, stored.Msg.GetPipeline()),
		"the base revision changed under the edit")
}

// TestOperationNamingSomethingThatDoesNotExistIsRejected: silently ignoring a
// step or port that is not there turns a typo into a pipeline nobody meant.
func TestOperationNamingSomethingThatDoesNotExistIsRejected(t *testing.T) {
	cases := map[string]struct {
		op   *dholev1.Operation
		want string
	}{
		"connect from a missing step": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_Connect{Connect: &dholev1.Connect{
				Edge: &dholev1.Edge{FromStep: "nope", FromPort: "out", ToStep: "b", ToPort: "aux"},
			}}},
			want: "nope",
		},
		"connect from a missing port": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_Connect{Connect: &dholev1.Connect{
				Edge: &dholev1.Edge{FromStep: "a", FromPort: "ghost", ToStep: "b", ToPort: "aux"},
			}}},
			want: "ghost",
		},
		"connect to a missing input port": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_Connect{Connect: &dholev1.Connect{
				Edge: &dholev1.Edge{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "ghost"},
			}}},
			want: "ghost",
		},
		"rename a missing step": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_Rename{Rename: &dholev1.Rename{
				StepId: "nope", Name: "x",
			}}},
			want: "nope",
		},
		"set a property on a missing step": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_SetProperty{SetProperty: &dholev1.SetProperty{
				StepId: "nope", Property: "effect_class", Value: "EFFECT_CLASS_PURE",
			}}},
			want: "nope",
		},
		"set a property that does not exist": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_SetProperty{SetProperty: &dholev1.SetProperty{
				StepId: "c", Property: "colour", Value: "blue",
			}}},
			want: "colour",
		},
		"remove a step that does not exist": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_RemoveStep{RemoveStep: &dholev1.RemoveStep{
				StepId: "nope",
			}}},
			want: "nope",
		},
		"remove a step that still has edges": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_RemoveStep{RemoveStep: &dholev1.RemoveStep{
				StepId: "a",
			}}},
			want: "still connected",
		},
		"remove an edge that never existed": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_RemoveEdge{RemoveEdge: &dholev1.RemoveEdge{
				Edge: &dholev1.Edge{FromStep: "c", FromPort: "out", ToStep: "b", ToPort: "aux"},
			}}},
			want: "no such edge",
		},
		"add a step that already exists": {
			op: &dholev1.Operation{Kind: &dholev1.Operation_AddStep{AddStep: &dholev1.AddStep{
				Step: &dholev1.Step{Id: "a"},
			}}},
			want: "already exists",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			h := newRealHarness(t)
			original, base := seed(t, h, tenantA)

			_, err := h.client.ApplyOperation(context.Background(),
				authed(&dholev1.ApplyOperationRequest{
					PipelineId:   original.GetId(),
					BaseRevision: base.ID,
					Operation:    tc.op,
				}, tokenAlice))
			require.Error(t, err)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// callWithoutAuthorization drives one RPC with no Authorization header.
func callWithoutAuthorization(t *testing.T, h *harness, rpc string) error {
	t.Helper()
	ctx := context.Background()
	switch rpc {
	case "GetPipeline":
		_, err := h.client.GetPipeline(ctx, connect.NewRequest(&dholev1.GetPipelineRequest{PipelineId: "pipe-1"}))
		return err
	case "ApplyOperation":
		_, err := h.client.ApplyOperation(ctx, connect.NewRequest(&dholev1.ApplyOperationRequest{
			PipelineId: "pipe-1", BaseRevision: "rev_x", Operation: fixtureFor(t, "add_step"),
		}))
		return err
	case "Validate":
		_, err := h.client.Validate(ctx, connect.NewRequest(&dholev1.ValidateRequest{PipelineId: "pipe-1"}))
		return err
	case "Plan":
		_, err := h.client.Plan(ctx, connect.NewRequest(&dholev1.PlanRequest{PipelineId: "pipe-1"}))
		return err
	case "ListRevisions":
		_, err := h.client.ListRevisions(ctx, connect.NewRequest(&dholev1.ListRevisionsRequest{PipelineId: "pipe-1"}))
		return err
	case "ApproveRevision":
		_, err := h.client.ApproveRevision(ctx, connect.NewRequest(&dholev1.ApproveRevisionRequest{RevisionId: "rev_x"}))
		return err
	case "StartRun":
		_, err := h.client.StartRun(ctx, connect.NewRequest(&dholev1.StartRunRequest{PipelineId: "pipe-1"}))
		return err
	case "WatchRun":
		stream, err := h.client.WatchRun(ctx, connect.NewRequest(&dholev1.WatchRunRequest{RunId: "run-x"}))
		if err != nil {
			return err
		}
		defer func() { _ = stream.Close() }()
		if stream.Receive() {
			return nil
		}
		return stream.Err()
	default:
		t.Fatalf("unknown rpc %q", rpc)
		return nil
	}
}

// serviceRPCs is every method of the service, read from the descriptor so an
// RPC added later is authenticated too or fails here.
func serviceRPCs(t *testing.T) []string {
	t.Helper()
	desc := dholev1.File_dhole_v1_api_proto.Services().ByName("PipelineService")
	require.NotNil(t, desc)
	names := make([]string, 0, desc.Methods().Len())
	for i := range desc.Methods().Len() {
		names = append(names, string(desc.Methods().Get(i).Name()))
	}
	return names
}

// TestEveryRPCRejectsACallWithoutAuthorization: there is no unscoped call in
// this system, and no endpoint that forgot.
func TestEveryRPCRejectsACallWithoutAuthorization(t *testing.T) {
	for _, rpc := range serviceRPCs(t) {
		t.Run(rpc, func(t *testing.T) {
			h := newHarness(t, newRecordingDefs())
			err := callWithoutAuthorization(t, h, rpc)
			require.Error(t, err, "%s served a call with no credential", rpc)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		})
	}
}

// TestAPrincipalWithNoTenantIsRejected: an authenticated caller who belongs to
// no tenant cannot be scoped, so there is nothing it may be shown.
func TestAPrincipalWithNoTenantIsRejected(t *testing.T) {
	h := newHarness(t, newRecordingDefs())
	_, err := h.client.GetPipeline(context.Background(),
		authed(&dholev1.GetPipelineRequest{PipelineId: "pipe-1"}, tokenGhost))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
	require.Contains(t, err.Error(), "tenant")
}

// TestABadCredentialIsRejected keeps the refusal uniform.
func TestABadCredentialIsRejected(t *testing.T) {
	h := newHarness(t, newRecordingDefs())
	_, err := h.client.GetPipeline(context.Background(),
		authed(&dholev1.GetPipelineRequest{PipelineId: "pipe-1"}, "not-a-token"))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
}

// TestEveryStoreCallCarriesThePrincipalsTenant compares the tenant the server
// passed down with the principal's own, against a store that enforces nothing.
// A store that refused the wrong tenant would answer this on the server's
// behalf and hide a server that had stopped scoping at all.
func TestEveryStoreCallCarriesThePrincipalsTenant(t *testing.T) {
	defs := newRecordingDefs()
	h := newHarness(t, defs)
	ctx := context.Background()

	p := basePipeline(tenantA)
	rev, err := defs.Save(ctx, tenantA, p, "alice")
	require.NoError(t, err)
	before := len(defs.seenTenants())

	_, err = h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId: p.GetId(), BaseRevision: rev.ID, Operation: fixtureFor(t, "add_step"),
	}, tokenAlice))
	require.NoError(t, err)

	_, err = h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	seen := defs.seenTenants()[before:]
	require.NotEmpty(t, seen, "the server reached the store without recording a tenant")
	for _, got := range seen {
		require.Equal(t, tenantA, got, "the server passed a tenant that is not the principal's")
	}
}

// TestAnotherTenantsPipelineIsNotReadable is the end-to-end half of the same
// property, against the real store.
func TestAnotherTenantsPipelineIsNotReadable(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	_, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: original.GetId(), RevisionId: base.ID,
	}, tokenBob))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId: original.GetId(), BaseRevision: base.ID, Operation: fixtureFor(t, "add_step"),
	}, tokenBob))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// And tenant A's revision is exactly as it was.
	got, err := h.client.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: original.GetId(), RevisionId: base.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.True(t, proto.Equal(original, got.Msg.GetPipeline()))
}

// TestApplyOperationRequiresABaseRevision: optimistic concurrency is not
// optional. An edit with no base is an edit that cannot conflict.
func TestApplyOperationRequiresABaseRevision(t *testing.T) {
	h := newRealHarness(t)
	original, _ := seed(t, h, tenantA)

	_, err := h.client.ApplyOperation(context.Background(),
		authed(&dholev1.ApplyOperationRequest{
			PipelineId: original.GetId(),
			Operation:  fixtureFor(t, "add_step"),
		}, tokenAlice))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "base_revision")
}

// TestListRevisionsReturnsTheTenantsRevisions.
func TestListRevisionsReturnsTheTenantsRevisions(t *testing.T) {
	defs := newRecordingDefs()
	h := newHarness(t, defs)
	ctx := context.Background()

	p := basePipeline(tenantA)
	rev, err := defs.Save(ctx, tenantA, p, "alice")
	require.NoError(t, err)
	applied, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId: p.GetId(), BaseRevision: rev.ID, Operation: fixtureFor(t, "add_step"),
	}, tokenAlice))
	require.NoError(t, err)

	list, err := h.client.ListRevisions(ctx, authed(&dholev1.ListRevisionsRequest{
		PipelineId: p.GetId(),
	}, tokenAlice))
	require.NoError(t, err)
	ids := make([]string, 0, len(list.Msg.GetRevisions()))
	for _, r := range list.Msg.GetRevisions() {
		ids = append(ids, r.GetId())
	}
	require.Equal(t, []string{rev.ID, applied.Msg.GetRevision().GetId()}, ids)
}

// TestListRevisionsReadsTheRealStoresHistory is what replaced the gap this
// endpoint used to report: the definition store answers the history itself, so
// ListRevisions is served rather than refused as unimplemented.
//
// It runs against the REAL SQL store rather than the recording double, because
// the double's listing proved only that the server calls something.
func TestListRevisionsReadsTheRealStoresHistory(t *testing.T) {
	h := newRealHarness(t)
	p, base := seed(t, h, tenantA)
	ctx := context.Background()

	applied, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId: p.GetId(), BaseRevision: base.ID, Operation: fixtureFor(t, "rename"),
	}, tokenAlice))
	require.NoError(t, err)

	list, err := h.client.ListRevisions(ctx,
		authed(&dholev1.ListRevisionsRequest{PipelineId: p.GetId()}, tokenAlice))
	require.NoError(t, err, "the definition store can list, so this must not be unimplemented")

	ids := make([]string, 0, len(list.Msg.GetRevisions()))
	for _, r := range list.Msg.GetRevisions() {
		ids = append(ids, r.GetId())
	}
	require.Equal(t, []string{base.ID, applied.Msg.GetRevision().GetId()}, ids,
		"the history is the pipeline's revisions, oldest first")

	// Another tenant sees none of it.
	other, err := h.client.ListRevisions(ctx,
		authed(&dholev1.ListRevisionsRequest{PipelineId: p.GetId()}, tokenBob))
	require.NoError(t, err)
	require.Empty(t, other.Msg.GetRevisions())
}

// TestApproveRevisionRecordsThePrincipalAsApprover.
func TestApproveRevisionRecordsThePrincipalAsApprover(t *testing.T) {
	h := newRealHarness(t)
	_, base := seed(t, h, tenantA)

	got, err := h.client.ApproveRevision(context.Background(),
		authed(&dholev1.ApproveRevisionRequest{RevisionId: base.ID}, tokenCarol))
	require.NoError(t, err)
	require.Equal(t, "carol", got.Msg.GetRevision().GetApprover())
	require.Equal(t, string(defstore.StateActive), got.Msg.GetRevision().GetState())
}

// TestApproveRevisionRefusesTheAuthor keeps the store's rule visible on the
// wire as a permission denial rather than an internal error.
func TestApproveRevisionRefusesTheAuthor(t *testing.T) {
	h := newRealHarness(t)
	_, base := seed(t, h, tenantA)

	_, err := h.client.ApproveRevision(context.Background(),
		authed(&dholev1.ApproveRevisionRequest{RevisionId: base.ID}, tokenAlice))
	require.Error(t, err)
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// TestStartRunPinsAnActiveRevisionAndRecordsIt.
func TestStartRunPinsAnActiveRevisionAndRecordsIt(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	_, err := h.client.ApproveRevision(ctx,
		authed(&dholev1.ApproveRevisionRequest{RevisionId: base.ID}, tokenCarol))
	require.NoError(t, err)

	started, err := h.client.StartRun(ctx, authed(&dholev1.StartRunRequest{
		PipelineId: original.GetId(),
	}, tokenAlice))
	require.NoError(t, err)
	require.NotEmpty(t, started.Msg.GetRunId())
	require.Equal(t, base.ID, started.Msg.GetRevisionId())

	events, err := h.runs.Replay(ctx, tenantA, started.Msg.GetRunId())
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, runstore.RunCreated, events[0].Type)
	created, err := scheduler.UnmarshalRunCreated(events[0].Payload)
	require.NoError(t, err)
	require.Equal(t, base.ID, created.RevisionID)

	require.Equal(t, []string{tenantA + "/" + started.Msg.GetRunId()}, h.advancer.seen(),
		"a run that is created and never advanced never starts")
}

// TestStartRunRefusesARevisionThatIsNotApproved: approval is what stands in
// for forge-native review, so an unapproved definition may not run.
func TestStartRunRefusesARevisionThatIsNotApproved(t *testing.T) {
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)

	_, err := h.client.StartRun(context.Background(), authed(&dholev1.StartRunRequest{
		PipelineId: original.GetId(), RevisionId: base.ID,
	}, tokenAlice))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

// TestWatchRunStreamsTheRunsEventsUntilItEnds.
func TestWatchRunStreamsTheRunsEventsUntilItEnds(t *testing.T) {
	h := newRealHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const runID = "run-watch"
	require.NoError(t, h.runs.Append(ctx, tenantA, runstore.Event{
		RunID: runID, Sequence: 1, Type: runstore.RunCreated, At: time.Now(),
	}))

	stream, err := h.client.WatchRun(ctx, authed(&dholev1.WatchRunRequest{RunId: runID}, tokenAlice))
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	require.True(t, stream.Receive(), "%v", stream.Err())
	require.Equal(t, string(runstore.RunCreated), stream.Msg().GetType())

	require.NoError(t, h.runs.Append(ctx, tenantA, runstore.Event{
		RunID: runID, StepID: "a", Sequence: 2, Type: runstore.StepSucceeded, At: time.Now(),
	}))
	require.True(t, stream.Receive(), "%v", stream.Err())
	require.Equal(t, "a", stream.Msg().GetStepId())

	require.NoError(t, h.runs.Append(ctx, tenantA, runstore.Event{
		RunID: runID, Sequence: 3, Type: runstore.RunCompleted, At: time.Now(),
	}))
	require.True(t, stream.Receive(), "%v", stream.Err())
	require.Equal(t, string(runstore.RunCompleted), stream.Msg().GetType())

	require.False(t, stream.Receive(), "the stream did not end with the run")
	require.NoError(t, stream.Err())
}

// TestWatchRunDoesNotShowAnotherTenantsRun.
func TestWatchRunDoesNotShowAnotherTenantsRun(t *testing.T) {
	h := newRealHarness(t)
	ctx := context.Background()

	const runID = "run-secret"
	require.NoError(t, h.runs.Append(ctx, tenantA, runstore.Event{
		RunID: runID, Sequence: 1, Type: runstore.RunCreated, At: time.Now(),
	}))

	stream, err := h.client.WatchRun(ctx, authed(&dholev1.WatchRunRequest{RunId: runID}, tokenBob))
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	require.False(t, stream.Receive())
	require.Error(t, stream.Err())
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(stream.Err()))
}

// TestNewServerRefusesAnIncompleteConfiguration: a server with no way to
// authenticate would serve every caller.
func TestNewServerRefusesAnIncompleteConfiguration(t *testing.T) {
	_, err := api.NewServer(api.Config{Definitions: newRecordingDefs()})
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication")

	_, err = api.NewServer(api.Config{Auth: fakeAuth{}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "definition")
}

// TestUnknownOperationIsRejected: an Operation whose oneof is empty names no
// edit at all.
func TestUnknownOperationIsRejected(t *testing.T) {
	_, _, _, err := api.Apply(basePipeline(tenantA), &dholev1.Operation{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no operation")

	_, _, _, err = api.Apply(basePipeline(tenantA), nil)
	require.Error(t, err)
}
