package defstore_test

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
)

// migrationPath is the one schema file this package owns. The test applies the
// real migration rather than a hand-written copy, so a schema that does not
// match the code fails here instead of in production.
const migrationPath = "../runstore/migrations/0005_definitions.sql"

const tenant = "acme"

// goldenHash pins the content hash of the pipeline built below. It is written
// into the database and compared across processes and builds, so a change to
// how it is derived must fail here rather than silently orphan every pinned
// revision in an existing deployment.
const goldenHash = "947107ebe467599ab5293459e97e21555fba216b1cebde3390aa6302d47ab27b"

// newStore opens a throwaway SQLite database with the definition schema
// applied, on the same connection shape the run store uses.
func newStore(t *testing.T) defstore.Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "definitions.db")
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	schema, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), string(schema))
	require.NoError(t, err)

	return defstore.New(db)
}

// pipeline builds a small but non-trivial definition: repeated fields and a
// oneof, so a reordering or a dropped field shows up in the content hash.
func pipeline(id, pluginRef string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenant},
		Steps: []*dholev1.Step{
			{
				Id:        "build",
				Name:      "Build",
				PluginRef: pluginRef,
				Outputs: []*dholev1.Port{{
					Name: "artifact",
					Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "application/gzip"}},
					},
				}},
			},
			{
				Id:        "test",
				Name:      "Test",
				PluginRef: "oci://dhole/test:1",
				Inputs: []*dholev1.Port{{
					Name: "artifact",
					Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "application/gzip"}},
					},
				}},
			},
		},
		Edges: []*dholev1.Edge{{
			FromStep: "build", FromPort: "artifact", ToStep: "test", ToPort: "artifact",
		}},
	}
}

// A saved definition gets a revision identity, and that identity is the
// content: two identical saves hash the same, and any edit hashes differently.
// Without that, "which definition ran?" has no answer after the fact.
func TestRevisionCreatedAndMirroredOnEdit(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	first, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NotEmpty(t, first.ID)
	require.Equal(t, "p1", first.PipelineID)
	require.NotEmpty(t, first.ContentHash)

	// Identical content, saved again — the hash must not move.
	again, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.Equal(t, first.ContentHash, again.ContentHash)

	// One field changed — the hash must move.
	edited, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:2"), "ada")
	require.NoError(t, err)
	require.NotEqual(t, first.ContentHash, edited.ContentHash)
	require.NotEqual(t, first.ID, edited.ID)
}

// A new revision is a draft. Until somebody approves it, the pipeline's active
// definition is the one that was already approved — an edit must never become
// what runs simply because it was saved last.
func TestNewRevisionStartsAsDraftAndOnlyBecomesActiveOnApproval(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	first, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.Equal(t, defstore.StateDraft, first.State)

	// Nothing is active yet: the pipeline has never been approved.
	_, err = store.Active(ctx, tenant, "p1")
	require.ErrorIs(t, err, defstore.ErrNoActiveRevision)

	require.NoError(t, store.Approve(ctx, tenant, first.ID, "grace"))
	active, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, first.ID, active.ID)
	require.Equal(t, defstore.StateActive, active.State)
	require.Equal(t, "grace", active.Approver)
	require.Equal(t, "ada", active.Author)

	// A second revision is a draft, and the active revision does not move.
	second, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:2"), "ada")
	require.NoError(t, err)
	require.Equal(t, defstore.StateDraft, second.State)

	stillFirst, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, first.ID, stillFirst.ID)

	// Only approval promotes it, and the approver is persisted.
	require.NoError(t, store.Approve(ctx, tenant, second.ID, "grace"))
	nowSecond, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, second.ID, nowSecond.ID)
	require.Equal(t, "grace", nowSecond.Approver)

	// The superseded revision is not deleted; it steps back to reviewed and
	// keeps who approved it, because history is the point of the record.
	superseded, err := store.Revision(ctx, tenant, first.ID)
	require.NoError(t, err)
	require.Equal(t, defstore.StateReviewed, superseded.State)
	require.Equal(t, "grace", superseded.Approver)
}

// A run executes the definition it started with, full stop. Editing and
// approving a newer revision mid-flight must not change what the running run
// reads back.
func TestRunPinsRevisionAndIsUnaffectedBySubsequentSaves(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	first, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, first.ID, "grace"))

	// The run pins the revision that was active when it started.
	pinned, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, first.ID, pinned.ID)

	// The definition is edited and approved while the run is in flight.
	second, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:2"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, second.ID, "grace"))

	// The run still reads its own revision, byte for byte.
	got, err := store.Get(ctx, tenant, "p1", pinned.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(pipeline("p1", "oci://dhole/build:1"), got))
	require.Equal(t, first.ContentHash, defstore.ContentHash(got))

	// And the newer revision is a different definition, so the assertion above
	// is not passing by accident.
	newer, err := store.Get(ctx, tenant, "p1", second.ID)
	require.NoError(t, err)
	require.Equal(t, "oci://dhole/build:2", newer.GetSteps()[0].GetPluginRef())
}

// Approval is not a lease on the definition. Revoking it — here by approving a
// different revision, which demotes the one that was active — must leave a run
// already pinned to it able to complete on exactly what it started with.
func TestApprovalRevokedMidRunDoesNotAlterRunningRun(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	first, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, first.ID, "grace"))

	pinned, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)

	// Mid-run, approval moves elsewhere: the pinned revision is no longer
	// active for the pipeline.
	replacement, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:3"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, replacement.ID, "grace"))

	revoked, err := store.Revision(ctx, tenant, pinned.ID)
	require.NoError(t, err)
	require.NotEqual(t, defstore.StateActive, revoked.State)

	// The in-flight run finishes on its pinned revision regardless.
	got, err := store.Get(ctx, tenant, "p1", pinned.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(pipeline("p1", "oci://dhole/build:1"), got))
	require.Equal(t, pinned.ContentHash, defstore.ContentHash(got))
}

// Every record in this system is tenant-scoped. An empty tenant is a bug in
// the caller, never a wildcard.
func TestEveryMethodRejectsAnEmptyTenant(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	saved, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)

	_, err = store.Save(ctx, "", pipeline("p1", "oci://dhole/build:1"), "ada")
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
	_, err = store.Get(ctx, "", "p1", saved.ID)
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
	_, err = store.Active(ctx, "", "p1")
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
	_, err = store.Revision(ctx, "", saved.ID)
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
	require.ErrorIs(t, store.Approve(ctx, "", saved.ID, "grace"), defstore.ErrTenantRequired)
	require.Equal(t, "tenant scope required", defstore.ErrTenantRequired.Error())
}

// One tenant's revisions are invisible to another, including by an id guessed
// or leaked from elsewhere.
func TestOneTenantNeverReadsOrApprovesAnother(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	mine, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, mine.ID, "grace"))

	_, err = store.Get(ctx, "other", "p1", mine.ID)
	require.ErrorIs(t, err, defstore.ErrNotFound)
	_, err = store.Revision(ctx, "other", mine.ID)
	require.ErrorIs(t, err, defstore.ErrNotFound)
	_, err = store.Active(ctx, "other", "p1")
	require.ErrorIs(t, err, defstore.ErrNoActiveRevision)
	require.ErrorIs(t, store.Approve(ctx, "other", mine.ID, "grace"), defstore.ErrNotFound)

	// The other tenant approving its own same-content pipeline must not
	// disturb the first tenant's active revision.
	theirs, err := store.Save(ctx, "other", pipeline("p1", "oci://dhole/build:9"), "linus")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, "other", theirs.ID, "grace"))
	active, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, mine.ID, active.ID)
	require.Equal(t, defstore.StateActive, active.State)
}

// Approval is a second pair of eyes. DB-primary storage forfeits forge-native
// PR review (ADR 0008), so the author of a revision may not be the person who
// approves it — otherwise the state machine records a review that never
// happened.
func TestAuthorCannotApproveTheirOwnRevision(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	saved, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)

	require.ErrorIs(t, store.Approve(ctx, tenant, saved.ID, "ada"), defstore.ErrSelfApproval)

	// The refusal leaves the revision exactly as it was.
	still, err := store.Revision(ctx, tenant, saved.ID)
	require.NoError(t, err)
	require.Equal(t, defstore.StateDraft, still.State)
	require.Empty(t, still.Approver)
	_, err = store.Active(ctx, tenant, "p1")
	require.ErrorIs(t, err, defstore.ErrNoActiveRevision)
}

// Re-approving what is already active is a retry, not a change: it succeeds
// and does not rewrite the approver who actually reviewed it.
func TestApproveIsIdempotentOnAnAlreadyActiveRevision(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	saved, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, saved.ID, "grace"))
	require.NoError(t, store.Approve(ctx, tenant, saved.ID, "mallory"))

	active, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, saved.ID, active.ID)
	require.Equal(t, "grace", active.Approver)
}

// Re-saving content identical to an approved revision must not quietly demote
// it back to draft: the hash is the identity, so the save is a no-op.
func TestSavingIdenticalContentDoesNotResetApprovalState(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	saved, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, saved.ID, "grace"))

	again, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "linus")
	require.NoError(t, err)
	require.Equal(t, saved.ID, again.ID)
	require.Equal(t, defstore.StateActive, again.State)
	require.Equal(t, "ada", again.Author)
	require.Equal(t, "grace", again.Approver)
}

// Unknown revisions, and revisions asked for under the wrong pipeline, are not
// found — Get must answer for the revision it was asked for and nothing else.
func TestGetRefusesAnUnknownOrMismatchedRevision(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	one, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	_, err = store.Save(ctx, tenant, pipeline("p2", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)

	_, err = store.Get(ctx, tenant, "p1", "no-such-revision")
	require.ErrorIs(t, err, defstore.ErrNotFound)
	_, err = store.Get(ctx, tenant, "p2", one.ID)
	require.ErrorIs(t, err, defstore.ErrNotFound)
	require.ErrorIs(t, store.Approve(ctx, tenant, "no-such-revision", "grace"), defstore.ErrNotFound)
}

// A revision carries a lockfile — empty until the resolver fills it — and it
// must survive the round trip rather than come back nil and panic a caller.
func TestRevisionCarriesALockfileAcrossTheRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	saved, err := store.Save(ctx, tenant, pipeline("p1", "oci://dhole/build:1"), "ada")
	require.NoError(t, err)
	require.NotNil(t, saved.Lockfile)
	require.Empty(t, saved.Lockfile)

	read, err := store.Revision(ctx, tenant, saved.ID)
	require.NoError(t, err)
	require.NotNil(t, read.Lockfile)
	require.Empty(t, read.Lockfile)
}

// The content hash is the revision's identity, so it must be a property of the
// message and nothing else: not of the pointer, not of which encoder ran, not
// of bytes a newer peer sent that this build does not understand.
func TestContentHashIsAPropertyOfTheMessageAlone(t *testing.T) {
	first := pipeline("p1", "oci://dhole/build:1")
	second := pipeline("p1", "oci://dhole/build:1")

	// Repeated over the same and over an equal message: a hash that wandered
	// between calls would make a pinned revision unresolvable.
	want := defstore.ContentHash(first)
	for range 32 {
		require.Equal(t, want, defstore.ContentHash(first))
		require.Equal(t, want, defstore.ContentHash(second))
	}

	// Unknown fields — what a newer peer's Pipeline round-trips through this
	// build — are retained by proto.Unmarshal and must not enter the hash.
	encoded, err := proto.Marshal(first)
	require.NoError(t, err)
	// Field 1000, wire type 2 (bytes), one byte of payload.
	withUnknown := &dholev1.Pipeline{}
	require.NoError(t, proto.Unmarshal(append(encoded, 0xc2, 0x3e, 0x01, 0x7f), withUnknown))
	require.NotEmpty(t, withUnknown.ProtoReflect().GetUnknown())
	require.Equal(t, want, defstore.ContentHash(withUnknown))

	// Every field is covered: changing any one of them moves the hash.
	seen := map[string]string{"base": want}
	variants := map[string]*dholev1.Pipeline{
		"id":        pipeline("p2", "oci://dhole/build:1"),
		"pluginRef": pipeline("p1", "oci://dhole/build:2"),
		"tenant":    pipeline("p1", "oci://dhole/build:1"),
		"stepName":  pipeline("p1", "oci://dhole/build:1"),
		"port":      pipeline("p1", "oci://dhole/build:1"),
		"edge":      pipeline("p1", "oci://dhole/build:1"),
		"stepOrder": pipeline("p1", "oci://dhole/build:1"),
	}
	variants["tenant"].Tenant.Id = "other"
	variants["stepName"].Steps[0].Name = "Compile"
	variants["port"].Steps[0].Outputs[0].Type = &dholev1.PortType{
		Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{SchemaId: "s1"}},
	}
	variants["edge"].Edges[0].ToPort = "other"
	variants["stepOrder"].Steps[0], variants["stepOrder"].Steps[1] =
		variants["stepOrder"].Steps[1], variants["stepOrder"].Steps[0]

	for name, variant := range variants {
		hash := defstore.ContentHash(variant)
		for other, taken := range seen {
			require.NotEqual(t, taken, hash, "%s hashes the same as %s", name, other)
		}
		seen[name] = hash
	}

	// A golden value: the hash of a stored revision is compared against hashes
	// computed by other builds and other processes, so a change to how it is
	// derived is a breaking change and has to be seen here first.
	require.Equal(t, goldenHash, defstore.ContentHash(pipeline("p1", "oci://dhole/build:1")))
}

// Determinism cannot be observed from a Pipeline as it stands today — it has
// no map field, so a non-deterministic marshal happens to agree with a
// deterministic one. The guarantee has to hold the day someone adds one, and
// by then every stored hash depends on it, so it is pinned at the source.
func TestContentHashMarshalsDeterministically(t *testing.T) {
	source, err := os.ReadFile("revision.go")
	require.NoError(t, err)
	require.Contains(t, string(source), "Deterministic: true",
		"the content hash must marshal deterministically or map ordering makes it unstable")
}
