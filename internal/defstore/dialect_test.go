package defstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// uniqueTenant keeps every dialect run in its own tenant. SQLite gets a fresh
// file per test, but Postgres is a shared, persistent database: reusing a
// tenant would make an assertion depend on what an earlier run left behind.
// Tenant scoping is the isolation this system already guarantees, so the test
// uses it rather than truncating tables.
var (
	tenantSeq atomic.Uint64
	tenantRun = strconv.FormatInt(time.Now().UnixNano(), 36)
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%s-%d", name, tenantRun, tenantSeq.Add(1))
}

// postgresDSN is the live Postgres the integration suite talks to. A test that
// cannot reach its dependency skips with a reason; it never passes quietly,
// because an integration test that silently degrades to nothing reports green
// while testing nothing.
func postgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	return dsn
}

// postgresDB opens a migrated Postgres handle. The migrations are the run
// store's, because there is one schema and one runner for it.
func postgresDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := runstore.OpenPostgres(context.Background(), postgresDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestSQLiteSatisfiesDefinitionStoreContract and its Postgres twin hold both
// dialects to one contract. SQLite is what a developer and a homelab run,
// Postgres is the tuned target, and a definition store that works on one and
// not the other is a defect a user discovers in production rather than here.
func TestSQLiteSatisfiesDefinitionStoreContract(t *testing.T) {
	definitionStoreContract(t, newStore(t))
}

func TestPostgresSatisfiesDefinitionStoreContract(t *testing.T) {
	definitionStoreContract(t, defstore.NewWithDialect(postgresDB(t), runstore.DialectPostgres, defstore.WithResolver(stubResolver{})))
}

// definitionStoreContract is the behaviour both dialects must show.
func definitionStoreContract(t *testing.T, store defstore.Store) {
	t.Helper()

	t.Run("SaveStartsAsADraftAndIsIdempotent", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		p := pipeline("p1", "oci://dhole/build:1")
		first, err := store.Save(ctx, scope, p, "ada")
		require.NoError(t, err)
		require.Equal(t, defstore.StateDraft, first.State)
		require.Equal(t, "ada", first.Author)

		again, err := store.Save(ctx, scope, p, "someone-else")
		require.NoError(t, err)
		require.Equal(t, first.ID, again.ID, "identical content is the same revision")
		require.Equal(t, "ada", again.Author, "a re-save must not rewrite the author")
	})

	t.Run("GetReturnsExactlyThePinnedDefinition", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		pinned, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)
		_, err = store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:2"), "ada")
		require.NoError(t, err)

		got, err := store.Get(ctx, scope, "p1", pinned.ID)
		require.NoError(t, err)
		require.Equal(t, pinned.ContentHash, defstore.ContentHash(got),
			"a run reads back the revision it pinned, not the newest one")
	})

	t.Run("ApprovePromotesAndDemotesTheSupersededRevision", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		first, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)
		_, err = store.Active(ctx, scope, "p1")
		require.ErrorIs(t, err, defstore.ErrNoActiveRevision)

		require.NoError(t, store.Approve(ctx, scope, first.ID, "grace"))
		active, err := store.Active(ctx, scope, "p1")
		require.NoError(t, err)
		require.Equal(t, first.ID, active.ID)
		require.Equal(t, defstore.StateActive, active.State)
		require.Equal(t, "grace", active.Approver)

		second, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:2"), "ada")
		require.NoError(t, err)
		require.NoError(t, store.Approve(ctx, scope, second.ID, "grace"))

		superseded, err := store.Revision(ctx, scope, first.ID)
		require.NoError(t, err)
		require.Equal(t, defstore.StateReviewed, superseded.State,
			"at most one active revision: the old one steps back rather than vanishing")
		active, err = store.Active(ctx, scope, "p1")
		require.NoError(t, err)
		require.Equal(t, second.ID, active.ID)
	})

	t.Run("SelfApprovalIsRefused", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		saved, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)
		require.ErrorIs(t, store.Approve(ctx, scope, saved.ID, "ada"), defstore.ErrSelfApproval)

		still, err := store.Revision(ctx, scope, saved.ID)
		require.NoError(t, err)
		require.Equal(t, defstore.StateDraft, still.State)
	})

	t.Run("RevisionsListsThePipelinesHistoryOldestFirst", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		first, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)
		second, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:2"), "ada")
		require.NoError(t, err)
		third, err := store.Save(ctx, scope, pipeline("p1", "oci://dhole/build:3"), "ada")
		require.NoError(t, err)
		// Another pipeline of the same tenant, which must not appear below.
		other, err := store.Save(ctx, scope, pipeline("p2", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)

		require.NoError(t, store.Approve(ctx, scope, second.ID, "grace"))

		history, err := store.Revisions(ctx, scope, "p1")
		require.NoError(t, err)

		ids := make([]string, 0, len(history))
		for _, rev := range history {
			ids = append(ids, rev.ID)
		}
		require.Equal(t, []string{first.ID, second.ID, third.ID}, ids,
			"the history is the pipeline's revisions, oldest first, and nobody else's")
		require.NotContains(t, ids, other.ID)

		// The metadata is the same metadata Revision() answers with: a
		// listing that lost the approval state would make the history a
		// different fact from the record.
		require.Equal(t, defstore.StateActive, history[1].State)
		require.Equal(t, "grace", history[1].Approver)
		require.Equal(t, "ada", history[0].Author)
	})

	t.Run("RevisionsOfAnotherTenantIsEmptyRatherThanTheirs", func(t *testing.T) {
		ctx := context.Background()
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)

		_, err := store.Save(ctx, mine, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)

		history, err := store.Revisions(ctx, theirs, "p1")
		require.NoError(t, err)
		require.Empty(t, history, "a pipeline of another tenant has no history here")

		_, err = store.Revisions(ctx, "", "p1")
		require.ErrorIs(t, err, defstore.ErrTenantRequired)
	})

	t.Run("ReadsAreScopedToTheirTenant", func(t *testing.T) {
		ctx := context.Background()
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)

		saved, err := store.Save(ctx, mine, pipeline("p1", "oci://dhole/build:1"), "ada")
		require.NoError(t, err)

		_, err = store.Get(ctx, theirs, "p1", saved.ID)
		require.ErrorIs(t, err, defstore.ErrNotFound,
			"another tenant's revision must be indistinguishable from one that does not exist")
		_, err = store.Revision(ctx, theirs, saved.ID)
		require.ErrorIs(t, err, defstore.ErrNotFound)
		require.ErrorIs(t, store.Approve(ctx, theirs, saved.ID, "grace"), defstore.ErrNotFound)
	})

	t.Run("AnUnscopedQueryIsRefused", func(t *testing.T) {
		ctx := context.Background()
		_, err := store.Save(ctx, "", pipeline("p1", "oci://dhole/build:1"), "ada")
		require.ErrorIs(t, err, defstore.ErrTenantRequired)
		_, err = store.Get(ctx, "", "p1", "rev_1")
		require.ErrorIs(t, err, defstore.ErrTenantRequired)
	})
}
