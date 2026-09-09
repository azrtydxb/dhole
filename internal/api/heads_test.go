package api_test

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// planeOn is one control plane over a database somebody else may also be
// holding. That is the whole point of this file: the check under test is not
// "does this process refuse its own second edit", it is "do two processes
// sharing a store refuse the second edit between them".
func planeOn(t *testing.T, db *sql.DB) dholev1connect.PipelineServiceClient {
	t.Helper()
	defs := defstore.NewWithDialect(db, runstore.DialectSQLite, defstore.WithoutPinning())
	srv, err := api.NewServer(api.Config{
		Definitions:  defs,
		Auth:         fakeAuth{},
		Heads:        defstore.NewHeads(db, runstore.DialectSQLite),
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	http := httptest.NewServer(srv.Handler())
	t.Cleanup(http.Close)
	return dholev1connect.NewPipelineServiceClient(http.Client(), http.URL)
}

// TestTwoPlanesCannotBothAcceptAnEditAgainstTheSameBase is the property a head
// kept in one process's memory cannot have.
//
// Optimistic concurrency is the whole justification for base_revision being
// mandatory (ADR 0013). While the head lived in a map, that check held within
// one plane and did not exist at all between two: both would read "no head I
// know of", both would accept, and the loser's edit would vanish with nothing
// anywhere recording that it had been made.
func TestTwoPlanesCannotBothAcceptAnEditAgainstTheSameBase(t *testing.T) {
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "shared.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	defs := defstore.NewWithDialect(db, runstore.DialectSQLite, defstore.WithoutPinning())
	base, err := defs.Save(ctx, tenantA, basePipeline(tenantA), "alice")
	require.NoError(t, err)

	one, two := planeOn(t, db), planeOn(t, db)

	// Both planes are told the same head, so both seed it, which is the
	// documented behaviour for a pipeline no plane has edited yet.
	_, err = one.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "rename"),
	}, tokenAlice))
	require.NoError(t, err, "the first edit against an unedited pipeline is accepted")

	// The second plane still believes base is current. It is not, and this is
	// the moment the two planes have to disagree with each other rather than
	// both agreeing with a caller.
	//
	// The edit OVERLAPS the first — both rename step "c" — because two edits
	// that touch different steps are merged rather than refused (presence.go).
	// The head's compare-and-set is what decides which of two planes gets to
	// write; this asserts that it decides.
	_, err = two.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_Rename{
			Rename: &dholev1.Rename{StepId: "c", Name: "the second plane"},
		}},
	}, tokenAlice))
	require.Error(t, err, "two planes both accepted an edit against the same base")
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))
}

// TestConcurrentPlanesAgreeOnExactlyOneWinner runs four planes at the same
// base at once and requires exactly one of them to win.
//
// It is honest about what it does NOT prove. Which of the two checks refuses
// the other three depends on timing: the read-then-compare in ApplyOperation
// usually gets there first, so this test passes even with the compare removed
// from the store's own update. The compare-and-set is held to its contract by
// headsContract in internal/defstore, where it can be exercised directly; this
// test is the end-to-end statement that four planes do not produce four heads.
func TestConcurrentPlanesAgreeOnExactlyOneWinner(t *testing.T) {
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "shared.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	defs := defstore.NewWithDialect(db, runstore.DialectSQLite, defstore.WithoutPinning())
	base, err := defs.Save(ctx, tenantA, basePipeline(tenantA), "alice")
	require.NoError(t, err)

	planes := []dholev1connect.PipelineServiceClient{
		planeOn(t, db), planeOn(t, db), planeOn(t, db), planeOn(t, db),
	}
	// One DIFFERENT edit per plane, so the four would produce four different
	// revisions and three of them have to be refused. Identical edits would
	// produce the same content-addressed revision and hide the race.
	names := []string{"one", "two", "three", "four"}

	var (
		start sync.WaitGroup
		done  sync.WaitGroup
		mu    sync.Mutex
		wins  []string
	)
	start.Add(1)
	for i, plane := range planes {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			res, err := plane.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
				PipelineId:   "pipe-1",
				BaseRevision: base.ID,
				Operation: &dholev1.Operation{Kind: &dholev1.Operation_Rename{
					Rename: &dholev1.Rename{StepId: "c", Name: names[i]},
				}},
			}, tokenAlice))
			if err != nil {
				require.Equal(t, connect.CodeAborted, connect.CodeOf(err))
				return
			}
			mu.Lock()
			wins = append(wins, res.Msg.GetRevision().GetId())
			mu.Unlock()
		}()
	}
	start.Done()
	done.Wait()

	require.Len(t, wins, 1, "more than one plane won an edit against the same base")

	heads := defstore.NewHeads(db, runstore.DialectSQLite)
	head, known, err := heads.Head(ctx, tenantA, "pipe-1")
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, wins[0], head, "the stored head is not the edit that won")
}

// TestTheStoredHeadIsWhatAnUnqualifiedReadResolvesTo: the head is a stored
// fact, so a second plane resolves "the current definition" to the edit the
// first one made, and not to the approved revision or to nothing.
func TestTheStoredHeadIsWhatAnUnqualifiedReadResolvesTo(t *testing.T) {
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "shared.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	defs := defstore.NewWithDialect(db, runstore.DialectSQLite, defstore.WithoutPinning())
	base, err := defs.Save(ctx, tenantA, basePipeline(tenantA), "alice")
	require.NoError(t, err)

	one, two := planeOn(t, db), planeOn(t, db)

	applied, err := one.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   "pipe-1",
		BaseRevision: base.ID,
		Operation:    fixtureFor(t, "rename"),
	}, tokenAlice))
	require.NoError(t, err)

	got, err := two.GetPipeline(ctx, authed(&dholev1.GetPipelineRequest{
		PipelineId: "pipe-1",
	}, tokenAlice))
	require.NoError(t, err)
	require.Equal(t, applied.Msg.GetRevision().GetId(), got.Msg.GetRevision().GetId(),
		"a second plane read a head the first plane had already moved")
}

// TestHeadsAreScopedToTheirTenant: two tenants editing pipelines of the same
// id hold two independent heads, as every other stored record here does.
func TestHeadsAreScopedToTheirTenant(t *testing.T) {
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "shared.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	heads := defstore.NewHeads(db, runstore.DialectSQLite)

	require.NoError(t, heads.CompareAndSetHead(ctx, tenantA, "pipe-1", "", "rev_a"))
	require.NoError(t, heads.CompareAndSetHead(ctx, tenantB, "pipe-1", "", "rev_b"))

	got, known, err := heads.Head(ctx, tenantA, "pipe-1")
	require.NoError(t, err)
	require.True(t, known)
	require.Equal(t, "rev_a", got)

	// The move that was not made from the current head is refused, which is
	// the compare half of compare-and-set: without it two callers who each
	// read rev_a would both write.
	require.ErrorIs(t, heads.CompareAndSetHead(ctx, tenantA, "pipe-1", "rev_stale", "rev_c"),
		defstore.ErrHeadMoved)
	require.ErrorIs(t, heads.CompareAndSetHead(ctx, tenantA, "pipe-1", "", "rev_c"),
		defstore.ErrHeadMoved)
	require.NoError(t, heads.CompareAndSetHead(ctx, tenantA, "pipe-1", "rev_a", "rev_c"))

	_, _, err = heads.Head(ctx, "", "pipe-1")
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
}
