package runstore_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestAPrecomputedSequenceSilentlyLosesAnEvent records the hazard that makes
// the guard below worth having.
//
// Append is ON CONFLICT DO NOTHING, which is what makes a redelivery
// idempotent. The cost is that two different events written under the same
// (run, step, attempt, sequence) are not a conflict a caller hears about: the
// loser is simply never stored, and no error is returned to anyone.
//
// Two callers that each read LastSequence and add one produce exactly that
// key. This test is what that looks like.
func TestAPrecomputedSequenceSilentlyLosesAnEvent(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "seq.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	last, err := store.LastSequence(ctx, tenant)
	require.NoError(t, err)

	for _, typ := range []runstore.EventType{runstore.StepReady, runstore.StepDispatched} {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", StepID: "s", Attempt: 1,
			Sequence: last + 1,
			Type:     typ, At: time.Now().UTC(),
		}), "no caller is told anything went wrong — that is the hazard")
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, events, 1,
		"both appends reported success and only one event exists")
}

// TestTheStoresAllocationDoesNotLoseAnEvent is the same two writes with the
// sequence left to the store, which is what every caller must do.
func TestTheStoresAllocationDoesNotLoseAnEvent(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "seq.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	for _, typ := range []runstore.EventType{runstore.StepReady, runstore.StepDispatched} {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", StepID: "s", Attempt: 1,
			Type: typ, At: time.Now().UTC(),
		}))
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, events, 2, "the store allocates, so neither write can claim the other's key")
}

// TestNoCallerComputesItsOwnSequence is the guard. It parses every package
// outside runstore and fails on a call to LastSequence, because the only
// reason to read the high-water mark is to add one to it — and that is the
// pattern above.
//
// A test that merely fixed today's three callers would not stop a fourth
// being written next month, and the failure it causes is silent.
func TestNoCallerComputesItsOwnSequence(t *testing.T) {
	root := filepath.Join("..", "..")
	var offenders []string

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "gen", "web", "runstore":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "LastSequence" {
				offenders = append(offenders, path)
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)

	require.Empty(t, offenders,
		"these call LastSequence, and the only use for it is to add one — "+
			"pass Sequence 0 and let the store allocate inside the transaction")
}
