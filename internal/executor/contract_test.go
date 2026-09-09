package executor_test

import (
	"testing"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
)

// executorContract is the name the plan gives the conformance suite. The body
// lives in executortest because a _test.go file cannot be imported by the
// backend packages that have to run it; this alias keeps the suite reachable
// under its planned name from inside the executor package's own tests.
func executorContract(t *testing.T, e executor.Executor) {
	t.Helper()
	executortest.Contract(t, e)
}

// The alias is exercised by each backend package (see the process executor's
// TestProcessExecutorContract); this keeps it referenced here as well.
var _ = executorContract
