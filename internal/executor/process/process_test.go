package process_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// executorContract is the conformance suite every executor backend must pass.
func executorContract(t *testing.T, e executor.Executor) {
	t.Helper()
	executortest.Contract(t, e)
}

// TestProcessExecutorContract holds the local process engine to the same
// contract a container or VM engine will be held to. Nothing in the suite is
// process-specific: if it stops passing, the backend has diverged from the
// shared semantics, not from a local convention.
func TestProcessExecutorContract(t *testing.T) {
	executorContract(t, process.New())
}

// TestProcessExecutorReportsNoEnvironmentIdentity pins the honest answer: a
// bare host process runs against whatever toolchain the host happens to have,
// which cannot be hashed. Returning a fabricated identity would let Task 16
// cache steps whose environment silently changed.
func TestProcessExecutorReportsNoEnvironmentIdentity(t *testing.T) {
	id, err := process.New().EnvironmentIdentity()
	require.Empty(t, id)
	require.ErrorIs(t, err, executor.ErrNoStableIdentity)
}

// TestProcessExecutorAdvertisesNoCapabilities pins the empty capability set: a
// process sandbox cannot promise privilege separation or host-mount control,
// so it must not advertise them and be scheduled work that assumes them.
func TestProcessExecutorAdvertisesNoCapabilities(t *testing.T) {
	require.Empty(t, process.New().Capabilities())
	require.Equal(t, "process", process.New().Kind())
}
