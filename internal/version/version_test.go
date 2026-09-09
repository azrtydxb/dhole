package version_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/version"
)

// TestVersionIsSetAtBuild fails if a binary cannot name itself: either the
// linker stamped it, or the embedded build info answered. "unknown" means
// both paths broke, and a bug report from that binary is uncorrelatable.
func TestVersionIsSetAtBuild(t *testing.T) {
	require.NotEmpty(t, version.Version())
	require.NotEqual(t, "unknown", version.Version())
}

// TestCommitIsAlwaysAnswered fails if Commit() ever returns the empty string.
// Go omits VCS stamping from test binaries, so "unknown" is the correct
// answer here; silence is not.
func TestCommitIsAlwaysAnswered(t *testing.T) {
	require.NotEmpty(t, version.Commit())
}
