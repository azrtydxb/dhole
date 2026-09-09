package bus_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
)

// TestSubjectsMatchDocumentedContract pins the builders to the subject table in
// docs/wire-contract.md. The document is the contract an engine in any language
// reads; if the code and the document disagree, the code is wrong.
func TestSubjectsMatchDocumentedContract(t *testing.T) {
	require.Equal(t, "job.dispatch.untrusted.abc", bus.SubjectDispatch("untrusted", "abc"))
	require.Equal(t, "job.dispatch.trusted.deadbeef", bus.SubjectDispatch("trusted", "deadbeef"))
	require.Equal(t, "job.status.run-1.build", bus.SubjectStatus("run-1", "build"))
	require.Equal(t, "job.logs.run-1.build", bus.SubjectLogs("run-1", "build"))
	require.Equal(t, "engine.control.e1", bus.SubjectEngineControl("e1"))
	require.Equal(t, "engine.heartbeat.e1", bus.SubjectEngineHeartbeat("e1"))
	require.Equal(t, "engine.registration", bus.SubjectEngineRegistration())
}

// TestTierWildcardsScopeToOneTier is what the bus permissions are written
// against: an engine subscribes to its own tier's dispatch wildcard and to
// nothing else.
func TestTierWildcardsScopeToOneTier(t *testing.T) {
	require.Equal(t, "job.dispatch.untrusted.*", bus.SubjectDispatchWildcard("untrusted"))
	require.Equal(t, "job.dispatch.trusted.*", bus.SubjectDispatchWildcard("trusted"))
}
