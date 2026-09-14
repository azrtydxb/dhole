package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// TestAPersonReadingARunLogSeesWhoDecidedAGateAndWhy. `dhole run logs` printed
// STEP_APPROVAL_DECIDED as a bare event type and a step id: the log of a
// refused deploy said a decision happened and nothing about who made it or
// why, although the event carried both. The JSON output had them; the text
// output a person actually reads did not.
func TestAPersonReadingARunLogSeesWhoDecidedAGateAndWhy(t *testing.T) {
	payload, err := json.Marshal(map[string]any{
		"approver": "pascal",
		"approved": false,
		"reason":   "the scan found a real CVE in the base image",
	})
	require.NoError(t, err)

	line := eventDetail(&dholev1.WatchRunResponse{
		Type: "STEP_APPROVAL_DECIDED", StepId: "deploy", Payload: payload,
	})
	require.Contains(t, line, "denied by pascal")
	require.Contains(t, line, "the scan found a real CVE in the base image")
}

// TestAnEventWithNothingToAddPrintsNoDetail keeps every other line unchanged:
// a detail column that appeared on every event would bury the one line that
// has something to say.
func TestAnEventWithNothingToAddPrintsNoDetail(t *testing.T) {
	require.Empty(t, eventDetail(&dholev1.WatchRunResponse{Type: "STEP_SUCCEEDED"}))
}
