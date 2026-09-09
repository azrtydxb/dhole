package cli_test

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// pipelineFile is the same definition internal/server runs end to end. `dhole
// local run` has to execute the document a user would actually write, not a
// fixture shaped to suit it.
const pipelineFile = "../../../testdata/pipelines/two-step.yaml"

// TestLocalRunExecutesWithoutServer is the promise the single binary makes to
// somebody who has not deployed anything: the pipeline they are editing runs
// on their laptop, through the real scheduler, the real bus and a real engine,
// with no control plane to point at.
//
// It asserts on the STEP OUTPUT rather than on an exit code, because a
// `local run` that started a run and printed "ok" without waiting for the
// bytes would satisfy every weaker assertion.
func TestLocalRunExecutesWithoutServer(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an embedded control plane")
	}
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"local", "run", pipelineFile, "--state-dir", filepath.Join(t.TempDir(), "state"),
	}, out, errOut)
	require.Zero(t, code, "stderr: %s", errOut)

	printed := out.String()
	require.Contains(t, printed, "hello from a", "step a's output must be printed")
	require.Contains(t, printed, "; b read it",
		"step b's output must be printed, and it can only exist if a's bytes reached b")
}

// TestLocalRunEmitsJSON is the same run as something a script can read.
func TestLocalRunEmitsJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an embedded control plane")
	}
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"--output", "json", "local", "run", pipelineFile,
		"--state-dir", filepath.Join(t.TempDir(), "state"),
	}, out, errOut)
	require.Zero(t, code, "stderr: %s", errOut)

	var decoded struct {
		RunID string `json:"run_id"`
		Steps []struct {
			StepID  string            `json:"step_id"`
			Status  string            `json:"status"`
			Outputs map[string]string `json:"outputs"`
		} `json:"steps"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded), "stdout was not JSON: %q", out.String())
	require.NotEmpty(t, decoded.RunID)
	require.Len(t, decoded.Steps, 2)
	require.Equal(t, "hello from a", decoded.Steps[0].Outputs["out"])
	require.Contains(t, decoded.Steps[1].Outputs["out"], "hello from a")
}

// TestLocalRunOnABrokenDefinitionFails keeps `local run` from reporting
// success for a pipeline that never ran.
func TestLocalRunOnABrokenDefinitionFails(t *testing.T) {
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{"local", "run", filepath.Join(t.TempDir(), "absent.yaml")}, out, errOut)
	require.NotZero(t, code)
	require.Empty(t, out.String())
	require.NotEmpty(t, errOut.String())
}
