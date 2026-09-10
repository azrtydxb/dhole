package conformance_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The reference engine is written against docs/wire-contract.md alone, so it
// is also where the contract's object store section is proved implementable by
// a stranger. Two obligations are asserted here rather than through a
// conformance case, because both are about an engine REFUSING to start: the
// suite only ever runs an engine it has configured correctly, so neither
// failure could show up there.
//
// The failure both guard against is the one that costs a day to find: an
// engine that writes a step's log and its outputs somewhere nothing else
// reads. Nothing errors, every step succeeds, and everything the run produced
// is unreachable.

func TestTheReferenceEngineRefusesAnObjectStoreBackendItCannotSpeak(t *testing.T) {
	out, err := runReferenceEngine(t, "DHOLE_OBJECT_STORE=s3", "DHOLE_S3_BUCKET=artifacts")
	require.Error(t, err,
		"an engine that implements only the filesystem backend must refuse `s3` "+
			"rather than write the deployment's artifacts to its own disk:\n%s", out)
	require.Contains(t, out, "DHOLE_OBJECT_STORE",
		"the refusal must name the variable an operator has to change")
}

func TestTheReferenceEngineRefusesAFilesystemStoreWithNoDirectory(t *testing.T) {
	out, err := runReferenceEngine(t, "DHOLE_OBJECT_STORE=filesystem")
	require.Error(t, err,
		"a filesystem store with no directory has nowhere the control plane "+
			"agreed on; defaulting to one invents a location nobody configured:\n%s", out)
	require.Contains(t, out, "DHOLE_BLOB_DIR",
		"the refusal must name the variable an operator has to set")
}

// runReferenceEngine starts the Python engine with a bus URL nothing answers
// on and returns its output. A store misconfiguration must be refused BEFORE
// the bus is dialled: an engine that announced itself and then failed every
// step is a fleet member advertising slots it cannot honour.
func runReferenceEngine(t *testing.T, env ...string) (string, error) {
	t.Helper()
	python := requirePython(t)
	engine := pythonEngine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, python, engine) // #nosec G204 -- both paths are this repository's own
	cmd.Env = append([]string{
		"PATH=" + pathEnv(),
		"DHOLE_BUS_URL=nats://127.0.0.1:1",
		"DHOLE_ENGINE_ID=store-check",
		"DHOLE_TIER=trusted",
	}, env...)
	out, err := cmd.CombinedOutput()
	require.NotEqual(t, context.DeadlineExceeded, ctx.Err(),
		"the engine kept running instead of refusing its configuration:\n%s", out)
	return string(out), err
}

func pathEnv() string {
	python, err := exec.LookPath("python3")
	if err != nil {
		return "/usr/bin:/bin"
	}
	return strings.TrimSuffix(python, "/python3")
}
