package conformance_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/conformance"
)

// The claim this file exists to test is "an engine can be written in any
// language". Running the suite against Dhole's own Go engine would only show
// that the suite agrees with one implementation — and with every assumption
// that implementation never had to write down. The engine under test here is
// Python, speaks the NATS wire protocol over a socket, and hand-encodes
// protobuf, so anything the contract leaves unsaid shows up as a case it
// cannot pass.
//
// The Python engine needs NO third-party package: python3 alone. That is a
// deliberate choice recorded in testdata/engines/minimal-python/README.md —
// nats-py cannot be installed into this repository's Python without either a
// virtualenv or --break-system-packages, and a conformance suite whose engine
// needs an install step is a suite people stop running.

func TestConformanceMinimalPythonEngine(t *testing.T) {
	python := requirePython(t)
	engine := pythonEngine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	report, err := conformance.Run(ctx, conformance.Config{
		Engine: []string{python, engine},
		Logf:   t.Logf,
	})
	require.NoError(t, err)
	t.Log("\n" + report.String())

	require.Equal(t, len(report.Results), report.Passed+report.Failed+report.Skipped,
		"every case must report an outcome; a case that neither passed nor failed certifies nothing")
	require.NotEmpty(t, report.Results, "a report with no cases is not a pass")
	require.Zero(t, report.Failed,
		"the minimal Python engine must satisfy every obligation in docs/wire-contract.md:\n%s", report.String())

	// Every case ran: a suite that silently skipped the expensive half would
	// report zero failures for an engine that fails them.
	names := map[string]bool{}
	for _, r := range report.Results {
		names[r.Name] = true
		require.Equal(t, conformance.StatusPassed, r.Status, "case %q: %s", r.Name, r.Detail)
	}
	for _, want := range []string{
		"registration",
		"version-negotiation",
		"dispatch-and-success",
		"non-zero-exit",
		"cancellation",
		"step-timeout",
		"log-throughput-10mb",
		"binary-artifact-round-trip",
		"secret-redemption",
		"lease-renewal-during-a-long-step",
		"fenced-out-attempt-refused",
	} {
		require.True(t, names[want], "case %q did not run; the suite must cover every contract obligation", want)
	}
}

// TestConformanceDetectsAnEngineThatIgnoresCancellation is the test that makes
// the one above mean something. A suite that cannot fail is worse than no
// suite, because it certifies compliance: this runs a variant of the same
// engine whose only difference is that it drops EngineControl{Cancel}, and
// requires that EXACTLY the cancellation case notices.
func TestConformanceDetectsAnEngineThatIgnoresCancellation(t *testing.T) {
	python := requirePython(t)
	engine := pythonEngine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	report, err := conformance.Run(ctx, conformance.Config{
		Engine: []string{python, engine, "--ignore-cancel"},
		Logf:   t.Logf,
	})
	require.NoError(t, err)
	t.Log("\n" + report.String())

	failed := []string{}
	for _, r := range report.Results {
		if r.Status == conformance.StatusFailed {
			failed = append(failed, r.Name)
		}
	}
	require.Equal(t, []string{"cancellation"}, failed,
		"an engine that ignores Cancel must fail the cancellation case and nothing else:\n%s", report.String())

	for _, r := range report.Results {
		if r.Name != "cancellation" {
			continue
		}
		require.Contains(t, r.Detail, "PHASE_CANCELLED",
			"the failure must tell an engine author what was expected, not merely that a case failed")
	}
}

// requirePython skips rather than fails when python3 is absent: `make test`
// runs on every Go-only task and must not be blocked by an interpreter nobody
// in that task needs.
func requirePython(t *testing.T) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("conformance: python3 is not on PATH — the minimal engine under test is written in Python; " +
			"install python3 to run this test (no third-party package is needed)")
	}
	out, err := exec.Command(python, "-c", "import sys; print(sys.version_info >= (3, 8))").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "True") {
		t.Skipf("conformance: %s is not usable (needs Python 3.8+): %v %s", python, err, out)
	}
	return python
}

func pythonEngine(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "testdata", "engines", "minimal-python", "engine.py"))
	require.NoError(t, err)
	_, err = os.Stat(path)
	require.NoError(t, err, "the engine under test must exist")
	return path
}
