package cli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/policy"
)

// TestServeRefusesToStartOnAnInvalidPolicy: a policy file that cannot work
// stops `dhole serve` before a plane exists, naming the file and the rule. The
// alternative — a plane that starts and refuses every step — is the outage a
// typo in a rule would otherwise be (ADR 0031).
func TestServeRefusesToStartOnAnInvalidPolicy(t *testing.T) {
	for name, c := range map[string]struct{ file, names string }{
		"a rule that does not compile": {
			file:  "revision: r1\nrules:\n  - id: half-written\n    expression: 'input.'\n",
			names: `"half-written"`,
		},
		"no rules at all": {file: "revision: r1\nrules: []\n", names: "no rules"},
		"a misspelt key": {
			file:  "revision: r1\nrules:\n  - id: a\n    expression: 'true'\n    reasn: a typo\n",
			names: "reasn",
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "policy.yaml")
			require.NoError(t, os.WriteFile(path, []byte(c.file), 0o600))

			out := &syncBuffer{}
			cmd := serveCmd(&options{env: Env{Stdout: out, Stderr: io.Discard}})
			cmd.SetArgs([]string{
				"--api-addr", "127.0.0.1:0",
				"--store-dsn", filepath.Join(dir, "dhole.db"),
				"--blob-root", filepath.Join(dir, "state"),
				"--policy", path,
			})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			err := cmd.ExecuteContext(ctx)
			require.Error(t, err, "serve started on a policy that cannot work; it printed:\n%s", out.String())
			require.Contains(t, err.Error(), path, "the refusal does not name the file")
			require.Contains(t, err.Error(), c.names, "the refusal does not say what is wrong")
			require.NotContains(t, out.String(), "control plane", "a plane was started before the policy was read")
			_, statErr := os.Stat(filepath.Join(dir, "dhole.db"))
			require.True(t, os.IsNotExist(statErr), "the run database was opened before the policy was refused")
		})
	}
}

// TestPolicyDefaultPrintsTheBuiltInDocument: the default an operator tightens
// is the document the binary runs, byte for byte, and it is a document `dhole
// policy test` reads.
func TestPolicyDefaultPrintsTheBuiltInDocument(t *testing.T) {
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	require.Zero(t, Main([]string{"policy", "default"}, out, errOut), errOut.String())
	require.Equal(t, string(policy.DefaultDocument()), out.String())

	path := filepath.Join(t.TempDir(), "default.yaml")
	require.NoError(t, os.WriteFile(path, out.Bytes(), 0o600))
	out.Reset()
	require.Zero(t, Main([]string{
		"policy", "test", "--policy", path, "--effect-class", "AT_MOST_ONCE", "--capability", "PRIVILEGED",
	}, out, errOut), "the printed default does not permit what the plane's default permits: %s", errOut.String())
	require.Contains(t, out.String(), "allow")
}

// policyRecorder is the plane's SetPolicy, as the file watcher sees it.
type policyRecorder struct {
	mu  sync.Mutex
	set []policy.TierPolicy
}

func (r *policyRecorder) SetPolicy(p policy.TierPolicy) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.set = append(r.set, p)
	return nil
}

func (r *policyRecorder) installed() []policy.TierPolicy {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]policy.TierPolicy(nil), r.set...)
}

// TestAChangedPolicyFileIsInstalledAndAnInvalidOneIsNot: the plane re-reads its
// policy file. A change is installed under a revision that moves with the
// CONTENT — an author who edits a rule and forgets the revision must not leave
// the engine evaluating programs compiled from the old one — and a change that
// cannot work is reported and not installed, so the policy in force stays.
func TestAChangedPolicyFileIsInstalledAndAnInvalidOneIsNot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	const first = "revision: r1\nrules:\n  - id: a\n    expression: 'true'\n"
	require.NoError(t, os.WriteFile(path, []byte(first), 0o600))
	initial, err := loadPolicyFile(path)
	require.NoError(t, err)

	rec := &policyRecorder{}
	var reports syncBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchPolicyFile(ctx, path, 10*time.Millisecond, initial, rec, &reports)
	}()
	defer func() { cancel(); <-done }()

	await := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("%s; installed %v, reported %q", what, rec.installed(), reports.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Same revision string, different rule.
	const edited = "revision: r1\nrules:\n  - id: b\n    expression: 'input.signed'\n"
	require.NoError(t, os.WriteFile(path, []byte(edited), 0o600))
	await("an edited policy file was not installed", func() bool { return len(rec.installed()) == 1 })
	got := rec.installed()[0]
	require.Equal(t, "b", got.Rules[0].ID)
	require.NotEqual(t, initial.Revision, got.Revision,
		"an edit that kept the revision string was installed under the revision of the rules it replaced")
	require.True(t, strings.HasPrefix(got.Revision, "r1"), "the author's revision is not kept: %s", got.Revision)

	require.NoError(t, os.WriteFile(path, []byte("revision: r2\nrules:\n  - id: c\n    expression: 'input.'\n"), 0o600))
	await("an invalid policy file was not reported", func() bool { return strings.Contains(reports.String(), `"c"`) })
	require.Len(t, rec.installed(), 1, "a policy file that cannot work was installed")
}
