package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// taintPolicyFile is a taint rule of the shape internal/taint's built-in one
// has: it says nothing about clean data, and refuses an effectful step that
// carries any.
const taintPolicyFile = `revision: rev-1
rules:
  - id: acme.taint
    expression: '!input.tainted || input.effect_class == "PURE"'
    reason: an effectful step may not act on untrusted data
`

// TestPolicyTestExercisesATaintRule: a taint rule is only auditable and
// changeable if its author can run it before shipping it. `dhole policy test`
// is where they do that, so the taint half of the input has to be reachable
// from the flags — otherwise the one dimension that used to be hard-coded in
// Go stays untestable in exactly the place a policy is proven.
func TestPolicyTestExercisesATaintRule(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(taintPolicyFile), 0o600))

	// Tainted data reaching an at-most-once step: refused.
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"policy", "test", "--policy", path, "--tier", "trusted",
		"--effect-class", "AT_MOST_ONCE",
		"--taint-source", "git:github:pushes",
		"--expect-allow=false",
	}, out, errOut)
	require.Zerof(t, code, "the rule denies and --expect-allow=false says so: %s", errOut.String())
	require.Contains(t, out.String(), "acme.taint")

	// The same step with no taint on it: the rule says nothing about clean
	// data, and a taint rule that refused it would be an effect-class policy
	// nobody wrote.
	out, errOut = &bytes.Buffer{}, &bytes.Buffer{}
	code = cli.Main([]string{
		"policy", "test", "--policy", path, "--tier", "trusted",
		"--effect-class", "AT_MOST_ONCE",
	}, out, errOut)
	require.Zerof(t, code, "clean data is untouched by a taint rule: %s", errOut.String())
	require.Contains(t, out.String(), "allow")
}

// TestPolicyTestExercisesTheEngineCapabilities: the privileged-engine rule
// reads the capabilities of the ENGINE, which are not the step's own. A CLI
// that could only express the step's would leave that rule untestable.
func TestPolicyTestExercisesTheEngineCapabilities(t *testing.T) {
	const file = `revision: rev-1
rules:
  - id: acme.privileged-engine
    expression: '!input.tainted || !("PRIVILEGED" in input.engine_capabilities)'
    reason: untrusted data may not reach a privileged engine
`
	path := filepath.Join(t.TempDir(), "policy.yaml")
	require.NoError(t, os.WriteFile(path, []byte(file), 0o600))

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"policy", "test", "--policy", path, "--tier", "trusted",
		"--effect-class", "PURE",
		"--taint-source", "http:public:hooks",
		"--engine-capability", "PRIVILEGED",
		"--expect-allow=false",
	}, out, errOut)
	require.Zerof(t, code, "a pure step still may not run tainted data on a privileged engine: %s",
		errOut.String())
	require.Contains(t, out.String(), "acme.privileged-engine")
}
