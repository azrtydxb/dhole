package docs

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// quickstartTimeout bounds the whole page. The blocks build two Go binaries
// and start a control plane, so this is generous; what it is really for is a
// block that waits for something that will never arrive.
const quickstartTimeout = 10 * time.Minute

// fencedBash matches a ```bash fence and captures its body. Only `bash` is
// run: a page may show output in a ```console block or a file in ```yaml
// without this test trying to execute it.
var fencedBash = regexp.MustCompile("(?s)\n```bash\n(.*?)\n```\n")

// TestQuickstartCommandsRunAsWritten runs the quickstart.
//
// Not "parses", not "mentions a command that exists": RUNS, in order, in a
// scratch directory, requiring exit 0 from every block. A quickstart nobody
// has run is a quickstart that does not work, and this repository has already
// shipped one config file that had evidently never been executed.
//
// The blocks are run in sequence in one directory, and exported variables
// carry from each block to the next, because that is what a reader pasting
// them into one terminal gets. Everything the page needs from the checkout it
// takes through DHOLE_SRC, which defaults to the working directory — so in a
// clone the blocks read as themselves.
func TestQuickstartCommandsRunAsWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("the quickstart builds two binaries and starts a control plane")
	}
	root := repoRoot(t)

	page, err := os.ReadFile(filepath.Join(root, "docs", "quickstart.md"))
	require.NoError(t, err, "the quickstart is the one page that must exist")

	blocks := fencedBash.FindAllStringSubmatch("\n"+string(page)+"\n", -1)
	require.NotEmpty(t, blocks,
		"docs/quickstart.md has no ```bash blocks: a quickstart with nothing to run "+
			"passes this test while helping nobody")

	scratch := t.TempDir()
	envFile := filepath.Join(scratch, "quickstart.env")

	// HOME is redirected into the scratch directory so that a block which
	// forgot a --store-dsn writes there rather than into the developer's own
	// state. Go derives GOPATH and its caches from HOME, though, so they are
	// pinned back to the real ones: otherwise every run of this test downloads
	// the module graph again into a directory it then cannot delete.
	goEnv := goEnv(t, "GOPATH", "GOCACHE", "GOMODCACHE")

	for i, block := range blocks {
		body := block[1]
		t.Run(firstLine(body), func(t *testing.T) {
			script := preamble(envFile) + "\n" + body + "\n" + postamble(envFile) + "\n"

			cmd := exec.Command("bash", "-c", script)
			cmd.Dir = scratch
			cmd.Env = append(os.Environ(),
				"DHOLE_SRC="+root,
				// A control plane on the well-known port would collide with
				// whatever the developer running the suite already has up.
				"DHOLE_API_PORT=0",
				// The scratch directory is the whole world of this run: a
				// block must not reach a `dhole` already installed on the
				// machine, or the page would be proven against a binary it
				// never built.
				"PATH="+scratch+string(os.PathListSeparator)+os.Getenv("PATH"),
				"HOME="+scratch,
			)
			cmd.Env = append(cmd.Env, goEnv...)
			out, err := runWithin(cmd, quickstartTimeout)
			require.NoError(t, err,
				"docs/quickstart.md block %d did not run as written:\n%s\n--- output ---\n%s",
				i+1, body, out)
		})
	}
}

// goEnv reads the named `go env` values so they can be pinned across the HOME
// redirection above.
func goEnv(t *testing.T, names ...string) []string {
	t.Helper()
	out, err := exec.Command("go", append([]string{"env"}, names...)...).Output()
	require.NoError(t, err, "reading go env %v", names)
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	require.Len(t, lines, len(names))
	env := make([]string, 0, len(names))
	for i, name := range names {
		env = append(env, name+"="+lines[i])
	}
	return env
}

// preamble restores what earlier blocks exported, so the page behaves like one
// terminal session rather than a series of unrelated ones.
//
// The readonly shell variables are filtered out: sourcing `declare -x
// SHELLOPTS=...` fails, and under `set -e` that would fail the block for a
// reason that has nothing to do with what the page told anyone to do.
func preamble(envFile string) string {
	return "set -euo pipefail\n" +
		"if [ -f " + envFile + " ]; then source " + envFile + "; fi\n"
}

func postamble(envFile string) string {
	return "export -p | grep -Ev " +
		"'^declare -x (SHELLOPTS|BASHOPTS|PWD|OLDPWD|UID|EUID|PPID|BASH_[A-Z]+)' > " +
		envFile + " || true\n"
}

// runWithin runs cmd, killing it if it outlives d, and returns its combined
// output either way.
func runWithin(cmd *exec.Cmd, d time.Duration) (string, error) {
	var out strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return out.String(), err
	case <-time.After(d):
		_ = cmd.Process.Kill()
		<-done
		return out.String(), errTimeout
	}
}

var errTimeout = &timeoutError{}

type timeoutError struct{}

func (*timeoutError) Error() string { return "the block did not finish" }

// firstLine names the subtest after the block's first meaningful line, so a
// failure says which block without counting fences.
func firstLine(body string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			return line
		}
	}
	return "empty"
}
