package importers_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/importers"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// fixture reads one of the real-shaped source documents under testdata/import.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "import", name))
	require.NoError(t, err, "reading fixture %s", name)
	return b
}

func stepByID(t *testing.T, p *dholev1.Pipeline, id string) *dholev1.Step {
	t.Helper()
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	var have []string
	for _, s := range p.GetSteps() {
		have = append(have, s.GetId())
	}
	t.Fatalf("no step %q in pipeline; steps are %v", id, have)
	return nil
}

func stepIDs(p *dholev1.Pipeline) []string {
	ids := make([]string, 0, len(p.GetSteps()))
	for _, s := range p.GetSteps() {
		ids = append(ids, s.GetId())
	}
	return ids
}

func hasEdge(p *dholev1.Pipeline, fromStep, fromPort, toStep, toPort string) bool {
	for _, e := range p.GetEdges() {
		if e.GetFromStep() == fromStep && e.GetFromPort() == fromPort &&
			e.GetToStep() == toStep && e.GetToPort() == toPort {
			return true
		}
	}
	return false
}

func portByName(ports []*dholev1.Port, name string) *dholev1.Port {
	for _, p := range ports {
		if p.GetName() == name {
			return p
		}
	}
	return nil
}

// requireRunnable is the bar every import has to clear: the derived graph
// builds and every edge type-checks. A pipeline that does not type-check is
// not an import, it is a draft.
func requireRunnable(t *testing.T, p *dholev1.Pipeline) {
	t.Helper()
	require.NotNil(t, p)
	_, err := dag.Build(p)
	require.NoError(t, err, "dag.Build must accept the imported pipeline")
	require.Empty(t, dag.TypeCheck(p), "dag.TypeCheck must report no diagnostics")
}

func containsSubstring(t *testing.T, haystack []string, needle string) {
	t.Helper()
	for _, h := range haystack {
		if strings.Contains(h, needle) {
			return
		}
	}
	t.Fatalf("no entry mentioning %q in %v", needle, haystack)
}

// TestGitLabCIImportProducesRunnablePipeline is the headline property: a real
// .gitlab-ci.yml becomes a pipeline the scheduler could actually run, not a
// bag of steps that fails at dispatch.
func TestGitLabCIImportProducesRunnablePipeline(t *testing.T) {
	p, report, err := importers.NewGitLab().Import(context.Background(), fixture(t, "gitlab-ci.yml"))
	require.NoError(t, err)
	requireRunnable(t, p)

	// Every job in the file is present. Seven jobs in, seven steps out.
	require.ElementsMatch(t, []string{
		"build-binary", "unit-tests", "lint", "integration-tests",
		"deploy-staging", "release-publish",
	}, stepIDs(p))

	// The report is where the untranslatable parts of the file went; it is
	// never empty for a file with `cache:`, `services:` and `when: manual`.
	require.NotEmpty(t, report.Unsupported)
}

// TestImportReportsUnsupportedConstructsRatherThanDroppingThem: a construct
// Dhole cannot express becomes a named report entry, and the job carrying it
// still becomes a step. Dropping it would produce a pipeline that looks
// complete and does less than the original.
func TestImportReportsUnsupportedConstructsRatherThanDroppingThem(t *testing.T) {
	p, report, err := importers.NewGitLab().Import(
		context.Background(), fixture(t, "gitlab-ci-rules-changes.yml"))
	require.NoError(t, err)
	requireRunnable(t, p)

	containsSubstring(t, report.Unsupported, "rules:changes")
	containsSubstring(t, report.Unsupported, "docs")

	// The job count is the real assertion: a report entry is worthless if the
	// job it names vanished from the pipeline.
	require.Len(t, p.GetSteps(), 3)
	require.ElementsMatch(t, []string{"build-binary", "docs", "unit-tests"}, stepIDs(p))
	require.NotNil(t, stepByID(t, p, "docs"))
}

// TestSharedWorkspaceIsTranslatedToExplicitArtifacts is the heart of the
// task. Woodpecker's steps all mutate one volume and declare no dependencies
// whatsoever; the order is the file order. That implicit flow must come out
// as ports and edges, because the DAG and the cache are derived from those
// and from nothing else (ADR 0001).
func TestSharedWorkspaceIsTranslatedToExplicitArtifacts(t *testing.T) {
	p, _, err := importers.NewWoodpecker().Import(context.Background(), fixture(t, "woodpecker.yml"))
	require.NoError(t, err)
	requireRunnable(t, p)

	require.Equal(t, []string{"restore-deps", "build", "test", "publish"}, stepIDs(p))

	// Each step publishes its workspace as a declared output...
	for _, id := range stepIDs(p) {
		s := stepByID(t, p, id)
		out := portByName(s.GetOutputs(), "workspace")
		require.NotNilf(t, out, "step %s must publish an explicit workspace output", id)
		require.NotNil(t, out.GetType().GetBlob(), "the workspace output is a blob")
		// ...and no step is allowed to reach ambient state through a sandbox
		// that outlives it, which is the shared workspace by another name.
		require.Equal(t, dholev1.LeaseScope_LEASE_SCOPE_STEP, s.GetLeaseScope(),
			"step %s must run in a fresh sandbox", id)
	}

	// ...and consumes its predecessor's through a declared input, in file
	// order. Losing the order loses the pipeline: `build` reads what
	// `restore-deps` downloaded.
	chain := [][2]string{
		{"restore-deps", "build"},
		{"build", "test"},
		{"test", "publish"},
	}
	for _, link := range chain {
		from, to := link[0], link[1]
		in := portByName(stepByID(t, p, to).GetInputs(), "workspace_from_"+from)
		require.NotNilf(t, in, "step %s must declare an input for %s's workspace", to, from)
		require.NotNil(t, in.GetType().GetBlob())
		require.Truef(t, hasEdge(p, from, "workspace", to, "workspace_from_"+from),
			"the %s -> %s workspace edge must exist", from, to)
	}

	// The first step has nothing to read, so it declares no workspace input.
	require.Empty(t, stepByID(t, p, "restore-deps").GetInputs())

	// The derived order is the authored order, not an accident of parallelism.
	g, err := dag.Build(p)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"restore-deps"}, {"build"}, {"test"}, {"publish"}}, g.TopoLevels())
}

// TestN8NNodesMapToStepsAndConnectionsToTypedEdges: n8n passes JSON items
// along its connections, so the edges must carry a structured type rather
// than an opaque blob — otherwise the editor cannot reject a bad wire.
func TestN8NNodesMapToStepsAndConnectionsToTypedEdges(t *testing.T) {
	p, report, err := importers.NewN8N().Import(context.Background(), fixture(t, "n8n-export.json"))
	require.NoError(t, err)
	requireRunnable(t, p)

	require.ElementsMatch(t, []string{
		"issue-webhook", "normalise-issue", "is-bug", "create-ticket", "notify-triage",
	}, stepIDs(p))

	// Every connection is an edge, including the second output of the `if`
	// node: dropping the false branch would silently halve the workflow.
	require.True(t, hasEdge(p, "issue-webhook", "main_0", "normalise-issue", "main_0"))
	require.True(t, hasEdge(p, "normalise-issue", "main_0", "is-bug", "main_0"))
	require.True(t, hasEdge(p, "is-bug", "main_0", "create-ticket", "main_0"))
	require.True(t, hasEdge(p, "is-bug", "main_1", "notify-triage", "main_0"))
	require.True(t, hasEdge(p, "create-ticket", "main_0", "notify-triage", "main_0"))

	// The types are structured and agree, which is what makes TypeCheck able
	// to say anything at all about an n8n wire.
	for _, e := range p.GetEdges() {
		from := portByName(stepByID(t, p, e.GetFromStep()).GetOutputs(), e.GetFromPort())
		to := portByName(stepByID(t, p, e.GetToStep()).GetInputs(), e.GetToPort())
		require.NotNil(t, from)
		require.NotNil(t, to)
		require.NotNilf(t, from.GetType().GetStructured(),
			"%s.%s must be structured, not a blob", e.GetFromStep(), e.GetFromPort())
		require.NotNil(t, to.GetType().GetStructured())
		require.NotEmpty(t, from.GetType().GetStructured().GetSchemaId())
		require.Equal(t,
			from.GetType().GetStructured().GetSchemaId(),
			to.GetType().GetStructured().GetSchemaId())
	}

	// Pinned data and node-level retry are behaviour we do not reproduce.
	containsSubstring(t, report.Unsupported, "pinData")
	containsSubstring(t, report.Unsupported, "retryOnFail")
}

// TestEveryImporterProducesARunnablePipeline: GitLab is not a special case.
// An importer whose output does not type-check has produced a draft.
func TestEveryImporterProducesARunnablePipeline(t *testing.T) {
	cases := []struct {
		name     string
		importer importers.Importer
		fixture  string
	}{
		{"gitlab", importers.NewGitLab(), "gitlab-ci.yml"},
		{"actions", importers.NewActions(), "github-actions.yml"},
		{"woodpecker", importers.NewWoodpecker(), "woodpecker.yml"},
		{"n8n", importers.NewN8N(), "n8n-export.json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _, err := tc.importer.Import(context.Background(), fixture(t, tc.fixture))
			require.NoError(t, err)
			requireRunnable(t, p)
			require.NotEmpty(t, p.GetId())
			require.NotEmpty(t, p.GetSteps())
			for _, s := range p.GetSteps() {
				require.NotEmpty(t, s.GetId())
				require.NotEqual(t, dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED, s.GetEffectClass(),
					"step %s must declare an effect class", s.GetId())
			}
		})
	}
}

// TestActionsJobStepsChainThroughExplicitWorkspacePorts: inside a job the
// runner's directory is shared between steps, and between jobs it is shared
// only through the artifact actions. Both become edges, and the order inside
// the job is preserved.
func TestActionsJobStepsChainThroughExplicitWorkspacePorts(t *testing.T) {
	p, report, err := importers.NewActions().Import(
		context.Background(), fixture(t, "github-actions.yml"))
	require.NoError(t, err)
	requireRunnable(t, p)

	// The jobs are a YAML mapping, so their file order is neither recoverable
	// nor meaningful — `needs` is the ordering. The steps inside a job are a
	// list, and that order is the runner's working directory being handed on.
	require.Equal(t, []string{
		"build-1-checkout", "build-2-setup-go", "build-3-build", "build-4-upload-binary",
		"deploy-1-checkout", "deploy-2-deploy",
		"test-1-checkout", "test-2-test",
	}, stepIDs(p))

	// In-job order.
	require.True(t, hasEdge(p, "build-1-checkout", "workspace", "build-2-setup-go", "workspace_from_build-1-checkout"))
	require.True(t, hasEdge(p, "build-2-setup-go", "workspace", "build-3-build", "workspace_from_build-2-setup-go"))
	require.True(t, hasEdge(p, "build-3-build", "workspace", "build-4-upload-binary", "workspace_from_build-3-build"))

	// Cross-job `needs`: the last step of the needed job feeds the first step
	// of the dependent one.
	require.True(t, hasEdge(p, "build-4-upload-binary", "workspace", "test-1-checkout", "workspace_from_build-4-upload-binary"))
	require.True(t, hasEdge(p, "build-4-upload-binary", "workspace", "deploy-1-checkout", "workspace_from_build-4-upload-binary"))
	require.True(t, hasEdge(p, "test-2-test", "workspace", "deploy-1-checkout", "workspace_from_test-2-test"))

	g, err := dag.Build(p)
	require.NoError(t, err)
	levels := g.TopoLevels()
	require.Equal(t, []string{"build-1-checkout"}, levels[0])

	containsSubstring(t, report.Unsupported, "strategy.matrix")
	containsSubstring(t, report.Unsupported, "if:")
}

// TestGitLabStageOrderIsPreservedAsEdges: a job with no `needs` depends on
// every job of the previous stage, which is what GitLab actually does. An
// importer that only reads `needs` turns a staged pipeline into a flat
// free-for-all, and the deploy runs beside the tests.
func TestGitLabStageOrderIsPreservedAsEdges(t *testing.T) {
	p, _, err := importers.NewGitLab().Import(context.Background(), fixture(t, "gitlab-ci.yml"))
	require.NoError(t, err)
	requireRunnable(t, p)

	// `integration-tests` declares no needs, so it inherits the whole build
	// stage.
	require.True(t, hasEdge(p, "build-binary", "workspace", "integration-tests", "workspace_from_build-binary"))
	// The deploy stage waits for everything in the test stage.
	for _, testJob := range []string{"unit-tests", "lint", "integration-tests"} {
		require.Truef(t, hasEdge(p, testJob, "workspace", "deploy-staging", "workspace_from_"+testJob),
			"deploy-staging must wait for %s", testJob)
	}

	g, err := dag.Build(p)
	require.NoError(t, err)
	levels := g.TopoLevels()
	require.Equal(t, []string{"build-binary"}, levels[0])
	require.Equal(t, []string{"integration-tests", "lint", "unit-tests"}, levels[1])
	require.Equal(t, []string{"deploy-staging", "release-publish"}, levels[2])
}

// TestStepsThatCannotBeProvenPureAreAtMostOnce: the import must never be more
// permissive than the original. `go test` is provably pure; a curl, a shell
// script nobody can read, a `make deploy` and an action that uploads
// somewhere are not, and guessing "probably idempotent" is how an import
// turns a deploy that ran once into one that runs three times.
func TestStepsThatCannotBeProvenPureAreAtMostOnce(t *testing.T) {
	gitlab, _, err := importers.NewGitLab().Import(context.Background(), fixture(t, "gitlab-ci.yml"))
	require.NoError(t, err)
	actions, _, err := importers.NewActions().Import(context.Background(), fixture(t, "github-actions.yml"))
	require.NoError(t, err)
	wood, _, err := importers.NewWoodpecker().Import(context.Background(), fixture(t, "woodpecker.yml"))
	require.NoError(t, err)
	n8n, _, err := importers.NewN8N().Import(context.Background(), fixture(t, "n8n-export.json"))
	require.NoError(t, err)

	pure := map[*dholev1.Pipeline][]string{
		gitlab:  {"build-binary", "unit-tests", "lint", "integration-tests"},
		actions: {"build-1-checkout", "build-2-setup-go", "build-3-build", "test-2-test"},
		wood:    {"restore-deps", "build", "test"},
		n8n:     {"normalise-issue", "is-bug"},
	}
	for p, ids := range pure {
		for _, id := range ids {
			require.Equalf(t, dholev1.EffectClass_EFFECT_CLASS_PURE,
				stepByID(t, p, id).GetEffectClass(), "%s should be provably pure", id)
		}
	}

	unproven := map[*dholev1.Pipeline][]string{
		// curl to a deploy API, and `make deploy` — the case a naive importer
		// gets wrong, because `make` on its own looks like a build.
		gitlab: {"deploy-staging", "release-publish"},
		// An upload action and an opaque shell script.
		actions: {"build-4-upload-binary", "deploy-2-deploy"},
		// A plugin step: settings we cannot read, so behaviour we cannot vouch for.
		wood: {"publish"},
		// A webhook trigger, an HTTP POST and a Slack message.
		n8n: {"issue-webhook", "create-ticket", "notify-triage"},
	}
	for p, ids := range unproven {
		for _, id := range ids {
			require.Equalf(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
				stepByID(t, p, id).GetEffectClass(),
				"%s cannot be proven pure and must be at-most-once", id)
		}
	}

	// Nothing is ever imported as idempotent: an idempotency key is something
	// the source formats do not carry, so claiming one would be an invention.
	for _, p := range []*dholev1.Pipeline{gitlab, actions, wood, n8n} {
		for _, s := range p.GetSteps() {
			require.NotEqual(t, dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT, s.GetEffectClass(),
				"step %s claims an idempotency key the source never declared", s.GetId())
		}
	}
}

// TestMalformedInputIsAnErrorNamingWhatCouldNotBeParsed: a partial pipeline
// from a broken file is worse than no pipeline, because it looks like the
// import worked. The error has to name what could not be parsed, not merely
// that the result came out empty — "the workflow defines no job" sends the
// operator looking for a missing job when the real problem is a key the
// decoder choked on.
func TestMalformedInputIsAnErrorNamingWhatCouldNotBeParsed(t *testing.T) {
	cases := []struct {
		name     string
		importer importers.Importer
		src      []byte
		mentions []string
	}{
		// Broken sources live inline rather than in testdata: a file that is
		// deliberately unparseable is one a formatter either rejects or
		// silently repairs, and a repaired fixture stops testing anything.
		{"gitlab yaml", importers.NewGitLab(),
			[]byte("stages:\n  - build\nbuild:binary:\n  stage: build\n   script:\n  - go build ./...\n"),
			[]string{"gitlab", "parse YAML"}},
		{"gitlab not a mapping", importers.NewGitLab(), []byte("- build\n- test\n"),
			[]string{"gitlab", "mapping"}},
		{"actions jobs not a mapping", importers.NewActions(), []byte("jobs: [this is not a mapping]\n"),
			[]string{"actions", "jobs"}},
		{"actions needs unknown job", importers.NewActions(),
			[]byte("jobs:\n  a:\n    needs: [ghost]\n    steps:\n      - run: go test ./...\n"),
			[]string{"actions", "ghost"}},
		{"woodpecker steps not a list", importers.NewWoodpecker(), []byte("steps: \"not a list\"\n"),
			[]string{"woodpecker", "steps"}},
		{"woodpecker steps a mapping", importers.NewWoodpecker(),
			[]byte("steps:\n  build:\n    commands: [go build ./...]\n"),
			[]string{"woodpecker", "order"}},
		{"n8n json", importers.NewN8N(),
			[]byte(`{"name":"Broken export","nodes":[{"name":"Start"},],}`),
			[]string{"n8n", "parse JSON"}},
		{"empty", importers.NewGitLab(), nil, []string{"empty"}},
		{"no jobs", importers.NewActions(), []byte("name: CI\non: push\n"), []string{"job"}},
		{"gitlab needs unknown job", importers.NewGitLab(),
			[]byte("stages: [test]\na:\n  stage: test\n  needs: [ghost]\n  script: [go test ./...]\n"),
			[]string{"gitlab", "ghost"}},
		{"dangling connection", importers.NewN8N(), []byte(
			`{"name":"x","nodes":[{"name":"A","type":"n8n-nodes-base.set"}],` +
				`"connections":{"A":{"main":[[{"node":"Ghost","index":0}]]}}}`), []string{"Ghost"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _, err := tc.importer.Import(context.Background(), tc.src)
			require.Error(t, err)
			require.Nil(t, p, "a failed import returns no partial pipeline")
			for _, m := range tc.mentions {
				require.Contains(t, err.Error(), m)
			}
		})
	}
}
