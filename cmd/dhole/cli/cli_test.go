package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// goodToken is the only credential the servers below accept.
const goodToken = "s3cret"

// TestMain clears the environment the CLI reads its defaults from. A developer
// with DHOLE_TOKEN exported would otherwise turn the "no credential" cases
// below green for the wrong reason — which is the failure this whole file
// exists to catch.
func TestMain(m *testing.M) {
	for _, key := range []string{"DHOLE_TOKEN", "DHOLE_SERVER"} {
		if err := os.Unsetenv(key); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

// TestEveryServerCommandSendsItsCredential is the property a CLI loses
// silently: a command that builds its own client and forgets the header keeps
// working against a server nobody authenticated in development, and fails in
// production as an unexplained RPC code.
//
// It is checked against the REAL internal/api server, whose real
// authentication refuses a call with no Authorization header — a stub server
// that ignored credentials would make this test prove nothing.
func TestEveryServerCommandSendsItsCredential(t *testing.T) {
	url := startRealAPI(t)

	for _, tc := range serverCommands() {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
			code := cli.Main(append([]string{"--server", url}, tc.args...), out, errOut)
			require.NotZero(t, code, "a call with no credential must fail, not succeed silently")
			require.Contains(t, errOut.String(), "credential",
				"the refusal must say what is missing, not just report a code")

			out, errOut = &bytes.Buffer{}, &bytes.Buffer{}
			code = cli.Main(append([]string{"--server", url, "--token", goodToken}, tc.args...), out, errOut)
			require.NotContains(t, errOut.String(), "Authorization",
				"with a credential the call must get past authentication; stderr: %s", errOut)
			if tc.succeeds {
				require.Zero(t, code, "stderr: %s", errOut)
			}
		})
	}
}

// TestUnauthenticatedCallIsLegible holds the second half: the error a person
// reads has to name the server and say what went wrong.
func TestUnauthenticatedCallIsLegible(t *testing.T) {
	url := startRealAPI(t)

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{"--server", url, "--token", "wrong", "pipeline", "get", "p1"}, out, errOut)
	require.NotZero(t, code)

	msg := errOut.String()
	require.Contains(t, msg, url, "the error must name the server that refused")
	require.Contains(t, msg, "authentication failed", "the server's own reason must survive")
	require.NotEqual(t, "unauthenticated", strings.TrimSpace(msg),
		"a bare RPC code is not a legible error")
}

// TestSuccessfulCallWithCredentialsReallySucceeds is the control the test
// above needs: without it, a CLI that always failed would pass every
// assertion about failure.
func TestSuccessfulCallWithCredentialsReallySucceeds(t *testing.T) {
	url := startRealAPI(t)

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{"--server", url, "--token", goodToken, "pipeline", "get", "p1"}, out, errOut)
	require.Zero(t, code, "stderr: %s", errOut)
	require.Contains(t, out.String(), "p1")
}

// TestCLIOutputIsJSONWhenRequested is the agent-facing half of ADR 0013:
// whatever a person sees, a program must be able to parse.
//
// Plan answers CodeUnimplemented on this branch (Task 28 implements it), so
// the server here is a stub returning a real PlanResponse. The CLI does
// nothing different when the response comes from the real implementation, so
// this keeps holding when Task 28 merges.
func TestCLIOutputIsJSONWhenRequested(t *testing.T) {
	url := startStubAPI(t, &planningStub{})

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"--server", url, "--token", goodToken, "--output", "json", "pipeline", "plan", "p1",
	}, out, errOut)
	require.Zero(t, code, "stderr: %s", errOut)

	// The WHOLE of stdout must parse: one human-readable line prepended to it
	// would break every consumer, and is exactly what creeps in.
	var decoded struct {
		Steps []struct {
			StepID     string `json:"stepId"`
			CacheHit   bool   `json:"cacheHit"`
			EngineKind string `json:"engineKind"`
		} `json:"steps"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded), "stdout was not JSON: %q", out.String())
	require.Len(t, decoded.Steps, 2)
	require.Equal(t, "a", decoded.Steps[0].StepID)
	require.Equal(t, "process", decoded.Steps[1].EngineKind)
}

// TestDiagnosticsGoToStderrNotStdout keeps the machine-readable channel clean
// on the path that matters most: the failing one.
func TestDiagnosticsGoToStderrNotStdout(t *testing.T) {
	url := startRealAPI(t)

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"--server", url, "--token", "wrong", "--output", "json", "pipeline", "get", "p1",
	}, out, errOut)
	require.NotZero(t, code)
	require.Empty(t, out.String(), "a failure must write nothing to stdout")
	require.NotEmpty(t, errOut.String(), "the diagnosis must go somewhere")
}

// TestUnknownCommandExitsNonZeroWithUsage covers the two ways a person mistypes.
func TestUnknownCommandExitsNonZeroWithUsage(t *testing.T) {
	for _, args := range [][]string{
		{"nonesuch"},
		{"pipeline", "nonesuch"},
		{"pipeline", "get", "--nonesuch"},
		{"--nonesuch"},
	} {
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		code := cli.Main(args, out, errOut)
		require.NotZero(t, code, "%v exited zero", args)
		require.Contains(t, strings.ToLower(errOut.String()), "usage",
			"%v printed no usage: %s", args, errOut)
	}
}

// TestHelpAndVersionSucceed is the other side of the same coin: asking for
// help is not a failure.
func TestHelpAndVersionSucceed(t *testing.T) {
	for _, args := range [][]string{
		{"--help"}, {"help"}, {"version"}, {"--version"}, {"pipeline", "--help"},
	} {
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		require.Zero(t, cli.Main(args, out, errOut), "%v: %s", args, errOut)
		require.NotEmpty(t, out.String(), "%v printed nothing to stdout", args)
	}
}

// TestBinaryExitsNonZeroAsAProcess is the property as a script sees it. The
// tests above check a return value; only running the real binary checks the
// exit status, and `dhole version` and `dhole serve` must have survived the
// CLI being rebuilt around them.
func TestBinaryExitsNonZeroAsAProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the binary")
	}
	binary := filepath.Join(t.TempDir(), "dhole")
	build := exec.Command("go", "build", "-o", binary, "github.com/azrtydxb/dhole/cmd/dhole")
	build.Stderr = os.Stderr
	require.NoError(t, build.Run())

	for _, args := range [][]string{{"version"}, {"--version"}} {
		printed, err := exec.Command(binary, args...).Output()
		require.NoError(t, err, "`dhole %v` must still work", args)
		require.Contains(t, string(printed), "dhole ")
	}

	help, err := exec.Command(binary, "--help").Output()
	require.NoError(t, err)
	require.Contains(t, string(help), "serve", "`dhole serve` must still be reachable")

	err = exec.Command(binary, "nonesuch").Run()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "an unknown command must exit non-zero")
	require.NotZero(t, exit.ExitCode())

	// A failing real command, not merely a parse error.
	err = exec.Command(binary, "--server", "http://127.0.0.1:1", "pipeline", "get", "p1").Run()
	require.ErrorAs(t, err, &exit)
	require.NotZero(t, exit.ExitCode())
}

// serverCommands is one invocation per command that talks to a server. It is
// written out rather than derived, because the arguments differ; the coverage
// test is what guarantees the set of commands is complete.
func serverCommands() []struct {
	name     string
	args     []string
	succeeds bool
} {
	operation := `{"rename":{"stepId":"a","name":"x"}}`
	return []struct {
		name     string
		args     []string
		succeeds bool
	}{
		{"pipeline create", []string{"pipeline", "create", "p2"}, true},
		{"plugin get", []string{"plugin", "get", "acme/deploy@1.0.0"}, false},
		{"engine list", []string{"engine", "list"}, false},
		{"engine drain", []string{"engine", "drain", "e1"}, false},
		{"run cancel", []string{"run", "cancel", "run_1"}, false},
		{"pipeline get", []string{"pipeline", "get", "p1"}, true},
		{"pipeline apply", []string{"pipeline", "apply", "p1", "--base", "rev_1", "--operation", operation}, false},
		{"pipeline validate", []string{"pipeline", "validate", "p1"}, false},
		{"pipeline plan", []string{"pipeline", "plan", "p1"}, false},
		{"pipeline revisions", []string{"pipeline", "revisions", "p1"}, true},
		{"pipeline approve", []string{"pipeline", "approve", "rev_1"}, true},
		{"run start", []string{"run", "start", "p1"}, false},
		{"run watch", []string{"run", "watch", "run_1"}, false},
		{"run logs", []string{"run", "logs", "run_1"}, false},
	}
}

// startRealAPI serves internal/api over HTTP, with real authentication and a
// definition store stubbed down to what these commands read.
func startRealAPI(t *testing.T) string {
	t.Helper()
	srv, err := api.NewServer(api.Config{Definitions: &oneRevisionStore{}, Auth: staticAuth{}})
	require.NoError(t, err)
	http := httptest.NewServer(srv.Handler())
	t.Cleanup(http.Close)
	return http.URL
}

func startStubAPI(t *testing.T, handler dholev1connect.PipelineServiceHandler) string {
	t.Helper()
	_, h := dholev1connect.NewPipelineServiceHandler(handler)
	mux := httptest.NewServer(h)
	t.Cleanup(mux.Close)
	return mux.URL
}

// staticAuth accepts exactly one credential. The refusal of everything else —
// including the absence of a credential — is internal/api's own, which is what
// makes the credential tests meaningful.
type staticAuth struct{}

func (staticAuth) Authenticate(_ context.Context, credential string) (identity.Principal, error) {
	if credential != goodToken {
		return identity.Principal{}, identity.ErrUnauthenticated
	}
	return identity.Principal{Subject: "tester", TenantID: "default", Kind: identity.PrincipalUser}, nil
}

// oneRevisionStore is a definition store holding a single approved revision of
// a single pipeline.
type oneRevisionStore struct{}

func (s *oneRevisionStore) Save(
	context.Context, string, *dholev1.Pipeline, string,
) (defstore.Revision, error) {
	return s.rev(), nil
}

func (s *oneRevisionStore) Get(_ context.Context, _, pipelineID, _ string) (*dholev1.Pipeline, error) {
	if pipelineID != "p1" {
		return nil, defstore.ErrNotFound
	}
	return &dholev1.Pipeline{Id: "p1", Steps: []*dholev1.Step{{Id: "a"}}}, nil
}

func (s *oneRevisionStore) Active(_ context.Context, _, pipelineID string) (defstore.Revision, error) {
	if pipelineID != "p1" {
		return defstore.Revision{}, defstore.ErrNotFound
	}
	return s.rev(), nil
}

func (s *oneRevisionStore) Approve(context.Context, string, string, string) error { return nil }

func (s *oneRevisionStore) Revision(_ context.Context, _, revisionID string) (defstore.Revision, error) {
	if revisionID != "rev_1" {
		return defstore.Revision{}, defstore.ErrNotFound
	}
	return s.rev(), nil
}

func (s *oneRevisionStore) Revisions(_ context.Context, _, pipelineID string) ([]defstore.Revision, error) {
	if pipelineID != "p1" {
		return nil, nil
	}
	return []defstore.Revision{s.rev()}, nil
}

func (s *oneRevisionStore) rev() defstore.Revision {
	return defstore.Revision{
		ID: "rev_1", PipelineID: "p1", ContentHash: "hash", State: defstore.StateActive, Author: "tester",
	}
}

// planningStub answers Plan with a dry run. Everything else refuses, which is
// what the generated Unimplemented handler is for.
type planningStub struct {
	dholev1connect.UnimplementedPipelineServiceHandler
}

func (*planningStub) Plan(
	_ context.Context, req *connect.Request[dholev1.PlanRequest],
) (*connect.Response[dholev1.PlanResponse], error) {
	if req.Header().Get("Authorization") == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("no credential"))
	}
	return connect.NewResponse(&dholev1.PlanResponse{Steps: []*dholev1.PlannedStep{
		{StepId: "a", CacheHit: true, EngineKind: "process"},
		{StepId: "b", EngineKind: "process", NonCacheableReason: "effectful"},
	}}), nil
}
