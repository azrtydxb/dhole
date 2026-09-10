package server_test

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/server"
)

// startWithAPI brings up an embedded plane whose API listens on an ephemeral
// port, and returns it. Port zero rather than the default: two packages of
// this suite run at the same time, and a well-known port would make them
// fight over a socket instead of testing anything.
func startWithAPI(ctx context.Context, t *testing.T) *server.Server {
	t.Helper()
	srv, _ := startWithAPIIn(ctx, t)
	return srv
}

// startWithAPIIn is startWithAPI, also returning the state directory — which
// is where the plane's database is, and therefore the only way for a test to
// publish into the catalog the plane serves. There is no PublishPlugin RPC.
func startWithAPIIn(ctx context.Context, t *testing.T) (*server.Server, string) {
	t.Helper()
	dir := t.TempDir()
	srv, err := server.New(server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		APIAddr:  "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	return srv, dir
}

// apiClient is a REAL client of the served contract: the generated Connect
// client over a real HTTP connection to the address the plane is listening on.
// Nothing in this file constructs an api.Server, because constructing one
// proves nothing about whether `dhole serve` mounts it.
func apiClient(t *testing.T, srv *server.Server) dholev1connect.PipelineServiceClient {
	t.Helper()
	addr := srv.APIAddr()
	require.NotEmpty(t, addr, "the plane is not listening for API calls")
	return dholev1connect.NewPipelineServiceClient(httpClient(), "http://"+addr)
}

// httpClient is the transport every real client in this package uses. The
// timeout is generous because WatchRun follows a run for as long as the run
// lasts.
func httpClient() *http.Client { return &http.Client{Timeout: 120 * time.Second} }

// brokenPipeline has one edge pointing at a step that does not exist, so a
// Validate that really reached internal/api and really ran the type checker
// answers with a diagnostic naming it. A handler that was built and never
// mounted, or a stub, cannot produce this string.
func brokenPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: "served-api",
		Steps: []*dholev1.Step{{
			Id:      "a",
			Outputs: []*dholev1.Port{{Name: "out"}},
		}},
		Edges: []*dholev1.Edge{{
			FromStep: "a", FromPort: "out", ToStep: "ghost", ToPort: "in",
		}},
	}
}

// TestServeMountsTheAuthenticatedAPI is the whole of Task 27b's fourth bullet
// in one assertion pair: a plane started through server.New/Start answers a
// real HTTP call on the contract ADR 0013 says serves everybody, and refuses
// one that carries no credential.
//
// It goes through server.Start and a real client on purpose. The gap this
// closes was invisible for four tasks precisely because internal/api's own
// tests construct an api.Server directly and pass whether or not anything
// ever serves it.
func TestServeMountsTheAuthenticatedAPI(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)

	// No credential: refused, and refused as unauthenticated rather than as a
	// connection error. A plane that is open until configured is open in
	// production.
	_, err := client.Validate(ctx, connect.NewRequest(&dholev1.ValidateRequest{
		Pipeline: brokenPipeline(),
	}))
	require.Error(t, err, "the API answered a call that carried no credential")
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	// The credential a freshly started plane hands its operator.
	token := srv.BootstrapToken()
	require.NotEmpty(t, token, "a fresh plane published no way to make a first call")

	req := connect.NewRequest(&dholev1.ValidateRequest{Pipeline: brokenPipeline()})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.Validate(ctx, req)
	require.NoError(t, err, "the bootstrap credential was refused by the API it is for")
	require.NotEmpty(t, res.Msg.GetDiagnostics())
	require.Contains(t, res.Msg.GetDiagnostics()[0].GetMessage(), `edge target step "ghost"`,
		"the answer did not come from the real Validate")
}

// TestStopClosesTheAPIListener holds the other half: an API socket that
// outlived Stop would leave a second plane unable to start and would serve the
// stores of a plane that has closed them.
func TestStopClosesTheAPIListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	before := dholeGoroutines()

	dir := t.TempDir()
	srv, err := server.New(server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		APIAddr:  "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	addr := srv.APIAddr()
	require.NotEmpty(t, addr)

	// Stop something that has served, not something idle.
	client := dholev1connect.NewPipelineServiceClient(
		&http.Client{Timeout: 30 * time.Second}, "http://"+addr)
	req := connect.NewRequest(&dholev1.ValidateRequest{Pipeline: brokenPipeline()})
	req.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	_, err = client.Validate(ctx, req)
	require.NoError(t, err)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer stopCancel()
	require.NoError(t, srv.Stop(stopCtx))

	// No grace period, for the reason TestStopEndsEverythingItStarted gives.
	require.Empty(t, dholeGoroutines().minus(before), "these goroutines outlived Stop")

	conn, dialErr := net.DialTimeout("tcp", addr, 2*time.Second)
	if dialErr == nil {
		_ = conn.Close()
		t.Fatalf("the API is still listening on %s after Stop", addr)
	}
	require.Empty(t, srv.APIAddr(), "a stopped plane still reports an API address")
}

// TestNoAPIMeansNoListener keeps the opt-out honest: a deployment that says it
// does not want the contract served must not have a socket open anyway.
func TestNoAPIMeansNoListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	srv, err := server.New(server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		NoAPI:    true,
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	require.Empty(t, srv.APIAddr())
	require.Empty(t, srv.BootstrapToken(),
		"a plane serving no API minted a credential nothing can be presented to")
}

// TestServeDefaultsToTheAddressTheCLIDialsFor is the pairing that is easy to
// get wrong in two files at once: `dhole --server` dials a default address,
// and a plane started with no address configured has to be listening there.
func TestServeDefaultsToTheAddressTheCLIDialsFor(t *testing.T) {
	require.True(t, strings.HasSuffix(server.DefaultAPIAddr, ":7777"),
		"the default API address moved away from the port the CLI dials")
}

// createPipeline creates one pipeline through the contract and returns the
// revision its creation produced.
func createPipeline(
	ctx context.Context, t *testing.T,
	client dholev1connect.PipelineServiceClient, token, id string,
) string {
	t.Helper()
	req := connect.NewRequest(&dholev1.CreatePipelineRequest{PipelineId: id})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.CreatePipeline(ctx, req)
	require.NoError(t, err)
	return res.Msg.GetRevision().GetId()
}

// apply is one ApplyOperation with a credential on it.
func apply(
	ctx context.Context, client dholev1connect.PipelineServiceClient, token string,
	msg *dholev1.ApplyOperationRequest,
) (*dholev1.ApplyOperationResponse, error) {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.ApplyOperation(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg, nil
}

// TestAPipelineCanBeCreatedThroughTheContract is the hole ADR 0013 could not
// have: ApplyOperation requires a base_revision, and until CreatePipeline
// existed nothing in the contract wrote a first one. Both the canvas suite and
// the CLI suite reached around the API to seed a pipeline directly into the
// definition store, which means the GUI could not create a pipeline either.
//
// It goes through the shipping binary, because a handler built in-process
// proves the handler works and says nothing about whether it is mounted.
func TestAPipelineCanBeCreatedThroughTheContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	created := createPipeline(ctx, t, client, token, "made-through-the-contract")
	require.NotEmpty(t, created)

	// The revision it produced is a real base an edit can be made against,
	// which is the whole point: creation and editing are one story.
	applied, err := apply(ctx, client, token, &dholev1.ApplyOperationRequest{
		PipelineId:   "made-through-the-contract",
		BaseRevision: created,
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_AddStep{
			AddStep: &dholev1.AddStep{Step: &dholev1.Step{Id: "a", Name: "fetch"}},
		}},
	})
	require.NoError(t, err)
	require.Len(t, applied.GetPipeline().GetSteps(), 1)

	// The definition carries the tenant from the credential, never from the
	// request: there is no unscoped record in this system.
	require.Equal(t, "default", applied.GetPipeline().GetTenant().GetId())
}

// TestCreatePipelineRefusesAnIdThatIsAlreadyTaken: a create that quietly
// returned the existing pipeline would let one caller's canvas open on
// another's work, and a create that overwrote it would destroy that work.
func TestCreatePipelineRefusesAnIdThatIsAlreadyTaken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	createPipeline(ctx, t, client, token, "taken")

	req := connect.NewRequest(&dholev1.CreatePipelineRequest{PipelineId: "taken"})
	req.Header().Set("Authorization", "Bearer "+token)
	_, err := client.CreatePipeline(ctx, req)
	require.Error(t, err, "a duplicate pipeline id was accepted")
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

// TestTheEditingHeadSurvivesAPlaneRestart is the head being STORED rather than
// remembered, asserted through the shipping binary.
//
// While the head lived in a map inside api.Server, a restarted plane knew
// nothing of the edits the previous one had accepted: an unqualified read fell
// back to the approved revision, and a pipeline that had never been approved
// answered "no active revision" for a definition that plainly existed. Worse,
// and invisible here, two planes over the same store each accepted an edit
// against the same base. The head is now a row, so a second plane over the
// same database — which is what a restart is — resolves the same head.
func TestTheEditingHeadSurvivesAPlaneRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	cfg := server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		APIAddr:  "127.0.0.1:0",
	}

	first, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Start(ctx))

	created := createPipeline(ctx, t, apiClient(t, first), first.BootstrapToken(), "head-survives")

	applied, err := apply(ctx, apiClient(t, first), first.BootstrapToken(), &dholev1.ApplyOperationRequest{
		PipelineId:   "head-survives",
		BaseRevision: created,
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_AddStep{
			AddStep: &dholev1.AddStep{Step: &dholev1.Step{Id: "a", Name: "fetch"}},
		}},
	})
	require.NoError(t, err)
	head := applied.GetRevision().GetId()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
	require.NoError(t, first.Stop(stopCtx))
	stopCancel()

	// The same state directory, a new process's worth of memory.
	second, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, second.Start(ctx))
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, second.Stop(c))
	})

	client := apiClient(t, second)
	req := connect.NewRequest(&dholev1.GetPipelineRequest{PipelineId: "head-survives"})
	req.Header().Set("Authorization", "Bearer "+second.BootstrapToken())
	got, err := client.GetPipeline(ctx, req)
	require.NoError(t, err, "the restarted plane lost the editing head")
	require.Equal(t, head, got.Msg.GetRevision().GetId(),
		"the restarted plane resolved a different head from the one it had accepted")

	// And the edit that was already applied cannot be applied again against
	// the revision it superseded.
	_, err = apply(ctx, client, second.BootstrapToken(), &dholev1.ApplyOperationRequest{
		PipelineId:   "head-survives",
		BaseRevision: created,
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_Rename{
			Rename: &dholev1.Rename{StepId: "a", Name: "other"},
		}},
	})
	require.Error(t, err, "the restarted plane accepted an edit against a superseded base")
	require.Equal(t, connect.CodeAborted, connect.CodeOf(err))
}

// TestNoTestBindsTheWellKnownAPIPort guards a fragility that cost a confusing
// failure: a server test that omits APIAddr takes the default 7777, and then
// fails with "address already in use" whenever anything else on the machine
// holds that port — a port-forward to a real cluster, say. The failure names
// the port and not the omission, so it reads as an environment problem.
func TestNoTestBindsTheWellKnownAPIPort(t *testing.T) {
	entries, err := filepath.Glob("*_test.go")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	for _, name := range entries {
		src, err := os.ReadFile(name)
		require.NoError(t, err)
		text := string(src)

		// Each server.Config in a test must choose its own port. Counting is
		// enough: a Config without an APIAddr beside it is the mistake.
		configs := strings.Count(text, "server.Config{")
		addrs := strings.Count(text, `APIAddr: "127.0.0.1:0"`) +
			strings.Count(text, `APIAddr:  "127.0.0.1:0"`) +
			strings.Count(text, "NoAPI:")
		require.GreaterOrEqual(t, addrs, configs,
			"%s builds %d server.Config values but only %d ask for an ephemeral port "+
				"(or no API): one of them will bind %s and fail when that port is taken",
			name, configs, addrs, server.DefaultAPIAddr)
	}
}
