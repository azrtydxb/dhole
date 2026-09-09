package server_test

import (
	"context"
	"net"
	"net/http"
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
	return srv
}

// apiClient is a REAL client of the served contract: the generated Connect
// client over a real HTTP connection to the address the plane is listening on.
// Nothing in this file constructs an api.Server, because constructing one
// proves nothing about whether `dhole serve` mounts it.
func apiClient(t *testing.T, srv *server.Server) dholev1connect.PipelineServiceClient {
	t.Helper()
	addr := srv.APIAddr()
	require.NotEmpty(t, addr, "the plane is not listening for API calls")
	return dholev1connect.NewPipelineServiceClient(&http.Client{Timeout: 30 * time.Second}, "http://"+addr)
}

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
