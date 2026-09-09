package cli

import (
	"context"
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

// TestTokenIssueMintsACredentialTheServedAPIAccepts is the repeatable half of
// the bootstrap story.
//
// `dhole serve` prints one credential so a FRESH plane is reachable at all.
// That is not a way to give a second tenant, a CI account or a replacement for
// a leaked token — and a system with no such path is a system where one
// long-lived secret gets shared around. This proves there is one, and that
// what it mints is accepted by the API the same binary serves.
func TestTokenIssueMintsACredentialTheServedAPIAccepts(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an embedded control plane")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	dsn := filepath.Join(dir, "dhole.db")
	srv, err := server.New(server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: dsn,
		BlobRoot: filepath.Join(dir, "state"),
		APIAddr:  "127.0.0.1:0",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		require.NoError(t, srv.Stop(stopCtx))
	})

	// Minted through the command line, against the database the running plane
	// holds — which is the situation an operator is actually in.
	out, errOut := &syncBuffer{}, &syncBuffer{}
	code := Main([]string{
		"token", "issue", "--store-dsn", dsn, "--tenant", "second", "--subject", "ci",
	}, out, errOut)
	require.Equal(t, exitOK, code, errOut.String())
	token := strings.TrimSpace(out.String())
	require.NotEmpty(t, token)

	client := dholev1connect.NewPipelineServiceClient(
		&http.Client{Timeout: 30 * time.Second}, "http://"+srv.APIAddr())
	req := connect.NewRequest(&dholev1.ValidateRequest{
		Pipeline: &dholev1.Pipeline{
			Id:    "issued",
			Edges: []*dholev1.Edge{{FromStep: "ghost", FromPort: "out", ToStep: "b", ToPort: "in"}},
		},
	})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.Validate(ctx, req)
	require.NoError(t, err, "a token `dhole token issue` minted was refused by `dhole serve`")
	require.NotEmpty(t, res.Msg.GetDiagnostics())

	// A subject is required: a credential nobody is attributed to cannot be
	// revoked or audited, and the command must refuse rather than invent one.
	code = Main([]string{"token", "issue", "--store-dsn", dsn, "--tenant", "second"},
		&syncBuffer{}, &syncBuffer{})
	require.Equal(t, exitUsage, code)
}
