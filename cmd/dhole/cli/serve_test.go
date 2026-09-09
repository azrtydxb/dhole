package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/server"
)

// syncBuffer is a writer a test can read while the command is still writing.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// apiLine finds the API endpoint and the bootstrap credential in what serve
// printed. Both are parsed out of the OUTPUT rather than asked of the server
// object, because what a person can act on is what reached their terminal.
var (
	apiLine   = regexp.MustCompile(`API (http://[^\s,]+)`)
	tokenLine = regexp.MustCompile(`DHOLE_TOKEN=(\S+)`)
)

// TestServePrintsAReachableAPIAndAWorkingCredential is `dhole serve` judged
// the way an operator judges it: start it, read the two things it prints, and
// use them.
//
// The binary spent four tasks starting a control plane with no contract on it,
// and every test passed because every test reached into the packages
// underneath. So this one goes through the command, the printed address and a
// real Connect client — a credential that is printed but not valid, or an
// address printed for a socket nobody is listening on, fails here and nowhere
// else.
func TestServePrintsAReachableAPIAndAWorkingCredential(t *testing.T) {
	if testing.Short() {
		t.Skip("starts an embedded control plane")
	}
	dir := t.TempDir()
	out := &syncBuffer{}
	cmd := serveCmd(&options{env: Env{Stdout: out, Stderr: io.Discard}})
	cmd.SetArgs([]string{
		"--api-addr", "127.0.0.1:0",
		"--store-dsn", filepath.Join(dir, "dhole.db"),
		"--blob-root", filepath.Join(dir, "state"),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()

	var endpoint, token string
	deadline := time.Now().Add(120 * time.Second)
	for endpoint == "" || token == "" {
		if time.Now().After(deadline) {
			t.Fatalf("serve printed no API endpoint and credential in time; it printed:\n%s", out.String())
		}
		printed := out.String()
		if m := apiLine.FindStringSubmatch(printed); m != nil {
			endpoint = m[1]
		}
		if m := tokenLine.FindStringSubmatch(printed); m != nil {
			token = m[1]
		}
		select {
		case err := <-done:
			t.Fatalf("serve exited before it was asked to: %v\n%s", err, out.String())
		case <-time.After(50 * time.Millisecond):
		}
	}

	// The credential is also on disk for a supervised process, readable by
	// nobody else.
	tokenPath := filepath.Join(dir, "state", server.BootstrapTokenFile)
	onDisk, err := os.ReadFile(tokenPath) //nolint:gosec // a path this test built
	require.NoError(t, err)
	require.Equal(t, token, strings.TrimSpace(string(onDisk)))
	info, err := os.Stat(tokenPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm(),
		"the bootstrap credential is readable by more than its owner")

	// And it works, against the address that was printed.
	client := dholev1connect.NewPipelineServiceClient(newHTTPClient(), endpoint)
	req := connect.NewRequest(&dholev1.ValidateRequest{
		Pipeline: &dholev1.Pipeline{
			Id:    "printed-credential",
			Edges: []*dholev1.Edge{{FromStep: "ghost", FromPort: "out", ToStep: "b", ToPort: "in"}},
		},
	})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := client.Validate(ctx, req)
	require.NoError(t, err, "the credential `dhole serve` printed was refused by the API it printed")
	require.NotEmpty(t, res.Msg.GetDiagnostics())

	// And the same call without it is refused: the plane is not open.
	_, err = client.Validate(ctx, connect.NewRequest(&dholev1.ValidateRequest{
		Pipeline: &dholev1.Pipeline{Id: "printed-credential"},
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(90 * time.Second):
		t.Fatal("serve did not return after its context was cancelled")
	}
}
