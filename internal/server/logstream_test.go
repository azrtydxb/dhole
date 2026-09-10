package server_test

import (
	"bufio"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/server"
)

// A deployment on a real cluster asked for a completed step's log and was told
// "this server was built without an object store" — while the store sat on the
// same struct as the API config, one field away from being passed in. The run
// view has no other way to show a log, so every log in the product was
// unreachable and no test noticed, because every test that read a log read it
// from the store directly rather than through the endpoint the UI calls.
func TestTheServedLogEndpointCanActuallyReachTheArchive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "a")

	source, body := readLogStream(ctx, t, srv, runID, "a")

	require.NotContains(t, source, "without an object store",
		"the API was built without the blob store the plane already holds")
	require.NotEqual(t, `"source":"none"`, source,
		"the endpoint served no log at all for a step that succeeded")
	require.NotEmpty(t, body, "the log stream carried no chunks for a step that ran")
}

// readLogStream reads the SSE log endpoint to its end and returns the first
// `source` event's data and everything after it. It reads through HTTP on the
// listening port for the same reason apiClient does: constructing the handler
// in-process would prove nothing about whether `dhole serve` wires it.
func readLogStream(ctx context.Context, t *testing.T, srv *server.Server, runID, stepID string) (source, body string) {
	t.Helper()

	url := "http://" + srv.APIAddr() + "/v1/runs/" + runID + "/steps/" + stepID + "/logs"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+srv.BootstrapToken())

	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var rest strings.Builder
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if source == "" && strings.HasPrefix(line, "data: ") && strings.Contains(line, `"source"`) {
			source = strings.TrimPrefix(line, "data: ")
			continue
		}
		rest.WriteString(line)
		rest.WriteString("\n")
	}
	return source, rest.String()
}

// The other copy. A finished step's log comes from the archive, so the archive
// being wired hides a missing live source entirely — and the live source is the
// one a person actually watches, because it is what a running step has.
//
// The step therefore runs long enough to be caught in the act.
func TestAStepStillRunningIsTailedLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)

	runID, err := srv.Submit(ctx, tenantID, slowTalkingPipeline())
	require.NoError(t, err)

	// Poll until the endpoint reports a source. Before the step is dispatched
	// there is neither a live subject nor an archived object, so an immediate
	// read would legitimately find nothing.
	deadline := time.Now().Add(60 * time.Second)
	for {
		source, _ := readLogStream(ctx, t, srv, runID, "talk")
		if strings.Contains(source, `"live"`) {
			return
		}
		if strings.Contains(source, `"archive"`) {
			t.Fatal("the step finished before it could be tailed; make it slower, this is not the assertion failing")
		}
		if time.Now().After(deadline) {
			t.Fatalf("no live log source ever appeared for a running step; last source was %q", source)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// slowTalkingPipeline prints, then stays alive long enough to be observed. The
// print comes first so there is something to tail rather than an open stream
// carrying nothing.
func slowTalkingPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     "slow-talker",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "talk",
			Name:        "talk",
			PluginRef:   `command:{"args":["/bin/sh","-c","echo tailing-me; sleep 20"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		}},
	}
}
