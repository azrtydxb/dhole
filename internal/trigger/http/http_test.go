package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/trigger"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
)

// TestHTTPTriggerMapsBodyToTypedInputs is the plan's case: a POST body is not
// a payload the pipeline receives, it is the source of the pipeline's DECLARED
// inputs, and a body that cannot fill them is refused at the boundary with the
// reason (ADR 0007).
//
// The 400 matters more than the 202. A webhook body is somebody else's data;
// letting it through untyped only moves the failure to the first step of a run
// that should never have been created.
func TestHTTPTriggerMapsBodyToTypedInputs(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("mapped"), sink)

	res, body := post(t, srv.URL, `{"ref":"main"}`)
	require.Equal(t, nethttp.StatusAccepted, res, "a well-formed body was refused: %s", body)

	fires := sink.fires()
	require.Len(t, fires, 1)
	require.Equal(t, testTenant, fires[0].tenantID)
	require.Equal(t, testPipelineID, fires[0].pipelineID)
	require.Contains(t, fires[0].inputs, "ref", "the bound input was not populated")
	require.Equal(t, "main", fires[0].inputs["ref"].GetStringValue())

	// A body that fails the pipeline's declared input schema: `ref` is
	// declared as a non-empty string.
	res, body = post(t, srv.URL, `{"ref":""}`)
	require.Equal(t, nethttp.StatusBadRequest, res,
		"a body failing the pipeline's input schema was not refused")
	require.Contains(t, body, "ref", "the 400 does not name the input that failed")
	require.Contains(t, strings.ToLower(body), "minlength",
		"the 400 does not carry the validation error, only that there was one: %s", body)

	// Wrong JSON type for the same port.
	res, body = post(t, srv.URL, `{"ref":42}`)
	require.Equal(t, nethttp.StatusBadRequest, res, "a body of the wrong type was accepted: %s", body)
	require.Contains(t, body, "ref")

	// A body that does not carry the mapped field at all.
	res, body = post(t, srv.URL, `{"branch":"main"}`)
	require.Equal(t, nethttp.StatusBadRequest, res, "a body missing the mapped field was accepted")
	require.Contains(t, body, "ref")

	// A body missing the mapped field is refused because the FIELD is
	// missing, not because an empty string happens to fail this port's
	// schema: a port that accepts any string would otherwise fire a run on a
	// value nobody sent.
	permissive := testConfig("permissive")
	permissive.Pipeline.Steps[0].Inputs[0].Type = &dholev1.PortType{
		Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
			SchemaId: "https://dhole.dev/schema/ref", Schema: `{"type":"string"}`,
		}},
	}
	loose := newRecordingSink()
	looseSrv := serve(t, permissive, loose)
	res, body = post(t, looseSrv.URL, `{"branch":"main"}`)
	require.Equal(t, nethttp.StatusBadRequest, res,
		"a body missing the mapped field fired a run with an empty value: %s", body)
	require.Contains(t, body, "ref", "the refusal does not name the field that was missing")
	require.Equal(t, 0, loose.count())

	res, body = post(t, srv.URL, `{"ref":`)
	require.Equal(t, nethttp.StatusBadRequest, res, "a malformed body was accepted: %s", body)

	require.Equal(t, 1, sink.count(),
		"a refused body started a run anyway: %d fires", sink.count())
}

// TestHTTPTriggerBoundsTheBodyItReads. The endpoint is public by
// construction; a handler that reads until EOF is a memory exhaustion vector
// that needs one curl to exploit.
//
// The oversized body is otherwise PERFECTLY VALID, so a trigger without the
// limit does not fail this test by erroring — it fails by succeeding.
func TestHTTPTriggerBoundsTheBodyItReads(t *testing.T) {
	t.Run("configured limit", func(t *testing.T) {
		cfg := testConfig("bounded")
		cfg.MaxBodyBytes = 1024
		sink := newRecordingSink()
		srv := serve(t, cfg, sink)

		res, body := post(t, srv.URL, oversized(4096))
		require.Equal(t, nethttp.StatusRequestEntityTooLarge, res,
			"a body over the configured limit was read anyway: %s", body)
		require.Contains(t, body, "1024", "the refusal does not say what the limit is")
		require.Equal(t, 0, sink.count(), "an oversized body started a run")

		// The limit is a ceiling, not a filter: a body under it still works.
		res, _ = post(t, srv.URL, `{"ref":"main"}`)
		require.Equal(t, nethttp.StatusAccepted, res)
		require.Equal(t, 1, sink.count())
	})

	t.Run("default limit", func(t *testing.T) {
		sink := newRecordingSink()
		srv := serve(t, testConfig("default-bounded"), sink)

		require.Positive(t, httptrigger.DefaultMaxBodyBytes)
		res, body := post(t, srv.URL, oversized(int(httptrigger.DefaultMaxBodyBytes)+1))
		require.Equal(t, nethttp.StatusRequestEntityTooLarge, res,
			"a trigger configured with no explicit limit read an unbounded body: %s", body)
		require.Equal(t, 0, sink.count())
	})
}

// TestHTTPTriggerMarksAnUntrustedEndpointsPayload. The plain HTTP trigger is
// the authenticated API surface, so it does not taint by default — but an
// endpoint published without authentication is exactly as trustworthy as a
// webhook, and saying so must be possible without reaching for the git
// trigger. Unmarked, that data reaches an effectful step as if an operator had
// typed it (ADR 0015).
func TestHTTPTriggerMarksAnUntrustedEndpointsPayload(t *testing.T) {
	cfg := testConfig("public")
	cfg.Untrusted = true
	sink := newRecordingSink()
	srv := serve(t, cfg, sink)

	res, body := post(t, srv.URL, `{"ref":"main"}`)
	require.Equal(t, nethttp.StatusAccepted, res, "%s", body)

	got := sink.fires()[0].inputs["ref"]
	require.True(t, trigger.IsTainted(got),
		"an untrusted endpoint's payload arrived unmarked")
	require.Contains(t, trigger.TaintSource(got), "public",
		"the mark does not name the trigger that admitted the value")
	require.Equal(t, "main", trigger.UntaintedValue(got).GetStringValue(),
		"the mark lost the value")

	// The default is the other way, and it is deliberate: this is the case
	// the plan's own assertion reads.
	plain := newRecordingSink()
	plainSrv := serve(t, testConfig("internal"), plain)
	res, _ = post(t, plainSrv.URL, `{"ref":"main"}`)
	require.Equal(t, nethttp.StatusAccepted, res)
	require.False(t, trigger.IsTainted(plain.fires()[0].inputs["ref"]))
}

// TestHTTPTriggerRefusesUnscopedConfiguration. There is no unscoped trigger:
// a fire lands in exactly one tenant, and an empty scope is a bug, never a
// wildcard.
func TestHTTPTriggerRefusesUnscopedConfiguration(t *testing.T) {
	cfg := testConfig("unscoped")
	cfg.TenantID = ""
	_, err := httptrigger.New(cfg)
	require.ErrorIs(t, err, trigger.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")
}

// TestHTTPTriggerFiresIntoItsOwnTenant. Two triggers over ONE pipeline
// definition, one per tenant: each fire carries the scope it was configured
// with and never the other's.
func TestHTTPTriggerFiresIntoItsOwnTenant(t *testing.T) {
	for _, tenant := range []string{"acme", "globex"} {
		cfg := testConfig("scoped")
		cfg.TenantID = tenant
		sink := newRecordingSink()
		srv := serve(t, cfg, sink)

		res, _ := post(t, srv.URL, `{"ref":"main"}`)
		require.Equal(t, nethttp.StatusAccepted, res)
		require.Len(t, sink.fires(), 1)
		require.Equal(t, tenant, sink.fires()[0].tenantID)
	}
}

// TestHTTPTriggerBindingIsCheckedAtConfiguration. ADR 0007's payoff, at the
// one moment somebody is there to read the error.
func TestHTTPTriggerBindingIsCheckedAtConfiguration(t *testing.T) {
	cfg := testConfig("checked")
	cfg.Binding.InputMapping = map[string]string{"reff": "ref"}
	_, err := httptrigger.New(cfg)
	require.Error(t, err, "a binding onto an input the pipeline does not declare was accepted")
	require.Contains(t, err.Error(), `"reff"`)

	cfg = testConfig("checked")
	cfg.Binding.InputMapping = map[string]string{"src": "ref"}
	_, err = httptrigger.New(cfg)
	require.Error(t, err, "a binding onto a blob port was accepted")
	require.Contains(t, err.Error(), "not a structured port")
}

// TestHTTPTriggerRejectsNonPost. A GET that starts a run is a run started by
// a link preview.
func TestHTTPTriggerRejectsNonPost(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("post-only"), sink)

	res, err := nethttp.Get(srv.URL) //nolint:noctx // the test server is local and closed by t.Cleanup.
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, nethttp.StatusMethodNotAllowed, res.StatusCode)
	require.Equal(t, 0, sink.count())
}

// TestHTTPTriggerStartServesAndStopsCleanly. Start owns what it started: it
// serves on the listener it was given and closes it on the way out, so a
// cancelled trigger is not still a live endpoint.
func TestHTTPTriggerStartServesAndStopsCleanly(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	cfg := testConfig("served")
	cfg.Listener = ln
	tr, err := httptrigger.New(cfg)
	require.NoError(t, err)

	sink := newRecordingSink()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tr.Start(ctx, sink) }()

	url := "http://" + ln.Addr().String()
	require.Eventually(t, func() bool {
		res, _ := post(t, url, `{"ref":"main"}`)
		return res == nethttp.StatusAccepted
	}, 5*time.Second, 20*time.Millisecond, "Start never served the endpoint")
	require.Equal(t, 1, sink.count())

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return within 5s of cancellation")
	}

	_, err = nethttp.Post(url, "application/json", strings.NewReader(`{"ref":"main"}`)) //nolint:noctx // deliberately unbounded: the endpoint must be gone.
	require.Error(t, err, "the endpoint is still accepting requests after cancellation")
	require.Equal(t, 1, sink.count())
}

// --- fixtures -------------------------------------------------------------

const (
	testPipelineID = "deploy"
	testTenant     = "acme"
)

// refSchema is deliberately strict: a schema that accepts everything would
// make the 400 case unprovable.
const refSchema = `{"$id":"https://dhole.dev/schema/ref","type":"string","minLength":1}`

// testPipeline declares one free structured input `ref`, one free blob input
// `src` (which no trigger may bind), and one port fed by an edge (which is not
// the outside world's to supply).
func testPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: testPipelineID,
		Steps: []*dholev1.Step{
			{
				Id: "build",
				Inputs: []*dholev1.Port{
					{Name: "ref", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
							SchemaId: "https://dhole.dev/schema/ref",
							Schema:   refSchema,
						}},
					}},
					{Name: "src", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
				Outputs: []*dholev1.Port{
					{Name: "artifact", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
			},
			{
				Id: "publish",
				Inputs: []*dholev1.Port{
					{Name: "artifact", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
			},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "build", FromPort: "artifact", ToStep: "publish", ToPort: "artifact"},
		},
	}
}

func testConfig(id string) httptrigger.Config {
	return httptrigger.Config{
		ID:       id,
		TenantID: testTenant,
		Binding: trigger.Binding{
			PipelineID:   testPipelineID,
			InputMapping: map[string]string{"ref": "ref"},
		},
		Pipeline: testPipeline(),
	}
}

func serve(t *testing.T, cfg httptrigger.Config, sink trigger.Sink) *httptest.Server {
	t.Helper()
	tr, err := httptrigger.New(cfg)
	require.NoError(t, err)
	require.Equal(t, "http", tr.Kind())
	srv := httptest.NewServer(tr.Handler(sink))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(
		context.Background(), nethttp.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, string(got)
}

// oversized builds a body of at least n bytes that is otherwise entirely
// valid: the only thing wrong with it is its size.
func oversized(n int) string {
	body, err := json.Marshal(map[string]string{"ref": "main", "padding": strings.Repeat("a", n)})
	if err != nil {
		panic(fmt.Sprintf("oversized: %v", err))
	}
	return string(body)
}

type fire struct {
	tenantID   string
	pipelineID string
	inputs     map[string]*structpb.Value
}

type recordingSink struct {
	mu  sync.Mutex
	got []fire
}

func newRecordingSink() *recordingSink { return &recordingSink{} }

func (s *recordingSink) Fire(
	_ context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fire{tenantID: tenantID, pipelineID: pipelineID, inputs: inputs})
	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func (s *recordingSink) fires() []fire {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fire(nil), s.got...)
}
