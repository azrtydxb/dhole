package trigger_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	nethttp "net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/trigger"
	"github.com/azrtydxb/dhole/internal/trigger/completion"
	gittrigger "github.com/azrtydxb/dhole/internal/trigger/git"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
	"github.com/azrtydxb/dhole/internal/trigger/schedule"
)

const (
	pipelineID = "release"
	tenantID   = "acme"
	secret     = "webhook-secret"
)

// TestAllFourTriggersStartSamePipeline is ADR 0007 stated as a test: ONE
// pipeline definition, unchanged and unaware, started by a cron boundary, an
// inbound POST, a git push and another pipeline's completion.
//
// The definition is built once and shared by all four triggers, and it is
// compared against a pristine copy at the end: a trigger that had to be
// accommodated by the pipeline — a field added for webhooks, a step that knows
// it was pushed to — would be the thing this test exists to catch, and Dhole
// would be a CI tool with a cron feature rather than a general orchestrator.
func TestAllFourTriggersStartSamePipeline(t *testing.T) {
	pipeline := releasePipeline()
	pristine := releasePipeline()

	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	byKind := map[string]fire{}

	// 1. A cron boundary.
	cron, err := schedule.New(schedule.Config{
		ID:         "nightly",
		TenantID:   tenantID,
		Expression: "* * * * * *",
		Binding: trigger.Binding{
			PipelineID:   pipelineID,
			InputMapping: map[string]string{"ref": schedule.FieldScheduledFor},
		},
		Pipeline: pipeline,
		Store:    store,
	})
	require.NoError(t, err)

	cronSink := newRecordingSink()
	base := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
	_, err = cron.Tick(ctx, cronSink, base)
	require.NoError(t, err)
	out, err := cron.Tick(ctx, cronSink, base.Add(2*time.Second))
	require.NoError(t, err)
	require.True(t, out.Fired, "the schedule did not fire")
	byKind[cron.Kind()] = cronSink.fires()[0]

	// 2. An inbound POST.
	inbound, err := httptrigger.New(httptrigger.Config{
		ID:       "api",
		TenantID: tenantID,
		Binding: trigger.Binding{
			PipelineID:   pipelineID,
			InputMapping: map[string]string{"ref": "ref"},
		},
		Pipeline: pipeline,
	})
	require.NoError(t, err)

	httpSink := newRecordingSink()
	srv := httptest.NewServer(inbound.Handler(httpSink))
	t.Cleanup(srv.Close)
	require.Equal(t, nethttp.StatusAccepted, post(t, srv.URL, `{"ref":"main"}`, nil))
	require.Len(t, httpSink.fires(), 1)
	byKind[inbound.Kind()] = httpSink.fires()[0]

	// 3. A git push.
	hook, err := gittrigger.New(gittrigger.Config{
		ID:       "pushes",
		TenantID: tenantID,
		Secret:   secret,
		Binding: trigger.Binding{
			PipelineID:   pipelineID,
			InputMapping: map[string]string{"ref": gittrigger.FieldRef},
		},
		Pipeline: pipeline,
	})
	require.NoError(t, err)

	gitSink := newRecordingSink()
	hookSrv := httptest.NewServer(hook.Handler(gitSink))
	t.Cleanup(hookSrv.Close)
	body := `{"ref":"refs/heads/main","after":"abc123","repository":{"full_name":"acme/widgets"},` +
		`"pusher":{"name":"octocat"}}`
	require.Equal(t, nethttp.StatusAccepted, post(t, hookSrv.URL, body, map[string]string{
		"X-GitHub-Event":      "push",
		"X-Hub-Signature-256": "sha256=" + sign(secret, body),
	}))
	require.Len(t, gitSink.fires(), 1)
	byKind[hook.Kind()] = gitSink.fires()[0]

	// 4. Another pipeline's completion.
	downstream, err := completion.New(completion.Config{
		ID:                 "after-build",
		TenantID:           tenantID,
		UpstreamPipelineID: "build",
		Binding: trigger.Binding{
			PipelineID:   pipelineID,
			InputMapping: map[string]string{"ref": completion.FieldUpstreamRunID},
		},
		Pipeline: pipeline,
	})
	require.NoError(t, err)

	completionSink := newRecordingSink()
	fired, err := downstream.Observe(ctx, completionSink, completion.Event{
		TenantID: tenantID, PipelineID: "build", RunID: "run-42",
		Type: runstore.RunCompleted, At: base,
	})
	require.NoError(t, err)
	require.True(t, fired.Fired)
	require.Len(t, completionSink.fires(), 1)
	byKind[downstream.Kind()] = completionSink.fires()[0]

	// Four kinds, four fires, one pipeline.
	require.Len(t, byKind, 4, "four triggers did not report four distinct kinds: %v", byKind)
	for kind, f := range byKind {
		require.Equal(t, tenantID, f.tenantID, "%s fired outside its tenant", kind)
		require.Equal(t, pipelineID, f.pipelineID, "%s started the wrong pipeline", kind)
		require.Contains(t, f.inputs, "ref", "%s did not fill the pipeline's declared input", kind)
		require.NotEmpty(t, trigger.UntaintedValue(f.inputs["ref"]).GetStringValue(),
			"%s filled the input with nothing", kind)
		for name := range f.inputs {
			require.Contains(t, trigger.DeclaredInputs(pipeline), name,
				"%s supplied %q, which the pipeline does not declare", kind, name)
		}
	}

	require.True(t, proto.Equal(pristine, pipeline),
		"a trigger modified the pipeline definition it fired:\n%v", pipeline)
}

// TestValidateInputsIsSharedAcrossTriggers. The three triggers of this task
// check their payload the same way, against the pipeline's own declared
// inputs, because a per-trigger copy of the rules is three places for them to
// drift apart.
func TestValidateInputsIsSharedAcrossTriggers(t *testing.T) {
	pipeline := releasePipeline()

	require.NoError(t, trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"ref": structpb.NewStringValue("main"),
	}))

	err := trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"ref": structpb.NewStringValue(""),
	})
	require.Error(t, err, "a value failing the port's declared schema was accepted")
	require.Contains(t, err.Error(), "ref")
	require.Contains(t, strings.ToLower(err.Error()), "minlength")

	err = trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"ref": structpb.NewNumberValue(42),
	})
	require.Error(t, err, "a value of the wrong JSON type was accepted")

	err = trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"reff": structpb.NewStringValue("main"),
	})
	require.Error(t, err, "an input the pipeline does not declare was accepted")
	require.Contains(t, err.Error(), "does not declare")

	err = trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"src": structpb.NewStringValue("main"),
	})
	require.Error(t, err, "a value supplied for a blob port was accepted")
	require.Contains(t, err.Error(), "not a structured port")

	// A tainted value is checked on what it carries, not on its wrapper:
	// otherwise every webhook payload would fail its own schema.
	require.NoError(t, trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"ref": trigger.MarkTainted(structpb.NewStringValue("main"), "git:github:pushes"),
	}))
	err = trigger.ValidateInputs(pipeline, map[string]*structpb.Value{
		"ref": trigger.MarkTainted(structpb.NewStringValue(""), "git:github:pushes"),
	})
	require.Error(t, err, "a tainted value was exempted from its port's schema")
}

// TestTaintMarkerCarriesTheValueAndItsSource. ADR 0015's boundary mark, at the
// smallest shape that is honest: it says the value is untrusted, says which
// trigger admitted it, and does not lose the value.
func TestTaintMarkerCarriesTheValueAndItsSource(t *testing.T) {
	clean := structpb.NewStringValue("refs/heads/main")
	require.False(t, trigger.IsTainted(clean))
	require.Empty(t, trigger.TaintSource(clean))
	require.True(t, proto.Equal(clean, trigger.UntaintedValue(clean)),
		"unwrapping an unmarked value changed it")

	marked := trigger.MarkTainted(clean, "git:github:pushes")
	require.True(t, trigger.IsTainted(marked))
	require.Equal(t, "git:github:pushes", trigger.TaintSource(marked))
	require.True(t, proto.Equal(clean, trigger.UntaintedValue(marked)),
		"the mark lost the value it was applied to")
	require.False(t, proto.Equal(clean, marked),
		"marking a value left it indistinguishable from an unmarked one")

	// Marking twice is not two wrappers: the mark is a property of the value,
	// and the source stays the one that admitted it.
	require.True(t, proto.Equal(marked, trigger.MarkTainted(marked, "git:github:pushes")))

	// Structured payloads carry the mark too, and a struct that merely
	// contains a similarly named key is not tainted by coincidence.
	obj, err := structpb.NewValue(map[string]any{"ref": "main", "taint": "not really"})
	require.NoError(t, err)
	require.False(t, trigger.IsTainted(obj))
	require.True(t, trigger.IsTainted(trigger.MarkTainted(obj, "http:api")))
	require.True(t, proto.Equal(obj, trigger.UntaintedValue(trigger.MarkTainted(obj, "http:api"))))
}

// --- fixtures -------------------------------------------------------------

// releasePipeline is the ONE definition all four triggers fire. It declares a
// structured `ref` input, which is the only thing any of them fills, plus a
// blob input no trigger may bind.
func releasePipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: pipelineID,
		Steps: []*dholev1.Step{{
			Id: "publish",
			Inputs: []*dholev1.Port{
				{Name: "ref", Type: &dholev1.PortType{
					Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
						SchemaId: "https://dhole.dev/schema/ref",
						Schema:   `{"type":"string","minLength":1}`,
					}},
				}},
				{Name: "src", Type: &dholev1.PortType{
					Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
				}},
			},
		}},
	}
}

func sign(key, body string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func post(t *testing.T, url, body string, headers map[string]string) int {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(
		context.Background(), nethttp.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	return res.StatusCode
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
	_ context.Context, tenant, pipeline string, inputs map[string]*structpb.Value,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fire{tenantID: tenant, pipelineID: pipeline, inputs: inputs})
	return nil
}

func (s *recordingSink) fires() []fire {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fire(nil), s.got...)
}
