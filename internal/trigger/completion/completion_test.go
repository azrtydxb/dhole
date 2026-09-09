package completion_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/trigger"
	"github.com/azrtydxb/dhole/internal/trigger/completion"
)

const (
	upstreamID   = "build-artifacts"
	downstreamID = "publish-release"
	testTenant   = "acme"
)

// TestCompletionTriggerFiresDownstreamPipeline is the plan's case, and its
// second half is the one that matters: a run that FAILED is not a run that
// completed, and a downstream pipeline fired off a failed upstream is how a
// broken build gets published.
func TestCompletionTriggerFiresDownstreamPipeline(t *testing.T) {
	sink := newRecordingSink()
	tr, err := completion.New(testConfig("on-build"))
	require.NoError(t, err)
	require.Equal(t, "completion", tr.Kind())

	completed := completion.Event{
		TenantID:   testTenant,
		PipelineID: upstreamID,
		RunID:      "run-7",
		Type:       runstore.RunCompleted,
		At:         time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC),
	}
	out, err := tr.Observe(context.Background(), sink, completed)
	require.NoError(t, err)
	require.True(t, out.Fired, "a completed upstream run did not fire the downstream pipeline")

	require.Len(t, sink.fires(), 1)
	got := sink.fires()[0]
	require.Equal(t, testTenant, got.tenantID)
	require.Equal(t, downstreamID, got.pipelineID,
		"the trigger fired the pipeline it was watching, not the one it drives")
	require.Equal(t, "run-7", got.inputs["run"].GetStringValue())
	require.Equal(t, upstreamID, got.inputs["source"].GetStringValue())
	require.Equal(t, string(runstore.RunCompleted), got.inputs["outcome"].GetStringValue())

	// A failed upstream run. The event type is the scheduler's own, so this
	// stays honest if that name ever moves.
	failed := completed
	failed.RunID = "run-8"
	failed.Type = scheduler.RunFailed
	out, err = tr.Observe(context.Background(), sink, failed)
	require.NoError(t, err)
	require.False(t, out.Fired, "a FAILED upstream run fired the downstream pipeline")
	require.Contains(t, out.Reason, string(scheduler.RunFailed),
		"the trigger did not say why it passed the event over")
	require.Equal(t, 1, sink.count(), "a failed upstream run started a downstream run")

	// Anything that is not a completion is not a completion. Default-deny:
	// the trigger fires on RUN_COMPLETED and on nothing else.
	for _, typ := range []runstore.EventType{
		runstore.StepSucceeded, runstore.StepFailed, runstore.RunCreated, "", "SOMETHING_NEW",
	} {
		other := completed
		other.RunID = "run-9"
		other.Type = typ
		out, err = tr.Observe(context.Background(), sink, other)
		require.NoError(t, err)
		require.False(t, out.Fired, "the trigger fired on a %q event", typ)
	}
	require.Equal(t, 1, sink.count())
}

// TestCompletionTriggerWatchesOnePipelineInOneTenant. A completion trigger is
// a subscription to one upstream pipeline's outcome, not to every run in the
// installation.
func TestCompletionTriggerWatchesOnePipelineInOneTenant(t *testing.T) {
	sink := newRecordingSink()
	tr, err := completion.New(testConfig("scoped"))
	require.NoError(t, err)

	base := completion.Event{
		TenantID: testTenant, PipelineID: upstreamID, RunID: "r",
		Type: runstore.RunCompleted, At: time.Now().UTC(),
	}

	elsewhere := base
	elsewhere.PipelineID = "some-other-pipeline"
	out, err := tr.Observe(context.Background(), sink, elsewhere)
	require.NoError(t, err)
	require.False(t, out.Fired, "the trigger fired on another pipeline's completion")

	otherTenant := base
	otherTenant.TenantID = "globex"
	out, err = tr.Observe(context.Background(), sink, otherTenant)
	require.NoError(t, err)
	require.False(t, out.Fired,
		"the trigger fired on another TENANT's run: a completion crossed a tenant boundary")

	require.Equal(t, 0, sink.count())

	out, err = tr.Observe(context.Background(), sink, base)
	require.NoError(t, err)
	require.True(t, out.Fired)
	require.Equal(t, 1, sink.count())
}

// TestCompletionTriggerRefusesToFireItself. The graph is acyclic by decision
// (ADR 0015) and a pipeline that starts itself on completion is a run loop
// that no iteration budget bounds — it is not one run looping, it is an
// unbounded number of runs.
func TestCompletionTriggerRefusesToFireItself(t *testing.T) {
	cfg := testConfig("ouroboros")
	cfg.UpstreamPipelineID = downstreamID
	_, err := completion.New(cfg)
	require.Error(t, err, "a trigger watching the pipeline it starts was accepted")
	require.Contains(t, err.Error(), downstreamID)
	require.Contains(t, err.Error(), "itself")
}

// TestCompletionTriggerRefusesACycleThroughOtherPipelines. The direct case is
// the easy one; the cycle that actually reaches production is a → b → c → a,
// wired by three people who each added one trigger.
func TestCompletionTriggerRefusesACycleThroughOtherPipelines(t *testing.T) {
	reg := completion.NewRegistry()
	link := func(id, upstream, downstream string) error {
		cfg := testConfig(id)
		cfg.UpstreamPipelineID = upstream
		cfg.Binding.PipelineID = downstream
		cfg.Pipeline = testPipeline(downstream)
		cfg.Registry = reg
		_, err := completion.New(cfg)
		return err
	}

	require.NoError(t, link("a-b", "a", "b"))
	require.NoError(t, link("b-c", "b", "c"))

	err := link("c-a", "c", "a")
	require.Error(t, err, "a completion trigger closing the cycle a -> b -> c -> a was accepted")
	require.Contains(t, err.Error(), "cycle")
	require.Contains(t, err.Error(), "a")

	// A branch that does not close a cycle is still allowed: the check must
	// refuse cycles, not fan-out.
	require.NoError(t, link("b-d", "b", "d"))

	// Two tenants wiring the same pipeline ids are not each other's cycle.
	other := completion.NewRegistry()
	cfg := testConfig("c-a-elsewhere")
	cfg.TenantID = "globex"
	cfg.UpstreamPipelineID = "c"
	cfg.Binding.PipelineID = "a"
	cfg.Pipeline = testPipeline("a")
	cfg.Registry = other
	_, err = completion.New(cfg)
	require.NoError(t, err)
}

// TestCompletionTriggerRefusesUnscopedConfiguration.
func TestCompletionTriggerRefusesUnscopedConfiguration(t *testing.T) {
	cfg := testConfig("unscoped")
	cfg.TenantID = ""
	_, err := completion.New(cfg)
	require.ErrorIs(t, err, trigger.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")

	cfg = testConfig("upstreamless")
	cfg.UpstreamPipelineID = ""
	_, err = completion.New(cfg)
	require.Error(t, err, "a completion trigger watching nothing was accepted")

	cfg = testConfig("misbound")
	cfg.Binding.InputMapping = map[string]string{"run": "phase_of_the_moon"}
	_, err = completion.New(cfg)
	require.Error(t, err, "a binding reading a field a completion event does not carry was accepted")
	require.Contains(t, err.Error(), `"phase_of_the_moon"`)
}

// TestCompletionStartConsumesUntilCancelled. Start owns what it started and
// returns the context's error, like every other trigger.
func TestCompletionStartConsumesUntilCancelled(t *testing.T) {
	events := make(chan completion.Event, 4)
	cfg := testConfig("streamed")
	cfg.Source = events
	tr, err := completion.New(cfg)
	require.NoError(t, err)

	sink := newRecordingSink()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tr.Start(ctx, sink) }()

	events <- completion.Event{
		TenantID: testTenant, PipelineID: upstreamID, RunID: "run-1",
		Type: runstore.RunCompleted, At: time.Now().UTC(),
	}
	require.Eventually(t, func() bool { return sink.count() == 1 },
		5*time.Second, 10*time.Millisecond, "Start never consumed the completion")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return within 5s of cancellation")
	}
}

// --- fixtures -------------------------------------------------------------

func testPipeline(id string) *dholev1.Pipeline {
	structured := func(name string) *dholev1.Port {
		return &dholev1.Port{Name: name, Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
				SchemaId: "https://dhole.dev/schema/" + name,
				Schema:   `{"type":"string","minLength":1}`,
			}},
		}}
	}
	return &dholev1.Pipeline{
		Id: id,
		Steps: []*dholev1.Step{{
			Id:     "publish",
			Inputs: []*dholev1.Port{structured("run"), structured("source"), structured("outcome")},
		}},
	}
}

func testConfig(id string) completion.Config {
	return completion.Config{
		ID:                 id,
		TenantID:           testTenant,
		UpstreamPipelineID: upstreamID,
		Binding: trigger.Binding{
			PipelineID: downstreamID,
			InputMapping: map[string]string{
				"run":     completion.FieldUpstreamRunID,
				"source":  completion.FieldUpstreamPipelineID,
				"outcome": completion.FieldOutcome,
			},
		},
		Pipeline: testPipeline(downstreamID),
	}
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
