// Package obs_test asks the only question observability exists to answer:
// where did a run's time go?
//
// Answering it needs one trace covering a whole run, and a run's steps do not
// execute in the process that scheduled them. So the trace has to cross the
// bus: if the engine starts a root span of its own, the result is two
// disconnected traces and nobody can see the run at all. Every test here is
// written so that it fails when that link breaks.
package obs_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/mirror"
	"github.com/azrtydxb/dhole/internal/obs"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/server"
)

const tenantID = "default"

// pipelineFile is the same two-step definition the end-to-end test runs. What
// is instrumented has to be the real pipeline path, not a stand-in: a fake
// scheduler and a fake engine in one process would carry a trace in a Go
// context and prove nothing about the wire.
const pipelineFile = "../../testdata/pipelines/two-step.yaml"

// TestEachStepEmitsOneSpanWithRunAndStepAttributes is the headline property:
// two step spans, each naming its run and its step, both hanging off the run's
// span — which lives in the control plane, while the steps ran in the engine.
func TestEachStepEmitsOneSpanWithRunAndStepAttributes(t *testing.T) {
	h := pipelineHarness(t)

	run := spansNamed(h.spans, obs.SpanRun)
	require.Len(t, run, 1, "one run span per run, or the trace has no root to hang off")
	require.Equal(t, h.runID, attrString(run[0], obs.AttrRunID))

	steps := spansNamed(h.spans, obs.SpanStep)
	require.Len(t, steps, 2, "one span per step: %v", spanSummary(h.spans))

	seen := map[string]bool{}
	for _, s := range steps {
		require.Equal(t, h.runID, attrString(s, obs.AttrRunID))
		stepID := attrString(s, obs.AttrStepID)
		require.NotEmpty(t, stepID)
		require.False(t, seen[stepID], "step %q emitted more than one span", stepID)
		seen[stepID] = true

		// The link that has to survive the process boundary. A step span with
		// no parent, or one in a different trace, means the engine started a
		// root span instead of joining the run's trace.
		require.True(t, s.Parent.IsValid(),
			"step %q span has no parent: the dispatch's trace context did not reach the engine", stepID)
		require.Equal(t, run[0].SpanContext.TraceID(), s.SpanContext.TraceID(),
			"step %q is in a different trace from its run", stepID)
		require.Equal(t, run[0].SpanContext.SpanID(), s.Parent.SpanID(),
			"step %q is not parented to the run span", stepID)
	}
	require.Equal(t, map[string]bool{"a": true, "b": true}, seen)
}

// TestStepResourceMetricsAreRecorded is what makes "this run got slower"
// answerable with a cause rather than a shrug: CPU and peak memory, per step.
func TestStepResourceMetricsAreRecorded(t *testing.T) {
	h := pipelineHarness(t)

	cpu := histogramPoints[float64](t, h.metrics, obs.MetricStepCPUSeconds)
	require.NotEmpty(t, cpu, "no %s was exported for any step", obs.MetricStepCPUSeconds)
	rss := histogramPoints[int64](t, h.metrics, obs.MetricStepMaxRSSBytes)
	require.NotEmpty(t, rss, "no %s was exported for any step", obs.MetricStepMaxRSSBytes)

	require.Equal(t, uint64(2), totalCount(cpu), "one CPU measurement per step")
	require.Equal(t, uint64(2), totalCount(rss), "one peak-memory measurement per step")

	// A recorded zero would be indistinguishable from an unmeasured one, which
	// is exactly the confusion this metric must not create.
	var maxRSS int64
	for _, p := range rss {
		if v, ok := p.Max.Value(); ok && v > maxRSS {
			maxRSS = v
		}
	}
	require.Positive(t, maxRSS, "peak RSS came back as zero: it was not measured, it was assumed")

	for _, p := range cpu {
		require.Equal(t, tenantID, attrValue(p.Attributes, obs.AttrTenant),
			"a resource metric with no tenant cannot be billed, capped or blamed")
	}
}

// TestBuildDurationRegressionMetricIsExported: a duration with no cache_hit
// label cannot tell a cold run from a warm one, so a build that got slower
// looks the same as one that simply missed the cache.
func TestBuildDurationRegressionMetricIsExported(t *testing.T) {
	h := pipelineHarness(t)

	points := histogramPoints[float64](t, h.metrics, obs.MetricStepDurationSeconds)
	require.NotEmpty(t, points)
	for _, p := range points {
		require.Equal(t, tenantID, attrValue(p.Attributes, obs.AttrTenant))
		_, ok := p.Attributes.Value(attribute.Key(obs.AttrCacheHit))
		require.True(t, ok, "%s carries no %s label: %v",
			obs.MetricStepDurationSeconds, obs.AttrCacheHit, p.Attributes.Encoded(attribute.DefaultEncoder()))
	}
	require.Equal(t, "false", attrValue(points[0].Attributes, obs.AttrCacheHit),
		"a step that really executed on an engine was not served from the cache")

	// And the other value is a distinct series, or the label separates nothing.
	reader := sdkmetric.NewManualReader()
	shutdown := initObs(t, nil, reader)
	obs.RecordStepDuration(context.Background(), tenantID, 3*time.Second, true, obs.OutcomeSucceeded)
	warm := histogramPoints[float64](t, collect(t, reader), obs.MetricStepDurationSeconds)
	require.NoError(t, shutdown(context.Background()))

	require.Len(t, warm, 1)
	require.Equal(t, "true", attrValue(warm[0].Attributes, obs.AttrCacheHit))
}

// TestTraceContextCrossesTheDispatchMessage is the property stated directly,
// with no pipeline around it: the parenting in the test above comes from bytes
// in the JobDispatch, not from a Go context that happened to be in scope.
func TestTraceContextCrossesTheDispatchMessage(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	shutdown := initObs(t, exporter, nil)

	runCtx, runSpan := obs.RunSpan(ctx, tenantID, "run-carried")
	carried := &dholev1.JobDispatch{RunId: "run-carried", StepId: "s"}
	obs.Inject(runCtx, carried)
	require.NotEmpty(t, carried.GetTraceContext(),
		"nothing was written to the dispatch, so no engine could ever join the trace")

	// The engine side: a fresh context, as it really is over there.
	_, joined := obs.StepSpan(obs.ContextFrom(ctx, carried), "run-carried", "s")
	joined.End()

	// The same message with its trace context removed — the mutation this test
	// exists to catch — must NOT produce a parented span.
	bare := &dholev1.JobDispatch{RunId: "run-bare", StepId: "s"}
	_, orphan := obs.StepSpan(obs.ContextFrom(ctx, bare), "run-bare", "s")
	orphan.End()

	runSpan.End()
	require.NoError(t, shutdown(ctx))

	spans := exporter.GetSpans()
	joinedStub := spanWithAttr(t, spans, obs.AttrRunID, "run-carried", obs.SpanStep)
	require.True(t, joinedStub.Parent.IsValid())
	require.Equal(t, runSpan.SpanContext().TraceID(), joinedStub.SpanContext.TraceID())
	require.Equal(t, runSpan.SpanContext().SpanID(), joinedStub.Parent.SpanID())

	orphanStub := spanWithAttr(t, spans, obs.AttrRunID, "run-bare", obs.SpanStep)
	require.False(t, orphanStub.Parent.IsValid(),
		"a dispatch with no trace context produced a parented span: the parenting cannot be coming from the wire")
}

// TestFailedStepStillEndsItsSpan: an unended span is never exported, so a
// failure — the case anyone actually goes looking for — would be the one case
// missing from the trace.
func TestFailedStepStillEndsItsSpan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	exporter := tracetest.NewInMemoryExporter()
	shutdown := initObs(t, exporter, sdkmetric.NewManualReader())

	srv := startEmbedded(ctx, t)
	runID, err := srv.Submit(ctx, tenantID, failingPipeline(t))
	require.NoError(t, err)
	awaitEvent(ctx, t, srv, runID, runstore.StepFailed)
	require.NoError(t, shutdown(ctx))

	stub := spanWithAttr(t, exporter.GetSpans(), obs.AttrRunID, runID, obs.SpanStep)
	require.Equal(t, "boom", attrString(stub, obs.AttrStepID))
	require.Equal(t, "Error", stub.Status.Code.String(),
		"a step that failed must be visible as a failure in its span, not merely present")
}

// TestCancelledStepSpanIsStillEnded holds the other end of the same rule: a
// context that is already dead does not excuse leaking a span.
func TestCancelledStepSpanIsStillEnded(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	shutdown := initObs(t, exporter, nil)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, span := obs.StepSpan(cancelled, "run-cancelled", "s")
	span.End()
	require.NoError(t, shutdown(context.Background()))

	require.Len(t, exporter.GetSpans(), 1,
		"the span of a cancelled step never reached the exporter")
}

// TestMetricLabelsAreNeverUnboundedUserInput. A run id as a label is one new
// time series per run: the metrics backend falls over, and it falls over in
// production rather than here. Run and step ids live on spans, which are
// sampled and indexed for exactly that.
func TestMetricLabelsAreNeverUnboundedUserInput(t *testing.T) {
	h := pipelineHarness(t)

	for _, sm := range h.metrics.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, set := range attributeSets(m) {
				for _, kv := range set.ToSlice() {
					key := string(kv.Key)
					require.NotContains(t, key, "run_id", "metric %s labels a run id", m.Name)
					require.NotContains(t, key, "step_id", "metric %s labels a step id", m.Name)
					require.NotEqual(t, h.runID, kv.Value.String(),
						"metric %s carries the run id as the value of label %q", m.Name, key)
				}
			}
		}
	}
}

// TestShutdownFlushesWhatWasNotYetExported. Spans are batched; a shutdown that
// forgets to flush loses the tail of every process that exits, which is
// precisely the process that crashed and that someone is now investigating.
func TestShutdownFlushesWhatWasNotYetExported(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	shutdown := initObs(t, exporter, nil)

	_, span := obs.StepSpan(ctx, "run-flush", "s")
	span.End()
	require.Empty(t, exporter.GetSpans(),
		"the span was exported without a flush, so this test cannot prove the flush happens")

	require.NoError(t, shutdown(ctx))
	require.Len(t, exporter.GetSpans(), 1, "shutdown returned without flushing the batch")
}

// TestInitWithoutACollectorStillRuns. A control plane with no OTLP endpoint is
// the normal single-binary case; failing to start because nobody is collecting
// traces would make observability a dependency of running at all.
func TestInitWithoutACollectorStillRuns(t *testing.T) {
	ctx := context.Background()
	shutdown, err := obs.Init(ctx, obs.Config{ServiceName: "dhole-test"})
	require.NoError(t, err, "no collector configured must not be a start-up failure")
	require.NotNil(t, shutdown)

	// And every call site still works, so no caller needs an "if observability
	// is on" branch.
	spanCtx, span := obs.StepSpan(ctx, "run-none", "s")
	obs.RecordStepDuration(spanCtx, tenantID, time.Second, false, obs.OutcomeSucceeded)
	obs.RecordStepUsage(spanCtx, tenantID, nil)
	span.End()
	obs.EndRun(tenantID, "run-none")

	require.NoError(t, shutdown(ctx))
}

// --- harness -------------------------------------------------------------

// harness is one real run of the two-step pipeline with everything exported
// captured. It runs once for the whole package: the pipeline is the expensive
// part, and every assertion above is about the same run.
type harness struct {
	runID   string
	spans   tracetest.SpanStubs
	metrics metricdata.ResourceMetrics
}

var (
	harnessOnce   sync.Once
	harnessResult harness
)

func pipelineHarness(t *testing.T) harness {
	t.Helper()
	harnessOnce.Do(func() { harnessResult = runPipelineOnce(t) })
	require.NotEmpty(t, harnessResult.runID, "the pipeline harness did not run")
	return harnessResult
}

func runPipelineOnce(t *testing.T) harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	exporter := tracetest.NewInMemoryExporter()
	reader := sdkmetric.NewManualReader()
	shutdown := initObs(t, exporter, reader)

	srv := startEmbedded(ctx, t)
	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	awaitEvent(ctx, t, srv, runID, runstore.RunCompleted)

	// Captured with the process still running, on purpose. A run span that is
	// only closed when the binary shuts down is a trace nobody sees while the
	// system is up — which is the only time anyone looks at one — so reading
	// the spans after shutdown would hide exactly that bug.
	spans := awaitRunSpan(t, exporter)
	metrics := collect(t, reader)
	require.NoError(t, shutdown(ctx))
	return harness{runID: runID, spans: spans, metrics: metrics}
}

// keepingExporter is an in-memory exporter that does NOT forget what it was
// given when it is shut down. The stock one resets on Shutdown, which would
// erase precisely the spans a flush-on-shutdown test exists to find.
type keepingExporter struct{ *tracetest.InMemoryExporter }

func (keepingExporter) Shutdown(context.Context) error { return nil }

// awaitRunSpan flushes until the completed run's own span has been exported.
// The wait is for the ordering of two writes — the event and the span's end —
// not for a shutdown that has not happened.
func awaitRunSpan(t *testing.T, exporter *tracetest.InMemoryExporter) tracetest.SpanStubs {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		require.NoError(t, obs.Flush(context.Background()))
		spans := exporter.GetSpans()
		if len(spansNamed(spans, obs.SpanRun)) > 0 {
			return spans
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run finished but its span was never ended: %v", spanSummary(spans))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func initObs(t *testing.T, exporter *tracetest.InMemoryExporter, reader sdkmetric.Reader) func(context.Context) error {
	t.Helper()
	cfg := obs.Config{ServiceName: "dhole-test", MetricReader: reader}
	if exporter != nil {
		cfg.SpanExporter = keepingExporter{exporter}
	}
	shutdown, err := obs.Init(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	return shutdown
}

func startEmbedded(ctx context.Context, t *testing.T) *server.Server {
	t.Helper()
	dir, err := os.MkdirTemp("", "dhole-obs-")
	require.NoError(t, err)
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, srv.Stop(stopCtx))
		_ = os.RemoveAll(dir)
	})
	return srv
}

func loadPipeline(t *testing.T) *dholev1.Pipeline {
	t.Helper()
	raw, err := os.ReadFile(pipelineFile)
	require.NoError(t, err)
	p, err := mirror.FromYAML(raw)
	require.NoError(t, err)
	return p
}

// failingPipeline is one step that exits non-zero, at-most-once so it is tried
// exactly once and the failure arrives without a retry backoff.
func failingPipeline(t *testing.T) *dholev1.Pipeline {
	t.Helper()
	const definition = `
id: one-failing-step
tenant:
  id: default
steps:
  - id: boom
    name: boom
    plugin_ref: 'command:{"args":["/bin/sh","-c","exit 3"]}'
    effect_class: EFFECT_CLASS_AT_MOST_ONCE
`
	p, err := mirror.FromYAML([]byte(definition))
	require.NoError(t, err)
	return p
}

func awaitEvent(ctx context.Context, t *testing.T, srv *server.Server, runID string, want runstore.EventType) {
	t.Helper()
	deadline := time.Now().Add(240 * time.Second)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		for _, e := range events {
			if e.Type == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s in run %s", want, runID)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no %s in run %s before the deadline", want, runID)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func collect(t *testing.T, reader sdkmetric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

// --- assertions on what was exported -------------------------------------

func spansNamed(spans tracetest.SpanStubs, name string) tracetest.SpanStubs {
	var out tracetest.SpanStubs
	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}
	return out
}

func spanWithAttr(t *testing.T, spans tracetest.SpanStubs, key, value, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range spans {
		if s.Name == name && attrString(s, key) == value {
			return s
		}
	}
	t.Fatalf("no %s span with %s=%q; exported: %v", name, key, value, spanSummary(spans))
	return tracetest.SpanStub{}
}

func attrString(s tracetest.SpanStub, key string) string {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	return ""
}

func spanSummary(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name+"["+strings.Join([]string{
			attrString(s, obs.AttrRunID), attrString(s, obs.AttrStepID),
		}, " ")+"]")
	}
	return out
}

func histogramPoints[N int64 | float64](t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[N] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[N])
			require.True(t, ok, "metric %s is not a histogram of the expected numeric type", name)
			return h.DataPoints
		}
	}
	return nil
}

func totalCount[N int64 | float64](points []metricdata.HistogramDataPoint[N]) uint64 {
	var n uint64
	for _, p := range points {
		n += p.Count
	}
	return n
}

func attrValue(set attribute.Set, key string) string {
	v, ok := set.Value(attribute.Key(key))
	if !ok {
		return ""
	}
	return v.String()
}

// attributeSets is every label set any point of a metric carries, whatever its
// aggregation.
func attributeSets(m metricdata.Metrics) []attribute.Set {
	var out []attribute.Set
	switch data := m.Data.(type) {
	case metricdata.Histogram[float64]:
		for _, p := range data.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Histogram[int64]:
		for _, p := range data.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Sum[int64]:
		for _, p := range data.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Sum[float64]:
		for _, p := range data.DataPoints {
			out = append(out, p.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, p := range data.DataPoints {
			out = append(out, p.Attributes)
		}
	}
	return out
}
