// Package obs is Dhole's telemetry: the traces and metrics that answer "where
// did this run's time go?".
//
// The question is harder than it looks because a run is not a call stack. A
// step is dispatched by the control plane and executed by an engine in a
// different process, on a different machine, minutes later. If each side starts
// a trace of its own the result is two disconnected traces, neither of which
// describes the run: the plane's shows a dispatch being enqueued, the engine's
// shows a command running, and nothing joins them.
//
// So the trace crosses the bus. The run's span context travels IN the
// JobDispatch — `trace_context`, W3C headers, documented in
// docs/wire-contract.md because a third-party engine that does not know to
// continue from it silently breaks the trace at the process boundary. That
// field is the whole reason this package is not just a wrapper around
// otel.Tracer.
//
// Two rules the rest of the package exists to keep:
//
// Every span is ended on every path, failure and cancellation included. An
// unended span is not merely a leak — it is never exported at all, so the
// failure someone is investigating is precisely the one missing from the trace.
//
// Nothing unbounded becomes a metric label. Run and step ids are attributes on
// spans, which are built to carry high-cardinality identity; as a metric label
// each new run would mint a new time series and take the metrics backend down.
// Metric labels here are the tenant, the outcome, and whether the cache was
// hit — all small, closed sets.
package obs

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Span names and attribute keys. They are exported because they are a
// contract: a dashboard, an alert and a test all name them, and a rename that
// only the emitter knows about is a dashboard that silently goes blank.
const (
	// SpanRun covers one pipeline run, in the control plane.
	SpanRun = "dhole.run"
	// SpanStep covers the execution of one step, in the engine that ran it.
	SpanStep = "dhole.step"

	// AttrRunID and AttrStepID are span attributes, never metric labels.
	AttrRunID  = "dhole.run_id"
	AttrStepID = "dhole.step_id"
	// AttrTenant is on both: a tenant is a closed set, and telemetry that
	// cannot be attributed to one cannot be billed, capped or blamed.
	AttrTenant = "dhole.tenant"
	// AttrOutcome is how a step ended.
	AttrOutcome = "outcome"
	// AttrCacheHit separates a cold run from a warm one.
	AttrCacheHit = "cache_hit"
)

// instrumentationName scopes every instrument and tracer this package creates.
const instrumentationName = "github.com/azrtydxb/dhole/internal/obs"

// Config is what Init needs. The zero value is valid and means "no collector":
// a single binary on a laptop has no OTLP endpoint and must still start.
type Config struct {
	// ServiceName identifies this process in the trace backend.
	ServiceName string
	// OTLPEndpoint is the collector's gRPC address, e.g. "localhost:4317".
	// Empty means telemetry is assembled but goes nowhere.
	OTLPEndpoint string
	// Insecure sends to the collector without TLS.
	Insecure bool
	// SpanExporter and MetricReader replace the OTLP pair. They exist so a
	// test can assert on what was really exported rather than on the fact
	// that an exporter was constructed.
	SpanExporter sdktrace.SpanExporter
	MetricReader sdkmetric.Reader
}

// providers is everything one Init built, so shutdown can undo exactly it.
type providers struct {
	tracer *sdktrace.TracerProvider
	meter  *sdkmetric.MeterProvider
}

var (
	// mu guards the process-wide telemetry state. Init is rare; the read path
	// takes it only to fetch already-built instruments.
	mu      sync.Mutex
	current *providers
)

// Init installs the global tracer and meter providers and returns the shutdown
// that flushes them.
//
// It never fails because no collector is configured. A control plane whose
// operator has not set up OTLP runs normally with telemetry that goes nowhere:
// making observability a start-up dependency would mean a broken collector
// takes the plane down, which is the opposite of what it is for.
//
// The returned shutdown must be called before the process exits. Spans are
// batched, so what has not been flushed has not been exported — and the
// unflushed tail belongs to the crash someone is about to investigate.
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName(cfg)),
	))
	if err != nil {
		return nil, err
	}

	spanExporter, err := spanExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}
	traceOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if spanExporter != nil {
		// Batched, not simple: a synchronous export would put the collector's
		// latency on the step's critical path.
		traceOpts = append(traceOpts, sdktrace.WithBatcher(spanExporter))
	}
	tracerProvider := sdktrace.NewTracerProvider(traceOpts...)

	reader, err := metricReader(ctx, cfg)
	if err != nil {
		return nil, errors.Join(err, tracerProvider.Shutdown(ctx))
	}
	meterOpts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	if reader != nil {
		meterOpts = append(meterOpts, sdkmetric.WithReader(reader))
	}
	meterProvider := sdkmetric.NewMeterProvider(meterOpts...)

	instruments, err := newInstruments(meterProvider)
	if err != nil {
		return nil, errors.Join(err, tracerProvider.Shutdown(ctx), meterProvider.Shutdown(ctx))
	}

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	// The propagator is what makes trace_context in a JobDispatch mean
	// something both sides agree on: W3C trace context, no Dhole invention.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	mu.Lock()
	previous := current
	current = &providers{tracer: tracerProvider, meter: meterProvider}
	mu.Unlock()
	setInstruments(instruments)
	// A re-Init leaves nothing half-open, and leaves no run span pointing into
	// a provider that is being torn down.
	endAllRunSpans()
	if previous != nil {
		_ = shutdownProviders(ctx, previous)
	}

	var once sync.Once
	return func(shutdownCtx context.Context) error {
		var err error
		once.Do(func() {
			endAllRunSpans()
			mu.Lock()
			p := current
			if p != nil && p.tracer == tracerProvider {
				current = nil
			}
			mu.Unlock()
			setInstruments(nil)
			err = shutdownProviders(shutdownCtx, &providers{tracer: tracerProvider, meter: meterProvider})
		})
		return err
	}, nil
}

func shutdownProviders(ctx context.Context, p *providers) error {
	// Shutdown flushes what is still batched. Skipping it is the bug that
	// makes a test end with an empty exporter and an engineer conclude the
	// instrumentation was never wired up.
	return errors.Join(p.tracer.Shutdown(ctx), p.meter.Shutdown(ctx))
}

// Flush exports everything buffered without shutting anything down.
func Flush(ctx context.Context) error {
	mu.Lock()
	p := current
	mu.Unlock()
	if p == nil {
		return nil
	}
	return errors.Join(p.tracer.ForceFlush(ctx), p.meter.ForceFlush(ctx))
}

func serviceName(cfg Config) string {
	if cfg.ServiceName != "" {
		return cfg.ServiceName
	}
	return "dhole"
}

func spanExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	if cfg.SpanExporter != nil {
		return cfg.SpanExporter, nil
	}
	if cfg.OTLPEndpoint == "" {
		return nil, nil //nolint:nilnil // no collector is a valid configuration, not an error.
	}
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return otlptracegrpc.New(ctx, opts...)
}

func metricReader(ctx context.Context, cfg Config) (sdkmetric.Reader, error) {
	if cfg.MetricReader != nil {
		return cfg.MetricReader, nil
	}
	if cfg.OTLPEndpoint == "" {
		return nil, nil //nolint:nilnil // as above.
	}
	opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint)}
	if cfg.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	exporter, err := otlpmetricgrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return sdkmetric.NewPeriodicReader(exporter), nil
}

func tracer() trace.Tracer { return otel.Tracer(instrumentationName) }

// StepSpan starts the span covering one step's execution.
//
// It is called on the ENGINE side, from a context recovered with ContextFrom,
// so the span joins the run's trace rather than starting one. The caller ends
// it — unconditionally, with defer — on every path.
func StepSpan(ctx context.Context, runID, stepID string) (context.Context, trace.Span) {
	return tracer().Start(ctx, SpanStep,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(AttrRunID, runID),
			attribute.String(AttrStepID, stepID),
		),
	)
}

// EndStepSpan closes a step span with the outcome it actually had. A failure
// that ends as an unmarked span is a failure nobody finds by searching.
func EndStepSpan(span trace.Span, outcome string, err error) {
	span.SetAttributes(attribute.String(AttrOutcome, outcome))
	switch {
	case err != nil:
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	case outcome == OutcomeFailed || outcome == OutcomeCancelled:
		span.SetStatus(codes.Error, outcome)
	default:
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

// runSpans holds the open span of every run this process is advancing.
//
// A run span cannot be a local variable: a run is a state machine driven by an
// event log, advanced by whichever call happens to arrive next, and there is no
// single function whose lifetime is the run's (ADR 0003). It is process-local
// and deliberately so — a run that outlives a control-plane restart gets a new
// span in a new trace, which is honest, where a fabricated continuation would
// claim a causal link the process cannot vouch for.
var runSpans = struct {
	mu    sync.Mutex
	spans map[string]trace.Span
}{spans: map[string]trace.Span{}}

// maxRunSpans bounds the map. Spans are ended and evicted on the way out, but a
// plane that is killed mid-run, or a run that is abandoned, would otherwise
// leave one behind forever.
const maxRunSpans = 4096

// RunSpan returns the context carrying this run's span, starting it the first
// time the run is seen. It is the parent every step span in the run hangs off.
func RunSpan(ctx context.Context, tenantID, runID string) (context.Context, trace.Span) {
	key := tenantID + "/" + runID

	runSpans.mu.Lock()
	span, ok := runSpans.spans[key]
	if !ok {
		if len(runSpans.spans) >= maxRunSpans {
			evictOneLocked()
		}
		// Started from a background context on purpose: the run's span
		// belongs to the run, not to whichever Advance happened to create it,
		// and parenting it under a caller's span would nest whole runs inside
		// an HTTP request that has since returned.
		_, span = tracer().Start(context.WithoutCancel(ctx), SpanRun,
			trace.WithNewRoot(),
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String(AttrRunID, runID),
				attribute.String(AttrTenant, tenantID),
			),
		)
		runSpans.spans[key] = span
	}
	runSpans.mu.Unlock()

	return trace.ContextWithSpan(ctx, span), span
}

// EndRun closes a run's span. It is idempotent, so the terminal paths — a run
// that completed and one that failed — can both call it unconditionally.
func EndRun(tenantID, runID string) {
	key := tenantID + "/" + runID
	runSpans.mu.Lock()
	span, ok := runSpans.spans[key]
	delete(runSpans.spans, key)
	runSpans.mu.Unlock()
	if ok {
		span.End()
	}
}

// evictOneLocked ends and drops one span so the map cannot grow without bound.
// Ending it — rather than dropping it — is what keeps an evicted run visible in
// the trace instead of vanishing.
func evictOneLocked() {
	for key, span := range runSpans.spans {
		delete(runSpans.spans, key)
		span.End()
		return
	}
}

// endAllRunSpans ends every open run span. Called on shutdown so nothing is
// left unexported, and on re-Init so no span outlives the provider that made it.
func endAllRunSpans() {
	runSpans.mu.Lock()
	spans := make([]trace.Span, 0, len(runSpans.spans))
	for key, span := range runSpans.spans {
		spans = append(spans, span)
		delete(runSpans.spans, key)
	}
	runSpans.mu.Unlock()
	for _, span := range spans {
		span.End()
	}
}

// dispatchCarrier adapts a JobDispatch's trace_context map to the propagator.
// It is a map, not a struct field, because the W3C carrier is headers and a
// future version of the spec may add one: an engine that copies the map
// forward keeps working, one that copies a named field does not.
type dispatchCarrier struct{ d *dholev1.JobDispatch }

func (c dispatchCarrier) Get(key string) string { return c.d.GetTraceContext()[key] }

func (c dispatchCarrier) Set(key, value string) {
	if c.d.TraceContext == nil {
		c.d.TraceContext = map[string]string{}
	}
	c.d.TraceContext[key] = value
}

func (c dispatchCarrier) Keys() []string {
	keys := make([]string, 0, len(c.d.GetTraceContext()))
	for k := range c.d.GetTraceContext() {
		keys = append(keys, k)
	}
	return keys
}

// Inject writes the trace context in ctx into the dispatch, so the engine that
// picks the message up can continue the run's trace instead of starting one.
//
// This is the single point where the trace crosses the process boundary. With
// it removed, everything still runs, every span is still emitted, and the trace
// is useless — which is why the test for it asserts parentage rather than
// presence.
func Inject(ctx context.Context, d *dholev1.JobDispatch) {
	if d == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, dispatchCarrier{d: d})
}

// ContextFrom returns ctx carrying the trace context the dispatch arrived with.
// A dispatch written by a plane with no tracing carries none, and the step then
// starts a trace of its own rather than failing.
func ContextFrom(ctx context.Context, d *dholev1.JobDispatch) context.Context {
	if d == nil || len(d.GetTraceContext()) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, dispatchCarrier{d: d})
}
