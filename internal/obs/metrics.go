package obs

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Metric names. Prometheus-shaped on purpose: these are what an operator types
// into a query box, and a rename breaks every dashboard built on them.
const (
	// MetricStepDurationSeconds is how long a step took, split by whether the
	// cache served it. Without that split a build that got slower is
	// indistinguishable from one that merely ran cold, and every regression
	// investigation starts by arguing about which it was.
	MetricStepDurationSeconds = "dhole_step_duration_seconds"
	// MetricStepCPUSeconds is CPU actually burned by the step's process tree.
	MetricStepCPUSeconds = "dhole_step_cpu_seconds"
	// MetricStepMaxRSSBytes is the peak resident memory of that tree.
	MetricStepMaxRSSBytes = "dhole_step_max_rss_bytes"
	// MetricStepUsageUnavailable counts steps whose resource usage could NOT
	// be measured. It exists so the absence is visible: exporting a zero CPU
	// figure for a platform that does not report one would put a lie in the
	// same series as the truth, and nobody reading the dashboard could tell.
	MetricStepUsageUnavailable = "dhole_step_usage_unavailable_total"
)

// Outcomes a step can end with. A closed set, which is what makes it safe as a
// metric label.
const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	OutcomeCancelled = "cancelled"
)

// UsageReporter is implemented by a sandbox that can say what the last command
// it ran consumed. It is an optional interface, discovered by assertion: a
// backend that cannot measure honestly does not implement it, and the caller
// needs no per-backend branch.
//
// The signature is deliberately made of primitives so an executor backend can
// satisfy it without importing this package: telemetry depends on the place
// steps run, not the other way round.
type UsageReporter interface {
	// LastUsage reports the resources of the most recently finished command.
	// ok is false when this platform or backend does not report them.
	LastUsage() (cpuSeconds float64, maxRSSBytes int64, ok bool)
}

// instruments is the set built by one Init. Held in an atomic pointer because
// every step records through it and Init happens once at start-up.
type instruments struct {
	duration    metric.Float64Histogram
	cpuSeconds  metric.Float64Histogram
	maxRSSBytes metric.Int64Histogram
	unavailable metric.Int64Counter
}

var live atomic.Pointer[instruments]

func setInstruments(i *instruments) { live.Store(i) }

func newInstruments(provider *sdkmetric.MeterProvider) (*instruments, error) {
	meter := provider.Meter(instrumentationName)

	duration, err := meter.Float64Histogram(MetricStepDurationSeconds,
		metric.WithUnit("s"),
		metric.WithDescription("Wall-clock duration of one step, by tenant, outcome and cache hit."))
	if err != nil {
		return nil, err
	}
	cpu, err := meter.Float64Histogram(MetricStepCPUSeconds,
		metric.WithUnit("s"),
		metric.WithDescription("CPU seconds consumed by one step's process tree."))
	if err != nil {
		return nil, err
	}
	rss, err := meter.Int64Histogram(MetricStepMaxRSSBytes,
		metric.WithUnit("By"),
		metric.WithDescription("Peak resident set size of one step's process tree."))
	if err != nil {
		return nil, err
	}
	unavailable, err := meter.Int64Counter(MetricStepUsageUnavailable,
		metric.WithDescription("Steps whose CPU and memory usage the backend could not measure."))
	if err != nil {
		return nil, err
	}
	return &instruments{duration: duration, cpuSeconds: cpu, maxRSSBytes: rss, unavailable: unavailable}, nil
}

// RecordStepDuration records how long a step took.
//
// The labels are the tenant, the outcome and the cache verdict — three closed
// sets. The run id and the step id are deliberately NOT here: they are
// unbounded user input, one new time series each, and a metrics backend given
// a label per run falls over in production rather than in a test. They are on
// the step's span, which is the thing built to carry identity.
func RecordStepDuration(ctx context.Context, tenantID string, d time.Duration, cacheHit bool, outcome string) {
	i := live.Load()
	if i == nil {
		return
	}
	i.duration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String(AttrTenant, tenantID),
		attribute.String(AttrOutcome, outcome),
		// A string, not a bool, so the exported label reads "true"/"false"
		// identically through every exporter.
		attribute.String(AttrCacheHit, strconv.FormatBool(cacheHit)),
	))
}

// RecordStepUsage records what a step's process tree consumed, taking the
// figures from the sandbox that ran it if that sandbox can report them.
//
// source is whatever executed the step; it is inspected for UsageReporter. When
// it cannot report — a backend with no accounting, or a platform whose
// os/exec does not carry rusage — nothing is recorded except a count of the
// omission. A zero exported as though it were a measurement would be worse than
// no metric at all: it reads as "this step used no CPU", which is never true.
func RecordStepUsage(ctx context.Context, tenantID string, source any) {
	i := live.Load()
	if i == nil {
		return
	}
	tenant := metric.WithAttributes(attribute.String(AttrTenant, tenantID))

	reporter, ok := source.(UsageReporter)
	if !ok {
		i.unavailable.Add(ctx, 1, tenant)
		return
	}
	cpuSeconds, maxRSSBytes, ok := reporter.LastUsage()
	if !ok {
		i.unavailable.Add(ctx, 1, tenant)
		return
	}
	i.cpuSeconds.Record(ctx, cpuSeconds, tenant)
	i.maxRSSBytes.Record(ctx, maxRSSBytes, tenant)
}
