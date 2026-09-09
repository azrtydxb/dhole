// Package acceptance is the definition of done for v1 (ADR 0016): three
// pipelines, one per profile, each running end to end through the real
// control plane rather than through a harness that resembles one.
//
// The claim under test is the one the whole design rests on — that CI,
// automation and agent orchestration are three PROFILES over one core, not
// three systems. Nothing here is allowed to prove that by argument. Each test
// starts a real `internal/server` control plane, submits a definition read
// from a YAML file a person could have written, and asserts on the run's
// event log, which is the only place a run's state lives (ADR 0003).
//
// Every test skips, with a reason naming what is missing, when the
// infrastructure it needs is absent: `make test` on a laptop with nothing
// running stays green, and `make acceptance-*` is where these actually run.
package acceptance_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	// The pgx database/sql driver, registered as "pgx", so a test can create
	// and drop a database of its own.
	_ "github.com/jackc/pgx/v5/stdlib"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/engine"
	dholeexecutor "github.com/azrtydxb/dhole/internal/executor"
	k8sexec "github.com/azrtydxb/dhole/internal/executor/kubernetes"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/mirror"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/steps/approval"
	"github.com/azrtydxb/dhole/internal/steps/llm"
	"github.com/azrtydxb/dhole/internal/steps/loop"
	"github.com/azrtydxb/dhole/internal/trigger"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
	"github.com/azrtydxb/dhole/internal/trigger/schedule"
	"github.com/azrtydxb/dhole/internal/wait"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// tenantID scopes every record and every subject these tests write. There is
// no unscoped write, even with a single tenant.
const tenantID = server.DefaultTenant

// sandboxImage is what a Kubernetes sandbox runs. It is a multi-arch tag with
// an arm64 variant because the cluster this was developed against is arm64,
// and it is a TAG rather than a digest only because the executor resolves it
// to a digest itself — that digest is the environment identity every cache
// key is folded over (ADR 0009), so a step here is cacheable exactly because
// the image is nameable.
const sandboxImage = "busybox:1.36"

// TestAcceptanceCICacheHit is the CI profile: a build that demonstrably hits
// the cache on a second run with unchanged inputs.
//
// The assertion that matters is the negative one. A second run that merely
// completed again proves nothing — it would complete just as happily by doing
// all the work twice, which is precisely the defect Task 15b closed. So this
// requires that the build step reports a cache hit AND that no step of the
// second run reached an engine at all, and only then looks at the clock.
func TestAcceptanceCICacheHit(t *testing.T) {
	kubeconfig := kubeconfigOrSkip(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	pipeline := loadPipeline(t, "ci/pipeline.yaml")

	namespace := ownNamespace(ctx, t, kubeconfig)
	exec, err := k8sexec.New(k8sexec.Config{
		Kubeconfig: kubeconfig,
		Namespace:  namespace,
		PodTemplate: &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "step", Image: sandboxImage}},
		},
	})
	require.NoError(t, err)

	srv := startPlane(ctx, t, t.TempDir(), exec)
	watcher := watchDispatches(ctx, t, srv)

	firstStart := time.Now()
	first, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	firstEvents := awaitRunCompleted(ctx, t, srv, first)
	firstDuration := time.Since(firstStart)

	// The first run really ran, and the build step really was cacheable. A
	// run whose steps were all refused by cache.Eligible would make
	// everything below pass for the wrong reason.
	require.Contains(t, watcher.stepsDispatchedFor(first), "build",
		"the first run has nothing recorded to reuse, so the build goes to an engine")
	firstBuild := dispatchedPayload(t, firstEvents, "build")
	require.True(t, firstBuild.Cacheable,
		"the build step must be cacheable, or nothing below tests the cache: %s", firstBuild.CacheIneligibleReason)
	require.False(t, firstBuild.CacheHit, "the first run cannot have hit a cache that was empty")

	secondStart := time.Now()
	second, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	secondEvents := awaitRunCompleted(ctx, t, srv, second)
	secondDuration := time.Since(secondStart)

	require.True(t, dispatchedPayload(t, secondEvents, "build").CacheHit,
		"the second run's build step was served from the cache and the log has to say so")
	require.Empty(t, watcher.stepsDispatchedFor(second),
		"the second run repeats work already recorded: no step may reach an engine")

	// And the reused outputs are the first run's, byte for byte.
	require.Equal(t,
		string(outputBytes(ctx, t, srv, firstEvents, "test", "report")),
		string(outputBytes(ctx, t, srv, secondEvents, "test", "report")))

	// Only now the clock. It is last because a fast run that did the work
	// again would be a passing timing assertion over a broken cache.
	t.Logf("CI pipeline: first run %s, second run %s", firstDuration, secondDuration)
	require.Less(t, secondDuration, firstDuration/5,
		"a cached run does no work: it must finish in well under a fifth of the first run's %s",
		firstDuration)
}

// TestCIPipelineBuildsTheCheckedInDockerfile keeps the checked-in Dockerfile
// load-bearing.
//
// A pipeline definition has no way to reference a file from the repository:
// a step's inputs come from edges and from nothing else (ADR 0001), and no
// trigger input reaches a step today. So the definition carries the
// Dockerfile's text in the step that emits it, and this test is what stops
// that copy drifting from the file it claims to be.
func TestCIPipelineBuildsTheCheckedInDockerfile(t *testing.T) {
	pipeline := loadPipeline(t, "ci/pipeline.yaml")
	want, err := os.ReadFile("ci/Dockerfile")
	require.NoError(t, err)

	var source *dholev1.Step
	for _, step := range pipeline.GetSteps() {
		if step.GetId() == "source" {
			source = step
		}
	}
	require.NotNil(t, source, "the CI pipeline has a step that emits the Dockerfile")

	ref := source.GetPluginRef()
	require.True(t, strings.HasPrefix(ref, scheduler.CommandScheme))
	var spec struct {
		Env map[string]string `json:"env"`
	}
	require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(ref, scheduler.CommandScheme)), &spec))
	require.Equal(t, string(want), spec.Env["DOCKERFILE"],
		"the source step emits text that is no longer the checked-in Dockerfile")
}

// ---------------------------------------------------------------------------
// Infrastructure: what these tests need, and how they say what is missing.
// ---------------------------------------------------------------------------

// kubeconfigOrSkip is the cluster the Kubernetes executor runs steps in.
//
// The plan asks for a real containerd engine here. There is none on this
// machine — Task 36's containerd backend is not built, and the README says so
// — so the acceptance pipelines run against Kubernetes, which passes the
// identical shared executor contract against a real cluster. It is a
// substitution, recorded rather than hidden.
func kubeconfigOrSkip(t *testing.T) string {
	t.Helper()
	path := os.Getenv("DHOLE_TEST_KUBECONFIG")
	if path == "" {
		t.Skip("DHOLE_TEST_KUBECONFIG is unset: the acceptance pipelines need a live cluster " +
			"to run steps in, because there is no container runtime on this machine")
	}
	return path
}

// ownNamespace creates a namespace this test owns alone and deletes it
// afterwards. It is never a namespace anything else uses: these runs create
// and destroy pods, and a shared namespace would make one test's cleanup
// another test's outage.
func ownNamespace(ctx context.Context, t *testing.T, kubeconfig string) string {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(t, err)
	client, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	name := fmt.Sprintf("dhole-acceptance-%d", time.Now().UnixNano())
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := client.CoreV1().Namespaces().Delete(
			cleanup, name, *metav1.NewDeleteOptions(0)); err != nil && !apierrors.IsNotFound(err) {
			t.Logf("deleting namespace %s: %v", name, err)
		}
	})
	return name
}

// startPlane brings up a single-binary control plane over a caller-owned
// directory, so several runs share one store and therefore one cache.
func startPlane(
	ctx context.Context, t *testing.T, dir string, exec dholeexecutor.Executor,
) *server.Server {
	t.Helper()
	srv := newPlane(t, dir, exec)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { stopPlane(t, srv) })
	return srv
}

// newPlane builds, but does not start, a control plane over a SQLite store in
// its own directory.
func newPlane(t *testing.T, dir string, exec dholeexecutor.Executor) *server.Server {
	t.Helper()
	return newPlaneOn(t, dir, filepath.Join(dir, "dhole.db"), exec)
}

// newPlaneOn builds a control plane over a named store. It is separate from
// startPlane because the automation pipeline stops one plane and starts a
// second over the same directory and the same database, which is what makes
// its restart a restart rather than a fresh system.
func newPlaneOn(t *testing.T, dir, storeDSN string, exec dholeexecutor.Executor) *server.Server {
	t.Helper()
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other and beside a
		// developer's own `dhole serve`.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: storeDSN,
		BlobRoot: filepath.Join(dir, "state"),
		// Nil is meaningful: it is the host-process backend, which honestly
		// reports it has no environment identity and so makes every step of
		// that plane uncacheable.
		Executor: exec,
	})
	require.NoError(t, err)
	return srv
}

func stopPlane(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	require.NoError(t, srv.Stop(ctx))
}

// ---------------------------------------------------------------------------
// Reading a run: the log is the only state, so everything is asked of it.
// ---------------------------------------------------------------------------

func loadPipeline(t *testing.T, path string) *dholev1.Pipeline {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	p, err := mirror.FromYAML(raw)
	require.NoError(t, err)
	return p
}

func awaitRunCompleted(
	ctx context.Context, t *testing.T, srv *server.Server, runID string,
) []runstore.Event {
	t.Helper()
	return awaitRunEnd(ctx, t, srv, runID, runstore.RunCompleted)
}

// awaitRunEnd polls the run log until the wanted terminal event appears, and
// fails with the whole log when it does not. A stuck run is the failure mode
// this design fears most, so the message says exactly where it stopped.
func awaitRunEnd(
	ctx context.Context, t *testing.T, srv *server.Server, runID string, want runstore.EventType,
) []runstore.Event {
	t.Helper()
	deadline := time.Now().Add(10 * time.Minute)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		for _, e := range events {
			if e.Type == want {
				return events
			}
			if e.Type == scheduler.RunFailed && want != scheduler.RunFailed {
				t.Fatalf("run %s failed while waiting for %s; log: %s", runID, want, describe(events))
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached %s; log so far: %s", runID, want, describe(events))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s never reached %s before the deadline; log so far: %s",
				runID, want, describe(events))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func describe(events []runstore.Event) string {
	out := ""
	for _, e := range events {
		out += "\n  " + string(e.Type) + " " + e.StepID + " " + string(e.Payload)
	}
	if out == "" {
		return "(empty)"
	}
	return out
}

func dispatchedPayload(t *testing.T, events []runstore.Event, stepID string) scheduler.Dispatched {
	t.Helper()
	for _, e := range events {
		if e.Type != runstore.StepDispatched || e.StepID != stepID {
			continue
		}
		payload, err := scheduler.UnmarshalDispatched(e.Payload)
		require.NoError(t, err)
		return payload
	}
	t.Fatalf("step %q has no %s event; log: %s", stepID, runstore.StepDispatched, describe(events))
	return scheduler.Dispatched{}
}

// outputBytes reads what a step actually produced, out of the
// content-addressed store, by the digest its terminal status reported.
func outputBytes(
	ctx context.Context, t *testing.T, srv *server.Server,
	events []runstore.Event, stepID, port string,
) []byte {
	t.Helper()
	for _, e := range events {
		if e.StepID != stepID || e.Type != runstore.StepSucceeded {
			continue
		}
		status := &dholev1.JobStatus{}
		require.NoError(t, proto.Unmarshal(e.Payload, status))
		for _, out := range status.GetOutputs() {
			if out.GetPort() != port {
				continue
			}
			rc, err := srv.CAS().Get(ctx, tenantID, out.GetDigest())
			require.NoError(t, err)
			defer func() { _ = rc.Close() }()
			data, err := io.ReadAll(rc)
			require.NoError(t, err)
			return data
		}
	}
	t.Fatalf("step %q reported no output on port %q; log: %s", stepID, port, describe(events))
	return nil
}

// dispatchWatcher records every JobDispatch that reaches the bus. It is a
// plain core-NATS observer, so watching costs the work queue nothing: it
// cannot take a dispatch from an engine.
type dispatchWatcher struct {
	mu   sync.Mutex
	seen []*dholev1.JobDispatch
}

func watchDispatches(ctx context.Context, t *testing.T, srv *server.Server) *dispatchWatcher {
	t.Helper()
	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)

	w := &dispatchWatcher{}
	stop, err := observer.SubscribeEphemeral(ctx, "job.dispatch.>", func(data []byte) {
		d := &dholev1.JobDispatch{}
		if err := proto.Unmarshal(data, d); err != nil {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		w.seen = append(w.seen, d)
	})
	require.NoError(t, err)
	t.Cleanup(stop)
	return w
}

func (w *dispatchWatcher) stepsDispatchedFor(runID string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var ids []string
	for _, d := range w.seen {
		if d.GetRunId() == runID {
			ids = append(ids, d.GetStepId())
		}
	}
	return ids
}

// automationWait is how long the automation pipeline's durable wait lasts.
//
// The plan asks for five seconds. It is longer here for one reason: the point
// of the wait is that it OUTLIVES a control plane, and stopping one plane and
// starting another over the same database takes longer than five seconds on
// this machine. A wait that had already elapsed before the second plane
// existed would still pass — the timer would fire late, which is correct
// behaviour — but it would no longer be evidence that a plane inherited a
// wait that was still outstanding.
const automationWait = 30 * time.Second

// TestAcceptanceAutomationTriggersAndWait is the automation profile: the same
// definition started by a cron schedule and by an HTTP call, each holding a
// durable wait across a deliberate control-plane restart, with one step run by
// the process engine and one by Kubernetes.
//
// The restart is the whole test. ADR 0003 says a run is a replayable state
// machine over a persisted log and not a goroutine; the only way to show that
// is to end the process that started the run and require a different one to
// finish it. So the first plane is stopped while both runs are mid-wait, and
// nothing that ends those waits or runs their last step exists until the
// second plane is up.
func TestAcceptanceAutomationTriggersAndWait(t *testing.T) {
	kubeconfig := kubeconfigOrSkip(t)
	dsn := postgresOrSkip(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	pipeline := loadPipeline(t, "automation/pipeline.yaml")

	// Its own database. The outbox claim is deployment-wide, so a second
	// control plane on a shared database claims and publishes rows it did not
	// enqueue — see internal/server's own note on this.
	dsn = isolatedDatabase(t, dsn)

	// The state directory is the caller's, because both planes are the SAME
	// deployment: same database, same blob root, same derived deployment id.
	dir := t.TempDir()

	first := newPlaneOn(t, dir, dsn, nil)
	require.NoError(t, first.Start(ctx))
	firstStopped := false
	defer func() {
		if !firstStopped {
			stopPlane(t, first)
		}
	}()

	// A second view of the same database, for the parts of the system the
	// control plane does not host: `dhole serve` wires neither triggers nor
	// the durable-timer poll, so this test supplies them. That is a finding,
	// not a convenience — see the report accompanying this task.
	store, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	timers := wait.NewTimers(store)

	firings := newFirings()

	// One sink per trigger, each firing exactly once: the cron expression is
	// every second and this test is about the first occurrence, not about how
	// many follow.
	sinkFor := func(kind string) trigger.Sink {
		var once sync.Once
		var failure error
		return trigger.SinkFunc(func(
			ctx context.Context, tenant, _ string, inputs map[string]*structpb.Value,
		) error {
			once.Do(func() {
				runID, err := first.Submit(ctx, tenant, pipeline)
				if err != nil {
					failure = err
					return
				}
				// The wait is armed the moment the run exists, before the
				// first step can finish and make the gated step ready.
				// Nothing in the plane does this: no step type maps to
				// internal/wait, so this test supplies what `dhole serve`
				// does not.
				if err := timers.Schedule(ctx, tenant, runID, "hold",
					time.Now().Add(automationWait)); err != nil {
					failure = err
					return
				}
				firings.record(kind, runID, inputs)
			})
			return failure
		})
	}

	scheduleRun := fireFromSchedule(ctx, t, pipeline, store, sinkFor(schedule.Kind), firings)
	httpRun := fireFromHTTP(ctx, t, pipeline, sinkFor(httptrigger.Kind), firings)
	require.NotEqual(t, scheduleRun, httpRun, "two triggers, two runs")

	// Both runs reach the gate under the FIRST plane.
	for _, runID := range []string{scheduleRun, httpRun} {
		awaitStep(ctx, t, first, runID, "host", runstore.StepSucceeded)
		awaitStep(ctx, t, first, runID, "hold", scheduler.StepAwaitingTimer)
	}

	// The restart. Nothing that could end a wait exists across it.
	restartedAt := time.Now().UTC()
	stopPlane(t, first)
	firstStopped = true

	// And the first plane is really gone. Without this, every assertion
	// below about "after the restart" would hold just as well if no restart
	// had happened at all — which is the mutation this exists to catch.
	require.Empty(t, first.APIAddr(), "the stopped plane is still serving its contract")
	_, err = first.Submit(ctx, tenantID, pipeline)
	require.Error(t, err, "the stopped plane still accepts runs")

	second := newPlaneOn(t, dir, dsn, nil)
	require.NoError(t, second.Start(ctx))
	t.Cleanup(func() { stopPlane(t, second) })

	// The timer poll starts here and only here, so a fired wait is proof the
	// SECOND plane ended it.
	runTimerPoll(ctx, t, timers)

	// And the Kubernetes engine, which is what the last step can run on and
	// the process engine cannot: `cluster` declares CAPABILITY_NETWORK, and
	// the host-process backend advertises no capability at all, so the
	// dispatch subject for that step is one only this engine subscribes to.
	stopEngine := startKubernetesEngine(ctx, t, second, kubeconfig)
	t.Cleanup(stopEngine)

	for _, runID := range []string{scheduleRun, httpRun} {
		events := awaitRunCompleted(ctx, t, second, runID)

		// The gate HELD: a step that was dispatched anyway did not wait for
		// anything, whatever its log says. This is the assertion that caught
		// the arming race described in automation/pipeline.yaml.
		for _, e := range events {
			require.False(t, e.StepID == "hold" && e.Type == runstore.StepDispatched,
				"run %s: the gated step was dispatched despite its timer; log: %s",
				runID, describe(events))
		}

		// The wait was outstanding when the first plane died, and was ended
		// after the second one started.
		firedEvent := eventOf(t, events, "hold", wait.StepTimerFired)
		var record wait.Fired
		require.NoError(t, json.Unmarshal(firedEvent.Payload, &record))
		require.True(t, record.FiredAt.After(restartedAt),
			"run %s: the wait was ended at %s, before the plane was restarted at %s — "+
				"it did not survive anything", runID, record.FiredAt, restartedAt)

		// The step before the wait ran under the first plane, and the step
		// after it under the second: the run genuinely spans the restart.
		require.True(t, eventOf(t, events, "host", runstore.StepSucceeded).At.Before(restartedAt),
			"run %s: the first step must have finished before the restart", runID)
		require.True(t, eventOf(t, events, "cluster", runstore.StepDispatched).At.After(restartedAt),
			"run %s: the last step must have been dispatched after the restart", runID)

		// Two engines, two environments — asserted on what the steps SAW
		// rather than on which engine we believe took them.
		require.Equal(t, hostUname(t), string(outputBytes(ctx, t, second, events, "host", "probe")),
			"run %s: the first step ran somewhere other than this host's process engine", runID)
		require.Equal(t, "Linux\n", string(outputBytes(ctx, t, second, events, "cluster", "probe")),
			"run %s: the last step ran somewhere other than a Linux pod", runID)
	}

	// The triggers really did bind their pipeline's declared input, even
	// though nothing carries it into the run — see the finding.
	require.NotEmpty(t, firings.inputOf(schedule.Kind, "event"),
		"the schedule bound the pipeline's declared input to an occurrence")
	require.Equal(t, "refs/heads/main", firings.inputOf(httptrigger.Kind, "event"),
		"the webhook bound the pipeline's declared input to a field of the body")
}

// firings is what each trigger produced: the run it started and the inputs it
// bound. The inputs are kept because nothing else keeps them — there is no
// path from a trigger's typed inputs into a run today (ADR 0007's other half),
// so this is the only place the binding is observable.
type firings struct {
	mu     sync.Mutex
	runs   map[string]string
	inputs map[string]map[string]*structpb.Value
}

func newFirings() *firings {
	return &firings{
		runs:   map[string]string{},
		inputs: map[string]map[string]*structpb.Value{},
	}
}

func (f *firings) record(kind, runID string, inputs map[string]*structpb.Value) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[kind] = runID
	f.inputs[kind] = inputs
}

func (f *firings) runOf(kind string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs[kind]
}

func (f *firings) inputOf(kind, name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return trigger.UntaintedValue(f.inputs[kind][name]).GetStringValue()
}

// fireFromSchedule runs a real cron trigger — its own poll loop over its own
// row in the run store — until it has fired once, and returns the run.
func fireFromSchedule(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline,
	store runstore.Store, sink trigger.Sink, firings *firings,
) string {
	t.Helper()
	cron, err := schedule.New(schedule.Config{
		ID:       "acceptance-every-second",
		TenantID: tenantID,
		// Six fields: seconds are optional in the parser, and a test that
		// waited for a minute boundary would be a test nobody runs.
		Expression: "* * * * * *",
		Binding: trigger.Binding{
			PipelineID:   pipeline.GetId(),
			InputMapping: map[string]string{"event": schedule.FieldScheduledFor},
		},
		Pipeline:     pipeline,
		Store:        store,
		PollInterval: 200 * time.Millisecond,
		OnError:      func(err error) { t.Logf("schedule trigger: %v", err) },
	})
	require.NoError(t, err)

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cron.Start(runCtx, sink)
	}()

	runID := awaitFiring(ctx, t, firings, schedule.Kind)
	stop()
	<-done
	return runID
}

// fireFromHTTP posts a body to a real HTTP trigger, over a real socket, and
// returns the run it started.
func fireFromHTTP(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline,
	sink trigger.Sink, firings *firings,
) string {
	t.Helper()
	hook, err := httptrigger.New(httptrigger.Config{
		ID:       "acceptance-webhook",
		TenantID: tenantID,
		Binding: trigger.Binding{
			PipelineID:   pipeline.GetId(),
			InputMapping: map[string]string{"event": "ref"},
		},
		Pipeline: pipeline,
		OnError:  func(err error) { t.Logf("http trigger: %v", err) },
	})
	require.NoError(t, err)

	endpoint := httptest.NewServer(hook.Handler(sink))
	t.Cleanup(endpoint.Close)

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, endpoint.URL,
		strings.NewReader(`{"ref":"refs/heads/main"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := endpoint.Client().Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, nethttp.StatusAccepted, resp.StatusCode, "the webhook was refused: %s", body)

	return awaitFiring(ctx, t, firings, httptrigger.Kind)
}

func awaitFiring(ctx context.Context, t *testing.T, firings *firings, kind string) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		if runID := firings.runOf(kind); runID != "" {
			return runID
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %s trigger never fired", kind)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("the %s trigger never fired before the deadline", kind)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// runTimerPoll starts the durable-timer poll. `dhole serve` does not run one,
// so this is the test standing in for a control plane's clock.
//
// The resumer does nothing on purpose. wait.Runner wants something to advance
// the run it just woke, and the plane exposes no Advance — but it re-advances
// every OPEN run on its own tick out of the store's index, which is exactly
// the mechanism ADR 0003 relies on. So the timer's job here is to end the
// wait durably; the plane's own loop notices.
func runTimerPoll(ctx context.Context, t *testing.T, timers *wait.Timers) {
	t.Helper()
	runner := wait.NewRunner(timers, resumeNothing{},
		wait.WithPollInterval(200*time.Millisecond),
		wait.WithErrorHandler(func(err error) { t.Logf("timer poll: %v", err) }))

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		<-done
	})
}

type resumeNothing struct{}

func (resumeNothing) Advance(context.Context, string, string) error { return nil }

// startKubernetesEngine runs a REAL engine agent — the same engine.Agent the
// single binary hosts — against a Kubernetes executor, reaching the plane only
// over the bus.
func startKubernetesEngine(
	ctx context.Context, t *testing.T, srv *server.Server, kubeconfig string,
) func() {
	t.Helper()
	exec, err := k8sexec.New(k8sexec.Config{
		Kubeconfig: kubeconfig,
		Namespace:  ownNamespace(ctx, t, kubeconfig),
		PodTemplate: &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "step", Image: sandboxImage}},
		},
	})
	require.NoError(t, err)

	conn, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)

	agent, err := engine.New(engine.Config{
		EngineID: fmt.Sprintf("engine-kubernetes-%d", time.Now().UnixNano()),
		Tier:     server.DefaultTier,
		Bus:      conn,
		Executor: exec,
		Blobs:    blobstore.NewFilesystem(t.TempDir()),
		CAS:      srv.CAS(),
		Slots:    2,
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- agent.Run(runCtx) }()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("kubernetes engine: %v", err)
				}
			case <-time.After(90 * time.Second):
				t.Error("the kubernetes engine did not stop")
			}
			conn.Close()
		})
	}
}

// hostUname is what a step running on THIS machine's process engine prints.
// It is read from the host rather than written down so the assertion means
// "the same machine the test is on", on whatever that machine is.
func hostUname(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("uname", "-s").Output()
	require.NoError(t, err)
	return string(out)
}

// awaitStep polls a run's log until one step has the wanted event.
func awaitStep(
	ctx context.Context, t *testing.T, srv *server.Server,
	runID, stepID string, want runstore.EventType,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		for _, e := range events {
			if e.StepID == stepID && e.Type == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s: step %q never reached %s; log: %s",
				runID, stepID, want, describe(events))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s: step %q never reached %s before the deadline; log: %s",
				runID, stepID, want, describe(events))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func eventOf(
	t *testing.T, events []runstore.Event, stepID string, want runstore.EventType,
) runstore.Event {
	t.Helper()
	for _, e := range events {
		if e.StepID == stepID && e.Type == want {
			return e
		}
	}
	t.Fatalf("step %q has no %s event; log: %s", stepID, want, describe(events))
	return runstore.Event{}
}

// postgresOrSkip is the database the automation and agent pipelines need.
//
// They need one that several processes can hold at once: the triggers, the
// timer table and two successive control planes all read and write the same
// run log, and SQLite's single-writer lock makes that a different test.
func postgresOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN is unset: this pipeline needs a live Postgres, " +
			"because two control planes and a trigger poll share one run log")
	}
	return dsn
}

// isolatedDatabase creates a database of its own for one test and drops it
// afterwards. The outbox claim is deployment-wide, so a plane on a shared
// database publishes rows another deployment enqueued.
func isolatedDatabase(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("dhole_acceptance_%d", time.Now().UnixNano())

	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	_, err = admin.Exec("CREATE DATABASE " + name)
	require.NoError(t, err)

	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	parsed.Path = "/" + name
	t.Cleanup(func() {
		cleanup, err := sql.Open("pgx", dsn)
		if err != nil {
			return
		}
		defer func() { _ = cleanup.Close() }()
		_, _ = cleanup.Exec("DROP DATABASE IF EXISTS " + name + " WITH (FORCE)")
	})
	return parsed.String()
}

// The agent pipeline's two out-of-band gates. They are due far enough apart
// that the steps complete in the order the graph declares, and far enough
// after the run starts that the harness has armed them before either step
// could be reached — see the arming race recorded in
// automation/pipeline.yaml.
const (
	classifyDue = 12 * time.Second
	refineDue   = 20 * time.Second
)

// TestAcceptanceAgentLoopAndApproval is the agent profile: a
// schema-validated model answer, a loop that cannot run away, a human gate,
// and a bill.
//
// It is the pipeline that most exposes what is NOT wired. Nothing in the
// control plane maps a `builtin:` plugin reference to a step type, so no
// dispatcher runs internal/steps/llm, internal/steps/loop or
// internal/steps/approval — they are libraries with tests and no caller. This
// test therefore does what that dispatcher would: it holds those steps behind
// the gates the scheduler already honours, does their work against the same
// run store and the same tenant, and lets the run continue. Everything the
// plane CAN do — the contract, the revision, the approval, the step behind
// the gate — goes through the plane.
func TestAcceptanceAgentLoopAndApproval(t *testing.T) {
	dsn := isolatedDatabase(t, postgresOrSkip(t))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	pipeline := loadPipeline(t, "agent/pipeline.yaml")
	dir := t.TempDir()
	srv := newPlaneOn(t, dir, dsn, nil)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { stopPlane(t, srv) })

	store, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db, err := runstore.OpenPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// A second principal, because a revision's author may not approve it.
	principals := identity.NewSQLStoreWithDialect(db, runstore.DialectPostgres)
	local := identity.NewLocal(principals)
	const approver = "release-manager"
	// Registered as a PRINCIPAL and then given a token, because those are two
	// different tables and both are needed — a finding, not a formality. A
	// token issued to a subject with no `principals` row authenticates every
	// API call perfectly and is then refused by approval.Decide with
	// "is not a principal of tenant", because the identity an API credential
	// resolves to and the identity an approval is verified against are looked
	// up in different places. Observed the first time this test ran.
	require.NoError(t, local.CreateUser(ctx, tenantID, approver, "acceptance-secret"))
	approverToken, err := local.IssueToken(ctx, identity.Principal{
		TenantID: tenantID, Subject: approver, Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	runID := startThroughTheContract(ctx, t, srv, pipeline, approverToken, approver)

	// The gates, armed before anything behind the first step is reachable.
	timers := wait.NewTimers(store)
	require.NoError(t, timers.Schedule(ctx, tenantID, runID, "classify",
		time.Now().Add(classifyDue)))
	require.NoError(t, timers.Schedule(ctx, tenantID, runID, "refine",
		time.Now().Add(refineDue)))

	gate, err := approval.New(approval.Config{
		Store:     store,
		TenantID:  tenantID,
		Approvers: principals,
		// The plane re-advances every open run out of the store's index on
		// its own tick, and exposes no Advance of its own.
		Resume: resumeNothing{},
	})
	require.NoError(t, err)
	require.NoError(t, gate.Request(ctx, runID, "approve",
		stepOf(t, pipeline, "approve").GetConfig()["prompt"]))

	runTimerPoll(ctx, t, timers)

	// --- the LLM step: an answer that must satisfy the port's own schema ---
	recorder, err := llm.NewRecorder(db, runstore.DialectPostgres, 24*time.Hour)
	require.NoError(t, err)
	classify := stepOf(t, pipeline, "classify")
	schema := classify.GetOutputs()[0].GetType().GetStructured().GetSchema()
	require.NotEmpty(t, schema, "the llm step's output port declares the schema its answer must satisfy")

	model := &scriptedModel{id: classify.GetConfig()["model"]}
	step, err := llm.New(llm.Config{
		Provider:     classify.GetConfig()["provider"],
		Model:        classify.GetConfig()["model"],
		OutputSchema: []byte(schema),
		MaxTokens:    atoi(t, classify.GetConfig()["max_tokens"]),
	}, llm.Options{
		Model: model, Store: store, Calls: recorder, TenantID: tenantID,
	})
	require.NoError(t, err)

	model.answer(`{"severity":"high","summary":"the release changes a public interface"}`, 120, 40)
	answer, err := step.Run(ctx, runID, "classify", classify.GetConfig()["prompt"])
	require.NoError(t, err)
	requireSatisfiesSchema(t, schema, answer)

	// And the schema is a refusal, not a decoration: an answer of the wrong
	// shape is rejected however well-formed its JSON is.
	//
	// It runs under a run id of its OWN. The llm step halts the run it is
	// given when it gives up, so asking it to refuse an answer inside the
	// acceptance run would fail that run — which is correct behaviour, and
	// was observed the first time this was written.
	model.answer(`{"severity":"catastrophic","summary":"nope"}`, 10, 10)
	_, err = step.Run(ctx, runID+"-off-schema", "classify",
		classify.GetConfig()["prompt"])
	require.ErrorIs(t, err, llm.ErrObjectInvalid)

	// --- the bounded loop: it stops at three, whatever the body says ---
	refine := stepOf(t, pipeline, "refine")
	ceiling := atoi(t, refine.GetConfig()["max_iterations"])
	require.Equal(t, 3, ceiling, "the acceptance pipeline's loop is capped at three")

	var passes atomic.Int64
	bounded, err := loop.New(loop.Node{
		// A loop's body is a Pipeline. The definition format has no syntax
		// for a nested one, so the body is built here from what the step
		// declares — another gap, recorded rather than hidden.
		Subgraph:      loopBody(refine),
		MaxIterations: ceiling,
		ExitCondition: refine.GetConfig()["exit_condition"],
	}, loop.Options{
		Store:    store,
		TenantID: tenantID,
		Body: func(ctx context.Context, it loop.Iteration) (map[string]any, error) {
			passes.Add(1)
			// Real work per pass: the same LLM step, so the loop's cost is
			// on the bill too.
			model.answer(`{"severity":"medium","summary":"pass"}`, 60, 20)
			if _, err := step.Run(ctx, runID, it.StepID, "refine"); err != nil {
				return nil, err
			}
			// Never satisfied: only the ceiling can stop this loop, which is
			// the property under test.
			return map[string]any{"done": false}, nil
		},
	})
	require.NoError(t, err)

	result, err := bounded.Run(ctx, runID, "refine", map[string]any{"done": false})
	require.ErrorIs(t, err, loop.ErrIterationCeiling,
		"a loop whose condition never holds must stop at its ceiling")
	require.Equal(t, 3, result.Iterations)
	require.False(t, result.Exited)
	require.EqualValues(t, 3, passes.Load(), "the body ran exactly three times")

	// --- the approval gate, decided by the principal the API authenticated --
	awaitStep(ctx, t, srv, runID, "refine", runstore.StepSucceeded)
	decidedAt := time.Now().UTC()
	require.NoError(t, gate.Decide(ctx, runID, "approve", approver, true))

	events := awaitRunCompleted(ctx, t, srv, runID)

	// The gate HELD: a step nobody dispatched is a step that waited. Without
	// this, a gate that failed to hold would only be caught indirectly, by
	// the engine refusing a `builtin:` reference it cannot run.
	for _, e := range events {
		require.False(t, e.StepID == "approve" && e.Type == runstore.StepDispatched,
			"the approval gate was dispatched to an engine; log: %s", describe(events))
	}

	// The step behind the gate did not run until the gate opened.
	require.True(t, eventOf(t, events, "apply", runstore.StepDispatched).At.After(decidedAt),
		"the step behind the approval was dispatched before anyone approved; log: %s",
		describe(events))
	var decision approval.Decision
	require.NoError(t, json.Unmarshal(
		eventOf(t, events, "approve", approval.StepApprovalDecided).Payload, &decision))
	require.Equal(t, approver, decision.Approver)
	require.True(t, decision.Approved)

	// --- the bill ---
	calls, err := recorder.Calls(ctx, tenantID, runID)
	require.NoError(t, err)
	require.NotEmpty(t, calls, "every model call is recorded, including the ones that failed")
	tokens := 0
	for _, c := range calls {
		tokens += c.PromptTokens + c.CompletionTokens
	}
	require.Positive(t, tokens, "the run spent tokens and the record has to say how many")
	t.Logf("agent pipeline: %d model calls, %d tokens", len(calls), tokens)
}

// startThroughTheContract creates, approves and starts the pipeline over the
// served API — the one contract the GUI, the CLI and an agent all use
// (ADR 0013). Nothing here reaches into the plane's internals to start a run,
// because that is exactly the back door the ADR exists to refuse.
//
// The approval gate itself cannot be decided this way: PipelineService has no
// RPC for a run's approval, so `approve` below is decided through
// internal/steps/approval, by the principal this function authenticated. That
// gap is a finding, recorded with this task.
func startThroughTheContract(
	ctx context.Context, t *testing.T, srv *server.Server,
	pipeline *dholev1.Pipeline, approverToken, approver string,
) string {
	t.Helper()
	addr := srv.APIAddr()
	require.NotEmpty(t, addr, "the plane is not serving its contract")
	client := dholev1connect.NewPipelineServiceClient(
		&nethttp.Client{Timeout: 120 * time.Second}, "http://"+addr)

	create := connect.NewRequest(&dholev1.CreatePipelineRequest{
		PipelineId: pipeline.GetId(),
		Pipeline:   pipeline,
	})
	create.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	created, err := client.CreatePipeline(ctx, create)
	require.NoError(t, err)
	revision := created.Msg.GetRevision().GetId()
	require.Equal(t, "draft", created.Msg.GetRevision().GetState(),
		"a saved revision is a draft until somebody approves it")

	// The author may not approve their own definition.
	selfApprove := connect.NewRequest(&dholev1.ApproveRevisionRequest{RevisionId: revision})
	selfApprove.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	_, err = client.ApproveRevision(ctx, selfApprove)
	require.Error(t, err, "the principal that saved a revision must not be the one that approves it")

	approve := connect.NewRequest(&dholev1.ApproveRevisionRequest{RevisionId: revision})
	approve.Header().Set("Authorization", "Bearer "+approverToken)
	approved, err := client.ApproveRevision(ctx, approve)
	require.NoError(t, err)
	require.Equal(t, approver, approved.Msg.GetRevision().GetApprover())

	start := connect.NewRequest(&dholev1.StartRunRequest{
		PipelineId: pipeline.GetId(),
		RevisionId: revision,
	})
	start.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	started, err := client.StartRun(ctx, start)
	require.NoError(t, err)
	require.NotEmpty(t, started.Msg.GetRunId())
	return started.Msg.GetRunId()
}

// loopBody is the pipeline one pass of the loop runs. It is built from the
// loop step's own declaration because the definition format has no syntax for
// a nested graph.
func loopBody(step *dholev1.Step) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     step.GetId() + "-body",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "pass",
			Name:        step.GetName(),
			PluginRef:   step.GetPluginRef(),
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
		}},
	}
}

func stepOf(t *testing.T, p *dholev1.Pipeline, id string) *dholev1.Step {
	t.Helper()
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	t.Fatalf("pipeline %q has no step %q", p.GetId(), id)
	return nil
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	require.NoError(t, err, "the step's config carries %q where a number belongs", s)
	return n
}

// requireSatisfiesSchema checks the model's answer against the port's schema
// with a compiler this test owns, so the assertion does not depend on the
// implementation it is checking.
func requireSatisfiesSchema(t *testing.T, schema string, answer json.RawMessage) {
	t.Helper()
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(schema))
	require.NoError(t, err)
	compiler := jsonschema.NewCompiler()
	require.NoError(t, compiler.AddResource("dhole:acceptance", doc))
	compiled, err := compiler.Compile("dhole:acceptance")
	require.NoError(t, err)

	instance, err := jsonschema.UnmarshalJSON(strings.NewReader(string(answer)))
	require.NoError(t, err)
	require.NoError(t, compiled.Validate(instance),
		"the model's answer does not satisfy the port's declared schema")
}

// scriptedModel is the language model the agent pipeline calls. It NEVER
// makes a network call: no acceptance test may spend somebody's money or
// depend on a provider being up, and the point of the step is the schema, the
// retry and the record — none of which need a real model to exercise.
//
// Its response names a RESOLVED model in the raw body, the way every real
// provider does, because that is what the step fingerprints.
type scriptedModel struct {
	id string

	mu   sync.Mutex
	next *provider.Response
}

func (m *scriptedModel) answer(text string, in, out int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next = &provider.Response{
		Content:      []provider.ContentPart{provider.TextPart{Text: text}},
		FinishReason: provider.FinishStop,
		Usage:        provider.Usage{InputTokens: in, OutputTokens: out, TotalTokens: in + out},
		Raw:          json.RawMessage(fmt.Sprintf(`{"model":%q}`, m.id+"-20260101")),
	}
}

func (m *scriptedModel) ModelID() string      { return m.id }
func (m *scriptedModel) ProviderName() string { return "stub" }

// Capabilities advertises native JSON, so the step asks for an object through
// the response format rather than through a forced tool call. Either path is
// real; this one is what a stub can answer without pretending to be a tool
// runtime.
func (m *scriptedModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: true}
}

func (m *scriptedModel) Generate(context.Context, provider.Call) (*provider.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next == nil {
		return nil, errors.New("scripted model: nothing scripted for this call")
	}
	return m.next, nil
}

func (m *scriptedModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, errors.New("the acceptance pipeline does not stream")
}
