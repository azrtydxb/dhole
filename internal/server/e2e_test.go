// Package server_test is the first place the architecture is asked to work
// rather than to compile. Every earlier task built one organ; this file runs a
// pipeline through all of them at once.
package server_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	// The pgx database/sql driver, registered as "pgx", so this test can
	// create and drop a database of its own.
	_ "github.com/jackc/pgx/v5/stdlib"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor/process"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/mirror"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/wire"
)

// tenantID scopes every record and every subject in this file. There is no
// unscoped write, even with a single tenant.
const tenantID = "default"

// pipelineFile is the definition under test, loaded from the repository rather
// than built in Go: what runs end to end has to be the document a user would
// actually write.
const pipelineFile = "../../testdata/pipelines/two-step.yaml"

// TestSingleBinaryRunsTwoStepPipeline is the checkpoint the first seventeen
// tasks exist for: one process, an embedded bus, a real engine, and a pipeline
// whose second step can only succeed if the first step's bytes actually
// travelled through the content-addressed store to reach it.
func TestSingleBinaryRunsTwoStepPipeline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	require.NotEmpty(t, runID)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "a")
	requireStepSucceeded(t, events, "b")

	aBytes := outputBytes(ctx, t, srv, events, "a", "out")
	bBytes := outputBytes(ctx, t, srv, events, "b", "out")
	require.Equal(t, "hello from a", string(aBytes))
	require.Contains(t, string(bBytes), string(aBytes),
		"b's output must contain a's bytes, or the edge carried nothing")
	require.NotEqual(t, string(aBytes), string(bBytes),
		"b must have done something of its own, or the test proves only that a ran")
}

// startEmbedded brings up a single-binary server and guarantees it is stopped.
func startEmbedded(ctx context.Context, t *testing.T) *server.Server {
	t.Helper()
	dir := t.TempDir()
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

// awaitRunCompleted polls the run log until RUN_COMPLETED appears. The log is
// the run's only position, so this asks the store rather than any in-memory
// notion of progress.
func awaitRunCompleted(ctx context.Context, t *testing.T, srv *server.Server, runID string) []runstore.Event {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		for _, e := range events {
			if e.Type == runstore.RunCompleted {
				return events
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not complete; log so far: %s", runID, describe(events))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s did not complete before the deadline; log so far: %s", runID, describe(events))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// describe renders a run log for a failure message. A stuck run is the failure
// mode this whole design fears most, so the test says exactly where it stopped.
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

func requireStepSucceeded(t *testing.T, events []runstore.Event, stepID string) {
	t.Helper()
	for _, e := range events {
		if e.StepID == stepID && e.Type == runstore.StepSucceeded {
			return
		}
	}
	t.Fatalf("step %q did not succeed; log: %s", stepID, describe(events))
}

// outputBytes reads what a step actually produced, out of the content-addressed
// store, by the digest its terminal status reported.
func outputBytes(ctx context.Context, t *testing.T, srv *server.Server, events []runstore.Event, stepID, port string) []byte {
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

// TestEmbeddedEngineUsesLoopbackBusNotDirectCall is the reason embedded mode
// is worth having at all.
//
// If the single binary quietly called the engine in-process, the single-binary
// path and the distributed path would be two different architectures wearing
// one name, and every bug found in one would say nothing about the other. The
// proof is that the dispatch is observable ON THE BUS SUBJECT by a third party
// that is not the engine: bytes on a subject cannot be produced by a direct
// function call.
func TestEmbeddedEngineUsesLoopbackBusNotDirectCall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	// A plain observer, subscribed before anything is submitted. It is core
	// NATS, so watching costs the work queue nothing: it cannot steal the
	// dispatch from the engine, and the run below still has to complete.
	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)

	seen := make(chan *dholev1.JobDispatch, 16)
	stop, err := observer.SubscribeEphemeral(ctx, "job.dispatch.>", func(data []byte) {
		d := &dholev1.JobDispatch{}
		if err := proto.Unmarshal(data, d); err != nil {
			return
		}
		select {
		case seen <- d:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)

	dispatched := awaitDispatch(ctx, t, seen, runID, "a")
	require.NotEmpty(t, dispatched.GetFenceToken(),
		"a dispatch on the bus carries the fence of the lease it was issued under")
	require.NotEmpty(t, dispatched.GetCommand(),
		"the command reaches the engine in the dispatch, not from the engine's own knowledge")
	require.Equal(t, tenantID, dispatched.GetTenant().GetId())

	// And the work was really consumed off that queue, not merely broadcast.
	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "b")
}

func awaitDispatch(ctx context.Context, t *testing.T, ch <-chan *dholev1.JobDispatch, runID, stepID string) *dholev1.JobDispatch {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		select {
		case d := <-ch:
			if d.GetRunId() == runID && d.GetStepId() == stepID {
				return d
			}
		case <-deadline:
			t.Fatalf("no JobDispatch for %s/%s appeared on the bus subject", runID, stepID)
		case <-ctx.Done():
			t.Fatalf("no JobDispatch for %s/%s appeared before the deadline", runID, stepID)
		}
	}
}

// TestSingleBinaryAndDistributedParity runs the identical definition through
// the distributed deployment — Postgres, a NATS server in its own OS process,
// MinIO — and requires the same bytes out of it.
//
// This is the claim the single binary makes and the one most easily broken:
// that development on SQLite and an embedded bus is development against the
// same system that runs in production. If the two paths could disagree, every
// bug reproduced on a laptop would have to be reproduced again in a cluster
// before anyone could believe it.
//
// The engine here runs in the test process, but it reaches the plane only over
// the network: a separate nats-server, dialled by TCP. It writes its
// authoritative logs to MinIO, which the test then reads back, so the object
// store is genuinely on the path rather than merely configured.
func TestSingleBinaryAndDistributedParity(t *testing.T) {
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	endpoint := os.Getenv("DHOLE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("DHOLE_TEST_S3_ENDPOINT not set: this test needs a live S3 (MinIO)")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Its own database, not the shared one.
	//
	// The outbox claim is deployment-wide: `SELECT ... WHERE sent_at IS NULL`
	// carries no tenant and no deployment id, so a second control plane on the
	// same database claims and publishes the first one's rows — onto its own
	// bus, where nobody is listening for them. Running against the shared test
	// database made this plane's drainer eat internal/outbox's fixtures and
	// fail that package. Isolating the test is the right thing for the test;
	// the sharp edge itself is reported, not papered over.
	dsn = isolatedDatabase(t, dsn)

	// The single-binary half.
	embedded := startEmbedded(ctx, t)
	embeddedRun, err := embedded.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	embeddedEvents := awaitRunCompleted(ctx, t, embedded, embeddedRun)
	embeddedOut := outputBytes(ctx, t, embedded, embeddedEvents, "b", "out")

	// The distributed half.
	busURL := startExternalNATS(t)
	dir := t.TempDir()
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeDistributed,
		StoreDSN: dsn,
		BusURL:   busURL,
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	blobs := s3Blobs(t, endpoint)
	stopEngine := startExternalEngine(ctx, t, busURL, blobs, srv.CAS())
	t.Cleanup(stopEngine)

	distributedRun, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	distributedEvents := awaitRunCompleted(ctx, t, srv, distributedRun)
	requireStepSucceeded(t, distributedEvents, "a")
	requireStepSucceeded(t, distributedEvents, "b")
	distributedOut := outputBytes(ctx, t, srv, distributedEvents, "b", "out")

	require.Equal(t, string(embeddedOut), string(distributedOut),
		"the same definition must produce the same bytes on both deployments")

	// The authoritative log really went to the object store, not to a
	// filesystem the single-binary case would also have had.
	logKey := logKeyOf(t, distributedEvents, "b")
	require.NotEmpty(t, logKey)
	rc, err := blobs.Read(ctx, tenantID, logKey)
	require.NoError(t, err, "the authoritative log for b must be readable from MinIO")
	require.NoError(t, rc.Close())
}

func logKeyOf(t *testing.T, events []runstore.Event, stepID string) string {
	t.Helper()
	for _, e := range events {
		if e.StepID != stepID || e.Type != runstore.StepSucceeded {
			continue
		}
		status := &dholev1.JobStatus{}
		require.NoError(t, proto.Unmarshal(e.Payload, status))
		return status.GetLogKey()
	}
	return ""
}

func s3Blobs(t *testing.T, endpoint string) blobstore.Store {
	t.Helper()
	cfg := blobstore.S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		Bucket:          fmt.Sprintf("dhole-e2e-%d", time.Now().UnixNano()),
		AccessKeyID:     envOr("DHOLE_TEST_S3_ACCESS_KEY", "dholetest"),
		SecretAccessKey: envOr("DHOLE_TEST_S3_SECRET_KEY", "dholetestsecret"),
	}
	createBucket(t, cfg)
	store, err := blobstore.NewS3(cfg)
	require.NoError(t, err)
	return store
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func createBucket(t *testing.T, cfg blobstore.S3Config) {
	t.Helper()
	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	require.NoError(t, err)
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
	var exists *s3types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &exists) {
		require.NoError(t, err)
	}
}

// startExternalEngine runs an engine agent that reaches the plane only over the
// network. It is the same engine.Agent the single binary embeds, which is the
// point: one implementation, two deployments.
func startExternalEngine(ctx context.Context, t *testing.T, busURL string, blobs blobstore.Store, store cas.Store) func() {
	t.Helper()
	conn, err := bus.Connect(ctx, busURL)
	require.NoError(t, err)

	agent, err := engine.New(engine.Config{
		EngineID: fmt.Sprintf("engine-external-%d", time.Now().UnixNano()),
		Tier:     server.DefaultTier,
		Bus:      conn,
		Executor: process.New(),
		Blobs:    blobs,
		CAS:      store,
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
					t.Errorf("external engine: %v", err)
				}
			case <-time.After(60 * time.Second):
				t.Error("external engine did not stop")
			}
			conn.Close()
		})
	}
}

// startExternalNATS runs a real nats-server as a separate OS process, built
// from the very dependency the embedded server uses, so the only difference
// between the two deployments is where the server lives.
//
// It is a single server, not a cluster: this machine has no clustered NATS, so
// nothing here proves anything about clustering.
func startExternalNATS(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	binary := filepath.Join(dir, "nats-server")
	build := exec.Command("go", "build", "-o", binary, "github.com/nats-io/nats-server/v2")
	build.Stderr = os.Stderr
	require.NoError(t, build.Run(), "building a real nats-server to run out of process")

	port := freePort(t)
	cmd := exec.Command(binary,
		"--addr", "127.0.0.1",
		"--port", strconv.Itoa(port),
		"--jetstream",
		"--store_dir", filepath.Join(dir, "js"),
	)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			require.NoError(t, conn.Close())
			return url
		}
		if time.Now().After(deadline) {
			t.Fatalf("the external nats-server never accepted connections on %s", url)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// TestStopEndsEverythingItStarted holds the other half of the single-binary
// promise: a server that has been stopped is not running.
//
// Without it, every test above would be a test of the process exiting. A
// drainer still publishing after Stop, or an embedded NATS still listening,
// would go unnoticed here and would show up as a second server racing the
// first one for the same outbox rows.
func TestStopEndsEverythingItStarted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	before := dholeGoroutines()

	dir := t.TempDir()
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

	busAddr := strings.TrimPrefix(srv.BusURL(), "nats://")

	// Stop something that has really been working, not something idle: the
	// outbox has drained, the engine has run a step, statuses have been
	// applied.
	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	awaitRunCompleted(ctx, t, srv, runID)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer stopCancel()
	require.NoError(t, srv.Stop(stopCtx))

	// Checked with no grace period on purpose. "Stop returns once they are
	// done" is the claim; a sleep here would turn it into "Stop returns and
	// they finish shortly afterwards", which is a different and much weaker
	// promise.
	require.Empty(t, dholeGoroutines().minus(before),
		"these goroutines outlived Stop")

	conn, err := net.DialTimeout("tcp", busAddr, 2*time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatalf("the embedded bus is still listening on %s after Stop", busAddr)
	}

	// Stopping twice is not an error: shutdown paths run unconditionally.
	require.NoError(t, srv.Stop(stopCtx))
}

// stacks is a set of goroutine stacks, keyed by the whole stack so two
// goroutines doing different things are never mistaken for one another.
type stacks map[string]struct{}

func (s stacks) minus(other stacks) []string {
	var out []string
	for stack := range s {
		if _, ok := other[stack]; !ok {
			out = append(out, stack)
		}
	}
	return out
}

// dholeGoroutines is every live goroutine running Dhole's own code.
//
// It is goleak in twelve lines, narrowed to what this package can be
// responsible for: a goroutine with no dhole frame in it belongs to the NATS
// client or the SQL driver, and blaming this test for one would make the test
// flaky rather than useful. The embedded NATS server, which has no dhole frames
// either, is checked directly by dialling its address instead.
func dholeGoroutines() stacks {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	out := stacks{}
	for _, block := range strings.Split(string(buf), "\n\n") {
		if !strings.Contains(block, "github.com/azrtydxb/dhole/internal/") {
			continue
		}
		if strings.Contains(block, "server_test.") {
			continue // The test's own goroutine, which is the one asking.
		}
		out[block] = struct{}{}
	}
	return out
}

// TestStartBoundsItsOwnStartupWithoutADeadline is here because the binary
// failed where every test passed.
//
// `dhole serve` starts on a signal context, which has no deadline, and the
// NATS client refuses a flush on one — so establishing the plane's own
// subscriptions returned "context requires a deadline" and the process died
// immediately. Every test above hid it by passing a context with a timeout.
// Start bounds its own start-up rather than trusting the caller to.
func TestStartBoundsItsOwnStartupWithoutADeadline(t *testing.T) {
	dir := t.TempDir()
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)

	require.NoError(t, srv.Start(context.Background()))

	stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	require.NoError(t, srv.Stop(stopCtx))
}

// TestRunSubmittedBeforeAnyEngineExistsStillRuns is the case the happy path
// hides: a run whose first step is ready when NO engine is registered.
//
// The scheduler records STEP_UNSCHEDULABLE and returns, and nothing about a
// later engine registration re-asks — Advance is otherwise driven only by an
// arriving status, and a run that never dispatched will never receive one. So
// a plane that only reacts leaves that run stuck forever, silently, which is
// the exact failure this system is built to make impossible. Something has to
// re-advance it.
func TestRunSubmittedBeforeAnyEngineExistsStillRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// A bus the plane does not own, so the plane hosts no engine and the test
	// decides when one appears.
	busServer, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(busServer.Close)

	dir := t.TempDir()
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeDistributed,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BusURL:   busServer.URL(),
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)

	// The run really is stuck for the reason we think, not merely slow.
	awaitEvent(ctx, t, srv, runID, "STEP_UNSCHEDULABLE")

	stopEngine := startExternalEngine(ctx, t, busServer.URL(),
		blobstore.NewFilesystem(t.TempDir()), srv.CAS())
	t.Cleanup(stopEngine)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "a")
	requireStepSucceeded(t, events, "b")
}

func awaitEvent(ctx context.Context, t *testing.T, srv *server.Server, runID string, want runstore.EventType) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		for _, e := range events {
			if e.Type == want {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s in run %s; log: %s", want, runID, describe(events))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// isolatedDatabase creates a database of its own for one test and drops it
// afterwards, returning the DSN that names it.
func isolatedDatabase(t *testing.T, dsn string) string {
	t.Helper()
	name := fmt.Sprintf("dhole_e2e_%d", time.Now().UnixNano())

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

// TestRestartedPlaneRediscoversAWaitingRun is ADR 0003's claim taken
// literally: a restart is a replay, not a loss.
//
// The run here is the one that only an index can save. It is not in flight —
// no engine ever took it — so nothing durable will arrive to drive it: a
// status on a durable subject rescues a dispatched step, and this step was
// never dispatched. Its whole existence, for the first plane, was a row in the
// log and an entry in a map; when that process ended, only the row was left.
//
// A plane that keeps the set of runs to advance in memory therefore abandons
// this run silently and forever, which is the failure mode this system is
// built to make impossible. The second plane must find it in the store.
func TestRestartedPlaneRediscoversAWaitingRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// A bus neither plane owns, so no engine exists until the test makes one.
	busServer, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(busServer.Close)

	dir := t.TempDir()
	cfg := server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeDistributed,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BusURL:   busServer.URL(),
		BlobRoot: filepath.Join(dir, "state"),
	}

	first, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Start(ctx))

	runID, err := first.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	awaitEvent(ctx, t, first, runID, "STEP_UNSCHEDULABLE")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
	require.NoError(t, first.Stop(stopCtx))
	stopCancel()

	// A second process, holding nothing the first one knew.
	second, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, second.Start(ctx))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, second.Stop(ctx))
	})

	stopEngine := startExternalEngine(ctx, t, busServer.URL(),
		blobstore.NewFilesystem(t.TempDir()), second.CAS())
	t.Cleanup(stopEngine)

	events := awaitRunCompleted(ctx, t, second, runID)
	requireStepSucceeded(t, events, "a")
	requireStepSucceeded(t, events, "b")
}

// TestEngineThatStartedBeforeThePlaneBecomesVisible closes the gap that made a
// lost registration permanent.
//
// EngineRegistration is fire-and-forget on a core subject, and Heartbeat
// deliberately refuses to rebuild an instance from a message that cannot
// describe one (registry.ErrNotRegistered): a heartbeat carries an engine id
// and what it holds, so an instance resurrected from one would advertise no
// platform and no capabilities, and every step matched against it would be
// unschedulable. Both halves of that are right, and together they meant an
// engine that announced itself while no plane was listening stayed invisible
// until somebody restarted it — an engine in a customer's network, which is
// exactly the engine nobody can restart.
//
// The engine here starts first, publishes its registration into an empty room,
// and must join the fleet anyway.
func TestEngineThatStartedBeforeThePlaneBecomesVisible(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	busServer, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(busServer.Close)

	dir := t.TempDir()
	blobRoot := filepath.Join(dir, "state")
	// The engine shares the plane's content-addressed store, as an engine
	// beside it would; the plane has not been built yet, so the path is the
	// one it will open rather than one asked of it.
	engineCAS := cas.NewFilesystem(filepath.Join(blobRoot, "cas"))
	require.NoError(t, os.MkdirAll(filepath.Join(blobRoot, "cas"), 0o750))

	stopEngine := startExternalEngine(ctx, t, busServer.URL(),
		blobstore.NewFilesystem(t.TempDir()), engineCAS)
	t.Cleanup(stopEngine)

	// Its registration is on the wire and gone before anything is listening.
	// A heartbeat or two follow it into the same empty room.
	time.Sleep(2 * engine.HeartbeatInterval)

	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeDistributed,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BusURL:   busServer.URL(),
		BlobRoot: blobRoot,
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "a")
	requireStepSucceeded(t, events, "b")
}

// TestThePlaneSweepsDeadLeases is the wiring the scheduler's own orphan tests
// cannot check: a sweeper nobody runs re-dispatches nothing.
//
// The lease here belongs to no run — it stands in for an engine that took a
// step and died — and the assertion is simply that the control plane notices
// it expired without anybody asking. A plane that never sweeps leaves it in
// the bucket forever, and with it every step whose holder is gone.
func TestThePlaneSweepsDeadLeases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	conn, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	token, err := leases.Claim(ctx, tenantID, "run-nobody-owns", "step", 1, 200*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, leases.Validate(ctx, token))

	deadline := time.Now().Add(60 * time.Second)
	for {
		err := leases.Validate(ctx, token)
		if errors.Is(err, lease.ErrFenced) {
			return // The plane swept it.
		}
		require.NoError(t, err)
		if time.Now().After(deadline) {
			t.Fatal("the control plane never swept a lease that expired: an engine that dies " +
				"holding a step leaves that step in flight forever")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestAHeartbeatKeepsALiveStepsLeaseAlive is the other half of sweeping, and
// without it the sweeper is worse than the gap it closes.
//
// A lease expires 30 seconds after it is claimed and NOTHING in this system
// renewed one: the wire contract says "a lease that stops being renewed
// expires", and an engine's heartbeat lists every job it holds with that job's
// fence token precisely so the plane can renew it. Until it did, a sweeper
// would take a step away from an engine that is alive and running it — every
// step longer than a lease TTL, re-dispatched forever, which is exactly the
// duplicate execution the fence exists to prevent.
//
// The engine here is a heartbeat and nothing else: the assertion is that the
// plane renews on what an engine SAYS it holds, whatever else it is doing.
func TestAHeartbeatKeepsALiveStepsLeaseAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	conn, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	// A lease far shorter than the sweep interval, so an unrenewed one is
	// certainly dead by the first sweep.
	token, err := leases.Claim(ctx, tenantID, "run-held", "step", 1, 500*time.Millisecond)
	require.NoError(t, err)

	engineBus, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(engineBus.Close)

	beating, stopBeating := context.WithCancel(ctx)
	defer stopBeating()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-beating.Done():
				return
			case <-ticker.C:
			}
			_ = engineBus.Publish(beating, bus.SubjectEngineHeartbeat("engine-holding"),
				wire.FrameHeartbeat(&dholev1.EngineHeartbeat{
					EngineId: "engine-holding",
					InFlight: []*dholev1.InFlight{{
						RunId:      "run-held",
						StepId:     "step",
						Attempt:    1,
						FenceToken: scheduler.EncodeFence(tenantID, token),
					}},
				}))
		}
	}()

	// Well past two sweeps and many lease lifetimes.
	time.Sleep(12 * time.Second)
	require.NoError(t, leases.Validate(ctx, token),
		"the plane took a step away from an engine that was telling it, every 100ms, "+
			"that it still held it")
}
