package server_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/server"
)

// partitionTTL is the claim deadline these tests hold instances to. It is
// short so a killed instance's partitions become available inside the five
// seconds the rebalance test allows, and so the suite does not sit waiting on
// a production-sized TTL.
const partitionTTL = time.Second

// ---------------------------------------------------------------------------
// The shared run log the single-writer test asserts against.
// ---------------------------------------------------------------------------

// runLog stands in for the event log two control planes would both be writing
// to. It deliberately does NOT deduplicate and it deliberately does NOT
// allocate the sequence under the same lock that appends: a writer reads the
// run's last position and then writes the one after it, which is exactly the
// decision a plane makes when it advances a run. Two planes advancing the same
// run therefore both decide "the next event here is sequence 1" and both write
// it — and that duplicate is what this test is looking for.
//
// A log that allocated the sequence in-transaction (which the real store does)
// would hide the collision behind two different sequences; hiding it here
// would make the test pass whatever the partitioner did.
type runLog struct {
	mu      sync.Mutex
	last    map[string]int
	entries []logEntry
}

type logEntry struct {
	runID  string
	seq    int
	writer string
}

func newRunLog() *runLog {
	return &runLog{last: map[string]int{}}
}

// advance records one plane's advance of one run, reading the run's position
// and writing the next one as two separate steps.
func (l *runLog) advance(runID, writer string) {
	l.mu.Lock()
	next := l.last[runID] + 1
	l.mu.Unlock()

	// A real plane does work between deciding the next sequence and writing
	// it. The gap is what a second plane races into.
	time.Sleep(time.Microsecond)

	l.mu.Lock()
	l.last[runID] = next
	l.entries = append(l.entries, logEntry{runID: runID, seq: next, writer: writer})
	l.mu.Unlock()
}

func (l *runLog) snapshot() []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]logEntry(nil), l.entries...)
}

// countFor is how many times one run has been advanced.
func (l *runLog) countFor(runID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries {
		if e.runID == runID {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// TestSingleWriterPerRun
// ---------------------------------------------------------------------------

// TestSingleWriterPerRun is the property the whole partitioning scheme exists
// for. Two control-plane instances consume the SAME stream — each with its own
// durable consumer, so each of them genuinely sees every one of the thousand
// advance messages — and they run at the same time, on their own goroutines,
// against their own NATS connections. Only the partition claims stop them both
// from advancing the same run.
//
// Remove the claims and both planes advance all 1000 runs: every run gets two
// writers and two events numbered 1, which is a step dispatched twice under
// two fences with the loser's work discarded after it ran.
func TestSingleWriterPerRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	const runs = 1000
	runIDs := make([]string, runs)
	for i := range runIDs {
		runIDs[i] = fmt.Sprintf("run_%04d_%s", i, "abcdef")
	}

	// One stream, one message per run.
	publisher := dialNATS(t, srv.URL())
	js, err := jetstream.New(publisher)
	require.NoError(t, err)
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "RUNS",
		Subjects:  []string{"run.advance.>"},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	for _, runID := range runIDs {
		_, err := js.Publish(ctx, "run.advance."+runID, []byte(runID))
		require.NoError(t, err)
	}

	// Two planes of ONE deployment, each on its own connection.
	//
	// A long TTL here on purpose: this test is about who may write, not about
	// expiry, and a claim that lapsed mid-run would leave runs unadvanced for
	// a reason that has nothing to do with the property under test.
	alpha := newPartitioner(ctx, t, srv.URL(), "deployment-1", server.WithPartitionTTL(60*time.Second))
	beta := newPartitioner(ctx, t, srv.URL(), "deployment-1", server.WithPartitionTTL(60*time.Second))
	held := awaitBalanced(ctx, t, map[string]*server.Partitioner{"alpha": alpha, "beta": beta})
	require.Len(t, held["alpha"], server.PartitionCount/2)
	require.Len(t, held["beta"], server.PartitionCount/2)

	log := newRunLog()
	var planes sync.WaitGroup
	for name, p := range map[string]*server.Partitioner{"alpha": alpha, "beta": beta} {
		planes.Add(1)
		go func(name string, p *server.Partitioner) {
			defer planes.Done()
			consumeAndAdvance(ctx, t, srv.URL(), name, p, log, runs)
		}(name, p)
	}
	planes.Wait()

	entries := log.snapshot()
	seen := map[string]string{}  // run -> the writer that advanced it
	pairs := map[string]string{} // "run/seq" -> writer that wrote it first
	for _, e := range entries {
		key := fmt.Sprintf("%s/%d", e.runID, e.seq)
		if first, dup := pairs[key]; dup {
			t.Fatalf("duplicate sequence: run %s sequence %d written by both %s and %s",
				e.runID, e.seq, first, e.writer)
		}
		pairs[key] = e.writer
		if other, ok := seen[e.runID]; ok && other != e.writer {
			t.Fatalf("run %s was advanced by both %s and %s", e.runID, other, e.writer)
		}
		seen[e.runID] = e.writer
	}

	require.Len(t, entries, runs, "every run must be advanced exactly once")
	require.Len(t, seen, runs, "every run must be advanced by exactly one plane")

	// Both planes must have done real work, or "no duplicates" would be
	// satisfied by one plane doing nothing at all.
	byWriter := map[string]int{}
	for _, e := range entries {
		byWriter[e.writer]++
	}
	require.Greater(t, byWriter["alpha"], 0)
	require.Greater(t, byWriter["beta"], 0)
}

// consumeAndAdvance is one control plane: its own connection, its own durable
// consumer over the whole stream, and a check of its claims before every
// advance.
func consumeAndAdvance(ctx context.Context, t *testing.T, url, name string, p *server.Partitioner, log *runLog, total int) {
	t.Helper()
	conn := dialNATS(t, url)
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	cons, err := js.CreateOrUpdateConsumer(ctx, "RUNS", jetstream.ConsumerConfig{
		Durable:   "plane-" + name,
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)

	seen := 0
	for seen < total {
		if ctx.Err() != nil {
			return
		}
		batch, err := cons.Fetch(200, jetstream.FetchMaxWait(2*time.Second))
		require.NoError(t, err)
		for msg := range batch.Messages() {
			runID := string(msg.Data())
			if p.Owns(runID) {
				log.advance(runID, name)
			}
			require.NoError(t, msg.Ack())
			seen++
		}
		require.NoError(t, batch.Error())
	}
}

// ---------------------------------------------------------------------------
// TestPartitionRebalanceOnInstanceLoss
// ---------------------------------------------------------------------------

// TestPartitionRebalanceOnInstanceLoss kills one of three instances and
// requires the survivors to hold every partition within five seconds.
//
// The kill is a kill. The instance's NATS connection is closed underneath it
// and its renewal stops without a word — it never releases anything, and after
// the close it could not write to the bucket if it tried. That is the case
// that matters: a plane that shuts down politely is a plane that got to run
// its deferred code, which is precisely the case a crash does not give you.
func TestPartitionRebalanceOnInstanceLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	instances := map[string]*server.Partitioner{}
	conns := map[string]*nats.Conn{}
	for _, name := range []string{"one", "two", "three"} {
		conn := dialNATS(t, srv.URL())
		p, err := server.NewPartitioner(ctx, conn, "deployment-1", server.WithPartitionTTL(partitionTTL))
		require.NoError(t, err)
		instances[name], conns[name] = p, conn
	}

	held := awaitBalanced(ctx, t, instances)
	for name, parts := range held {
		require.NotEmpty(t, parts, "instance %s claimed nothing", name)
	}
	doomed := held["three"]
	require.NotEmpty(t, doomed)

	// The kill: the connection goes, nothing is released, and "three" never
	// calls ClaimPartitions again.
	conns["three"].Close()
	delete(instances, "three")

	survivors := instances
	deadline := time.Now().Add(5 * time.Second)
	var union map[int]string
	for time.Now().Before(deadline) {
		union = map[int]string{}
		for name, p := range survivors {
			parts, err := p.ClaimPartitions(ctx, name)
			require.NoError(t, err)
			for _, part := range parts {
				union[part] = name
			}
		}
		if len(union) == server.PartitionCount {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Len(t, union, server.PartitionCount,
		"the survivors did not take over every partition within 5s")
	for _, part := range doomed {
		require.Contains(t, union, part, "partition %d of the killed instance was never reclaimed", part)
	}
}

// ---------------------------------------------------------------------------
// TestBackpressureStopsPullingWhenAckPendingReached
// ---------------------------------------------------------------------------

// TestBackpressureStopsPullingWhenAckPendingReached is the other half of
// scaling out. A plane that keeps pulling work it cannot start turns a slow
// plane into a dead one: the messages it holds are neither being worked on nor
// available to anybody else until their ack wait elapses.
//
// The assertion is on the FETCH COUNT, not just on "Next blocked". The server
// also refuses to deliver past MaxAckPending, so a client with no gate of its
// own would still look correct while spinning a fetch at the server every two
// seconds forever. Counting fetches is what tells the two apart.
func TestBackpressureStopsPullingWhenAckPendingReached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	subject := bus.SubjectDispatch("trusted", "abc")
	require.NoError(t, conn.EnsureWorkQueue(ctx, "BACKPRESSURE", []string{"job.dispatch.>"}))
	for i := range 6 {
		require.NoError(t, conn.Publish(ctx, subject, dispatchFor(i)))
	}

	sub, err := conn.SubscribePullWithOptions(ctx, "BACKPRESSURE", "bounded", subject,
		bus.WithMaxAckPending(2))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	// The SERVER is told the limit as well. The client's own gate is what stops
	// it pulling; MaxAckPending on the consumer is what protects the stream
	// from a client that ignores its gate, and only the server can enforce
	// that — so a limit that lived only in this process would be an honour
	// system.
	inspect, err := jetstream.New(dialNATS(t, srv.URL()))
	require.NoError(t, err)
	cons, err := inspect.Consumer(ctx, "BACKPRESSURE", "bounded")
	require.NoError(t, err)
	require.Equal(t, 2, cons.CachedInfo().Config.MaxAckPending,
		"the consumer was created without a server-side ack-pending limit")

	first, err := sub.Next(ctx)
	require.NoError(t, err)
	second, err := sub.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, sub.Outstanding())

	// Two outstanding, the limit reached: no further fetch may leave.
	fetches := sub.Fetches()
	blocked, blockCancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer blockCancel()
	_, err = sub.Next(blocked)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, fetches, sub.Fetches(),
		"the consumer fetched while its ack-pending limit was reached")

	// An ack frees a slot, and pulling resumes.
	require.NoError(t, first.Ack())
	third, err := sub.Next(ctx)
	require.NoError(t, err)
	require.Greater(t, sub.Fetches(), fetches, "the consumer never resumed fetching")
	require.NoError(t, second.Ack())
	require.NoError(t, third.Ack())
}

// ---------------------------------------------------------------------------
// PartitionFor
// ---------------------------------------------------------------------------

// TestPartitionForIsStableAndEvenlyDistributed pins both halves of what a
// partition function has to be. Stable, or a run changes owner between two
// advances and both planes write it. Even, or one plane starves while another
// burns: a hash that sends everything to partition 0 is perfectly stable and
// completely useless.
func TestPartitionForIsStableAndEvenlyDistributed(t *testing.T) {
	const ids = 10000
	counts := make([]int, server.PartitionCount)
	for i := range ids {
		runID := fmt.Sprintf("run_%d_%x", i, i*2654435761)
		part := server.PartitionFor(runID, server.PartitionCount)
		require.GreaterOrEqual(t, part, 0)
		require.Less(t, part, server.PartitionCount)
		// Stable: asking again, and again, gives the same answer.
		for range 3 {
			require.Equal(t, part, server.PartitionFor(runID, server.PartitionCount))
		}
		counts[part]++
	}

	mean := float64(ids) / float64(server.PartitionCount)
	for part, n := range counts {
		require.Positive(t, n, "partition %d got nothing out of %d ids", part, ids)
		require.Less(t, float64(n), mean*2,
			"partition %d took %d of %d ids — the hash is lopsided", part, n, ids)
		require.Greater(t, float64(n), mean/2,
			"partition %d took only %d of %d ids — the hash is lopsided", part, n, ids)
	}

	// Different ids do not all share one partition.
	distinct := map[int]struct{}{}
	for i := range 200 {
		distinct[server.PartitionFor(fmt.Sprintf("run_%d", i), server.PartitionCount)] = struct{}{}
	}
	require.Greater(t, len(distinct), server.PartitionCount/2)
}

// ---------------------------------------------------------------------------
// Deployment scoping
// ---------------------------------------------------------------------------

// TestClaimsAreScopedToTheDeployment is the tenancy rule this feature inherits
// sideways. Claims are tenant-agnostic — a partition holds whatever runs hash
// into it, whoever they belong to — but they are strictly per DEPLOYMENT: two
// deployments sharing one NATS each own the whole ring, and neither can be
// made to give a partition up because the other one showed up.
//
// Without the deployment in the key the two would see each other as members,
// split the ring in half, and each ignore the runs it decided were the other's
// — a plane silently not advancing runs it is the only owner of.
func TestClaimsAreScopedToTheDeployment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	alpha := newPartitioner(ctx, t, srv.URL(), "deployment-alpha", server.WithPartitionTTL(60*time.Second))
	beta := newPartitioner(ctx, t, srv.URL(), "deployment-beta", server.WithPartitionTTL(60*time.Second))

	// DISTINCT instance ids, so the deployment scope is the only thing that can
	// keep them apart. Sharing an id would have hidden the bug: two instances
	// writing one key look like one plane renewing its own claim, and both
	// would report the whole ring however the keys were named.
	for range 4 {
		a, err := alpha.ClaimPartitions(ctx, "alpha-instance")
		require.NoError(t, err)
		b, err := beta.ClaimPartitions(ctx, "beta-instance")
		require.NoError(t, err)
		require.Len(t, a, server.PartitionCount,
			"deployment-alpha lost partitions to another deployment")
		require.Len(t, b, server.PartitionCount,
			"deployment-beta lost partitions to another deployment")
	}
	require.True(t, alpha.Owns("run_anything"))
	require.True(t, beta.Owns("run_anything"))
}

// ---------------------------------------------------------------------------
// Losing a partition mid-run
// ---------------------------------------------------------------------------

// TestLosingAPartitionStopsAdvancingThoseRuns holds the plane to the promptness
// half of single-writer. A plane that keeps advancing runs it no longer owns
// is a second writer for as long as it takes to notice, and "as long as it
// takes to notice" is exactly the window in which a step is dispatched twice.
//
// The loop below is the advance loop's shape: ownership is consulted before
// EVERY run, not once per pass, so the plane abandons a partition at the
// granularity of a single run-advance rather than finishing the pass it began.
func TestLosingAPartitionStopsAdvancingThoseRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	runIDs := make([]string, 400)
	for i := range runIDs {
		runIDs[i] = fmt.Sprintf("run_mid_%d", i)
	}

	incumbent := newPartitioner(ctx, t, srv.URL(), "deployment-1", server.WithPartitionTTL(partitionTTL))
	first, err := incumbent.ClaimPartitions(ctx, "incumbent")
	require.NoError(t, err)
	require.Len(t, first, server.PartitionCount, "a lone instance must hold the whole ring")

	// The incumbent advances its runs continuously while the ring is taken
	// out from under it.
	log := newRunLog()
	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	var loop sync.WaitGroup
	loop.Add(1)
	go func() {
		defer loop.Done()
		for loopCtx.Err() == nil {
			for _, runID := range runIDs {
				if loopCtx.Err() != nil {
					return
				}
				if incumbent.Owns(runID) {
					log.advance(runID, "incumbent")
				}
			}
		}
	}()

	// A second instance joins and the ring is rebalanced.
	joiner := newPartitioner(ctx, t, srv.URL(), "deployment-1", server.WithPartitionTTL(partitionTTL))
	held := awaitBalanced(ctx, t, map[string]*server.Partitioner{
		"incumbent": incumbent,
		"joiner":    joiner,
	})
	require.Len(t, held["joiner"], server.PartitionCount/2)

	// A run that now belongs to the joiner.
	var lost string
	for _, runID := range runIDs {
		if joiner.Owns(runID) {
			lost = runID
			break
		}
	}
	require.NotEmpty(t, lost, "the joiner took no partition holding a run")
	require.False(t, incumbent.Owns(lost), "the incumbent still claims a run it gave up")

	// Whatever it had already written, it writes nothing more for that run.
	before := log.countFor(lost)
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, before, log.countFor(lost),
		"the incumbent kept advancing %s after losing its partition", lost)

	// And it is still advancing the runs it kept, so the check above is not
	// passing because the loop simply stopped.
	var kept string
	for _, runID := range runIDs {
		if incumbent.Owns(runID) {
			kept = runID
			break
		}
	}
	require.NotEmpty(t, kept)
	keptBefore := log.countFor(kept)
	require.Eventually(t, func() bool { return log.countFor(kept) > keptBefore },
		2*time.Second, 20*time.Millisecond,
		"the incumbent stopped advancing runs it still owns")

	stopLoop()
	loop.Wait()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func dialNATS(t *testing.T, url string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

func newPartitioner(ctx context.Context, t *testing.T, url, deploymentID string, opts ...server.PartitionOption) *server.Partitioner {
	t.Helper()
	p, err := server.NewPartitioner(ctx, dialNATS(t, url), deploymentID, opts...)
	require.NoError(t, err)
	return p
}

// awaitBalanced drives every instance's claim until the ring is fully covered
// and nobody is holding more than its fair share. Convergence takes more than
// one pass by design: an instance that was alone holds everything, and it is
// its NEXT claim that gives the surplus back.
func awaitBalanced(ctx context.Context, t *testing.T, instances map[string]*server.Partitioner) map[string][]int {
	t.Helper()
	// One less than an even split: with three instances the ceiling share is
	// 22, so somebody legitimately ends up with 20.
	fair := max(server.PartitionCount/len(instances)-1, 1)
	deadline := time.Now().Add(20 * time.Second)
	held := map[string][]int{}
	for time.Now().Before(deadline) {
		union := map[int]struct{}{}
		balanced := true
		for name, p := range instances {
			parts, err := p.ClaimPartitions(ctx, name)
			require.NoError(t, err)
			held[name] = parts
			for _, part := range parts {
				union[part] = struct{}{}
			}
			if len(parts) < fair {
				balanced = false
			}
		}
		if balanced && len(union) == server.PartitionCount {
			return held
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("partitions never balanced across %d instances: %v", len(instances), held)
	return nil
}

func dispatchFor(i int) *dholev1.JobDispatch {
	return &dholev1.JobDispatch{
		RunId:      fmt.Sprintf("run-%d", i),
		StepId:     fmt.Sprintf("step-%d", i),
		FenceToken: fmt.Sprintf("fence-%d", i),
	}
}

// ---------------------------------------------------------------------------
// The plane's own advance loop
// ---------------------------------------------------------------------------

// TestAPlaneAdvancesOnlyTheRunsItOwns is the wiring the rest of this file
// assumes. A Partitioner that is correct and a plane that ignores it is worth
// nothing: the ring only stops a run being advanced twice if the advance loop —
// and Submit, which advances the run it has just created — actually ask.
//
// The second member here is a Partitioner and nothing else. It claims its half
// of the ring and advances nothing at all, so the runs that hash into its half
// have exactly one possible writer: the plane, if the plane is willing to write
// runs it does not own. Their logs must stay at RUN_CREATED while the runs on
// the plane's own half run to completion — which is what proves the plane is
// alive and dispatching rather than merely stuck.
func TestAPlaneAdvancesOnlyTheRunsItOwns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	busServer, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(busServer.Close)

	dir := t.TempDir()
	srv, err := server.New(server.Config{
		Mode:     server.ModeDistributed,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BusURL:   busServer.URL(),
		BlobRoot: filepath.Join(dir, "state"),
		// An ephemeral port: the default is the well-known 7777, and a test
		// that takes it fails whenever anything else on the machine holds it —
		// a port-forward to a real cluster, or a second test binary.
		APIAddr: "127.0.0.1:0",
		// Named, not derived: the spare below has to join the SAME deployment,
		// and a derived id would be this plane's state directory.
		DeploymentID: "deployment-under-test",
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	t.Cleanup(startExternalEngine(ctx, t, busServer.URL(),
		blobstore.NewFilesystem(t.TempDir()), srv.CAS()))

	// The spare's TTL outlives the test, so it holds its half without a
	// renewal loop of its own.
	spare, err := server.NewPartitioner(ctx, dialNATS(t, busServer.URL()),
		"deployment-under-test", server.WithPartitionTTL(5*time.Minute))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		held, err := spare.ClaimPartitions(ctx, "spare")
		return err == nil && len(held) == server.PartitionCount/2
	}, 60*time.Second, 200*time.Millisecond,
		"the running plane never gave half the ring to the instance that joined it")

	var theirs, ours []string
	for range 16 {
		runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
		require.NoError(t, err)
		if spare.Owns(runID) {
			theirs = append(theirs, runID)
			continue
		}
		ours = append(ours, runID)
	}
	require.NotEmpty(t, theirs, "no run landed in the spare's half of the ring")
	require.NotEmpty(t, ours, "no run landed in the plane's own half of the ring")

	// The plane is working: a run it owns goes all the way through.
	events := awaitRunCompleted(ctx, t, srv, ours[0])
	requireStepSucceeded(t, events, "a")

	// And it has not touched a single run it does not own.
	for _, runID := range theirs {
		log, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		require.Len(t, log, 1,
			"the plane advanced %s, which belongs to another instance: %s", runID, describe(log))
		require.Equal(t, runstore.RunCreated, log[0].Type)
	}
}
