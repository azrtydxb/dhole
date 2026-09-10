// Package server assembles the Dhole control plane out of the parts the
// earlier tasks built, and is the first place they have to work together.
//
// The two deployments it offers are one system. In ModeEmbedded the process
// starts a NATS server inside itself, an engine beside it, and then makes them
// talk over the bus like strangers; in ModeDistributed the same control plane
// dials somebody else's NATS and somebody else's Postgres and hosts no engine
// at all. Everything between those two ends — the scheduler, the outbox, the
// lease manager, the registry, the wire — is the same code on the same
// subjects. That is the claim `dhole serve` makes, and internal/server's tests
// are where it is checked rather than assumed.
//
// Three rules from the architecture show up here as structure rather than as
// comments elsewhere:
//
// A run is not a goroutine (ADR 0003). Nothing in Server holds a run's
// position, and nothing holds the set of runs either: the advance loop asks
// the store which runs are open, so a plane that restarts inherits the work of
// the one it replaced instead of starting empty.
//
// The bus is not the source of truth (ADR 0005). Nothing here publishes a
// dispatch. The scheduler enqueues it in the same transaction as the event
// that justifies it, and the outbox drainer started by Start is what later
// hands it to NATS.
//
// Nothing dials an engine. The plane subscribes and the engine subscribes;
// every message between them travels a subject named in docs/wire-contract.md.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/gate"
	"github.com/azrtydxb/dhole/internal/wait"
	"github.com/azrtydxb/dhole/internal/wire"
)

// Mode is which of the two deployments this process is.
type Mode string

const (
	// ModeEmbedded is the single binary: an in-process NATS server, SQLite,
	// filesystem stores, and one engine hosted beside the plane — reached over
	// the bus like any other.
	ModeEmbedded Mode = "embedded"
	// ModeDistributed is the control plane alone, against infrastructure it
	// did not start and engines it does not host.
	ModeDistributed Mode = "distributed"
)

// DefaultTier is the trust tier this plane dispatches to, and therefore the
// tier an engine must run in to be given its work. Tiers become a deployment
// choice when there is more than one; until then there is exactly one name and
// both halves have to agree on it.
const DefaultTier = "trusted"

// StatusStream is the durable stream carrying job.status.>. Status is the one
// engine-to-plane subject the plane must not miss, so it is a stream with an
// acknowledged consumer rather than a core subscription that drops what
// arrives while the plane is restarting.
const StatusStream = "STATUS"

// engineTTL is how long an instance survives without a heartbeat. The engine
// beats every five seconds (docs/wire-contract.md), so this tolerates several
// missed beats before the fleet forgets it.
const engineTTL = 30 * time.Second

// advanceInterval is how often the plane re-advances the runs the store says
// are open.
//
// Advance is otherwise driven only by an arriving status, so a run whose step
// could not be placed when it was submitted — no engine registered yet, the
// usual case one second after start-up — would sit unscheduled forever because
// nothing would ever ask again. The same is true of a step waiting out a retry
// backoff: the thing it is waiting for is time, and time sends no message.
//
// What changed from the first version of this loop is not the tick but where
// the SET comes from. It used to be a map this process filled as it submitted
// runs, so a plane that restarted advanced only what it had itself started and
// abandoned every waiting run it inherited. It now comes from the open-run
// index, which is the store's answer and not this process's memory — which is
// what makes a restart a replay (ADR 0003).
const advanceInterval = 250 * time.Millisecond

// orphanSweepInterval is how often the plane looks for leases nobody renewed.
//
// A step whose engine died is in flight as far as the log is concerned until
// something says otherwise, so this interval is the delay between an engine
// dying and its work being available to somebody else. It is well under the
// lease TTL: the TTL decides when a holder is presumed dead, and this decides
// how promptly we act on it.
const orphanSweepInterval = 5 * time.Second

// startTimeout bounds bringing the plane up when the caller's context does not.
const startTimeout = 60 * time.Second

// Config is the deployment. Everything in it is infrastructure: there is no
// per-run field, because a field holding one would be the in-memory run
// position ADR 0003 exists to keep out.
type Config struct {
	// Mode selects the deployment.
	Mode Mode
	// StoreDSN is a Postgres DSN or, for anything else, a SQLite file path.
	StoreDSN string
	// BusURL is the NATS server to dial. Ignored in ModeEmbedded, which
	// starts its own and reports the URL through BusURL().
	BusURL string
	// EnvironmentIdentity is what this deployment declares its hosted engine's
	// environment to be. Empty means the executor answers for itself, which
	// for a host process is "no stable identity" and therefore no caching.
	//
	// It is a declaration and not a discovery: an operator whose engines are
	// built from one immutable image knows something the process backend
	// cannot see, and an operator who declares it wrongly gets one host's
	// results served as another's (ADR 0021).
	EnvironmentIdentity string

	// Blobs is the object store every process in this deployment shares.
	//
	// Nil means the filesystem under BlobRoot, which is right for a single
	// binary and wrong for anything else: a plane and its engines are separate
	// processes, and a local disk each is not a store they share. The content
	// addressed store is built over whatever this is, so setting it moves both.
	Blobs blobstore.Store

	// BlobRoot is the directory the content-addressed store, the blob store
	// and the embedded bus keep their data under.
	BlobRoot string
	// DeploymentID names this control plane, and is what scopes its outbox
	// claim. Two planes that share a database MUST NOT share it: an outbox row
	// is addressed to one plane's bus, and a claim that does not name the
	// plane makes each of them publish the other's dispatches to engines that
	// have never heard of the run.
	//
	// Every process of ONE plane must share it, though — that is what lets two
	// of them drain the same backlog for availability. Left empty it is
	// derived from BlobRoot, which is the state directory this plane owns
	// alone: stable across restarts, and different for two planes that share
	// nothing but a database.
	DeploymentID string
	// Executor is where the hosted engine runs steps, in ModeEmbedded. Nil
	// means the host process backend.
	//
	// It is configuration rather than a constant because the backend is what
	// reports the environment identity every cache key is folded over
	// (ADR 0009). A host process has none and says so, which makes every step
	// of such a deployment non-cacheable — correctly, since nobody can name
	// the compilers and libraries the host happens to carry. A deployment
	// running steps somewhere reproducible supplies that backend here.
	Executor executor.Executor
	// APIAddr is where the control plane serves its one contract — the
	// contract the GUI, the CLI and agents all use (ADR 0013). Empty means
	// DefaultAPIAddr; "127.0.0.1:0" asks the operating system for a free
	// port, which is what a test wants and what a second plane on one machine
	// needs.
	APIAddr string
	// NoAPI serves no contract at all. It is for an embedding that runs a
	// pipeline and exits — `dhole local run` — where a well-known port would
	// only collide with the `dhole serve` the developer already has up.
	//
	// It is an opt-OUT, not an opt-in. A plane that serves nothing is a plane
	// whose CLI, web client and agents have nothing to talk to, and that was
	// the state of this binary for four tasks precisely because serving the
	// API was something somebody had to remember to switch on.
	NoAPI bool
	// Triggers are the event sources this plane runs: cron schedules on their
	// own poll, and webhook endpoints on the API's listener under
	// TriggerPrefix. Empty means the plane runs none, which is what it did
	// for every task up to this one — internal/trigger/* were four
	// implementations nothing in the binary imported.
	Triggers []TriggerSpec
	// Models resolves the language model a `builtin:llm` step names. Nil
	// means this plane calls no model: such a step fails with that reason
	// rather than silently doing nothing.
	//
	// It is supplied rather than built here because a model client holds an
	// API key and nothing in this system hands the control plane one — see
	// ModelFactory.
	Models ModelFactory
	// APIAllowedOrigins are the browser origins allowed to make cross-origin
	// calls. Empty — the default — allows none, so a page on any site cannot
	// reach a plane on the developer's own loopback address.
	APIAllowedOrigins []string
}

// Server is one control plane.
type Server struct {
	cfg Config
	log *slog.Logger

	// mu guards start-up and shutdown against each other and against a second
	// call of either.
	mu      sync.Mutex
	running bool
	infra   *infra
	sched   *scheduler.Scheduler
	leases  lease.Manager
	defs    defstore.Store
	out     *outbox.Outbox
	fleet   *registry.KV
	// builtins runs the step types the plane hosts itself, and timers is the
	// durable timer table its poll fires out of. Both are held so that Stop
	// and Approve can reach them.
	builtins *builtins
	timers   *wait.Timers
	// triggerMux holds the webhook trigger handlers rootHandler serves. Nil
	// when this deployment configured none.
	triggerMux *http.ServeMux
	// partitions is this process's share of the run-id ring. Nil means the
	// plane owns every run — see ownsRun.
	partitions *planePartitions

	// apiHTTP serves internal/api; apiAddr is the address it resolved to and
	// bootstrap the credential minted for it. All three are guarded by mu.
	apiHTTP   *http.Server
	apiAddr   string
	bootstrap string

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopSub []func()
}

// New validates cfg. It opens nothing: a Server that has not been started
// holds no file, no socket and no goroutine, so a configuration error costs
// nothing to recover from.
func New(cfg Config) (*Server, error) {
	switch cfg.Mode {
	case ModeEmbedded, ModeDistributed:
	case "":
		return nil, errors.New("server: a mode is required")
	default:
		return nil, fmt.Errorf("server: unknown mode %q", cfg.Mode)
	}
	if cfg.StoreDSN == "" {
		return nil, errors.New("server: a store DSN is required")
	}
	if cfg.BlobRoot == "" {
		return nil, errors.New("server: a blob root is required")
	}
	if cfg.Mode == ModeDistributed && cfg.BusURL == "" {
		return nil, errors.New("server: distributed mode needs a bus URL")
	}
	if cfg.DeploymentID == "" {
		cfg.DeploymentID = derivedDeploymentID(cfg.BlobRoot)
	}
	return &Server{cfg: cfg, log: slog.Default()}, nil
}

// derivedDeploymentID names a plane that was not given a name, from the one
// thing it owns alone and keeps across restarts: its state directory. It is
// hashed rather than used raw so the id stays a short, subject-safe token
// whatever the path looks like.
//
// A random id per process would be worse than none: the rows a plane enqueued
// before a restart would be owed to a deployment that no longer exists, and no
// drainer would ever claim them again.
func derivedDeploymentID(blobRoot string) string {
	root, err := filepath.Abs(blobRoot)
	if err != nil {
		root = blobRoot
	}
	sum := sha256.Sum256([]byte(root))
	return "plane-" + hex.EncodeToString(sum[:])[:16]
}

// Start brings the plane up and returns once it is serving.
//
// ctx bounds start-up only. The background work runs on a context this server
// owns, so Stop is the one thing that ends it: a server whose goroutines died
// because the caller's context expired would be running and not working, which
// is the failure mode hardest to see.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return errors.New("server: already started")
	}

	// Start-up is bounded here rather than by the caller. `dhole serve` starts
	// on a signal context, which has no deadline, and a bus subscription
	// cannot be confirmed without one — so trusting the caller meant the
	// binary refused to start while every test, which passes a timeout,
	// passed. Bringing a plane up is also exactly the operation that must not
	// hang: a half-started server serves nothing and says nothing.
	if _, ok := ctx.Deadline(); !ok {
		var release context.CancelFunc
		ctx, release = context.WithTimeout(ctx, startTimeout)
		defer release()
	}

	in, err := openInfra(ctx, s.cfg)
	if err != nil {
		return err
	}

	fleet, err := registry.New(ctx, in.conn, DefaultTenant, engineTTL)
	if err != nil {
		in.close()
		return err
	}
	leases, err := lease.New(ctx, in.conn)
	if err != nil {
		in.close()
		return err
	}

	// WithoutPinning is deliberate and temporary. The steps this server runs
	// today carry `command:` references, which are inline commands rather than
	// artifacts, so there is nothing a resolver could pin them to. The moment a
	// step type maps a resolved artifact to a command — the mapping neither
	// Task 32 nor 35 provides — this becomes WithResolver, and saving unpinned
	// stops being allowed here.
	defs := defstore.NewWithDialect(in.db, in.dialect, defstore.WithoutPinning())
	out := outbox.New(in.store, in.plane, s.cfg.DeploymentID, outbox.WithErrorHandler(func(err error) {
		s.log.Error("outbox drain failed", "error", err)
	}))
	// The step types the plane hosts itself, BEFORE the scheduler, because the
	// scheduler has to be given them: a `builtin:` reference offered to an
	// engine is a reference no engine can resolve, and for the whole of Tasks
	// 20-51 that is exactly what happened — internal/steps/* and
	// internal/wait were libraries with tests and no caller in the binary.
	built, err := newBuiltins(in, s.cfg.Models, nil, s.log)
	if err != nil {
		in.close()
		return err
	}

	// The fair queue, the per-pipeline budget and the tenant quota. All three
	// were built and tested and NONE of them had a call site, so a fleet ran
	// with dispatch in arrival order, no pipeline cap and no tenant limit:
	// three mechanisms passing their own tests and governing nothing.
	queue, err := scheduler.NewQueue(scheduler.QueueConfig{})
	if err != nil {
		in.close()
		return err
	}
	budgets, err := scheduler.NewBudgets(ctx, in.conn, scheduler.BudgetConfig{
		PlaneID: s.cfg.DeploymentID,
	})
	if err != nil {
		in.close()
		return err
	}
	in.onClose(budgets.Close)

	// The gate step type. A step that WAITS is armed in the transaction that
	// found it ready instead of being dispatched — without this the plane
	// sends `builtin:wait` to an engine that has no idea what it is, and the
	// wait is skipped entirely. NOTE: nothing here runs the durable-timer
	// poll yet, so a gate armed by a plain `dhole serve` waits until whoever
	// polls the timers exists; that is the open item recorded against this
	// task, and the acceptance harness supplies the poll meanwhile.
	waits, err := gate.New(wait.NewTimers(in.store), gate.Options{})
	if err != nil {
		in.close()
		return err
	}

	sched, err := scheduler.New(scheduler.Config{
		Store:       in.store,
		Outbox:      out,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        DefaultTier,
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		// There is deliberately no environment identity here. It belongs to
		// the engines, arrives on their registrations, and is agreed per tier
		// (ADR 0021): a plane reading it off an executor of its own reported
		// none in every shipped configuration, and cached nothing anywhere.
		Log: s.log,
		// The cache, wired to the thing that actually runs steps. It was
		// built, tested and consulted only by Plan — a dry run — so every real
		// run executed every step again while Plan truthfully reported which
		// ones "would" be skipped (ADR 0009).
		Cache:     in.cache,
		Revisions: defs,
		BlobRefs:  in.refs,
		Queue:     queue,
		Budgets:   budgets,
		Quotas:    in.quotas,
		// The plane's own step types. Without this the scheduler offers a
		// durable timer, a human gate, a model call and a bounded loop to
		// engines, none of which can run any of the four.
		Builtins: built,
		Gate:     waits,
	})
	if err != nil {
		in.close()
		return err
	}
	// Closing the loop the other way: an approval that is decided has to
	// advance the run it just unblocked, and the scheduler is what advances
	// runs. It is assigned rather than passed because each needs the other.
	built.resume = sched

	// The ring BEFORE anything advances a run: exactly one plane may advance a
	// given run, so a plane that has not claimed yet must not advance at all.
	parts, err := startPartitioning(ctx, in.conn, s.cfg.DeploymentID)
	if err != nil {
		in.close()
		return err
	}

	s.infra, s.fleet, s.defs, s.out, s.sched, s.leases, s.partitions = in, fleet, defs, out, sched, leases, parts
	s.builtins, s.timers = built, wait.NewTimers(in.store)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel

	if err := s.serve(ctx, runCtx); err != nil {
		cancel()
		s.stopAPI(ctx)
		s.wg.Wait()
		s.closeSubscriptions()
		in.close()
		s.infra = nil
		return err
	}

	s.running = true
	return nil
}

// serve starts everything that runs until Stop. startCtx bounds the
// subscriptions being established; runCtx is what the goroutines live on.
func (s *Server) serve(startCtx, runCtx context.Context) error {
	if err := s.consumeRegistrations(startCtx, runCtx); err != nil {
		return err
	}
	if err := s.consumeStatuses(startCtx, runCtx); err != nil {
		return err
	}

	s.spawn(func() {
		// Drain is what turns an enqueued dispatch into a message. Nothing
		// else in this process publishes one.
		if err := s.out.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("outbox stopped", "error", err)
		}
	})
	// The store and the scheduler are passed in rather than read back off the
	// Server: Stop holds s.mu while it waits for this goroutine to end, so a
	// loop that reached for the lock would deadlock against the shutdown it
	// is supposed to notice.
	s.spawn(func() { s.advanceLoop(runCtx, s.infra.store, s.sched) })
	s.spawn(func() { s.sweepLoop(runCtx, s.sched) })
	s.spawn(func() { s.renewLoop(runCtx) })
	s.spawn(func() { s.timerLoop(runCtx, s.timers, s.sched) })

	// The workers that run the plane's own step types. A POOL rather than a
	// goroutine per step: Stop waits on the WaitGroup these were added to,
	// and an Add from a background goroutine would be racing that Wait.
	built := s.builtins
	for range builtinWorkers {
		s.spawn(func() { built.work(runCtx) })
	}

	// Last, and only in embedded mode: the engine starts once the plane can
	// already hear it. A registration is a fire-and-forget message on a core
	// subject, so an engine that announces itself before anyone is listening
	// is invisible until it restarts — see the note in consumeRegistrations.
	if s.cfg.Mode == ModeEmbedded {
		if err := s.startEngine(runCtx); err != nil {
			return err
		}
	}

	// The triggers BEFORE the contract, because a webhook trigger is served
	// on the contract's own listener and rootHandler mounts what is there
	// when it is built.
	if err := s.startTriggers(startCtx, runCtx); err != nil {
		return err
	}

	// And the contract itself, last: everything it answers about — the
	// definition store, the run log, the scheduler it hands a new run to, the
	// fleet a plan matches against — is already running by the time the first
	// call can arrive.
	return s.startAPI(runCtx)
}

func (s *Server) spawn(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// startEngine runs the hosted engine. It is a normal engine.Agent with a
// normal bus connection: the only thing "embedded" about it is that its
// process happens to also contain a scheduler.
func (s *Server) startEngine(ctx context.Context) error {
	agent, err := engine.New(engine.Config{
		EngineID: "embedded-" + randomID(),
		Tier:     DefaultTier,
		Bus:      s.infra.engineBus,
		Executor: s.infra.exec,
		Blobs:    s.infra.blobs,
		CAS:      s.infra.cas,
		Slots:    runtime.NumCPU(),
	})
	if err != nil {
		return err
	}
	s.spawn(func() {
		if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Error("embedded engine stopped", "error", err)
		}
	})
	return nil
}

// consumeRegistrations keeps the fleet up to date from what engines announce.
//
// ONE subscription, over engine.>, rather than one per subject. A NATS client
// delivers each subscription on its own goroutine, so two subscriptions can
// reorder against each other — and an engine publishes its registration and
// its first heartbeat back to back. Split, the heartbeat routinely overtook
// the registration, was refused as ErrNotRegistered, and the engine stayed
// invisible until its next beat five seconds later. One subscription is one
// delivery goroutine, and the order the engine sent them in survives.
//
// Both subjects are core NATS and best-effort, which is what the wire contract
// describes and which has a consequence worth naming: a registration published
// while no control plane is listening is gone, and Heartbeat deliberately
// refuses to rebuild an instance from a message that cannot describe one. Such
// an engine is invisible until it restarts. The plane logs the refusal rather
// than swallowing it, because the alternative symptom — every step
// unschedulable, forever, with no reason given — is the one this project is
// most afraid of.
func (s *Server) consumeRegistrations(startCtx, runCtx context.Context) error {
	stop, err := s.infra.plane.SubscribeEphemeralOnSubjects(startCtx, "engine.>",
		func(subject string, data []byte) { s.handleEngineMessage(runCtx, subject, data) })
	if err != nil {
		return err
	}
	s.stopSub = append(s.stopSub, stop)
	return nil
}

// handleEngineMessage applies one engine-to-plane message.
//
// The BYTES decide what the message is. An EngineHeartbeat decodes cleanly as
// an EngineRegistration — both start with engine_id, and protobuf cannot tell
// a packed repeated uint32 from a repeated message on the wire — so guessing
// by content once registered engines with no platform and no capabilities, and
// every step after that was unschedulable. Engines now publish inside an
// EngineMessage frame, whose oneof carries the type (docs/wire-contract.md,
// "Message framing").
//
// A frame with no body is an engine speaking the earlier framing, and the
// subject is what its type is recovered from — the control plane accepts
// engines one version behind, and an engine in someone else's network is not
// upgraded on our schedule.
func (s *Server) handleEngineMessage(ctx context.Context, subject string, data []byte) {
	framed, err := wire.DecodeEngineMessage(data)
	if err != nil {
		s.log.Error("undecodable engine message", "subject", subject, "error", err)
		return
	}
	switch {
	case framed.GetRegistration() != nil:
		s.register(ctx, framed.GetRegistration())
	case framed.GetHeartbeat() != nil:
		s.beat(ctx, framed.GetHeartbeat())
	case subject == bus.SubjectEngineRegistration():
		reg := &dholev1.EngineRegistration{}
		if err := proto.Unmarshal(data, reg); err != nil {
			s.log.Error("undecodable engine registration", "error", err)
			return
		}
		s.register(ctx, reg)
	case strings.HasPrefix(subject, "engine.heartbeat."):
		beat := &dholev1.EngineHeartbeat{}
		if err := proto.Unmarshal(data, beat); err != nil {
			s.log.Error("undecodable engine heartbeat", "error", err)
			return
		}
		s.beat(ctx, beat)
	default:
		// engine.control.* is the plane talking to an engine; it is on this
		// pattern only because one subscription is what keeps the other two
		// in order.
	}
}

func (s *Server) register(ctx context.Context, reg *dholev1.EngineRegistration) {
	if err := s.fleet.Register(ctx, reg); err != nil && ctx.Err() == nil {
		s.log.Error("registering engine", "engine", reg.GetEngineId(), "error", err)
	}
}

func (s *Server) beat(ctx context.Context, beat *dholev1.EngineHeartbeat) {
	// The leases FIRST, and whatever the registry then makes of the engine.
	//
	// A heartbeat is the only evidence a step is still being worked on: a
	// lease expires thirty seconds after it is claimed, and the sweeper takes
	// every expired one as an engine that died. Renewing on what the engine
	// says it holds is what stops a step longer than a lease TTL from being
	// re-dispatched out from under the engine running it
	// (docs/wire-contract.md, "Heartbeats and orphans").
	//
	// It happens even for an engine the registry refuses. The refusal is about
	// what the engine can be given NEXT — a heartbeat cannot describe a fleet
	// member — and says nothing about the work it is already holding, which is
	// real whether or not we can schedule to it.
	s.renewHeld(ctx, beat)
	if err := s.fleet.Heartbeat(ctx, beat); err != nil && ctx.Err() == nil {
		logHeartbeatRefusal(s.log, beat.GetEngineId(), err)
	}
}

// logHeartbeatRefusal reports a refused heartbeat at the level it deserves.
//
// An unregistered engine is routine and self-healing: a plane that restarted
// has no record of the engines still running against it, and the wire contract
// obliges each of them to announce itself again — which they do, on their next
// registration interval. Logging it at ERROR made every plane restart produce
// a burst of errors describing a system that was recovering correctly, which
// is how a log stops being read.
//
// Everything else is a real refusal and stays an error.
func logHeartbeatRefusal(log *slog.Logger, engineID string, err error) {
	if errors.Is(err, registry.ErrNotRegistered) {
		log.Info("engine must announce itself again", "engine", engineID)
		return
	}
	log.Error("engine heartbeat refused", "engine", engineID, "error", err)
}

// renewHeld extends the lease behind every job an engine says it is holding.
//
// A fence that is no longer current is not an error and not news: the engine
// has been superseded and is about to find out, and renewing nothing is
// exactly right — its report will be discarded too.
func (s *Server) renewHeld(ctx context.Context, beat *dholev1.EngineHeartbeat) {
	for _, held := range beat.GetInFlight() {
		_, token, err := scheduler.DecodeFence(held.GetFenceToken())
		if err != nil {
			s.log.Error("undecodable fence in a heartbeat",
				"engine", beat.GetEngineId(), "run", held.GetRunId(),
				"step", held.GetStepId(), "error", err)
			continue
		}
		if err := s.leases.Renew(ctx, token); err != nil &&
			!errors.Is(err, lease.ErrFenced) && ctx.Err() == nil {
			s.log.Error("renewing a held lease",
				"engine", beat.GetEngineId(), "run", held.GetRunId(),
				"step", held.GetStepId(), "error", err)
		}
	}
}

// consumeStatuses applies every engine report to the run it belongs to.
//
// The message is acknowledged only after OnStatus returns, so a plane that
// dies mid-apply gets the status again rather than losing the step's result.
// Applying it twice is safe: the scheduler recognises a terminal event it has
// already recorded.
func (s *Server) consumeStatuses(startCtx, runCtx context.Context) error {
	sub, err := s.infra.plane.SubscribePull(startCtx, StatusStream, "plane-status", "job.status.>")
	if err != nil {
		return err
	}
	s.stopSub = append(s.stopSub, func() { _ = sub.Close() })

	s.spawn(func() {
		for {
			msg, err := sub.Next(runCtx)
			if err != nil {
				if runCtx.Err() != nil || errors.Is(err, bus.ErrSubscriptionClosed) {
					return
				}
				s.log.Error("reading job status", "error", err)
				continue
			}
			status := &dholev1.JobStatus{}
			if err := proto.Unmarshal(msg.Data(), status); err != nil {
				// Redelivering bytes that will not parse loops forever.
				s.log.Error("undecodable job status", "error", err)
				_ = msg.Ack()
				continue
			}
			if err := s.sched.OnStatus(runCtx, status); err != nil {
				if runCtx.Err() != nil {
					return
				}
				s.log.Error("applying job status",
					"run", status.GetRunId(), "step", status.GetStepId(), "error", err)
				_ = msg.Nak()
				continue
			}
			_ = msg.Ack()
		}
	})
	return nil
}

// advanceLoop re-advances, on a tick, every run the STORE says is unfinished.
// See advanceInterval for why the tick is here and why the set is not.
func (s *Server) advanceLoop(ctx context.Context, store runstore.Store, sched *scheduler.Scheduler) {
	ticker := time.NewTicker(advanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if s.advanceOpenRuns(ctx, store, sched) {
			return
		}
	}
}

// advanceOpenRuns advances one pass over the open-run index. It reports
// whether the server is shutting down.
//
// Nothing here removes a run from anything. A run leaves the index because the
// scheduler wrote RUN_COMPLETED or RUN_FAILED and the store took it out in the
// same transaction — so the plane holds no opinion of its own about which runs
// are finished, and two planes advancing the same tenant cannot disagree.
func (s *Server) advanceOpenRuns(ctx context.Context, store runstore.Store, sched *scheduler.Scheduler) bool {
	for _, tenantID := range s.tenants() {
		open, err := store.OpenRuns(ctx, tenantID)
		if err != nil {
			if ctx.Err() != nil {
				return true
			}
			s.log.Error("listing open runs", "tenant", tenantID, "error", err)
			continue
		}
		for _, runID := range open {
			// Checked per RUN, not per pass: a plane that loses a partition
			// must stop advancing its runs at the next one.
			if !s.ownsRun(runID) {
				continue
			}
			if err := sched.Advance(ctx, tenantID, runID); err != nil {
				if ctx.Err() != nil {
					return true
				}
				s.log.Error("advancing run", "tenant", tenantID, "run", runID, "error", err)
			}
		}
	}
	return false
}

// tenants is the set of tenants this plane advances runs for.
//
// It is one tenant today because nothing yet writes the tenant register that
// migration 0009 created: `dhole serve` serves DefaultTenant. It is a method
// rather than a constant at the call site because the loop above must iterate
// tenants rather than assume one, so that filling this in from the register is
// a change to this function and to nothing else.
func (s *Server) tenants() []string {
	return []string{DefaultTenant}
}

// sweepLoop expires the leases nobody renewed and records the attempts that
// died with their holders.
//
// It runs on its own tick rather than inside advanceLoop because it is a
// different question: advanceLoop asks what a run can do next, and this asks
// which engines stopped answering. Running them together would tie how quickly
// a dead engine is noticed to how often runs are re-examined.
func (s *Server) sweepLoop(ctx context.Context, sched *scheduler.Scheduler) {
	ticker := time.NewTicker(orphanSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		lost, err := sched.SweepOrphans(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("sweeping orphaned leases", "error", err)
			continue
		}
		if lost > 0 {
			// Worth a line: an engine died holding work, and somebody
			// reading the log after an incident needs to see when.
			s.log.Warn("re-dispatching steps whose engine stopped answering", "steps", lost)
		}
	}
}

// timerLoop is the plane's clock: it fires the durable timers that have come
// due and advances the runs they woke.
//
// It runs here rather than in whatever armed the wait, because a wait is a ROW
// and not a sleeping goroutine (ADR 0003) — the process that scheduled one is
// routinely not the process that fires it, and a plane that ran no poll left
// every outstanding wait outstanding forever. For four tasks the only thing
// that ever ran this was the acceptance harness.
func (s *Server) timerLoop(ctx context.Context, timers *wait.Timers, sched *scheduler.Scheduler) {
	runner := wait.NewRunner(timers, sched, wait.WithErrorHandler(func(err error) {
		s.log.Error("firing durable timers", "error", err)
	}))
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Error("timer poll stopped", "error", err)
	}
}

// Stop ends every goroutine this server started and closes everything it
// opened, in reverse order. It returns only once they are done.
//
// "Only once they are done" is the whole contract. A Stop that returned while
// a drainer was still publishing would make every test that stops a server a
// test of the process exiting, and would leave a second server started
// afterwards racing an outbox nobody remembers owning.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return nil
	}
	s.running = false

	s.cancel()

	// The listener before anything else: a call accepted after this point
	// would be served by stores that are about to close under it.
	s.stopAPI(ctx)

	// Closing the subscriptions unblocks whatever is parked in Next: a pull
	// consumer's fetch does not notice a cancelled context until its wait
	// elapses.
	s.closeSubscriptions()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("server: stop: %w", ctx.Err())
	}

	s.infra.close()
	s.infra = nil
	s.builtins, s.timers, s.triggerMux = nil, nil, nil
	return nil
}

func (s *Server) closeSubscriptions() {
	for _, stop := range s.stopSub {
		stop()
	}
	s.stopSub = nil
}

// DefaultTenant is the tenant a deployment that has not been told otherwise
// uses. It exists so that "single tenant" is one named tenant rather than an
// unscoped path through the code: there is no unscoped record and no unscoped
// subject, even now.
const DefaultTenant = "default"

// Submit records a pipeline as a revision and starts a run of it.
//
// The run pins the revision. Whatever is edited or approved while it is in
// flight, it reads back the definition it began with — which is why the
// RUN_CREATED payload carries the revision id and not just the pipeline's.
func (s *Server) Submit(ctx context.Context, tenantID string, p *dholev1.Pipeline) (string, error) {
	s.mu.Lock()
	running, sched, defs, store := s.running, s.sched, s.defs, s.storeLocked()
	parts := s.partitions
	s.mu.Unlock()
	if !running {
		return "", errors.New("server: not started")
	}
	if tenantID == "" {
		return "", runstore.ErrTenantRequired
	}
	if p == nil {
		return "", errors.New("server: submit: nil pipeline")
	}
	if diags := dag.TypeCheck(p); len(diags) > 0 {
		return "", fmt.Errorf("server: pipeline %q does not type-check: %s", p.GetId(), diagnostics(diags))
	}

	revision, err := defs.Save(ctx, tenantID, p, "system")
	if err != nil {
		return "", err
	}

	// Written through MarshalRunCreated, not by hand: the scheduler reads this
	// payload back to learn which revision the run pinned, and a hand-rolled
	// encoding that drifted from it would leave every run unable to find its
	// own definition.
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: p.GetId(),
		RevisionID: revision.ID,
	})
	if err != nil {
		return "", err
	}

	// The sequence is the store's to allocate: two planes submitting at once
	// would otherwise read the same last position and write the same one.
	runID := "run_" + randomID()
	if err := store.Append(ctx, tenantID, runstore.Event{
		RunID:   runID,
		Type:    runstore.RunCreated,
		Payload: payload,
		At:      time.Now().UTC(),
	}); err != nil {
		return "", err
	}

	// Advancing it here saves a tick of latency when this plane is also the
	// run's owner — and it is skipped when it is not. A plane that advanced a
	// run it does not own, even once, even the moment it created it, is a
	// second writer for that run: the owner is already responsible for it and
	// is about to advance it too.
	if parts != nil && !parts.parts.Owns(runID) {
		return runID, nil
	}
	if err := sched.Advance(ctx, tenantID, runID); err != nil {
		return runID, err
	}
	return runID, nil
}

// Events is one run's log, which is the only place its state lives.
func (s *Server) Events(ctx context.Context, tenantID, runID string) ([]runstore.Event, error) {
	s.mu.Lock()
	store := s.storeLocked()
	s.mu.Unlock()
	if store == nil {
		return nil, errors.New("server: not started")
	}
	return store.Replay(ctx, tenantID, runID)
}

// OpenRuns is every run this tenant has that has not finished, out of the
// store's own index rather than any memory of this process.
//
// It is the same answer the advance loop works from, and it is exposed because
// a caller that did not start a run has no other way to find it: a trigger
// starts runs nobody handed an id to. There is no unscoped form.
func (s *Server) OpenRuns(ctx context.Context, tenantID string) ([]string, error) {
	s.mu.Lock()
	store := s.storeLocked()
	s.mu.Unlock()
	if store == nil {
		return nil, errors.New("server: not started")
	}
	if tenantID == "" {
		return nil, runstore.ErrTenantRequired
	}
	return store.OpenRuns(ctx, tenantID)
}

// CAS is the content-addressed store this deployment reads and writes. It is
// exposed because an engine elsewhere must be pointed at the same one, and
// because a caller reading what a step produced resolves a digest through it.
func (s *Server) CAS() cas.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infra == nil {
		return nil
	}
	return s.infra.cas
}

// BusURL is where an engine dials this deployment. In embedded mode it is the
// in-process server's own address, which is the address the hosted engine used
// too — there is no second, shorter path.
func (s *Server) BusURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infra == nil {
		return ""
	}
	return s.infra.busURL
}

func (s *Server) storeLocked() runstore.Store {
	if s.infra == nil {
		return nil
	}
	return s.infra.store
}

func diagnostics(diags []dag.Diagnostic) string {
	msgs := make([]string, 0, len(diags))
	for _, d := range diags {
		msgs = append(msgs, d.Message)
	}
	return strings.Join(msgs, "; ")
}

// randomID is a short unique suffix. It is random rather than sequential
// because two control planes generate them without talking to each other.
func randomID() string {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		// crypto/rand does not fail on any platform this runs on, and a
		// fabricated id would collide two runs into one log.
		panic("server: reading random bytes: " + err.Error())
	}
	return hex.EncodeToString(raw)
}
