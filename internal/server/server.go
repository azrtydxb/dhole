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
// position. Advance reads the log and writes the log; the only thing kept in
// memory is a set of run ids to poll, and even that is a workaround noted in
// runs() below.
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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
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

// advanceInterval is how often every live run is re-advanced.
//
// A poll is not elegant and it is not free, and it is here because Advance is
// otherwise driven only by an arriving status: a run whose step could not be
// placed when it was submitted — no engine registered yet, the usual case one
// second after start-up — would sit unscheduled forever, because nothing else
// would ever ask again.
const advanceInterval = 250 * time.Millisecond

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
	// BlobRoot is the directory the content-addressed store, the blob store
	// and the embedded bus keep their data under.
	BlobRoot string
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
	defs    defstore.Store
	out     *outbox.Outbox
	fleet   *registry.KV

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopSub []func()

	// live is the set of runs this process polls. See runs().
	liveMu sync.Mutex
	live   map[runKey]struct{}
}

// runKey identifies a run. The tenant is part of it because there is no
// unscoped anything, even while only one tenant exists.
type runKey struct {
	tenantID string
	runID    string
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
	return &Server{
		cfg:  cfg,
		log:  slog.Default(),
		live: map[runKey]struct{}{},
	}, nil
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
	out := outbox.New(in.store, in.plane, outbox.WithErrorHandler(func(err error) {
		s.log.Error("outbox drain failed", "error", err)
	}))
	sched, err := scheduler.New(scheduler.Config{
		Store:       in.store,
		Outbox:      out,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        DefaultTier,
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		EnvIdentity: in.envIdentity,
	})
	if err != nil {
		in.close()
		return err
	}

	s.infra, s.fleet, s.defs, s.out, s.sched = in, fleet, defs, out, sched

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel

	if err := s.serve(ctx, runCtx); err != nil {
		cancel()
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
	s.spawn(func() { s.advanceLoop(runCtx) })

	// Last, and only in embedded mode: the engine starts once the plane can
	// already hear it. A registration is a fire-and-forget message on a core
	// subject, so an engine that announces itself before anyone is listening
	// is invisible until it restarts — see the note in consumeRegistrations.
	if s.cfg.Mode == ModeEmbedded {
		return s.startEngine(runCtx)
	}
	return nil
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
// The SUBJECT decides what the bytes are, and it has to: an EngineHeartbeat
// decodes cleanly as an EngineRegistration — both start with engine_id, and
// protobuf cannot tell a packed repeated uint32 from a repeated message on the
// wire — so guessing by content silently registered engines with no platform
// and no capabilities, and every step after that was unschedulable.
func (s *Server) handleEngineMessage(ctx context.Context, subject string, data []byte) {
	switch {
	case subject == bus.SubjectEngineRegistration():
		reg := &dholev1.EngineRegistration{}
		if err := proto.Unmarshal(data, reg); err != nil {
			s.log.Error("undecodable engine registration", "error", err)
			return
		}
		if err := s.fleet.Register(ctx, reg); err != nil && ctx.Err() == nil {
			s.log.Error("registering engine", "engine", reg.GetEngineId(), "error", err)
		}
	case strings.HasPrefix(subject, "engine.heartbeat."):
		beat := &dholev1.EngineHeartbeat{}
		if err := proto.Unmarshal(data, beat); err != nil {
			s.log.Error("undecodable engine heartbeat", "error", err)
			return
		}
		if err := s.fleet.Heartbeat(ctx, beat); err != nil && ctx.Err() == nil {
			s.log.Error("engine heartbeat refused", "engine", beat.GetEngineId(), "error", err)
		}
	default:
		// engine.control.* is the plane talking to an engine; it is on this
		// pattern only because one subscription is what keeps the other two
		// in order.
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

// advanceLoop re-advances every live run on a tick. See advanceInterval for
// why polling is here at all.
func (s *Server) advanceLoop(ctx context.Context) {
	ticker := time.NewTicker(advanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, key := range s.runs() {
			if err := s.sched.Advance(ctx, key.tenantID, key.runID); err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("advancing run", "run", key.runID, "error", err)
				continue
			}
			done, err := s.completed(ctx, key)
			if err == nil && done {
				s.forget(key)
			}
		}
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

	runID := "run_" + randomID()
	sequence, err := store.LastSequence(ctx, tenantID)
	if err != nil {
		return "", fmt.Errorf("server: reading sequence for %s: %w", tenantID, err)
	}
	if err := store.Append(ctx, tenantID, runstore.Event{
		RunID:    runID,
		Sequence: sequence + 1,
		Type:     runstore.RunCreated,
		Payload:  payload,
		At:       time.Now().UTC(),
	}); err != nil {
		return "", err
	}

	key := runKey{tenantID: tenantID, runID: runID}
	s.liveMu.Lock()
	s.live[key] = struct{}{}
	s.liveMu.Unlock()

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

// runs is the set of runs this process re-advances.
//
// It is in memory, and that is a gap rather than a design: runstore has no way
// to enumerate a tenant's unfinished runs — Replay needs a run id you already
// have — so a control plane that restarts cannot rediscover a run that is
// waiting on nothing but somebody asking it again. A run whose steps are all
// in flight recovers anyway, because the engine's status arrives on a durable
// subject and drives Advance from there; a run that was stuck unschedulable
// does not. Closing this needs an index of open runs in the store.
func (s *Server) runs() []runKey {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	out := make([]runKey, 0, len(s.live))
	for key := range s.live {
		out = append(out, key)
	}
	return out
}

func (s *Server) forget(key runKey) {
	s.liveMu.Lock()
	delete(s.live, key)
	s.liveMu.Unlock()
}

func (s *Server) completed(ctx context.Context, key runKey) (bool, error) {
	events, err := s.Events(ctx, key.tenantID, key.runID)
	if err != nil {
		return false, err
	}
	for _, e := range events {
		if e.Type == runstore.RunCompleted {
			return true, nil
		}
	}
	return false, nil
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
