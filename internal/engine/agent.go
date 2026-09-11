// Package engine is the agent that runs steps: the outbound-only half of the
// wire contract in docs/wire-contract.md.
//
// Three rules from that document shape everything here and are the reason the
// code is not simpler than it is.
//
// A dispatch is acknowledged ONLY after its terminal status is published. Ack
// first and a crash in between loses the step in silence: nothing is left on
// the bus and no status ever arrived, so the plane has no evidence anything
// went wrong until the lease expires.
//
// The fence token is echoed unchanged on every status and every in-flight
// entry. It is what lets the plane discard a report from an attempt that has
// already been superseded, which is what makes duplicate delivery safe.
//
// A step's output has two copies with different jobs. The LogChunks on
// job.logs.* are live, ephemeral and droppable. The object under the dispatch's
// output_prefix is the authoritative one, and it is finished BEFORE the
// terminal status goes out — a reader that acts on the status must never find a
// half-written log.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/obs"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/wire"
)

// DispatchStream names the work-queue stream the control plane publishes this
// engine's tier's dispatches into. An engine only ever binds a consumer on it:
// engine credentials cannot create a stream, and an engine that could would be
// able to reshape the plane's queue.
//
// It is per TIER, not one stream for the fleet. A consumer is addressed by
// `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>`, and a permission cannot
// narrow the consumer NAME — a wildcard matches a whole token — so while every
// tier shared one `DISPATCH`, an untrusted connection that guessed the name
// `engines-trusted-<caps>` pulled trusted work off the consumer a trusted
// engine had created. The tier has to be in the stream token for the
// permission to see it.
func DispatchStream(tier string) (string, error) {
	return bus.DispatchStreamName(tier)
}

// subscribeRetry is how long Run waits between attempts to bind its consumers.
//
// There is deliberately no deadline. An engine may start before the control
// plane has created the stream, and this retry exists so start-up ordering is
// not load-bearing — but it used to give up after a minute, which made the
// ordering load-bearing again for any plane slower than that. On a real
// cluster a crash-looping bus took ninety seconds, and the engine then sat
// registered, heartbeating and reported `ready` with no consumer: every step
// dispatched to its tier waited out a lease and was re-dispatched, forever.
//
// Retrying without end is the safe direction. The work queue holds the
// dispatches meanwhile, so an engine that binds late still does the work, and
// an engine that never binds says so on every attempt instead of going quiet.
const subscribeRetry = 500 * time.Millisecond

// bindTimeout bounds ONE subscribe round trip — not the retrying above. The
// bus confirms a subscription with a round trip and refuses a context that
// could wait forever, while an engine's own context has no deadline because an
// engine runs until it is stopped.
const bindTimeout = 60 * time.Second

// complainEvery is how often an engine repeats that it still cannot bind.
const complainEvery = 30 * time.Second

// slotYield bounds how long one dispatch consumer may hold a slot while it is
// only WAITING for work.
//
// An engine binds one consumer per capability subset it can serve, and each
// takes a slot before fetching so it never holds an unacknowledged message it
// has no room to run. Held for the whole wait, that starves every consumer
// beyond the slot count permanently: an engine with one slot and two
// capability sets fetched from the first set forever and never once looked at
// the second, so steps on that subject sat in the work queue with a warm idle
// engine subscribed to them and nothing anywhere reporting a fault. It became
// reachable the moment an engine advertised a capability at all — before that
// there was one subset, one consumer, and nothing to starve.
//
// Yielding turns starvation into a turn each. The cost is latency when slots
// are scarce, and only then: a consumer that holds a slot and gets a message
// keeps it for the job.
const slotYield = 3 * time.Second

// releaseTimeout bounds sandbox teardown. It runs on a context detached from
// the job's, because a cancelled job still has to leave nothing behind.
const releaseTimeout = 30 * time.Second

// Config is everything an agent needs. There is no callback into the control
// plane anywhere in it: a JobDispatch is self-contained, and the stores it
// names are reached directly.
type Config struct {
	// EngineID is this engine's stable identity on the bus.
	EngineID string
	// Tier is the trust tier it runs in. Its bus credentials permit its own
	// tier's dispatch subjects and nothing else; this field only decides which
	// of them it asks for.
	Tier string
	// Bus is the transport. The engine dials it; nothing dials the engine.
	Bus bus.Bus
	// Executor is where steps actually run.
	Executor executor.Executor
	// Blobs holds the authoritative log of every attempt.
	Blobs blobstore.Store
	// CAS holds the inputs a step declares and the outputs it produces.
	CAS cas.Store
	// Slots is how many jobs this engine runs at once.
	Slots int
	// Secrets redeems the SecretRefs a dispatch carries. Nil means this engine
	// has no way to redeem: it then does not advertise CAPABILITY_SECRETS, and
	// it refuses a dispatch that carries a secret anyway.
	//
	// It is here rather than on the Executor because redeeming a short-lived
	// reference is something the AGENT does, over the bus it already dialled,
	// before any sandbox exists. NETWORK, PRIVILEGED and HOST_MOUNT are
	// isolation guarantees a sandbox backend either can or cannot make, and
	// sourcing SECRETS from the same place meant no shipped backend advertised
	// it — so every dispatch carrying a secret was refused by every engine,
	// always, and nothing in the product could redeem anything.
	Secrets secrets.Redeemer

	// SubscribeBackoff is how long to wait between attempts to bind a dispatch
	// consumer. Zero means subscribeRetry; a test sets it small so it can
	// exercise many attempts without waiting for them.
	SubscribeBackoff time.Duration
}

// Agent is one running engine.
type Agent struct {
	cfg      Config
	registry *registryClient
	// slots is the concurrency bound, taken before a dispatch is fetched so
	// the engine never holds an unacknowledged message it has no room to run.
	slots chan struct{}
	// jobs is what is running right now, so a Cancel on this engine's control
	// subject can reach the sandbox rather than only the bookkeeping.
	jobs *running
}

// New validates cfg and returns an agent that has not started.
func New(cfg Config) (*Agent, error) {
	switch {
	case cfg.EngineID == "":
		return nil, errors.New("engine: EngineID is required")
	case cfg.Tier == "":
		return nil, errors.New("engine: Tier is required")
	case cfg.Bus == nil:
		return nil, errors.New("engine: Bus is required")
	case cfg.Executor == nil:
		return nil, errors.New("engine: Executor is required")
	case cfg.Blobs == nil:
		return nil, errors.New("engine: Blobs is required")
	case cfg.CAS == nil:
		return nil, errors.New("engine: CAS is required")
	}
	if cfg.Slots <= 0 {
		cfg.Slots = 1
	}
	// A kind is one subject token and one durable-consumer name segment. A
	// kind carrying a dot, a wildcard or a space would silently widen, split
	// or break the route this engine binds — so it is refused here, loudly, at
	// the one place a backend's kind enters the bus.
	if kind := cfg.Executor.Kind(); !validKindToken(kind) {
		return nil, fmt.Errorf("engine: executor kind %q is not a usable subject token", kind)
	}
	registry, err := newRegistryClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Agent{
		cfg:      cfg,
		registry: registry,
		slots:    make(chan struct{}, cfg.Slots),
		jobs:     newRunning(),
	}, nil
}

// Run registers the engine, heartbeats, and works dispatches until ctx is done.
// It returns nil on a clean shutdown; a cancelled context is how an engine is
// asked to stop, not an error to report.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.registry.register(ctx); err != nil {
		return err
	}

	hashes, err := satisfiableCapsHashes(advertisedCapabilities(a.cfg))
	if err != nil {
		return err
	}

	// The inbound path, before any dispatch is pulled: an engine that took
	// work before it could be told to stop has a window in which a cancel is
	// published to nobody.
	stopControl, err := a.watchControl(ctx)
	if err != nil {
		return err
	}
	defer stopControl()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.registry.run(ctx)
	}()

	stream, err := DispatchStream(a.cfg.Tier)
	if err != nil {
		return err
	}

	queues := a.workQueues(hashes)

	subs := make([]bus.Subscription, 0, len(queues))
	defer func() {
		for _, sub := range subs {
			_ = sub.Close()
		}
	}()

	errs := make(chan error, len(queues))
	for _, q := range queues {
		sub, err := a.subscribe(ctx, stream, q.consumer, q.subject)
		if err != nil {
			wg.Wait()
			return err
		}
		subs = append(subs, sub)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.pump(ctx, sub); err != nil {
				errs <- err
			}
		}()
	}

	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}
	return nil
}

// workQueue is one durable consumer this engine pulls from: a subject and the
// name of the queue every engine eligible for that subject shares.
type workQueue struct {
	consumer string
	subject  string
}

// workQueues is every queue this engine takes work from: for each capability
// set it can serve, the tier's UNRESTRICTED dispatch subject and the subject
// naming this engine's own executor kind.
//
// Both, and on separate consumers, because of what kw proved: a step naming
// engine_type vm, in a tier holding a vm-backed and a kubernetes-backed
// engine, ran on the kubernetes one. Every engine in the tier pulled the one
// job.dispatch.<tier>.<caps> queue and whichever grabbed the message first ran
// it, so the scheduler's filter decided whether the step COULD be placed and
// had no say in where it went.
//
// A second FILTER SUBJECT on the existing consumer would not do: that consumer
// is named for the tier and capability set alone and is therefore SHARED by
// every engine in the tier, so widening it would hand kind-targeted work to
// engines of the wrong kind — the bug, rebuilt. The kind's queue needs a name
// of its own, which only engines of that kind bind.
func (a *Agent) workQueues(hashes []string) []workQueue {
	kind := a.cfg.Executor.Kind()
	queues := make([]workQueue, 0, 2*len(hashes))
	for _, hash := range hashes {
		queues = append(queues, workQueue{
			consumer: "engines-" + a.cfg.Tier + "-" + hash,
			subject:  bus.SubjectDispatch(a.cfg.Tier, hash),
		})
		if kind == "" {
			continue
		}
		queues = append(queues, workQueue{
			// A durable name may not contain a dot, and a kind that carried
			// one would not be a single subject token either — New refuses it
			// rather than binding a queue nobody publishes to.
			consumer: "engines-" + a.cfg.Tier + "-" + hash + "-" + kind,
			subject:  bus.SubjectDispatchKind(a.cfg.Tier, hash, kind),
		})
	}
	return queues
}

// subscribe binds the durable consumer named consumer. The name is the tier,
// the capability set and — for a kind's queue — the executor kind, rather than
// this engine's id, which is what makes the dispatch subject a work queue:
// every engine that can serve it pulls from the same consumer, and exactly one
// of them gets each message.
func (a *Agent) subscribe(ctx context.Context, stream, consumer, subject string) (bus.Subscription, error) {
	backoff := a.cfg.SubscribeBackoff
	if backoff <= 0 {
		backoff = subscribeRetry
	}
	var lastComplaint time.Time
	for attempt := 1; ; attempt++ {
		sub, err := a.cfg.Bus.SubscribePull(ctx, stream, consumer, subject)
		if err == nil {
			return sub, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Said once immediately, then occasionally. An engine that cannot bind
		// takes no work, so silence is wrong — but a line every backoff for as
		// long as the engine runs is its own outage, and the operator stops
		// reading before the useful line arrives.
		if attempt == 1 || time.Since(lastComplaint) >= complainEvery {
			slog.Warn("cannot bind the dispatch consumer yet; retrying",
				"stream", stream, "subject", subject, "consumer", consumer,
				"attempt", attempt, "error", err)
			lastComplaint = time.Now()
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
}

// pump takes one dispatch at a time, having first taken a slot. Fetching only
// what it can run keeps the unacknowledged set as small as the work actually in
// progress — whatever this engine holds when it dies is what has to be
// redelivered.
func (a *Agent) pump(ctx context.Context, sub bus.Subscription) error {
	var running sync.WaitGroup
	defer running.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case a.slots <- struct{}{}:
		}

		// Bounded, so the slot is given back to the other consumers if
		// nothing arrives. A fetch that times out has taken no message off the
		// stream, so there is nothing to lose by abandoning it.
		waitCtx, waited := context.WithTimeout(ctx, slotYield)
		msg, err := sub.Next(waitCtx)
		waited()
		if err != nil {
			<-a.slots
			if ctx.Err() != nil || errors.Is(err, bus.ErrSubscriptionClosed) {
				return nil
			}
			if waitCtx.Err() != nil {
				continue
			}
			return fmt.Errorf("engine: next dispatch: %w", err)
		}

		running.Add(1)
		go func() {
			defer running.Done()
			defer func() { <-a.slots }()
			a.handle(ctx, msg)
		}()
	}
}

// handle takes one dispatch from delivery to acknowledgement.
func (a *Agent) handle(ctx context.Context, msg bus.Message) {
	var d dholev1.JobDispatch
	if err := proto.Unmarshal(msg.Data(), &d); err != nil {
		// There is no run or step to report against, and redelivering bytes
		// that will not parse would loop forever. Acknowledging drops exactly
		// one unparseable message rather than stalling the whole queue.
		_ = msg.Ack()
		return
	}

	// The span continues the RUN's trace, taken from the dispatch itself: the
	// scheduler that sent this message is in another process, and a span
	// started from a root here would be a second, disconnected trace of the
	// same run (docs/wire-contract.md, "Trace context").
	ctx, span := obs.StepSpan(obs.ContextFrom(ctx, &d), d.GetRunId(), d.GetStepId())
	outcome, stepErr := obs.OutcomeFailed, error(nil)
	// Deferred, so the span is ended on every path out of here — a failing
	// step, a cancelled one, a panic. An unended span is never exported at
	// all, which would lose exactly the attempts anyone goes looking for.
	defer func() { obs.EndStepSpan(span, outcome, stepErr) }()

	// An engine that says nothing is indistinguishable from an engine that is
	// not being given work, and the two have opposite fixes. Both ends of the
	// attempt are logged so a `kubectl logs` answers "is it working?" without
	// decoding a run event out of the database.
	slog.Info("step accepted",
		"engine", a.cfg.EngineID, "run", d.GetRunId(), "step", d.GetStepId(),
		"attempt", d.GetAttempt(), "tenant", d.GetTenant().GetId())

	started := time.Now()
	status := a.run(ctx, &d)
	outcome, stepErr = outcomeOf(status)
	a.logOutcome(&d, status, outcome, stepErr, time.Since(started))
	// cache_hit is false here and can only be false here: a step served from
	// the cache is never dispatched to an engine at all, so every step this
	// process sees really executed. The true side of the label is recorded by
	// the scheduler, which is where a hit is served (ADR 0009) — until it was,
	// the label had one value in the whole system and could not tell a cold
	// build from a slow one, which is the comparison it exists to make.
	obs.RecordStepDuration(ctx, d.GetTenant().GetId(), time.Since(started), false, outcome)

	// The ordering rule, and the reason this function is not shorter: the
	// terminal status goes out first, and only a successful publish earns the
	// ack. A publish that fails leaves the dispatch outstanding, so it is
	// redelivered rather than lost.
	if err := a.publish(ctx, &d, status); err != nil {
		return
	}
	_ = msg.Ack()
}

// logOutcome reports how an attempt ended, once, at a level that matches it.
//
// A failure logs the message the status carries rather than a summary of it:
// that string is the whole diagnosis for anyone reading the engine's log, and
// the alternative — "step failed" with the reason only in the run event — is
// what made a missing /bin/sh take a database query to find.
func (a *Agent) logOutcome(d *dholev1.JobDispatch, status *dholev1.JobStatus, outcome string, stepErr error, took time.Duration) {
	attrs := []any{
		"engine", a.cfg.EngineID, "run", d.GetRunId(), "step", d.GetStepId(),
		"attempt", d.GetAttempt(), "outcome", outcome, "took", took,
		"exit_code", status.GetExitCode(),
	}
	if msg := status.GetError(); msg != "" {
		attrs = append(attrs, "error", msg)
	} else if stepErr != nil {
		attrs = append(attrs, "error", stepErr)
	}
	if stepErr != nil {
		slog.Error("step finished", attrs...)
		return
	}
	slog.Info("step finished", attrs...)
}

// run does the work and returns the terminal status for it. Every path through
// it returns a status: silence would be indistinguishable from a dead engine,
// and the step would hang until its lease expired.
func (a *Agent) run(ctx context.Context, d *dholev1.JobDispatch) *dholev1.JobStatus {
	if _, err := wire.Negotiate([]uint32{d.GetProtocolVersion()}); err != nil {
		return a.failure(d, err.Error())
	}
	if d.GetTenant().GetId() == "" {
		return a.failure(d, "dispatch carries no tenant; every stored object is tenant-scoped")
	}
	if err := a.checkSecrets(d); err != nil {
		return a.failure(d, err.Error())
	}
	if len(d.GetCommand()) == 0 {
		return a.failure(d, "dispatch carries no command")
	}

	// From here the engine owns the step, so it says so before running it and
	// declares it on every heartbeat until the terminal status.
	key := a.registry.hold(d)
	defer a.registry.release(key)

	// A context of this job's own, so a Cancel naming this attempt stops this
	// attempt and nothing else. It is derived from the pump's context, so a
	// shutdown still stops everything.
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	a.jobs.add(key, d.GetFenceToken(), cancel)
	defer a.jobs.remove(key)

	if err := a.publish(ctx, d, a.phase(d, dholev1.Phase_PHASE_ACCEPTED)); err != nil {
		return a.failure(d, "publishing the accepted status: "+err.Error())
	}

	status := a.execute(jobCtx, d)
	// A job whose own context ended while the engine is still running is one
	// somebody cancelled, and it is reported as cancelled rather than as a
	// failure: a failure would be retried by its effect class, which is the
	// opposite of what was asked for. The publish below uses the OUTER
	// context, which is still live.
	if jobCtx.Err() != nil && ctx.Err() == nil {
		return a.phase(d, dholev1.Phase_PHASE_CANCELLED)
	}
	return status
}

// sandboxCapabilities drops the members a sandbox backend cannot answer for,
// leaving what it genuinely decides: the isolation guarantees of the sandbox
// itself.
//
// It is the mirror of advertisedCapabilities, which adds SECRETS on the
// strength of the agent holding a redeemer rather than the backend claiming
// anything. The two must stay mirrors: a member that is advertised by the
// agent and demanded of the backend is a step no engine can run.
func sandboxCapabilities(want []dholev1.Capability) []dholev1.Capability {
	out := make([]dholev1.Capability, 0, len(want))
	for _, c := range want {
		if c == dholev1.Capability_CAPABILITY_SECRETS {
			continue
		}
		out = append(out, c)
	}
	return out
}

// checkSecrets refuses a secret this engine could not redeem. The refusal names
// the binding, never the handle: a status is durable and archived, and a handle
// in one is a credential at rest in the run history.
//
// It asks the same thing the registration advertises — whether this agent holds
// a redeemer — rather than asking the executor. Asking the executor was the
// original bug in both directions at once: no backend advertised the
// capability, so this refused every dispatch carrying a secret; and the fix
// that suggests itself, teaching the process backend to advertise it, would
// have put an agent property back in the sandbox component with the answer
// merely inverted.
func (a *Agent) checkSecrets(d *dholev1.JobDispatch) error {
	if len(d.GetSecrets()) == 0 {
		return nil
	}
	if a.cfg.Secrets != nil {
		return nil
	}
	names := make([]string, 0, len(d.GetSecrets()))
	for _, s := range d.GetSecrets() {
		names = append(names, s.GetName())
	}
	return fmt.Errorf("dispatch carries secrets %v but this engine has no redemption endpoint "+
		"and does not advertise CAPABILITY_SECRETS", names)
}

// redeem exchanges every SecretRef the dispatch carries for its value and
// returns the environment the step runs with: the dispatch's own env plus one
// binding per secret.
//
// The result goes to Exec and to nothing else. In particular it does not go
// into the executor.Spec handed to Acquire: the Kubernetes backend turns that
// into a pod template the API server keeps, which would be the exact
// secret-at-rest this whole design exists to avoid — in a store nothing in this
// repository controls.
func (a *Agent) redeem(ctx context.Context, d *dholev1.JobDispatch) (map[string]string, error) {
	env := make(map[string]string, len(d.GetEnv())+len(d.GetSecrets()))
	for k, v := range d.GetEnv() {
		env[k] = v
	}
	for _, ref := range d.GetSecrets() {
		value, err := a.cfg.Secrets.Redeem(ctx, ref)
		if err != nil {
			// Wrapped, not re-worded. The redeemer's error already names the
			// binding and nothing else, and this string is about to be
			// published in a JobStatus and written into the run's event log.
			return nil, fmt.Errorf("redeeming a secret reference: %w", err)
		}
		env[ref.GetName()] = value
	}
	return env, nil
}

// execute acquires a sandbox, materialises the declared inputs, runs the
// command, finishes the authoritative log, and collects the declared outputs.
func (a *Agent) execute(ctx context.Context, d *dholev1.JobDispatch) *dholev1.JobStatus {
	tenantID := d.GetTenant().GetId()

	// The dispatch's OWN env, with no redeemed value in it. A backend is free
	// to persist a Spec — Kubernetes writes one into a pod template the API
	// server keeps — so a secret placed here would outlive the step in a store
	// this repository does not control. Secrets reach the process through
	// Cmd.Env below and nowhere else.
	sandbox, err := a.cfg.Executor.Acquire(ctx, executor.Spec{
		// The pipeline's image, not the backend's default. A backend with no
		// notion of an image ignores it, which is not an error (ADR 0006).
		Image: d.GetStep().GetImage(),
		Env:   d.GetEnv(),
		Lease: leaseScopeFrom(d.GetStep().GetLeaseScope()),
		Requirements: executor.Requirements{
			// SANDBOX capabilities only. CAPABILITY_SECRETS describes this
			// AGENT — it redeems a reference over the bus, in this process,
			// before any sandbox exists — and asking a backend for it asks a
			// question the backend has no way to answer yes to.
			//
			// Found on kw by running the conformance suite against the vm
			// executor: "vm executor: CAPABILITY_SECRETS: capability not
			// advertised by this backend (it advertises
			// [CAPABILITY_PRIVILEGED])". The vm and containerd backends refuse
			// what they do not advertise; process and kubernetes do not check,
			// so the same step passed everywhere the suite had been run and
			// failed on the two backends it had not.
			Capabilities: sandboxCapabilities(d.GetStep().GetCapabilities()),
		},
	})
	if err != nil {
		return a.failure(d, "acquiring a sandbox: "+err.Error())
	}
	defer func() {
		// Detached from ctx on purpose: a cancelled job still has to leave
		// nothing running on the host.
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		_ = sandbox.Release(release)
	}()

	// Both port directories exist before the command runs, whether or not the
	// step declared anything. A step whose command is `... > outputs/copy`
	// exited 1 with "No such file or directory" when nothing created outputs/
	// first: Put creates the parents of a file it is given, and an output port
	// has no file until the step writes one.
	for _, dir := range []string{inputDir, outputDir} {
		if err := sandbox.Mkdir(ctx, dir); err != nil {
			return a.failure(d, fmt.Sprintf("creating the %s directory: %s", dir, err))
		}
	}

	if err := a.materialise(ctx, tenantID, sandbox, d); err != nil {
		return a.failure(d, err.Error())
	}

	// Redeemed at the moment they are needed, and after the sandbox exists so
	// a value is never held across an acquisition that might block. Before the
	// log sink, so a refusal produces no half-written authoritative log.
	stepEnv := d.GetEnv()
	if len(d.GetSecrets()) > 0 {
		var err error
		if stepEnv, err = a.redeem(ctx, d); err != nil {
			return a.failure(d, err.Error())
		}
	}

	sink, err := newLogSink(ctx, a.cfg.Bus, d)
	if err != nil {
		return a.failure(d, "opening the log: "+err.Error())
	}
	defer sink.discard()

	// The step's own bound, from Step.timeout_seconds (protocol version 3).
	// An engine that enforces nothing holds its slot and renews its lease for
	// the step's full runtime, so a runaway step is indistinguishable from a
	// slow one and nothing ever reclaims the capacity. Zero means unbounded,
	// which is what every pipeline written before the field carries.
	execCtx := ctx
	timeout := time.Duration(d.GetStep().GetTimeoutSeconds()) * time.Second
	if timeout > 0 {
		var cancelExec context.CancelFunc
		execCtx, cancelExec = context.WithTimeout(ctx, timeout)
		defer cancelExec()
	}

	exitCode, execErr := sandbox.Exec(execCtx, executor.Cmd{
		Args:   d.GetCommand(),
		Env:    stepEnv,
		Stdout: sink.writer(dholev1.Stream_STREAM_STDOUT),
		Stderr: sink.writer(dholev1.Stream_STREAM_STDERR),
	})

	// What the step's process tree actually consumed, straight from the
	// sandbox that ran it. A backend that cannot measure reports nothing
	// rather than a zero (see obs.RecordStepUsage).
	obs.RecordStepUsage(ctx, tenantID, sandbox)

	// The authoritative copy is finished here, before any terminal status can
	// be published. Anyone who reads log_key off a status finds the whole log.
	logKey := logKeyFor(d)
	if err := sink.flush(ctx, a.cfg.Blobs, tenantID, logKey); err != nil {
		return a.failure(d, "writing the authoritative log: "+err.Error())
	}

	// A step the engine killed at its timeout is a FAILURE, not a
	// cancellation: a cancellation says an operator asked for this and is not
	// retried, and a step that ran out of time is exactly the kind that its
	// effect class may want retried. The check is here, after the log is
	// flushed, so the evidence of the timed-out attempt is durable before its
	// status is published. ctx.Err() distinguishes this engine's own deadline
	// from a shutdown or a Cancel, which the caller reports for itself.
	if timeout > 0 && errors.Is(execCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		status := a.failure(d, fmt.Sprintf("step timed out after %s", timeout))
		status.ExitCode = killedExitCode
		status.LogKey = logKey
		return status
	}

	if execErr != nil {
		status := a.failure(d, "running the step: "+execErr.Error())
		status.LogKey = logKey
		return status
	}

	status := a.phase(d, dholev1.Phase_PHASE_SUCCEEDED)
	status.ExitCode = exitCode
	status.LogKey = logKey
	if exitCode != 0 {
		status.Phase = dholev1.Phase_PHASE_FAILED
		status.Error = fmt.Sprintf("step exited %d", exitCode)
		return status
	}

	outputs, err := a.collect(ctx, tenantID, sandbox, d)
	if err != nil {
		status.Phase = dholev1.Phase_PHASE_FAILED
		status.Error = err.Error()
		return status
	}
	status.Outputs = outputs
	return status
}

// materialise puts every declared input into the sandbox at inputs/<port>. A
// step never inherits ambient filesystem state (ADR 0001): what it can read is
// exactly what it declared.
func (a *Agent) materialise(ctx context.Context, tenantID string, sandbox executor.Sandbox, d *dholev1.JobDispatch) error {
	for _, in := range d.GetInputs() {
		r, err := a.open(ctx, tenantID, in)
		if err != nil {
			return fmt.Errorf("fetching input %q: %w", in.GetPort(), err)
		}
		err = sandbox.Put(ctx, path.Join(inputDir, in.GetPort()), r)
		closeErr := r.Close()
		if err != nil {
			return fmt.Errorf("placing input %q: %w", in.GetPort(), err)
		}
		if closeErr != nil {
			return fmt.Errorf("reading input %q: %w", in.GetPort(), closeErr)
		}
	}
	return nil
}

// open resolves one input reference. Content-addressed bytes come from the CAS;
// bytes the plane named itself come from the blob store.
func (a *Agent) open(ctx context.Context, tenantID string, in *dholev1.InputRef) (io.ReadCloser, error) {
	if in.GetDigest().GetHex() != "" {
		return a.cfg.CAS.Get(ctx, tenantID, in.GetDigest())
	}
	if in.GetKey() != "" {
		return a.cfg.Blobs.Read(ctx, tenantID, in.GetKey())
	}
	return nil, errors.New("input reference names neither a digest nor a key")
}

// collect stores every declared output by content and reports its digest. An
// output left in the sandbox would vanish with it, and the next step cannot see
// another engine's host.
func (a *Agent) collect(ctx context.Context, tenantID string, sandbox executor.Sandbox, d *dholev1.JobDispatch) ([]*dholev1.OutputRef, error) {
	ports := d.GetStep().GetOutputs()
	outputs := make([]*dholev1.OutputRef, 0, len(ports))
	for _, port := range ports {
		r, err := sandbox.Get(ctx, path.Join(outputDir, port.GetName()))
		if err != nil {
			return nil, fmt.Errorf("reading output %q: %w", port.GetName(), err)
		}
		counted := &countingReader{r: r}
		digest, putErr := a.cfg.CAS.Put(ctx, tenantID, counted)
		closeErr := r.Close()
		if putErr != nil {
			return nil, fmt.Errorf("storing output %q: %w", port.GetName(), putErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("reading output %q: %w", port.GetName(), closeErr)
		}
		outputs = append(outputs, &dholev1.OutputRef{
			Port:      port.GetName(),
			Digest:    digest,
			SizeBytes: counted.n,
		})
	}
	return outputs, nil
}

// publish sends one status on the step's status subject.
func (a *Agent) publish(ctx context.Context, d *dholev1.JobDispatch, status *dholev1.JobStatus) error {
	// Detached from the job's context: a shutdown or a cancelled step must
	// still be reported, or the plane learns nothing until the lease expires.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	return a.cfg.Bus.Publish(ctx, bus.SubjectStatus(d.GetRunId(), d.GetStepId()), status)
}

// phase builds a status for this dispatch, echoing the fence token unchanged.
// Every status in this package is built here so there is one place a fence can
// be got wrong instead of six.
func (a *Agent) phase(d *dholev1.JobDispatch, p dholev1.Phase) *dholev1.JobStatus {
	return &dholev1.JobStatus{
		RunId:      d.GetRunId(),
		StepId:     d.GetStepId(),
		Attempt:    d.GetAttempt(),
		FenceToken: d.GetFenceToken(),
		Phase:      p,
	}
}

func (a *Agent) failure(d *dholev1.JobDispatch, message string) *dholev1.JobStatus {
	status := a.phase(d, dholev1.Phase_PHASE_FAILED)
	status.Error = message
	return status
}

// outcomeOf reads a terminal status as the pair telemetry needs: the closed-set
// outcome that is safe as a metric label, and the error the span records.
func outcomeOf(status *dholev1.JobStatus) (string, error) {
	var err error
	if msg := status.GetError(); msg != "" {
		err = errors.New(msg)
	}
	switch status.GetPhase() {
	case dholev1.Phase_PHASE_SUCCEEDED:
		return obs.OutcomeSucceeded, err
	case dholev1.Phase_PHASE_CANCELLED:
		return obs.OutcomeCancelled, err
	case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED,
		dholev1.Phase_PHASE_RUNNING, dholev1.Phase_PHASE_FAILED:
		return obs.OutcomeFailed, err
	default:
		return obs.OutcomeFailed, err
	}
}

// inputDir and outputDir are the port layout on disk, from
// docs/wire-contract.md, "Port layout on disk": an input port's bytes appear at
// inputs/<port> and an output port's are read back from outputs/<port>, both
// relative to the sandbox root the step's command runs in.
//
// They are two directories rather than the sandbox root because a step may
// declare an input and an output with the SAME port name — an in-place
// transform is the ordinary case — and at the root the engine materialised the
// input over the output's path and then collected the untouched input back as
// the step's result. A step that did nothing at all passed.
const (
	inputDir  = "inputs"
	outputDir = "outputs"
)

// killedExitCode is what the contract reserves for a step the engine killed —
// a cancellation, a drain that ran out of patience, or a timeout — on every
// platform and every backend, so one code means one thing wherever the step
// ran (docs/wire-contract.md, "Exit codes").
const killedExitCode int32 = 137

// logKeyFor names the authoritative log under the dispatch's output prefix, per
// attempt: a retry must not overwrite the evidence of the attempt before it.
func logKeyFor(d *dholev1.JobDispatch) string {
	return path.Join(d.GetOutputPrefix(), fmt.Sprintf("attempt-%d.log", d.GetAttempt()))
}

// countingReader counts what passed through it, so an output's size is known
// without reading the bytes twice.
type countingReader struct {
	r io.Reader
	n uint64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += uint64(n) // #nosec G115 -- n is never negative.
	return n, err
}

// logSink is both copies of a step's output. Every write lands in a spool file
// — the authoritative copy, finished before any terminal status — and is
// mirrored as a LogChunk on the ephemeral subject for whoever is watching.
type logSink struct {
	ctx     context.Context //nolint:containedctx // the sink outlives no call; it is written to from Exec's own goroutines.
	bus     bus.Bus
	subject string
	runID   string
	stepID  string
	attempt uint32

	mu   sync.Mutex
	seq  uint64
	file *os.File
}

func newLogSink(ctx context.Context, b bus.Bus, d *dholev1.JobDispatch) (*logSink, error) {
	file, err := os.CreateTemp("", "dhole-log-")
	if err != nil {
		return nil, err
	}
	return &logSink{
		ctx:     ctx,
		bus:     b,
		subject: bus.SubjectLogs(d.GetRunId(), d.GetStepId()),
		runID:   d.GetRunId(),
		stepID:  d.GetStepId(),
		attempt: d.GetAttempt(),
		file:    file,
	}, nil
}

// writer returns the io.Writer for one stream. Both streams share the sink's
// sequence and spool file, so the authoritative log is in the order the step
// actually produced it.
func (s *logSink) writer(stream dholev1.Stream) io.Writer {
	return &logStream{sink: s, stream: stream}
}

func (s *logSink) write(stream dholev1.Stream, p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.file.Write(p)
	if err != nil {
		return n, err
	}
	s.seq++

	// The live copy is best-effort by contract: a viewer that misses a chunk
	// sees the gap in seq, and the whole log is in the object either way. A
	// failure here must never fail the step.
	chunk := &dholev1.LogChunk{
		RunId:   s.runID,
		StepId:  s.stepID,
		Seq:     s.seq,
		Data:    append([]byte(nil), p[:n]...),
		Stream:  stream,
		Attempt: s.attempt,
	}
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), publishTimeout)
	defer cancel()
	_ = s.bus.Publish(pubCtx, s.subject, chunk)
	return n, nil
}

// flush finishes the authoritative copy. It returns only once the store holds
// the whole log, which is what lets the terminal status point at it.
func (s *logSink) flush(ctx context.Context, blobs blobstore.Store, tenantID, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return blobs.Write(ctx, tenantID, key, s.file)
}

// discard removes the spool file. The durable copy is in the object store by
// then; this is only the scratch space it was built in.
func (s *logSink) discard() {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := s.file.Name()
	_ = s.file.Close()
	_ = os.Remove(name)
}

type logStream struct {
	sink   *logSink
	stream dholev1.Stream
}

func (w *logStream) Write(p []byte) (int, error) { return w.sink.write(w.stream, p) }

// validKindToken reports whether kind can be both a NATS subject token and
// part of a durable consumer name: printable, no dot, no wildcard, no space.
// An EMPTY kind is not malformed, only silent: such an engine binds no kind
// queue and is never credited with a kind by the scheduler either, since an
// unstated engine type is unknown rather than universal.
func validKindToken(kind string) bool {
	for _, r := range kind {
		if r == '.' || r == '*' || r == '>' || r <= ' ' || r == 0x7f {
			return false
		}
	}
	return true
}
