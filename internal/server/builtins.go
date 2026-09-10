package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/approval"
	"github.com/azrtydxb/dhole/internal/steps/gate"
	"github.com/azrtydxb/dhole/internal/steps/llm"
	"github.com/azrtydxb/dhole/internal/steps/loop"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// BuiltinScheme is the plugin reference a step carries when the CONTROL PLANE
// runs it rather than an engine. `builtin:llm`, `builtin:loop`,
// `builtin:approval`, `builtin:timer`.
//
// The scheme is not a convenience. A bounded loop's ceiling and an approval's
// verdict have to be enforced somewhere no tenant-supplied code can reach, and
// a durable timer and a human gate are ROWS rather than work — no engine could
// be given any of the four (ADR 0003, ADR 0015).
const BuiltinScheme = "builtin:"

// The four step types the plane hosts. They are string constants rather than
// an enum because they are what a pipeline author types into `plugin_ref`, and
// therefore a public contract.
const (
	// BuiltinTimer makes a run wait. `config.duration` is a Go duration.
	//
	// It is an ALIAS for the gate step type, not a second implementation.
	// Arming a wait outside the transaction that decided the step was ready is
	// precisely the bug migration 0023 and internal/steps/gate exist to close:
	// the sequence is allocated inside the transaction and the visibility is
	// not, so the wait could be recorded after the dispatch it was supposed to
	// prevent and be skipped entirely. Two step types that both wait, one of
	// them armed the old way, would reintroduce it under a different name.
	BuiltinTimer = BuiltinScheme + "timer"
	// BuiltinApproval makes a run wait for a person. `config.prompt` is what
	// that person is asked.
	BuiltinApproval = BuiltinScheme + "approval"
	// BuiltinLLM calls a language model and validates the answer against the
	// schema the step's output port declares.
	BuiltinLLM = BuiltinScheme + "llm"
	// BuiltinLoop repeats another builtin step type up to a hard ceiling.
	BuiltinLoop = BuiltinScheme + "loop"
)

// builtinWorkers is how many builtin steps run at once.
//
// It is small and it is a POOL rather than a goroutine per step because Stop
// must end everything Start began: a goroutine spawned per step from a
// background loop would be racing the WaitGroup that Stop waits on, and
// TestStopEndsEverythingItStarted counts exactly that. Gates cost a
// transaction and finish immediately; only a model call is slow, and four
// concurrent model calls is far past what a single-binary plane wants.
const builtinWorkers = 4

// builtinQueue bounds the work waiting to start. A full queue is not an error
// and nothing is dropped: the step was never marked as taken, so the next
// advance tick — 250ms later — offers it again.
const builtinQueue = 64

// minRenewInterval floors how often a builtin step's lease is renewed. A
// deployment that configured a very short TTL would otherwise renew in a tight
// loop, which is a write to the bus per step per few milliseconds.
const minRenewInterval = 100 * time.Millisecond

// llmRetention is how long a recorded prompt is kept. Prompts are the one
// thing here that can carry a person's data, so they expire.
const llmRetention = 24 * time.Hour

// ModelRequest is one resolution of a model client: which model, for whose
// step, with which credential.
//
// The credential is REDEEMED, not configured. A model configuration names a
// secret by reference and the plane resolves it at call time through its own
// broker (ADR 0024), so the value in this struct is live for the duration of
// one call and is gone with the client built from it. A factory that squirrels
// it away has reintroduced the credential at rest that the reference exists to
// avoid.
//
// APIKey is empty when the step named no secret, which is the honest state for
// a model that needs none — a local runtime, a gateway that authenticates by
// network position, a test double.
type ModelRequest struct {
	// TenantID is the tenant whose step is running. It is here so that a
	// deployment can hand out different clients to different tenants; nothing
	// requires that yet, and a factory that dropped it would have to be
	// redesigned the day one does.
	TenantID string
	Provider string
	Model    string
	APIKey   string
}

// ModelFactory resolves the provider and model a `builtin:llm` step names.
//
// It is a function on Config rather than a provider registry inside this
// package because a deployment may reach its models through a gateway, a proxy
// or a runtime nobody here has heard of. What it no longer has to solve is the
// credential: the plane redeems that itself and passes it in (ADR 0024), so a
// factory is a constructor rather than a place a key lives. A plane with no
// factory refuses `builtin:llm` steps with that named reason rather than
// pretending to run them.
type ModelFactory func(ctx context.Context, req ModelRequest) (provider.LanguageModel, error)

// secretResolver is the plane's own way to a secret value. It is an interface
// rather than *secrets.PlaneResolver so that this dispatcher can be assembled
// without a bus in a test, and so that the ONE thing it is allowed to do —
// exchange a name for a value, for one call — is the whole of what it can do.
type secretResolver interface {
	Resolve(ctx context.Context, tenantID, name string) (string, error)
}

// builtins is the plane's own dispatcher for the step types it hosts.
//
// It is the thing whose absence made internal/steps/{llm,loop,approval} and
// internal/wait libraries with tests and no caller: every acceptance pipeline
// that used them had its harness stand in for this file.
type builtins struct {
	log   *slog.Logger
	store runstore.Store
	cas   cas.Store
	// approvers is where an approver is verified. An approval whose approver
	// is not a principal of the tenant is not an approval.
	approvers approval.Approvers
	// resume advances the run a gate has just opened. It is the scheduler.
	resume approval.Resumer
	models ModelFactory
	// secrets is how a model call gets its credential: named in the step's
	// config, redeemed from the plane's own broker at CALL time, held for the
	// length of that call and no longer (ADR 0024). Nil means this plane can
	// resolve none, and a step that names one fails saying so.
	secrets secretResolver
	calls   *llm.Recorder
	// leases is what makes a builtin step recoverable. A step this plane runs
	// is leased exactly as a step dispatched to an engine is, because the
	// sweeper that recovers a dead holder knows nothing else: before this, a
	// builtin wrote STEP_DISPATCHED and held no lease, so a plane that died
	// mid-step left the run in flight forever with nothing anywhere able to
	// notice — no engine would ever report a status for it either.
	leases lease.Manager
	// ttl is how long that lease lives without renewal, and it is the
	// scheduler's own so that a plane holding one engine step and one builtin
	// step gives both back at the same moment.
	ttl time.Duration

	jobs chan builtinJob

	// running is what this process has already taken and not yet finished.
	//
	// A gate leaves no in-flight mark in the log — that is the point of it —
	// so without this the 250ms advance tick would arm the same gate four
	// times a second. It is deliberately in-memory and deliberately NOT the
	// durability story: the log is, and everything below is idempotent per
	// (run, step) against it.
	mu      sync.Mutex
	running map[string]bool
}

type builtinJob struct {
	tenantID string
	runID    string
	step     *dholev1.Step
}

// Take implements scheduler.BuiltinSteps.
func (b *builtins) Take(
	ctx context.Context, tenantID, runID string, step *dholev1.Step,
) (bool, error) {
	if !strings.HasPrefix(step.GetPluginRef(), BuiltinScheme) {
		return false, nil
	}
	// A gate is a builtin that this registry deliberately does NOT claim. It
	// is armed inside the transaction that decided the step was ready
	// (internal/steps/gate, migration 0023), and claiming it here would run it
	// on a worker instead — arming out of band, which is the bug that fix
	// exists to close.
	if gate.IsGate(step.GetPluginRef()) {
		return false, nil
	}
	key := tenantID + "/" + runID + "/" + step.GetId()
	b.mu.Lock()
	if b.running[key] {
		b.mu.Unlock()
		return true, nil
	}
	b.running[key] = true
	b.mu.Unlock()

	select {
	case b.jobs <- builtinJob{tenantID: tenantID, runID: runID, step: step}:
		return true, nil
	case <-ctx.Done():
		b.release(key)
		return true, ctx.Err()
	default:
		// Every worker is busy. Leaving the step untaken would send a
		// `builtin:` reference to an engine that cannot resolve it, so it
		// stays this dispatcher's and waits for the next tick.
		b.release(key)
		return true, nil
	}
}

func (b *builtins) release(key string) {
	b.mu.Lock()
	delete(b.running, key)
	b.mu.Unlock()
}

// work is one worker. It ends when ctx does, which is what Stop cancels.
func (b *builtins) work(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-b.jobs:
			b.run(ctx, job)
			b.release(job.tenantID + "/" + job.runID + "/" + job.step.GetId())
		}
	}
}

// run does one builtin step, and records the failure if it cannot.
func (b *builtins) run(ctx context.Context, job builtinJob) {
	token, err := b.execute(ctx, job)
	if err == nil || ctx.Err() != nil {
		return
	}
	b.log.Error("builtin step failed",
		"run", job.runID, "step", job.step.GetId(),
		"plugin_ref", job.step.GetPluginRef(), "error", err)
	// The failure goes in the LOG, not only in the log file. A step that
	// failed and said so nowhere the run can see leaves the run open forever,
	// which is the failure mode this project is most afraid of.
	if writeErr := b.fail(ctx, job, token, err); writeErr != nil && ctx.Err() == nil {
		b.log.Error("recording a builtin step's failure",
			"run", job.runID, "step", job.step.GetId(), "error", writeErr)
	}
}

// execute runs one builtin step and returns the lease it held while doing it.
//
// The token travels back so a FAILURE can be written under the same fence the
// dispatch was: a step whose lease was swept while it ran belongs to a later
// attempt, and recording this one's failure against it would fail work that is
// currently running.
func (b *builtins) execute(ctx context.Context, job builtinJob) (lease.Token, error) {
	switch ref := job.step.GetPluginRef(); ref {
	case BuiltinApproval:
		// A gate holds no lease. It is not work in flight: it writes no
		// STEP_DISPATCHED, nothing is running, and a run waiting at one is
		// waiting for a person rather than for a process that could die.
		return lease.Token{}, b.request(ctx, job)
	case BuiltinLLM:
		return b.attempt(ctx, job, b.callModel)
	case BuiltinLoop:
		return b.attempt(ctx, job, b.iterate)
	default:
		return lease.Token{}, fmt.Errorf("no step type is registered for %q", ref)
	}
}

// --- the human gate --------------------------------------------------------

// The timer gate is NOT here any more, and its absence is the point. Arming a
// timer used to be a builtin job like any other, which made it a different
// transaction from the readiness decision that gated the step — so
// STEP_AWAITING_TIMER could land at a lower sequence than the STEP_DISPATCHED
// of the step it gated, a log reading "gated, then dispatched" and a wait that
// never happened. It now runs inside the transaction that would otherwise have
// dispatched (scheduler.Config.Gate), and the copy here was left behind by
// that move with nothing calling it.

// request opens a human gate. It is armed here and decided by Server.Approve,
// which is the one call an approval RPC or a CLI has to make.
func (b *builtins) request(ctx context.Context, job builtinJob) error {
	gate, err := b.gate(job.tenantID)
	if err != nil {
		return err
	}
	return gate.Request(ctx, job.runID, job.step.GetId(), job.step.GetConfig()["prompt"])
}

// gate builds the approval step for one tenant. It is built per call rather
// than held because it is tenant-scoped and costs nothing: the state it acts
// on is entirely in the run log.
func (b *builtins) gate(tenantID string) (*approval.Step, error) {
	if b.approvers == nil {
		return nil, errors.New("this control plane has no principal store, " +
			"so there is nobody an approval could be verified against")
	}
	return approval.New(approval.Config{
		Store:     b.store,
		TenantID:  tenantID,
		Approvers: b.approvers,
		Resume:    b.resume,
	})
}

// --- the two that do work --------------------------------------------------

// attempt runs a step type that EXECUTES, under the same attempt accounting a
// dispatch to an engine gets.
//
// The STEP_DISPATCHED comes first and it is not cosmetic: `plan` counts
// attempts off that event and nothing else, so a builtin step that failed
// without one would be retried by the scheduler forever — including a step
// declared AT_MOST_ONCE, whose whole promise is that it is not. Observed the
// first time a model call was allowed to fail.
//
// The step is LEASED for as long as it runs, under the same manager and the
// same fence discipline a dispatch to an engine uses. Without that a builtin
// in flight when the plane died was not recovered at all: the sweeper finds
// dead holders through lease.Expire and by no other means, and no engine would
// ever report a status for a step no engine ever had — so the run stayed in
// flight forever. The lease is claimed BEFORE the dispatch event, because a
// dispatch that is recorded and then fails to be leased is the same
// unrecoverable step this closes.
func (b *builtins) attempt(
	ctx context.Context, job builtinJob,
	do func(ctx context.Context, job builtinJob, attempt uint32) ([]*dholev1.OutputRef, error),
) (lease.Token, error) {
	attempt, err := b.nextAttempt(ctx, job)
	if err != nil {
		return lease.Token{}, err
	}
	token, err := b.claim(ctx, job, attempt)
	if err != nil {
		return lease.Token{}, err
	}
	// Renewed for as long as the work runs. A model call or a bounded loop can
	// outlive the TTL, and a holder that stopped proving it was alive while it
	// was still working would have its own step swept out from under it.
	stopRenewing := b.renew(ctx, token)
	defer stopRenewing()

	payload, err := scheduler.MarshalDispatched(scheduler.Dispatched{
		Attempt: attempt,
		Fence:   token.Fence,
		// Never cacheable, and the reason travels with the dispatch so that
		// nobody hunts for a cache that was never going to apply: a model
		// answer is not reproducible and a loop is not one step.
		CacheIneligibleReason: "the control plane runs this step type itself",
	})
	if err != nil {
		return token, err
	}
	if err := b.store.Append(ctx, job.tenantID, runstore.Event{
		RunID:   job.runID,
		StepID:  job.step.GetId(),
		Attempt: attempt,
		Type:    runstore.StepDispatched,
		Payload: payload,
		At:      time.Now().UTC(),
	}); err != nil {
		return token, err
	}

	outputs, err := do(ctx, job, attempt)
	if err != nil {
		return token, err
	}
	return token, b.succeed(ctx, job, attempt, token, outputs)
}

// claim takes the lease on a builtin step for one attempt.
func (b *builtins) claim(ctx context.Context, job builtinJob, attempt uint32) (lease.Token, error) {
	if b.leases == nil {
		// Never in the shipping binary: Start builds this with the lease
		// manager it already opened. A builtins assembled without one would
		// run steps nothing could recover, which is the bug this closes, so it
		// refuses rather than running them.
		return lease.Token{}, errors.New(
			"this control plane has no lease manager, so a builtin step it ran could never be recovered")
	}
	token, err := b.leases.Claim(ctx, job.tenantID, job.runID, job.step.GetId(), attempt, b.ttl)
	if err != nil {
		return lease.Token{}, fmt.Errorf("claiming the lease on %s/%s: %w",
			job.runID, job.step.GetId(), err)
	}
	return token, nil
}

// renew keeps a lease alive while the step runs, and returns the function that
// stops doing so.
//
// A third of the TTL, so two renewals can be missed — a slow database, a
// reconnecting bus — before a step that is perfectly healthy is declared lost.
func (b *builtins) renew(ctx context.Context, token lease.Token) func() {
	renewCtx, stop := context.WithCancel(ctx)
	interval := b.ttl / 3
	if interval < minRenewInterval {
		interval = minRenewInterval
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
			}
			if err := b.leases.Renew(renewCtx, token); err != nil {
				if errors.Is(err, lease.ErrFenced) {
					// Somebody else holds this step now. Renewing again would
					// be this plane propping up a claim it has lost; the work
					// carries on and its result is refused at the commit.
					return
				}
				if renewCtx.Err() == nil {
					b.log.Warn("renewing a builtin step's lease", "fence", token.Fence, "error", err)
				}
			}
		}
	}()
	return func() {
		stop()
		<-done
	}
}

// nextAttempt is one past the highest attempt the LOG records for this step.
// It is read back rather than counted in memory because a plane that restarts
// mid-run must not start counting from one again.
func (b *builtins) nextAttempt(ctx context.Context, job builtinJob) (uint32, error) {
	events, err := b.store.Replay(ctx, job.tenantID, job.runID)
	if err != nil {
		return 0, err
	}
	var highest uint32
	for _, e := range events {
		if e.StepID == job.step.GetId() && e.Type == runstore.StepDispatched && e.Attempt > highest {
			highest = e.Attempt
		}
	}
	return highest + 1, nil
}

// callModel is `builtin:llm`: one model call, validated against the schema the
// step's own output port declares. There is no second place a schema could
// live, and an answer that fails it is refused rather than passed on.
func (b *builtins) callModel(
	ctx context.Context, job builtinJob, _ uint32,
) ([]*dholev1.OutputRef, error) {
	answer, err := b.answer(ctx, job, job.step.GetConfig()["prompt"], job.step.GetId())
	if err != nil {
		return nil, err
	}
	ref, err := b.emit(ctx, job, answer)
	if err != nil {
		return nil, err
	}
	return ref, nil
}

// answer builds the llm step from the config and asks it once.
func (b *builtins) answer(
	ctx context.Context, job builtinJob, prompt, stepID string,
) (json.RawMessage, error) {
	cfg := job.step.GetConfig()
	if b.models == nil {
		return nil, errors.New(
			"this control plane was given no model factory, so it can call no language model; " +
				"see server.Config.Models")
	}
	if strings.TrimSpace(prompt) == "" {
		return nil, errors.New("an llm step needs a `prompt` in its config")
	}
	maxTokens := 0
	if raw := strings.TrimSpace(cfg["max_tokens"]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("an llm step's max_tokens %q: %w", raw, err)
		}
		maxTokens = n
	}
	// The credential, redeemed for THIS call. It is resolved here rather than
	// at start-up on purpose: a value the plane held for the lifetime of the
	// deployment is the secret at rest that SecretRef exists to avoid, and a
	// run that takes an hour must not hold a key for an hour (ADR 0024). The
	// cost is one local request against an in-memory broker per call.
	apiKey, err := b.credential(ctx, job.tenantID, cfg["api_key_secret"])
	if err != nil {
		return nil, err
	}
	model, err := b.models(ctx, ModelRequest{
		// The STEP's tenant, both to the resolver above and to the factory, so
		// that per-tenant model credentials are expressible the day a
		// deployment wants them.
		TenantID: job.tenantID,
		Provider: cfg["provider"],
		Model:    cfg["model"],
		APIKey:   apiKey,
	})
	if err != nil {
		return nil, fmt.Errorf("resolving model %q of provider %q: %w",
			cfg["model"], cfg["provider"], err)
	}
	step, err := llm.New(llm.Config{
		Provider:     cfg["provider"],
		Model:        cfg["model"],
		OutputSchema: []byte(structuredSchema(job.step)),
		MaxTokens:    maxTokens,
	}, llm.Options{
		Model:    model,
		Store:    b.store,
		Calls:    b.calls,
		TenantID: job.tenantID,
		Logger:   b.log,
	})
	if err != nil {
		return nil, err
	}
	return step.Run(ctx, job.runID, stepID, prompt)
}

// credential redeems the secret a model configuration names, or returns
// nothing at all when it names none.
//
// The error names the SECRET and never the value: it is on its way to a
// JobStatus, which is durable and archived. An unresolvable credential fails
// the step here rather than being passed to a provider as an empty string,
// which comes back as an authentication failure naming no secret at all.
func (b *builtins) credential(ctx context.Context, tenantID, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", nil
	}
	if b.secrets == nil {
		return "", fmt.Errorf(
			"this control plane can redeem no secrets, so it cannot resolve the credential named %q; "+
				"see server.Config.SecretSource", name)
	}
	return b.secrets.Resolve(ctx, tenantID, name)
}

// iterate is `builtin:loop`: another builtin step type repeated up to a hard
// ceiling, which is the property ADR 0015 is about.
//
// The body is named by `config.body` and is itself a builtin reference, and
// the loop step's own config configures it. That is narrower than what
// internal/steps/loop can express — its Node.Subgraph is a whole pipeline —
// and the reason is that the definition format has no syntax for a nested
// pipeline and no run can contain another. A body that dispatched steps to
// engines needs nested runs; this is what can be wired without inventing that.
func (b *builtins) iterate(
	ctx context.Context, job builtinJob, attempt uint32,
) ([]*dholev1.OutputRef, error) {
	cfg := job.step.GetConfig()
	ceiling, err := strconv.Atoi(strings.TrimSpace(cfg["max_iterations"]))
	if err != nil {
		return nil, fmt.Errorf("a loop step's max_iterations %q: %w", cfg["max_iterations"], err)
	}
	body := cfg["body"]
	if body != BuiltinLLM {
		return nil, fmt.Errorf(
			"a loop step's `body` must be %q; %q names no body this control plane can repeat",
			BuiltinLLM, body)
	}

	bounded, err := loop.New(loop.Node{
		// The body as a pipeline in its own right, which is what loop.New
		// validates. One step, because that is what `body` names.
		Subgraph: &dholev1.Pipeline{
			Id:     job.step.GetId() + "-body",
			Tenant: &dholev1.Tenant{Id: job.tenantID},
			Steps: []*dholev1.Step{{
				Id:          "pass",
				PluginRef:   body,
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			}},
		},
		MaxIterations: ceiling,
		ExitCondition: cfg["exit_condition"],
	}, loop.Options{
		Store:    b.store,
		TenantID: job.tenantID,
		Body: func(ctx context.Context, it loop.Iteration) (map[string]any, error) {
			// Each pass is recorded under its own unrolled step id, so a run
			// view can expand the container into what actually ran — and
			// under the LOOP's attempt as well from the second one on. The
			// model-call record is unique per (run, step, attempt), so a
			// retried loop whose second pass one reused the first's id
			// failed on that constraint instead of running. Observed.
			answer, err := b.answer(ctx, job, cfg["prompt"], bodyStepID(it.StepID, attempt))
			if err != nil {
				return nil, err
			}
			var state map[string]any
			if err := json.Unmarshal(answer, &state); err != nil {
				return nil, fmt.Errorf("a loop body's answer is not an object: %w", err)
			}
			return state, nil
		},
	})
	if err != nil {
		return nil, err
	}

	result, err := bounded.Run(ctx, job.runID, job.step.GetId(), map[string]any{})
	if err != nil {
		return nil, err
	}
	final, err := json.Marshal(result.State)
	if err != nil {
		return nil, err
	}
	return b.emit(ctx, job, final)
}

// bodyStepID is the id one pass of a loop records its model call under. The
// loop's own attempt appears only from the second one, so an ordinary loop's
// ids are exactly what internal/steps/loop unrolled.
func bodyStepID(stepID string, attempt uint32) string {
	if attempt <= 1 {
		return stepID
	}
	return fmt.Sprintf("%s@%d", stepID, attempt)
}

// --- writing the step down -------------------------------------------------

// emit puts a structured answer in the content-addressed store and references
// it on the step's first output port, which is how the next step reads it: a
// step's inputs come from edges and from nothing else (ADR 0001).
func (b *builtins) emit(
	ctx context.Context, job builtinJob, body []byte,
) ([]*dholev1.OutputRef, error) {
	ports := job.step.GetOutputs()
	if len(ports) == 0 {
		return nil, nil
	}
	digest, err := b.cas.Put(ctx, job.tenantID, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	return []*dholev1.OutputRef{{
		Port:      ports[0].GetName(),
		Digest:    digest,
		SizeBytes: uint64(len(body)),
	}}, nil
}

// succeed writes the step's terminal event, in the shape an engine's would
// have: the scheduler reads a STEP_SUCCEEDED payload as a JobStatus, and a
// second encoding here would be a second thing to keep in step.
func (b *builtins) succeed(
	ctx context.Context, job builtinJob, attempt uint32,
	token lease.Token, outputs []*dholev1.OutputRef,
) error {
	return b.terminal(ctx, job, attempt, token, &dholev1.JobStatus{
		RunId:   job.runID,
		StepId:  job.step.GetId(),
		Attempt: attempt,
		Phase:   dholev1.Phase_PHASE_SUCCEEDED,
		Outputs: outputs,
	}, runstore.StepSucceeded)
}

// fail writes the step's failure with the reason attached. The reason is the
// whole value of the event: a builtin step that failed silently would leave
// the run open with nothing to read.
func (b *builtins) fail(ctx context.Context, job builtinJob, token lease.Token, cause error) error {
	attempt, err := b.attemptOf(ctx, job)
	if err != nil {
		return err
	}
	if attempt == 0 {
		// A gate that could not be armed was never dispatched, so there is no
		// attempt to fail. It fails the step at attempt one so the run stops
		// instead of arming forever.
		attempt = 1
	}
	return b.terminal(ctx, job, attempt, token, &dholev1.JobStatus{
		RunId:   job.runID,
		StepId:  job.step.GetId(),
		Attempt: attempt,
		Phase:   dholev1.Phase_PHASE_FAILED,
		Error:   cause.Error(),
	}, runstore.StepFailed)
}

// attemptOf is the attempt currently in flight, or zero when none is.
func (b *builtins) attemptOf(ctx context.Context, job builtinJob) (uint32, error) {
	next, err := b.nextAttempt(ctx, job)
	if err != nil {
		return 0, err
	}
	return next - 1, nil
}

// terminal writes a builtin step's verdict, under the fence it was run with.
//
// The fence is validated INSIDE the transaction that writes the event, exactly
// as a cache hit's is: a step whose lease was swept while it ran has been given
// to a later attempt, and a verdict written after that would decide a step
// somebody else is currently running. A fenced write is discarded silently —
// this plane did nothing wrong, it was simply overtaken.
func (b *builtins) terminal(
	ctx context.Context, job builtinJob, attempt uint32,
	token lease.Token, status *dholev1.JobStatus, kind runstore.EventType,
) error {
	payload, err := proto.Marshal(status)
	if err != nil {
		return err
	}
	event := runstore.Event{
		RunID:   job.runID,
		StepID:  job.step.GetId(),
		Attempt: attempt,
		Type:    kind,
		Payload: payload,
		At:      time.Now().UTC(),
	}
	if token.Value == "" || b.leases == nil {
		// A gate: no lease was ever taken, because nothing was in flight.
		return b.store.Append(ctx, job.tenantID, event)
	}
	err = b.store.WithTx(ctx, func(tx runstore.Tx) error {
		if err := tx.Append(ctx, job.tenantID, event); err != nil {
			return err
		}
		return b.leases.Validate(ctx, token)
	})
	if errors.Is(err, lease.ErrFenced) {
		b.log.Warn("a builtin step's result was superseded before it was recorded",
			"run", job.runID, "step", job.step.GetId(), "attempt", attempt)
		return nil
	}
	return err
}

// structuredSchema is the JSON schema a step's first output port declares, or
// empty when it declares none. It is where an llm step's answer is judged.
func structuredSchema(step *dholev1.Step) string {
	for _, port := range step.GetOutputs() {
		if s := port.GetType().GetStructured().GetSchema(); s != "" {
			return s
		}
	}
	return ""
}

// Approve decides an approval gate a `builtin:approval` step armed, and lets
// the run continue.
//
// It is on Server because the gate is the plane's, not a caller's: the store
// it is written to, the principals the approver is checked against and the
// scheduler that advances the run afterwards are all this server's. A CLI or
// an approval RPC calls this and does not assemble its own.
func (s *Server) Approve(
	ctx context.Context, tenantID, runID, stepID, approver string, approved bool,
) error {
	s.mu.Lock()
	running, built := s.running, s.builtins
	s.mu.Unlock()
	if !running || built == nil {
		return errors.New("server: not started")
	}
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	gate, err := built.gate(tenantID)
	if err != nil {
		return err
	}
	return gate.Decide(ctx, runID, stepID, approver, approved)
}

// newBuiltins assembles the plane's step types over the infrastructure it
// already opened. Nothing here opens a store, a bus or a connection of its
// own: a step type writing to a second copy of the run log would be a
// different system from the one the scheduler is advancing.
func newBuiltins(
	in *infra, models ModelFactory, plane secretResolver, resume approval.Resumer,
	leases lease.Manager, ttl time.Duration, log *slog.Logger,
) (*builtins, error) {
	calls, err := llm.NewRecorder(in.db, in.dialect, llmRetention)
	if err != nil {
		return nil, err
	}
	return &builtins{
		log:       log,
		store:     in.store,
		cas:       in.cas,
		approvers: identity.NewSQLStoreWithDialect(in.db, in.dialect),
		resume:    resume,
		models:    models,
		secrets:   plane,
		calls:     calls,
		leases:    leases,
		ttl:       ttl,
		jobs:      make(chan builtinJob, builtinQueue),
		running:   map[string]bool{},
	}, nil
}
