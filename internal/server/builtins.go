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

// llmRetention is how long a recorded prompt is kept. Prompts are the one
// thing here that can carry a person's data, so they expire.
const llmRetention = 24 * time.Hour

// ModelFactory resolves the provider and model a `builtin:llm` step names.
//
// It is a function on Config rather than a provider registry inside this
// package for one reason: a model client holds an API key, and there is no
// path in this system today by which a control plane obtains one — engines
// never receive secret values (ADR 0010) and nothing yet leases them to the
// plane either. So the deployment that HAS a key constructs the client and
// hands it in, and a plane with no factory refuses `builtin:llm` steps with
// that named reason rather than pretending to run them.
type ModelFactory func(ctx context.Context, providerName, modelID string) (provider.LanguageModel, error)

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
	calls  *llm.Recorder

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
	err := b.execute(ctx, job)
	if err == nil || ctx.Err() != nil {
		return
	}
	b.log.Error("builtin step failed",
		"run", job.runID, "step", job.step.GetId(),
		"plugin_ref", job.step.GetPluginRef(), "error", err)
	// The failure goes in the LOG, not only in the log file. A step that
	// failed and said so nowhere the run can see leaves the run open forever,
	// which is the failure mode this project is most afraid of.
	if writeErr := b.fail(ctx, job, err); writeErr != nil && ctx.Err() == nil {
		b.log.Error("recording a builtin step's failure",
			"run", job.runID, "step", job.step.GetId(), "error", writeErr)
	}
}

func (b *builtins) execute(ctx context.Context, job builtinJob) error {
	switch ref := job.step.GetPluginRef(); ref {
	case BuiltinApproval:
		return b.request(ctx, job)
	case BuiltinLLM:
		return b.attempt(ctx, job, b.callModel)
	case BuiltinLoop:
		return b.attempt(ctx, job, b.iterate)
	default:
		return fmt.Errorf("no step type is registered for %q", ref)
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
// The cost of writing it is that a builtin in flight when the plane dies is
// not recovered: it holds no lease, so the orphan sweeper cannot see it. That
// is a real gap and it is named here rather than hidden — closing it means
// giving a builtin step a lease of its own.
func (b *builtins) attempt(
	ctx context.Context, job builtinJob,
	do func(ctx context.Context, job builtinJob, attempt uint32) ([]*dholev1.OutputRef, error),
) error {
	attempt, err := b.nextAttempt(ctx, job)
	if err != nil {
		return err
	}
	payload, err := scheduler.MarshalDispatched(scheduler.Dispatched{
		Attempt: attempt,
		// Never cacheable, and the reason travels with the dispatch so that
		// nobody hunts for a cache that was never going to apply: a model
		// answer is not reproducible and a loop is not one step.
		CacheIneligibleReason: "the control plane runs this step type itself",
	})
	if err != nil {
		return err
	}
	if err := b.store.Append(ctx, job.tenantID, runstore.Event{
		RunID:   job.runID,
		StepID:  job.step.GetId(),
		Attempt: attempt,
		Type:    runstore.StepDispatched,
		Payload: payload,
		At:      time.Now().UTC(),
	}); err != nil {
		return err
	}

	outputs, err := do(ctx, job, attempt)
	if err != nil {
		return err
	}
	return b.succeed(ctx, job, attempt, outputs)
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
	model, err := b.models(ctx, cfg["provider"], cfg["model"])
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
	ctx context.Context, job builtinJob, attempt uint32, outputs []*dholev1.OutputRef,
) error {
	return b.terminal(ctx, job, attempt, &dholev1.JobStatus{
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
func (b *builtins) fail(ctx context.Context, job builtinJob, cause error) error {
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
	return b.terminal(ctx, job, attempt, &dholev1.JobStatus{
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

func (b *builtins) terminal(
	ctx context.Context, job builtinJob, attempt uint32,
	status *dholev1.JobStatus, kind runstore.EventType,
) error {
	payload, err := proto.Marshal(status)
	if err != nil {
		return err
	}
	return b.store.Append(ctx, job.tenantID, runstore.Event{
		RunID:   job.runID,
		StepID:  job.step.GetId(),
		Attempt: attempt,
		Type:    kind,
		Payload: payload,
		At:      time.Now().UTC(),
	})
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
func newBuiltins(in *infra, models ModelFactory, resume approval.Resumer, log *slog.Logger) (*builtins, error) {
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
		calls:     calls,
		jobs:      make(chan builtinJob, builtinQueue),
		running:   map[string]bool{},
	}, nil
}
