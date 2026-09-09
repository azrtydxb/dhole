// Package llm is the model-call step type: it asks a language model for a
// JSON object, holds the answer to a declared schema, and records what the
// call was and what it cost.
//
// Three decisions here are worth more than the code that implements them.
//
// The effect class is COMPUTED, not declared. ADR 0002 makes the class the
// single input to caching and retry, and a model call is only `pure` — the
// class that says "the same inputs must produce the same output" — when
// nothing about it can move: temperature 0 AND a digest-pinned model.
// Everything else is `idempotent`: retried, never cached. An alias like
// "latest" silently repoints, and a cache key folded over an alias serves
// yesterday's model's answer as today's with nothing in the run to say so.
// This is the one place in the system where a step's class is derived rather
// than taken on trust, precisely because a pipeline author writing
// `model: claude-latest` has not thought about repointing and should not have
// to.
//
// The step returns a validated object or NOTHING. A model that answers with
// unparseable JSON is retried, and a step that runs out of attempts fails with
// the provider's own error. It never returns the part that parsed: a
// half-formed object flowing downstream fails somewhere else, later, with no
// connection to the model that caused it, and the failure it replaced was
// visible.
//
// The token ceiling HALTS the run. A model that hits its budget stops
// mid-sentence and reports it, and the JSON it produced up to that point often
// parses. A truncated answer that looks complete is the worst outcome here, so
// a ceiling breach fails the step and closes the run rather than trimming and
// carrying on.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/go-ai-sdk/ai"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// The refusals. Each is a distinct thing that went wrong and a distinct thing
// to do about it, so none of them collapses into a generic error.
var (
	// ErrNoObject: the model never produced parseable JSON, across every
	// attempt. The provider's own message is carried in the text.
	ErrNoObject = errors.New("llm: the model produced no object")

	// ErrObjectInvalid: the model produced JSON that does not satisfy the
	// pipeline's declared output schema.
	ErrObjectInvalid = errors.New("llm: the object does not satisfy the declared schema")

	// ErrTokenCeiling: the call went over its token budget. The run is halted;
	// the answer, complete-looking or not, is discarded.
	ErrTokenCeiling = errors.New("llm: the call exceeded its token ceiling")

	// ErrProviderCall: the provider refused the call outright.
	ErrProviderCall = errors.New("llm: the provider refused the call")

	// ErrModelUnattributable: the response does not name the model that
	// produced it, so the call cannot be fingerprinted. An unattributable
	// answer is not usable output — it cannot be cached, reproduced, or
	// explained afterwards.
	ErrModelUnattributable = errors.New("llm: the response does not name the model that produced it")
)

// Config is the pipeline author's half of a model call: which model, how it
// samples, what shape the answer must take, and what it may spend.
//
// MaxTokens is the step's whole token budget. It is passed to the provider as
// its output cap AND enforced here against the reported total, because the
// provider's cap only bounds what it generates — a prompt that ballooned
// still spent the budget.
type Config struct {
	Provider     string
	Model        string
	Temperature  float32
	OutputSchema []byte
	MaxTokens    int
}

// Options is everything else the step needs: the model to call and the two
// places its work is written down.
//
// Model is already constructed, and constructed elsewhere. This package never
// sees an API key, which is the primary reason no key can leak from it; the
// redaction in record.go is the second net, not the first.
type Options struct {
	Model    provider.LanguageModel
	Store    runstore.Store
	Calls    *Recorder
	TenantID string
	// Attempts is how many times the model is asked before the step gives
	// up on getting a parseable, schema-valid object. Default 3.
	Attempts int
	Logger   *slog.Logger
	Now      func() time.Time
}

// Step is the LLM step type. It is safe for concurrent use.
type Step struct {
	cfg      Config
	model    provider.LanguageModel
	schema   *jsonschema.Schema
	store    runstore.Store
	calls    *Recorder
	tenantID string
	attempts int
	log      *slog.Logger
	now      func() time.Time
}

const defaultAttempts = 3

// New validates the configuration and builds a Step.
//
// It deliberately does NOT require a store or a recorder. EffectClass has to
// be answerable while a pipeline is being planned or validated, long before
// anything is executed, and a class that could only be computed with a
// database open would be computed somewhere else instead. Run refuses without
// them.
func New(cfg Config, opts Options) (*Step, error) {
	if opts.TenantID == "" {
		return nil, fmt.Errorf("llm: %w", runstore.ErrTenantRequired)
	}
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, errors.New("llm: a model is required")
	}
	if opts.Model == nil {
		return nil, errors.New("llm: a language model is required")
	}
	if cfg.MaxTokens < 0 {
		return nil, errors.New("llm: a negative token ceiling is not a ceiling")
	}
	schema, err := compileSchema(cfg.OutputSchema)
	if err != nil {
		return nil, err
	}

	s := &Step{
		cfg:      cfg,
		model:    opts.Model,
		schema:   schema,
		store:    opts.Store,
		calls:    opts.Calls,
		tenantID: opts.TenantID,
		attempts: opts.Attempts,
		log:      opts.Logger,
		now:      opts.Now,
	}
	if s.attempts <= 0 {
		s.attempts = defaultAttempts
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// compileSchema turns the declared output schema into something that can
// refuse an answer. It compiles OFFLINE: a pipeline definition must not be
// able to make the control plane fetch a URL in order to validate a model's
// reply.
func compileSchema(doc []byte) (*jsonschema.Schema, error) {
	if len(bytes.TrimSpace(doc)) == 0 {
		return nil, errors.New("llm: an output schema is required; " +
			"an unvalidated model answer is a guess with a JSON wrapper")
	}
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return nil, fmt.Errorf("llm: the output schema is not valid JSON: %w", err)
	}
	c := jsonschema.NewCompiler()
	c.UseLoader(offlineLoader{})
	const resource = "dhole:llm-output-schema"
	if err := c.AddResource(resource, parsed); err != nil {
		return nil, fmt.Errorf("llm: the output schema is not a valid JSON Schema: %w", err)
	}
	compiled, err := c.Compile(resource)
	if err != nil {
		return nil, fmt.Errorf("llm: the output schema is not a valid JSON Schema: %w", err)
	}
	return compiled, nil
}

type offlineLoader struct{}

func (offlineLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("remote schema reference %q is not allowed", url)
}

// pinnedDigest matches a model name that carries an explicit content digest,
// which is the only form that names ONE model for ever.
//
// The pattern is strict on purpose. A dated name like
// "claude-sonnet-20260101" is a convention the provider may or may not honour,
// and a short digest is not a digest — both would be accepted by a looser rule
// and would each mean a step claiming purity it does not have.
var pinnedDigest = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)

// EffectClass is the class this step operates under, derived from its
// configuration rather than declared.
//
// PURE requires BOTH conditions, and neither is sufficient alone. A pinned
// model with temperature 0.7 samples a different answer each time. An alias at
// temperature 0 is deterministic only until the alias moves, at which point
// every cached answer under the old key becomes a lie about which model
// produced it. Anything short of both is IDEMPOTENT: safe to retry, never
// cached.
func (s *Step) EffectClass() dholev1.EffectClass {
	if s.cfg.Temperature == 0 && pinnedDigest.MatchString(s.cfg.Model) {
		return dholev1.EffectClass_EFFECT_CLASS_PURE
	}
	return dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT
}

// Run asks the model, validates the answer, records the call, and returns the
// object — or fails, having returned nothing at all.
func (s *Step) Run(ctx context.Context, runID, stepID, prompt string) (json.RawMessage, error) {
	switch {
	case runID == "" || stepID == "":
		return nil, errors.New("llm: a run and a step are required")
	case strings.TrimSpace(prompt) == "":
		return nil, errors.New("llm: a prompt is required")
	case s.store == nil:
		return nil, errors.New("llm: a run store is required; a step that cannot halt its run must not start it")
	case s.calls == nil:
		return nil, errors.New("llm: a call recorder is required; an unrecorded model call is an unauditable one")
	}

	var lastErr error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		obj, err := s.attemptOnce(ctx, runID, stepID, uint32(attempt), prompt) //nolint:gosec // attempt is bounded by s.attempts
		if err == nil {
			return obj, nil
		}
		lastErr = err
		if !retryable(err) {
			break
		}
	}

	// Out of attempts, or stopped on something no repeat would fix. Either
	// way the step is over and the run stops with it.
	reason := redact(lastErr.Error())
	if err := s.halt(ctx, runID, stepID, reason); err != nil {
		return nil, errors.Join(lastErr, err)
	}
	return nil, lastErr
}

// retryable says whether asking the same model the same question again could
// plausibly give a different answer. Only the model's own failure to follow
// instructions qualifies — models do this and then stop doing it. A provider
// that refused the call, or a ceiling that was breached, will refuse and
// breach again.
func retryable(err error) bool {
	return errors.Is(err, ErrNoObject) || errors.Is(err, ErrObjectInvalid)
}

// attemptOnce is one call to the model, recorded whatever its outcome.
func (s *Step) attemptOnce(
	ctx context.Context, runID, stepID string, attempt uint32, prompt string,
) (json.RawMessage, error) {
	// A fresh observer per attempt: it is the only way to get at the
	// provider's own response, which GenerateObject does not return, and
	// sharing one across attempts would attribute this call's fingerprint to
	// the last one's response.
	obs := &observedModel{inner: s.model, schema: s.cfg.OutputSchema}

	started := s.now()
	result, genErr := ai.GenerateObject[rawObject](ctx, ai.GenerateObjectOpts{
		Model:       obs,
		Prompt:      prompt,
		SchemaName:  "output",
		MaxTokens:   s.maxTokens(),
		Temperature: s.temperature(),
		// The SDK's transport retry is switched off: ADR 0002 makes the
		// effect class the one thing that decides whether repeating a call is
		// safe, and a retry budget hidden inside a library is a second,
		// invisible answer to that question.
		MaxRetries: new(int),
	})
	latency := s.now().Sub(started)

	resp := obs.response()
	call := Call{
		RunID: runID, StepID: stepID, Attempt: attempt,
		Prompt: prompt, Latency: latency, At: started,
	}
	if resp != nil {
		call.ModelFingerprint = Fingerprint(*resp)
		call.Response = responseText(resp)
		call.PromptTokens = resp.Usage.InputTokens
		call.CompletionTokens = resp.Usage.OutputTokens
	}
	if result != nil && result.RawText != "" {
		// In forced-tool mode the object arrives as tool arguments rather
		// than as text, and the arguments are what the step was answered
		// with.
		call.Response = result.RawText
	}

	// Recorded BEFORE the outcome is judged. A call that has been made has
	// cost money and has shown somebody's data to a third party, and that is
	// true whether or not its answer turned out to be usable.
	recErr := s.calls.Record(ctx, s.tenantID, call)
	s.logCall(call, genErr)
	if recErr != nil {
		return nil, recErr
	}

	if genErr != nil {
		return nil, s.callError(runID, stepID, attempt, genErr)
	}
	if resp == nil {
		return nil, fmt.Errorf("llm: %s/%s: %w", runID, stepID, ErrModelUnattributable)
	}
	// The ceiling is checked BEFORE the object is looked at, because a
	// truncated answer that happens to parse is the case this guards.
	if over, detail := s.overCeiling(resp); over {
		return nil, fmt.Errorf("llm: %s/%s: %w: %s", runID, stepID, ErrTokenCeiling, detail)
	}
	if call.ModelFingerprint == "" {
		return nil, fmt.Errorf("llm: %s/%s: %w", runID, stepID, ErrModelUnattributable)
	}
	if err := s.validate(result.Object.bytes); err != nil {
		return nil, fmt.Errorf("llm: %s/%s attempt %d: %w", runID, stepID, attempt, err)
	}
	return result.Object.bytes, nil
}

// callError turns a provider or decode failure into this package's vocabulary,
// with the provider's message REDACTED and the original error deliberately
// dropped from the chain — a caller unwrapping to the SDK's error could read
// the unredacted text back out of it.
func (s *Step) callError(runID, stepID string, attempt uint32, err error) error {
	var noObject *ai.NoObjectGeneratedError
	if errors.As(err, &noObject) {
		return fmt.Errorf("llm: %s/%s attempt %d: %w: %s",
			runID, stepID, attempt, ErrNoObject, redact(err.Error()))
	}
	return fmt.Errorf("llm: %s/%s attempt %d: %w: %s",
		runID, stepID, attempt, ErrProviderCall, redact(err.Error()))
}

// overCeiling reports whether the call broke its token budget, and how.
//
// Both halves matter. A provider that stopped because it hit the output cap
// says so in its finish reason and the text it produced is truncated; a call
// whose reported total went past the budget spent more than it was allowed
// whether or not it was cut off.
func (s *Step) overCeiling(resp *provider.Response) (bool, string) {
	if s.cfg.MaxTokens <= 0 {
		return false, ""
	}
	if resp.FinishReason == provider.FinishLength {
		return true, fmt.Sprintf(
			"the model stopped at the %d-token cap, so its answer is truncated", s.cfg.MaxTokens)
	}
	total := resp.Usage.TotalTokens
	if total == 0 {
		total = resp.Usage.InputTokens + resp.Usage.OutputTokens
	}
	if total > s.cfg.MaxTokens {
		return true, fmt.Sprintf("the call spent %d tokens against a ceiling of %d",
			total, s.cfg.MaxTokens)
	}
	return false, ""
}

// validate holds the model's answer to the schema the pipeline declared.
func (s *Step) validate(raw json.RawMessage) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: the model returned nothing", ErrObjectInvalid)
	}
	value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%w: %s", ErrObjectInvalid, redact(err.Error()))
	}
	if err := s.schema.Validate(value); err != nil {
		return fmt.Errorf("%w: %s", ErrObjectInvalid, redact(err.Error()))
	}
	return nil
}

// halt closes the run. Returning an error from Run is not enough on its own:
// the run is a persisted state machine (ADR 0003), and a scheduler reading a
// log with no failure in it will dispatch whatever comes next.
func (s *Step) halt(ctx context.Context, runID, stepID, reason string) error {
	failure, err := scheduler.MarshalRunFailure(scheduler.RunFailure{Steps: []string{stepID}})
	if err != nil {
		return fmt.Errorf("llm: halting %s: %w", runID, err)
	}
	stepFailure, err := json.Marshal(struct {
		StepID string `json:"step_id"`
		Reason string `json:"reason"`
	}{StepID: stepID, Reason: reason})
	if err != nil {
		return fmt.Errorf("llm: halting %s: %w", runID, err)
	}

	at := s.now().UTC()

	// One transaction: a step recorded as failed in a run that was never
	// closed is a run that sits ready to advance past it.
	return s.store.WithTx(ctx, func(tx runstore.Tx) error {
		for _, e := range []runstore.Event{
			{RunID: runID, StepID: stepID, Type: runstore.StepFailed, Payload: stepFailure, At: at},
			{RunID: runID, Type: scheduler.RunFailed, Payload: failure, At: at},
		} {
			// Sequence stays 0: the store allocates it inside this
			// transaction. A precomputed one can collide with another
			// caller's, and the loser is discarded without an error.
			if err := tx.Append(ctx, s.tenantID, e); err != nil {
				return fmt.Errorf("llm: halting %s: %w", runID, err)
			}
		}
		return nil
	})
}

// logCall records that a call happened, and what it cost. It does NOT log the
// prompt or the response: those are the privacy surface, they live in
// llm_calls under that table's own retention, and a log ships off the box to
// somewhere with different retention and a different audience.
func (s *Step) logCall(c Call, err error) {
	attrs := []any{
		"run", c.RunID, "step", c.StepID, "attempt", c.Attempt,
		"model_fingerprint", c.ModelFingerprint,
		"prompt_tokens", c.PromptTokens, "completion_tokens", c.CompletionTokens,
		"latency_ms", c.Latency.Milliseconds(),
	}
	if err != nil {
		s.log.Warn("llm call failed", append(attrs, "error", redact(err.Error()))...)
		return
	}
	s.log.Info("llm call", attrs...)
}

func (s *Step) maxTokens() *int {
	if s.cfg.MaxTokens <= 0 {
		return nil
	}
	n := s.cfg.MaxTokens
	return &n
}

func (s *Step) temperature() *float64 {
	t := float64(s.cfg.Temperature)
	return &t
}

// responseText is what the model said, for the record. Tool-call arguments are
// included because in forced-tool mode that IS the answer, and a record whose
// response column is empty for every provider without native JSON mode records
// nothing about most calls.
func responseText(resp *provider.Response) string {
	if text := resp.Text(); text != "" {
		return text
	}
	var b strings.Builder
	for _, c := range resp.ToolCalls() {
		b.Write(c.Args)
	}
	return b.String()
}

// rawObject is the type GenerateObject decodes into.
//
// The SDK derives its schema by reflecting a Go type, but a Dhole output
// schema is DATA — a pipeline author writes it in YAML — so there is no Go
// type to reflect. rawObject therefore captures the whole decoded document
// verbatim, the declared schema is substituted into the outgoing call by
// observedModel, and the answer is validated against that same declared schema
// here rather than against anything reflected.
type rawObject struct {
	bytes json.RawMessage
}

// UnmarshalJSON keeps the document as it arrived. encoding/json has already
// checked it is syntactically valid by the time this is called, which is
// exactly the check the retry loop treats as "the model did not answer in
// JSON".
func (o *rawObject) UnmarshalJSON(b []byte) error {
	o.bytes = append(json.RawMessage(nil), b...)
	return nil
}

// observedModel wraps the real model to do two things GenerateObject does not
// offer a hook for: send the PIPELINE's declared schema rather than the one
// reflected from rawObject, and keep the provider's own response so the call
// can be fingerprinted and its usage recorded.
type observedModel struct {
	inner  provider.LanguageModel
	schema json.RawMessage

	mu   sync.Mutex
	last *provider.Response
}

func (m *observedModel) ModelID() string      { return m.inner.ModelID() }
func (m *observedModel) ProviderName() string { return m.inner.ProviderName() }

func (m *observedModel) Capabilities() provider.Capabilities { return m.inner.Capabilities() }

func (m *observedModel) Generate(ctx context.Context, call provider.Call) (*provider.Response, error) {
	resp, err := m.inner.Generate(ctx, m.pin(call))
	if resp != nil {
		m.mu.Lock()
		m.last = resp
		m.mu.Unlock()
	}
	return resp, err
}

func (m *observedModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, errors.New("llm: the model step does not stream")
}

func (m *observedModel) response() *provider.Response {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last
}

// pin substitutes the declared schema into whichever slot the SDK chose —
// native JSON response format, or a forced tool — without mutating the
// caller's structures.
func (m *observedModel) pin(call provider.Call) provider.Call {
	if call.ResponseFormat != nil {
		format := *call.ResponseFormat
		format.Schema = m.schema
		call.ResponseFormat = &format
	}
	if len(call.Tools) > 0 {
		tools := make([]provider.ToolDef, len(call.Tools))
		copy(tools, call.Tools)
		for i := range tools {
			tools[i].Schema = m.schema
		}
		call.Tools = tools
	}
	return call
}
