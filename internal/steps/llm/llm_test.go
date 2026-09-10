package llm_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/llm"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

const (
	runID  = "run-1"
	stepID = "summarise"

	// alias is what a pipeline author types. It resolves to whatever the
	// provider is serving today, which is the whole problem this package's
	// fingerprint exists for.
	alias = "claude-sonnet-latest"

	// pinnedModel carries an explicit digest, so it names ONE model for ever.
	pinnedModel = "claude-sonnet@sha256:" +
		"1111111111111111111111111111111111111111111111111111111111111111"

	// The two things the alias resolved to on two different days.
	resolvedA = "claude-sonnet-20260101"
	resolvedB = "claude-sonnet-20260601"
)

// outputSchema is the contract the step holds the model to. It is DECLARED
// data, not a Go type: a pipeline author writes it in YAML, so the step has to
// enforce it at runtime rather than at compile time.
var outputSchema = []byte(`{
	"type": "object",
	"properties": {"summary": {"type": "string"}},
	"required": ["summary"],
	"additionalProperties": false
}`)

// TestLLMStepSchemaFingerprintAndBudgetCeiling is the step's core contract:
// a validated object comes back, the call is recorded in full, and a run that
// blows its token ceiling STOPS rather than carrying a truncated answer
// forward. The last one is the dangerous case — a truncated answer that parses
// looks exactly like a complete one to everything downstream.
func TestLLMStepSchemaFingerprintAndBudgetCeiling(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		t.Run("object is returned, validated and recorded", func(t *testing.T) {
			ctx := testContext(t)
			h := newHarness(t, open, config(), reply(
				`{"summary":"the quarter went well"}`, resolvedA, 120, 30, provider.FinishStop))

			obj, err := h.step.Run(ctx, runID, stepID, "summarise the report")
			require.NoError(t, err)
			require.JSONEq(t, `{"summary":"the quarter went well"}`, string(obj))

			calls := h.records(ctx, t)
			require.Len(t, calls, 1, "every model call is recorded, once")
			rec := calls[0]
			require.Equal(t, runID, rec.RunID)
			require.Equal(t, stepID, rec.StepID)
			require.Equal(t, uint32(1), rec.Attempt)
			require.Equal(t, "summarise the report", rec.Prompt,
				"the prompt is the record's reason for existing")
			require.JSONEq(t, `{"summary":"the quarter went well"}`, rec.Response)
			require.Equal(t, 120, rec.PromptTokens)
			require.Equal(t, 30, rec.CompletionTokens)
			require.Equal(t, tick, rec.Latency, "the wall time the call cost is recorded")

			require.NotEmpty(t, rec.ModelFingerprint)
			require.Equal(t, llm.Fingerprint(*h.model.last()), rec.ModelFingerprint,
				"the recorded fingerprint is the one Fingerprint computes from the response")
			require.NotContains(t, rec.ModelFingerprint, alias,
				"a fingerprint that quotes the alias records the question, not the answer")

			// The declared schema — not a Go type reflected by the SDK — is
			// what the provider was asked to produce.
			require.JSONEq(t, string(outputSchema), string(h.model.schemaSent(t)),
				"the provider is sent the pipeline's declared output schema")
		})

		t.Run("an object that violates the schema is not returned", func(t *testing.T) {
			ctx := testContext(t)
			// Valid JSON, wrong shape: `summary` is a number, and the schema
			// says string. Without validation this reaches the next step and
			// fails somewhere with no connection to the model that caused it.
			h := newHarness(t, open, config(),
				reply(`{"summary":42}`, resolvedA, 10, 5, provider.FinishStop),
				reply(`{"summary":42}`, resolvedA, 10, 5, provider.FinishStop),
				reply(`{"summary":42}`, resolvedA, 10, 5, provider.FinishStop))

			obj, err := h.step.Run(ctx, runID, stepID, "summarise the report")
			require.Error(t, err)
			require.ErrorIs(t, err, llm.ErrObjectInvalid)
			require.Nil(t, obj, "a schema violation must not flow downstream")
		})

		t.Run("the token ceiling fails the step", func(t *testing.T) {
			ctx := testContext(t)
			cfg := config()
			cfg.MaxTokens = 100
			// Perfectly parseable, schema-valid, and TRUNCATED: the provider
			// says it stopped because it ran out of budget. Returning this is
			// the failure mode — it looks complete.
			h := newHarness(t, open, cfg, reply(
				`{"summary":"the quarter went"}`, resolvedA, 90, 40, provider.FinishLength))

			obj, err := h.step.Run(ctx, runID, stepID, "summarise the report")
			require.Error(t, err)
			require.ErrorIs(t, err, llm.ErrTokenCeiling)
			require.Nil(t, obj, "a truncated answer is not an answer")

			// Failing means the LOG says so, not merely that Run returned an
			// error: a scheduler reading a log with no failure in it goes on
			// dispatching as though the step had never run.
			events := h.events(ctx, t)
			require.Contains(t, events, runstore.StepFailed,
				"the step is recorded as failed")
			require.NotContains(t, events, scheduler.RunFailed,
				"whether the run survives a failed step is the graph's decision, not this step's")

			// What it cost is still recorded: a call that burned budget
			// happened whether or not its answer was usable.
			calls := h.records(ctx, t)
			require.Len(t, calls, 1)
			require.Equal(t, 90, calls[0].PromptTokens)
			require.Equal(t, 40, calls[0].CompletionTokens)
		})

		t.Run("a total over the ceiling fails even when the model says stop", func(t *testing.T) {
			ctx := testContext(t)
			cfg := config()
			cfg.MaxTokens = 50
			h := newHarness(t, open, cfg, reply(
				`{"summary":"fine"}`, resolvedA, 200, 10, provider.FinishStop))

			_, err := h.step.Run(ctx, runID, stepID, "summarise the report")
			require.ErrorIs(t, err, llm.ErrTokenCeiling,
				"the ceiling is a budget, and the provider's own finish reason is not the only way past it")
			require.Contains(t, h.events(ctx, t), runstore.StepFailed)
		})
	})
}

// TestMalformedObjectIsRetriedThenFailsWithProviderError. A model that answers
// with something that is not JSON is retried — models do this and then stop
// doing it — but a step that has run out of attempts fails with the provider's
// own error and returns NOTHING. A half-parsed object flowing downstream is
// worse than a failure, because the failure is visible.
func TestMalformedObjectIsRetriedThenFailsWithProviderError(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		// Three answers, each of which begins as if it were the object and is
		// then cut off — the shape a partial-object bug would happily accept.
		garbage := reply(`{"summary":"the quarter went we`, resolvedA, 10, 5, provider.FinishStop)
		h := newHarness(t, open, config(), garbage, garbage, garbage)

		obj, err := h.step.Run(ctx, runID, stepID, "summarise the report")
		require.Error(t, err)
		require.Nil(t, obj, "NEVER a partial object")
		require.ErrorIs(t, err, llm.ErrNoObject)
		require.Contains(t, err.Error(), "no object generated",
			"the provider's own error is what gets reported, not a summary of it")

		require.Equal(t, 3, h.model.callCount(), "three attempts, then stop")

		calls := h.records(ctx, t)
		require.Len(t, calls, 3, "each attempt is recorded — including the ones that failed")
		for i, c := range calls {
			require.Equal(t, uint32(i+1), c.Attempt) //nolint:gosec // small loop index
			require.Equal(t, `{"summary":"the quarter went we`, c.Response,
				"what the model actually said is recorded, so the failure can be diagnosed")
			require.NotEmpty(t, c.ModelFingerprint)
		}

		require.Contains(t, h.events(ctx, t), runstore.StepFailed,
			"a step out of attempts fails, in the log and not only in its return value")
	})
}

// TestAnOffSchemaAnswerFailsTheStepAndLeavesTheRunToTheGraph is the default a
// step type is entitled to choose, and halting the run was the wrong one.
//
// A model that will not answer on schema is a STEP that failed. Whether the
// run continues is the graph's business and nobody else's — the effect class
// decides whether the step may be tried again, the edges decide what was
// downstream of it, and a branch that has nothing to do with the model call
// carries on. A step type that closed the run took that decision away from
// every pipeline that used it, and made an off-schema answer impossible to
// assert inside a run that has to keep going.
//
// What does NOT change is that the step fails. Nothing off-schema reaches a
// downstream step, Run returns no object, and the log says the step failed
// where anyone looking at the run can see it.
func TestAnOffSchemaAnswerFailsTheStepAndLeavesTheRunToTheGraph(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		// Valid JSON, wrong shape, three times over: the model has had every
		// attempt it is allowed and has never answered on schema.
		offSchema := reply(`{"summary":42}`, resolvedA, 10, 5, provider.FinishStop)
		h := newHarness(t, open, config(), offSchema, offSchema, offSchema)

		obj, err := h.step.Run(ctx, runID, stepID, "summarise the report")
		require.ErrorIs(t, err, llm.ErrObjectInvalid)
		require.Nil(t, obj, "an off-schema answer must never flow downstream")

		events := h.events(ctx, t)
		require.Contains(t, events, runstore.StepFailed,
			"a step that cannot produce a valid answer FAILS; it does not quietly pass")
		require.NotContains(t, events, scheduler.RunFailed,
			"the step type closed the run it was given; that is the graph's decision, not its own")
	})
}

// TestAStepAskedToHaltDoesEndTheRunItIsGiven keeps the behaviour available for
// the caller that genuinely wants it — a run whose whole purpose is the model
// call has nothing left to do when the model will not answer — while making it
// something a deployment asks for rather than something a step type imposes.
func TestAStepAskedToHaltDoesEndTheRunItIsGiven(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		offSchema := reply(`{"summary":42}`, resolvedA, 10, 5, provider.FinishStop)
		h := newHaltingHarness(t, open, config(), offSchema, offSchema, offSchema)

		_, err := h.step.Run(ctx, runID, stepID, "summarise the report")
		require.ErrorIs(t, err, llm.ErrObjectInvalid)

		events := h.events(ctx, t)
		require.Contains(t, events, runstore.StepFailed)
		require.Contains(t, events, scheduler.RunFailed,
			"a step asked to halt its run must still be able to")
	})
}

// TestFingerprintIncludesResolvedModelNotAlias is the property ADR 0002's
// purity claim rests on. "latest" repoints under you; a cache key folded over
// the alias serves yesterday's model's answer as today's, and nothing about
// the run says anything changed.
func TestFingerprintIncludesResolvedModelNotAlias(t *testing.T) {
	// Two responses to the SAME alias, from two different underlying models.
	first := reply(`{"summary":"a"}`, resolvedA, 10, 5, provider.FinishStop)
	second := reply(`{"summary":"a"}`, resolvedB, 10, 5, provider.FinishStop)

	fpA, fpB := llm.Fingerprint(*first), llm.Fingerprint(*second)
	require.NotEmpty(t, fpA)
	require.NotEqual(t, fpA, fpB,
		"the same alias served by two models must not fingerprint the same")
	require.Equal(t, fpA, llm.Fingerprint(*reply(
		`{"summary":"completely different text"}`, resolvedA, 999, 1, provider.FinishStop)),
		"the fingerprint identifies the MODEL, not the answer it happened to give")

	// A response nobody can attribute gets no fingerprint at all. Falling back
	// to the alias here is the exact bug this test exists to prevent.
	require.Empty(t, llm.Fingerprint(provider.Response{}),
		"an unattributable response has no model fingerprint")

	// And the Task 15 cache key moves with it: the fingerprint is the LLM
	// step's environment identity.
	step := &dholev1.Step{
		Id:          stepID,
		PluginRef:   "llm://anthropic",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	}
	inputs := []*dholev1.Digest{{Algo: "sha256", Hex: "abc"}}
	keyA, err := cache.Key(step, fpA, inputs, nil)
	require.NoError(t, err)
	keyB, err := cache.Key(step, fpB, inputs, nil)
	require.NoError(t, err)
	require.NotEqual(t, keyA.GetHex(), keyB.GetHex(),
		"a repointed alias must change the cache key, or the cache serves the old model's answer")

	// The step's own record must carry the resolved fingerprint too — a step
	// that fingerprints s.cfg.Model would pass the checks above and still be
	// wrong.
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(t, open, config(), first)
		_, err := h.step.Run(ctx, runID, stepID, "summarise the report")
		require.NoError(t, err)

		h2 := newHarness(t, open, config(), second)
		_, err = h2.step.Run(ctx, "run-2", stepID, "summarise the report")
		require.NoError(t, err)

		a := h.records(ctx, t)
		b := h2.recordsFor(ctx, t, "run-2")
		require.Len(t, a, 1)
		require.Len(t, b, 1)
		require.Equal(t, fpA, a[0].ModelFingerprint)
		require.Equal(t, fpB, b[0].ModelFingerprint)
		require.NotEqual(t, a[0].ModelFingerprint, b[0].ModelFingerprint,
			"one alias, two models, two records that must not claim to be the same model")
	})
}

// TestLLMStepIsPureOnlyWithPinnedModelAndZeroTemperature. PURE means "the same
// inputs must produce the same output" (ADR 0002), and a model call earns that
// only when nothing about it can move: temperature 0 AND a digest-pinned
// model. Everything else is IDEMPOTENT — retried, never cached.
func TestLLMStepIsPureOnlyWithPinnedModelAndZeroTemperature(t *testing.T) {
	for _, tc := range []struct {
		name  string
		model string
		temp  float32
		want  dholev1.EffectClass
		why   string
	}{
		{"pinned and deterministic", pinnedModel, 0,
			dholev1.EffectClass_EFFECT_CLASS_PURE,
			"nothing can move: the digest names one model and the sampler is off"},
		{"pinned but sampled", pinnedModel, 0.7,
			dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			"a sampled answer is a different answer each time"},
		{"barely sampled", pinnedModel, 0.01,
			dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			"a small temperature is still a temperature"},
		{"alias at zero temperature", alias, 0,
			dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			"an alias silently repoints, so the same input yields a different model's output"},
		{"dated but unpinned", "claude-sonnet-20260101", 0,
			dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			"a date in the name is a convention, not a pin"},
		{"truncated digest", "claude-sonnet@sha256:1111", 0,
			dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			"a digest that is not a digest pins nothing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.Model = tc.model
			cfg.Temperature = tc.temp
			step, err := llm.New(cfg, llm.Options{
				Model:    &stubModel{alias: tc.model, nativeJSON: true},
				Store:    nil,
				Calls:    nil,
				TenantID: "acme",
			})
			require.NoError(t, err, "effect class is answerable without a store")
			require.Equal(t, tc.want, step.EffectClass(), tc.why)
		})
	}

	// And the cache agrees: only the pure configuration is cacheable at all.
	pure := &dholev1.Step{Id: stepID, PluginRef: "llm://x",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE}
	idempotent := &dholev1.Step{Id: stepID, PluginRef: "llm://x",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT}
	_, err := cache.Key(pure, "sha256:fp", nil, nil)
	require.NoError(t, err)
	_, err = cache.Key(idempotent, "sha256:fp", nil, nil)
	require.ErrorIs(t, err, cache.ErrNotCacheable,
		"a sampled or aliased model call must never be cached")
}

// TestRecordsAreTenantScopedAndRetentionIsHonoured. The call record is a
// privacy surface: it holds the prompt, which is whatever the pipeline fed the
// model — a customer's complaint, a candidate's CV, a patient's note. Two
// things follow, and both are enforced rather than documented. It is scoped to
// one tenant on every path, and the retention window is something that
// actually deletes rows rather than a column nobody reads.
func TestRecordsAreTenantScopedAndRetentionIsHonoured(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		t.Run("an empty tenant is refused everywhere", func(t *testing.T) {
			ctx := testContext(t)
			_, db, dialect := open(t)
			rec, err := llm.NewRecorder(db, dialect, time.Hour)
			require.NoError(t, err)

			_, err = llm.New(config(), llm.Options{
				Model: &stubModel{alias: pinnedModel}, Calls: rec, TenantID: "",
			})
			require.ErrorContains(t, err, "tenant scope required")

			require.ErrorIs(t, rec.Record(ctx, "", llm.Call{RunID: runID}), runstore.ErrTenantRequired)
			_, err = rec.Calls(ctx, "", runID)
			require.ErrorIs(t, err, runstore.ErrTenantRequired)
		})

		t.Run("one tenant never sees another's prompts", func(t *testing.T) {
			ctx := testContext(t)
			_, db, dialect := open(t)
			rec, err := llm.NewRecorder(db, dialect, time.Hour)
			require.NoError(t, err)

			mine, theirs := uniqueTenant(t), uniqueTenant(t)
			require.NoError(t, rec.Record(ctx, mine, llm.Call{
				RunID: runID, StepID: stepID, Attempt: 1,
				Prompt: "my customer's complaint", Response: "{}", At: clockStart,
			}))
			require.NoError(t, rec.Record(ctx, theirs, llm.Call{
				RunID: runID, StepID: stepID, Attempt: 1,
				Prompt: "their customer's complaint", Response: "{}", At: clockStart,
			}))

			got, err := rec.Calls(ctx, mine, runID)
			require.NoError(t, err)
			require.Len(t, got, 1, "the same run id in another tenant is another run")
			require.Equal(t, "my customer's complaint", got[0].Prompt)
		})

		t.Run("retention deletes what is past its window and nothing else", func(t *testing.T) {
			ctx := testContext(t)
			_, db, dialect := open(t)
			// A one-hour window: prompts do not sit in the database for ever
			// because nobody wrote the sweep.
			rec, err := llm.NewRecorder(db, dialect, time.Hour)
			require.NoError(t, err)

			tenant := uniqueTenant(t)
			require.NoError(t, rec.Record(ctx, tenant, llm.Call{
				RunID: "old", StepID: stepID, Attempt: 1,
				Prompt: "an old prompt", Response: "{}", At: clockStart.Add(-2 * time.Hour),
			}))
			require.NoError(t, rec.Record(ctx, tenant, llm.Call{
				RunID: "fresh", StepID: stepID, Attempt: 1,
				Prompt: "a fresh prompt", Response: "{}", At: clockStart,
			}))

			n, err := rec.Purge(ctx, clockStart)
			require.NoError(t, err)
			require.Equal(t, int64(1), n, "exactly the expired record is reclaimed")

			old, err := rec.Calls(ctx, tenant, "old")
			require.NoError(t, err)
			require.Empty(t, old, "a prompt past its retention window is GONE, not merely marked")

			fresh, err := rec.Calls(ctx, tenant, "fresh")
			require.NoError(t, err)
			require.Len(t, fresh, 1, "a purge that takes live records is worse than no purge")
		})

		t.Run("a recorder with no retention window is refused", func(t *testing.T) {
			_, db, dialect := open(t)
			_, err := llm.NewRecorder(db, dialect, 0)
			require.ErrorContains(t, err, "retention",
				"prompt content kept for ever is a decision nobody gets to make by omission")
		})
	})
}

// TestCredentialsNeverReachRecordsLogsOrErrors. The step is handed a
// constructed model, never a key — but a provider echoes the request back in
// its error bodies, and a prompt can be built from a template that pulled one
// in. Anything this package writes down goes through the redactor, because the
// call record outlives the process and the log leaves the tenant boundary.
func TestCredentialsNeverReachRecordsLogsOrErrors(t *testing.T) {
	const secret = "sk-ant-api03-SUPERSECRETVALUE0000000000"

	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		var logged strings.Builder
		logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

		failing := &stubModel{alias: alias, nativeJSON: true, turns: []stubTurn{{
			err: errors.New("401 unauthorized: header Authorization: Bearer " + secret),
		}}}
		step, h := newHarnessWith(t, open, config(), failing, logger)
		_ = h

		obj, err := step.Run(ctx, runID, stepID, "summarise, api_key="+secret)
		require.Error(t, err)
		require.Nil(t, obj)
		require.NotContains(t, err.Error(), secret,
			"an error message is the most-copied string in an incident")

		for _, c := range h.records(ctx, t) {
			require.NotContains(t, c.Prompt, secret,
				"a credential in the prompt must not be persisted alongside it")
			require.NotContains(t, c.Response, secret)
		}

		out := logged.String()
		require.NotEmpty(t, out, "the step logs its calls, or this test proves nothing")
		require.NotContains(t, out, secret, "no credential reaches a log line")
		require.NotContains(t, out, "summarise, api_key=",
			"the prompt itself never reaches the log: logs have their own retention")
	})
}

// --- harness ------------------------------------------------------------

// tick is how far the test clock moves per reading, so a recorded latency is
// an exact value rather than "something positive".
const tick = 250 * time.Millisecond

var clockStart = time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

func config() llm.Config {
	return llm.Config{
		Provider:     "anthropic",
		Model:        pinnedModel,
		Temperature:  0,
		OutputSchema: outputSchema,
		MaxTokens:    4096,
	}
}

// reply builds a provider response that NAMES the model that produced it, the
// way every real provider does in its response body. The alias is not in here
// at all: the resolved id is the only model identity a response carries.
func reply(text, resolvedModel string, in, out int, finish provider.FinishReason) *provider.Response {
	return &provider.Response{
		Content:      []provider.ContentPart{provider.TextPart{Text: text}},
		FinishReason: finish,
		Usage:        provider.Usage{InputTokens: in, OutputTokens: out, TotalTokens: in + out},
		Raw:          json.RawMessage(fmt.Sprintf(`{"model":%q}`, resolvedModel)),
	}
}

// stubModel is the language model under test. It NEVER makes a network call.
//
// Its ModelID is the alias and its responses name a different, resolved model,
// which is deliberate: a stub whose id matches what it returns cannot tell a
// fingerprint over the alias apart from one over the resolved model, and would
// pass whichever the implementation used.
type stubModel struct {
	alias      string
	nativeJSON bool

	mu    sync.Mutex
	turns []stubTurn
	calls []provider.Call
	resps []*provider.Response
}

type stubTurn struct {
	resp *provider.Response
	err  error
}

func (m *stubModel) ModelID() string      { return m.alias }
func (m *stubModel) ProviderName() string { return "stub" }

func (m *stubModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: m.nativeJSON}
}

func (m *stubModel) Generate(_ context.Context, call provider.Call) (*provider.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, call)
	if len(m.turns) == 0 {
		return nil, fmt.Errorf("stub: no scripted answer for call %d", len(m.calls))
	}
	turn := m.turns[0]
	if len(m.turns) > 1 {
		m.turns = m.turns[1:]
	}
	m.resps = append(m.resps, turn.resp)
	return turn.resp, turn.err
}

func (m *stubModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	panic("the llm step does not stream")
}

func (m *stubModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *stubModel) last() *provider.Response {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resps[len(m.resps)-1]
}

// schemaSent is the JSON Schema the provider was actually asked to honour,
// from either the native-JSON response format or the forced tool.
func (m *stubModel) schemaSent(t *testing.T) json.RawMessage {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	require.NotEmpty(t, m.calls, "the model was never called")
	call := m.calls[0]
	if call.ResponseFormat != nil {
		return call.ResponseFormat.Schema
	}
	require.Len(t, call.Tools, 1, "object generation forces exactly one tool")
	return call.Tools[0].Schema
}

type harness struct {
	step   *llm.Step
	model  *stubModel
	rec    *llm.Recorder
	store  runstore.Store
	tenant string
}

func newHarness(t *testing.T, open storeOpener, cfg llm.Config, replies ...*provider.Response) *harness {
	t.Helper()
	turns := make([]stubTurn, 0, len(replies))
	for _, r := range replies {
		turns = append(turns, stubTurn{resp: r})
	}
	_, h := newHarnessWith(t, open, cfg, &stubModel{alias: alias, nativeJSON: true, turns: turns}, nil)
	return h
}

// newHaltingHarness is newHarness for the caller that asked the step to close
// the run when it gives up.
func newHaltingHarness(
	t *testing.T, open storeOpener, cfg llm.Config, replies ...*provider.Response,
) *harness {
	t.Helper()
	turns := make([]stubTurn, 0, len(replies))
	for _, r := range replies {
		turns = append(turns, stubTurn{resp: r})
	}
	_, h := newHarnessOptions(t, open, cfg,
		&stubModel{alias: alias, nativeJSON: true, turns: turns}, nil,
		func(o *llm.Options) { o.HaltsRun = true })
	return h
}

func newHarnessWith(
	t *testing.T, open storeOpener, cfg llm.Config, model *stubModel, logger *slog.Logger,
) (*llm.Step, *harness) {
	t.Helper()
	return newHarnessOptions(t, open, cfg, model, logger)
}

func newHarnessOptions(
	t *testing.T, open storeOpener, cfg llm.Config, model *stubModel, logger *slog.Logger,
	tune ...func(*llm.Options),
) (*llm.Step, *harness) {
	t.Helper()
	store, db, dialect := open(t)
	rec, err := llm.NewRecorder(db, dialect, time.Hour)
	require.NoError(t, err)

	tenant := uniqueTenant(t)
	opts := llm.Options{
		Model:    model,
		Store:    store,
		Calls:    rec,
		TenantID: tenant,
		Attempts: 3,
		Logger:   logger,
		Now:      testClock(),
	}
	for _, fn := range tune {
		fn(&opts)
	}
	step, err := llm.New(cfg, opts)
	require.NoError(t, err)
	return step, &harness{step: step, model: model, rec: rec, store: store, tenant: tenant}
}

// testClock advances by a fixed tick on every reading, so the latency the step
// records is an exact, asserted number rather than "more than zero".
func testClock() func() time.Time {
	var mu sync.Mutex
	n := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		at := clockStart.Add(time.Duration(n) * tick)
		n++
		return at
	}
}

func (h *harness) records(ctx context.Context, t *testing.T) []llm.Call {
	t.Helper()
	return h.recordsFor(ctx, t, runID)
}

func (h *harness) recordsFor(ctx context.Context, t *testing.T, run string) []llm.Call {
	t.Helper()
	calls, err := h.rec.Calls(ctx, h.tenant, run)
	require.NoError(t, err)
	return calls
}

func (h *harness) events(ctx context.Context, t *testing.T) []runstore.EventType {
	t.Helper()
	events, err := h.store.Replay(ctx, h.tenant, runID)
	require.NoError(t, err)
	kinds := make([]runstore.EventType, 0, len(events))
	for _, e := range events {
		kinds = append(kinds, e.Type)
	}
	return kinds
}

// --- both dialects ------------------------------------------------------

type storeOpener func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect)

// eachStore runs a case against BOTH dialects. A store proven only against
// SQLite is a store that has never met the deployment target.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		dir := t.TempDir()
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			path := filepath.Join(dir, "run.db")
			store, err := runstore.NewSQLite(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			db, err := runstore.OpenSQLite(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			return store, db, runstore.DialectSQLite
		})
	})
	t.Run("postgres", func(t *testing.T) {
		if scopedDSN == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			ctx := context.Background()
			store, err := runstore.NewPostgres(ctx, scopedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			db, err := runstore.OpenPostgres(ctx, scopedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			return store, db, runstore.DialectPostgres
		})
	})
}

// Postgres is one shared database for the whole suite, so this binary gets its
// own schema: the migrations run inside it and nothing here can be mistaken
// for another package's rows.
const testSchema = "dhole_llm_test"

var scopedDSN string

func TestMain(m *testing.M) {
	code := func() int {
		dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
		if dsn == "" {
			return m.Run()
		}
		schema := fmt.Sprintf("%s_%d", testSchema, os.Getpid())
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "postgres: %v\n", err)
			return 1
		}
		defer func() { _ = db.Close() }()
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + schema); err != nil {
			fmt.Fprintf(os.Stderr, "postgres: create schema: %v\n", err)
			return 1
		}
		defer func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") }()
		scopedDSN = dsn + "&search_path=" + schema
		return m.Run()
	}()
	os.Exit(code)
}

// uniqueTenant keeps every case in its own tenant. Postgres is shared and
// persistent, so an assertion that only holds because an earlier run's rows
// were absent is not an assertion.
var (
	tenantMu  sync.Mutex
	tenantSeq int
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	tenantMu.Lock()
	defer tenantMu.Unlock()
	tenantSeq++
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%d-%d", name, os.Getpid(), tenantSeq)
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}
