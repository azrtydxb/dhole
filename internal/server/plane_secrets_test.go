package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// modelSecretName is what a model configuration names its credential by. It is
// a NAME in the step's config and never a value: ADR 0024.
const modelSecretName = "ANTHROPIC_API_KEY"

// modelKey is the credential every case in this file resolves. It is shaped
// like a real Anthropic key and it is long and unlikely, so "does this byte
// sequence appear in the run's history" is a question with one honest answer.
const modelKey = "sk-ant-correcthorsebatterystaple0123456789"

// --- what the plane is configured with -------------------------------------

// recordedCall is one resolution of the model factory, with the credential the
// plane redeemed for it.
type recordedCall struct {
	tenantID string
	provider string
	model    string
	apiKey   string
}

// keyedModel is a scripted model plus the notebook of what the plane handed
// its factory. It is how a case tells "the plane redeemed the credential for
// this call" apart from "the plane called a model with nothing at all".
type keyedModel struct {
	*scriptedModel

	mu    sync.Mutex
	calls []recordedCall
	// fail, when set, is returned by Generate with the credential this call
	// was constructed with spliced into the message — which is what a real
	// provider does when it rejects a key, and the reason a JobStatus error
	// is a leak surface.
	failWithKey bool
}

func newKeyedModel(id string) *keyedModel {
	return &keyedModel{scriptedModel: &scriptedModel{id: id}}
}

func (m *keyedModel) factory() server.ModelFactory {
	return func(_ context.Context, req server.ModelRequest) (provider.LanguageModel, error) {
		m.mu.Lock()
		m.calls = append(m.calls, recordedCall{
			tenantID: req.TenantID, provider: req.Provider, model: req.Model, apiKey: req.APIKey,
		})
		failing, key := m.failWithKey, req.APIKey
		m.mu.Unlock()
		if failing {
			return &rejectingModel{id: m.id, key: key}, nil
		}
		return m.scriptedModel, nil
	}
}

func (m *keyedModel) resolved() []recordedCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedCall{}, m.calls...)
}

func (m *keyedModel) keys() []string {
	out := []string{}
	for _, c := range m.resolved() {
		out = append(out, c.apiKey)
	}
	return out
}

// rejectingModel is the provider that refuses the credential and QUOTES it
// back, which is the ordinary shape of a 401 body.
type rejectingModel struct {
	id  string
	key string
}

func (m *rejectingModel) ModelID() string      { return m.id }
func (m *rejectingModel) ProviderName() string { return "stub" }
func (m *rejectingModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: true}
}

func (m *rejectingModel) Generate(context.Context, provider.Call) (*provider.Response, error) {
	return nil, fmt.Errorf("401 unauthorized: invalid x-api-key: %s", m.key)
}

func (m *rejectingModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, fmt.Errorf("rejecting model %q does not stream", m.id)
}

// rotatingSource hands out a different value each time it is asked. A plane
// that resolved once and cached would go on using the first key for ever,
// which is what "a provider key rotates without restarting the plane" rules
// out.
type rotatingSource struct {
	mu sync.Mutex
	n  int
}

func (s *rotatingSource) Value(_ context.Context, tenantID, name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("%s-%s-%s-%d", modelKey, tenantID, name, s.n), nil
}

// --- the pipelines ---------------------------------------------------------

// llmSecretPipeline is llmPipeline whose model configuration NAMES its
// credential rather than carrying one.
func llmSecretPipeline(id, prompt, secretName string) *dholev1.Pipeline {
	p := llmPipeline(id, prompt)
	p.GetSteps()[0].GetConfig()["api_key_secret"] = secretName
	return p
}

// loopSecretPipeline is the same for a bounded loop, whose body is a model
// call per pass — the case that must redeem once per pass rather than once.
func loopSecretPipeline(id string, ceiling int, secretName string) *dholev1.Pipeline {
	p := loopPipeline(id, ceiling)
	p.GetSteps()[0].GetConfig()["api_key_secret"] = secretName
	return p
}

// --- the cases -------------------------------------------------------------

// TestThePlaneRedeemsAModelCredentialAtCallTimeScopedToTheStepsTenant is
// ADR 0024 as a running plane sees it. A model configuration names a secret;
// the plane resolves it through the broker it already serves, as a principal
// of the tenant whose step is running; the model client is built with it and
// discarded with the call.
func TestThePlaneRedeemsAModelCredentialAtCallTimeScopedToTheStepsTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := newKeyedModel("test-model")
	model.always(`{"severity":"high","done":true}`)
	source := secrets.NewMapSource()
	source.Set(tenantID, modelSecretName, modelKey)

	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
		cfg.SecretSource = source
	})

	runID, err := srv.Submit(ctx, tenantID, llmSecretPipeline("plane-secret-llm", "classify this release", modelSecretName))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "classify")

	calls := model.resolved()
	require.Len(t, calls, 1, "the plane resolved a model a different number of times than it called one")
	require.Equal(t, modelKey, calls[0].apiKey,
		"the plane built a model client with no credential; log: %s", describe(events))
	require.Equal(t, tenantID, calls[0].tenantID,
		"the resolution must carry the tenant whose step is running, or per-tenant credentials are unexpressible")
}

// TestTwoModelCallsRedeemTwiceSoAProviderKeyRotatesWithoutRestartingThePlane is
// the consequence ADR 0024 names. A value held for a whole run is the secret at
// rest that SecretRef exists to avoid, and an hour-long run must not hold a
// credential for an hour.
//
// A bounded loop is the honest shape of it: two passes, two model calls, two
// redemptions — and the second pass sees the rotated key with nothing having
// restarted.
func TestTwoModelCallsRedeemTwiceSoAProviderKeyRotatesWithoutRestartingThePlane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := newKeyedModel("test-model")
	model.script(
		`{"severity":"medium","done":false}`,
		`{"severity":"low","done":true}`,
	)
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
		cfg.SecretSource = &rotatingSource{}
	})

	runID, err := srv.Submit(ctx, tenantID, loopSecretPipeline("plane-secret-loop", 3, modelSecretName))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "refine")

	keys := model.keys()
	require.Len(t, keys, 2, "the loop ran two passes, so it must have redeemed twice; log: %s", describe(events))
	require.NotEqual(t, keys[0], keys[1],
		"the second pass reused the first pass's credential instead of redeeming again")
	for _, key := range keys {
		require.Contains(t, key, tenantID, "every redemption is scoped to the step's tenant")
	}
}

// TestARedeemedModelCredentialReachesNoRunEventNoOutputAndNoLogLine is the
// plane-side half of what internal/engine already asserts for a step's process:
// a redeemed value reaches the thing that needed it and nothing else — never
// the run log, never the content-addressed store, never a log line that leaves
// the box.
//
// A model API key in a run's history is the failure this whole design exists
// to prevent.
func TestARedeemedModelCredentialReachesNoRunEventNoOutputAndNoLogLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logged := captureDefaultLogger(t)

	model := newKeyedModel("test-model")
	model.always(`{"severity":"high","done":true}`)
	source := secrets.NewMapSource()
	source.Set(tenantID, modelSecretName, modelKey)

	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
		cfg.SecretSource = source
	})

	runID, err := srv.Submit(ctx, tenantID, llmSecretPipeline("plane-secret-clean", "classify this release", modelSecretName))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "classify")
	require.Equal(t, []string{modelKey}, model.keys(),
		"the case proves nothing unless the credential really was redeemed")

	requireNoCredentialAnywhere(ctx, t, srv, events, modelKey)
	require.NotContains(t, logged.String(), modelKey,
		"a model credential reached a log line, which leaves the box for somewhere with a different audience")
}

// TestAProviderThatQuotesTheCredentialBackDoesNotPutItInTheStepsJobStatus is
// the failure path of the same property, and the harder one. A provider that
// rejects a key routinely echoes it in the 401 body; a JobStatus error is
// durable and archived, so the echo would be a credential at rest in the run
// history.
func TestAProviderThatQuotesTheCredentialBackDoesNotPutItInTheStepsJobStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	logged := captureDefaultLogger(t)

	model := newKeyedModel("test-model")
	model.failWithKey = true
	source := secrets.NewMapSource()
	source.Set(tenantID, modelSecretName, modelKey)

	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
		cfg.SecretSource = source
	})

	runID, err := srv.Submit(ctx, tenantID, llmSecretPipeline("plane-secret-401", "classify this release", modelSecretName))
	require.NoError(t, err)

	events := awaitRunEnded(ctx, t, srv, runID, scheduler.RunFailed)
	require.True(t, hasEvent(events, "classify", runstore.StepFailed),
		"the step must fail: the provider refused the call; log: %s", describe(events))
	require.NotEmpty(t, model.keys(), "the case proves nothing unless a credential was redeemed")

	requireNoCredentialAnywhere(ctx, t, srv, events, modelKey)
	require.NotContains(t, logged.String(), modelKey,
		"a rejected credential was written to a log line")
}

// TestAnLLMStepNamingASecretThePlaneCannotResolveFailsWithTheNameAndNotTheValue
// is the honest refusal. A deployment that named a credential it did not
// configure gets a reason an operator can act on — the NAME — and the run
// stops rather than an empty key being handed to a provider.
func TestAnLLMStepNamingASecretThePlaneCannotResolveFailsWithTheNameAndNotTheValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := newKeyedModel("test-model")
	model.always(`{"severity":"high","done":true}`)
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
		// Deliberately no SecretSource: the plane can redeem nothing.
	})

	runID, err := srv.Submit(ctx, tenantID,
		llmSecretPipeline("plane-secret-missing", "classify this release", modelSecretName))
	require.NoError(t, err)

	events := awaitRunEnded(ctx, t, srv, runID, scheduler.RunFailed)
	failure := stepFailureReason(t, events, "classify")
	require.Contains(t, failure, modelSecretName,
		"the failure must name the secret the step asked for; log: %s", describe(events))
	require.Empty(t, model.resolved(),
		"a step whose credential could not be resolved must not reach a model at all")
}

// TestAPlaneWithNoModelFactoryStillFailsAnLLMStepWithThatNamedReason is the
// behaviour ADR 0024 keeps rather than changes. Redeeming a credential does
// not build a model client; a deployment that supplied no factory fails such a
// step with that reason, as it did before, rather than with a nil dereference
// or a silent skip.
func TestAPlaneWithNoModelFactoryStillFailsAnLLMStepWithThatNamedReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbeddedWith(ctx, t, func(*server.Config) {})

	runID, err := srv.Submit(ctx, tenantID, llmPipeline("plane-no-factory", "classify this release"))
	require.NoError(t, err)

	events := awaitRunEnded(ctx, t, srv, runID, scheduler.RunFailed)
	require.Contains(t, stepFailureReason(t, events, "classify"), "model factory",
		"the reason must name what is missing; log: %s", describe(events))
}

// --- the helpers these cases need ------------------------------------------

// requireNoCredentialAnywhere reads back everything a run leaves behind — every
// event payload in the log, and every artifact those events point at — and
// refuses to find the value in any of it.
func requireNoCredentialAnywhere(
	ctx context.Context, t *testing.T, srv *server.Server, events []runstore.Event, value string,
) {
	t.Helper()
	for _, e := range events {
		require.NotContains(t, string(e.Payload), value,
			"a model credential is in a %s event, which is durable and archived", e.Type)
		status := &dholev1.JobStatus{}
		if err := proto.Unmarshal(e.Payload, status); err != nil {
			continue
		}
		require.NotContains(t, status.GetError(), value,
			"a model credential is in a JobStatus error")
		for _, out := range status.GetOutputs() {
			require.NotContains(t, string(readArtifact(ctx, t, srv, out)), value,
				"a model credential is in an output artifact on port %q", out.GetPort())
		}
	}
}

// readArtifact fetches what an output ref points at.
func readArtifact(ctx context.Context, t *testing.T, srv *server.Server, out *dholev1.OutputRef) []byte {
	t.Helper()
	rc, err := srv.CAS().Get(ctx, tenantID, out.GetDigest())
	if err != nil {
		return nil
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
}

// stepFailureReason is the text a failed step recorded, whichever shape it was
// written in: a JobStatus from the builtin dispatcher, or the JSON the llm
// step type writes when it gives up.
func stepFailureReason(t *testing.T, events []runstore.Event, stepID string) string {
	t.Helper()
	reasons := []string{}
	for _, e := range events {
		if e.StepID != stepID || e.Type != runstore.StepFailed {
			continue
		}
		status := &dholev1.JobStatus{}
		if err := proto.Unmarshal(e.Payload, status); err == nil && status.GetError() != "" {
			reasons = append(reasons, status.GetError())
			continue
		}
		var body struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal(e.Payload, &body); err == nil {
			reasons = append(reasons, body.Reason)
		}
	}
	require.NotEmpty(t, reasons, "step %q recorded no failure; log: %s", stepID, describe(events))
	return strings.Join(reasons, "\n")
}

// captureDefaultLogger points slog's default at a buffer for the duration of
// one case. The plane logs through it, and a log ships off the box to somewhere
// with a different retention and a different audience — which is why "the value
// never reaches a log line" is asserted rather than assumed.
func captureDefaultLogger(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
