package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// secretValue is the value every case in this file redeems. It is long and
// unlikely so that "does this byte sequence appear in the log" is a question
// with one honest answer.
const secretValue = "correcthorsebatterystaple"

// fakeRedeemer stands in for the control plane's responder. It records what it
// was asked for, so a case can tell "the engine bound the right value" apart
// from "the engine never redeemed and the step happened to pass".
type fakeRedeemer struct {
	mu      sync.Mutex
	handles []string
	value   string
	err     error
}

func (f *fakeRedeemer) Redeem(_ context.Context, ref *dholev1.SecretRef) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handles = append(f.handles, ref.GetHandle())
	if f.err != nil {
		return "", f.err
	}
	return f.value, nil
}

func (f *fakeRedeemer) redeemed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.handles...)
}

// TestAnEngineAdvertisesCapabilitySecretsBecauseItCanRedeemNotBecauseItsExecutorSaysSo
// is the capability-sourcing rule. CAPABILITY_SECRETS means "may redeem secret
// references", which is something the AGENT does over the bus it dialled —
// unlike NETWORK, PRIVILEGED and HOST_MOUNT, which are isolation guarantees a
// sandbox backend either can or cannot make. Sourcing it from the executor
// meant no shipped backend advertised it, so every dispatch carrying a secret
// was refused by every engine, always.
//
// The process executor still advertises nothing, and that is the point: the
// capability appears on the registration without the backend having lied.
func TestAnEngineAdvertisesCapabilitySecretsBecauseItCanRedeemNotBecauseItsExecutorSaysSo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	require.Empty(t, process.New().Capabilities(),
		"the backend under test must still advertise nothing of its own")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-1",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  &fakeRedeemer{value: secretValue},
	})

	reg := receive(ctx, t, registrations, "registration")
	require.Contains(t, reg.GetCapabilities(), dholev1.Capability_CAPABILITY_SECRETS)
}

// TestAnEngineWithNoRedeemerAdvertisesNoSecretsCapability is the other half:
// the answer must still be able to be no. An engine given no redemption
// endpoint must not claim it can redeem, or the scheduler hands it work that
// every attempt will refuse.
func TestAnEngineWithNoRedeemerAdvertisesNoSecretsCapability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	registrations := h.registrations(ctx, t)

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-2",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	reg := receive(ctx, t, registrations, "registration")
	require.NotContains(t, reg.GetCapabilities(), dholev1.Capability_CAPABILITY_SECRETS)
}

// TestAnEngineWithNoRedeemerRefusesADispatchCarryingASecretWithoutNamingTheHandle
// is the defensive half of the same rule. The bus filter should already keep
// such a dispatch away, but a dispatch that arrives anyway must fail with a
// status an operator can act on — and that status is durable and archived, so
// it names the BINDING and never the handle.
func TestAnEngineWithNoRedeemerRefusesADispatchCarryingASecretWithoutNamingTheHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-secret-refuse", "step")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-3",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
	})

	d := newDispatch("run-secret-refuse", "step", "/bin/sh", "-c", "true")
	d.Secrets = []*dholev1.SecretRef{{
		Name:      "DHOLE_TEST_SECRET",
		Handle:    "handle-that-must-not-be-logged",
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase())
	require.Contains(t, status.GetError(), "DHOLE_TEST_SECRET",
		"the refusal must name the binding the step expected")
	require.NotContains(t, status.GetError(), "handle-that-must-not-be-logged",
		"a handle in a durable status is a credential at rest in the run history")
}

// TestAnEngineRedeemsEveryHandleAndBindsTheValueToTheStepsEnvironment is the
// redemption path itself. The step asserts the binding rather than printing it,
// so this case proves the value arrived without putting it anywhere the leak
// case below would then find it.
func TestAnEngineRedeemsEveryHandleAndBindsTheValueToTheStepsEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-secret-bind", "step")
	redeemer := &fakeRedeemer{value: secretValue}

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-4",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  redeemer,
	})

	d := newDispatch("run-secret-bind", "step", "/bin/sh", "-c",
		`test "$DHOLE_TEST_SECRET" = "`+secretValue+`"`)
	d.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	d.Secrets = []*dholev1.SecretRef{{
		Name:      "DHOLE_TEST_SECRET",
		Handle:    "handle-4",
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase(),
		"the step compares $DHOLE_TEST_SECRET against the redeemed value; error %q", status.GetError())
	require.Equal(t, []string{"handle-4"}, redeemer.redeemed(),
		"the engine must redeem the handle it was given, once")
}

// TestARedeemedValueNeverReachesALogChunkTheAuthoritativeLogAnOutputRefOrAStatusError
// is the whole reason references exist rather than values. Every one of these
// four is a place a value would outlive the step: the live subject reaches
// every viewer of the run, the authoritative log and the CAS outlive the run
// entirely, and a JobStatus error is written into the run's event log.
//
// The step proves it held the value by writing it ROT13'd, exactly as the
// conformance suite does: a step that echoed the secret to prove it had it
// would make this check untestable.
func TestARedeemedValueNeverReachesALogChunkTheAuthoritativeLogAnOutputRefOrAStatusError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	chunks := h.logs(ctx, t, "run-secret-leak", "step")
	statuses := h.statuses(ctx, t, "run-secret-leak", "step")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-5",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  &fakeRedeemer{value: secretValue},
	})

	// stdout as well as the file, so the live subject and the authoritative
	// log both genuinely carry the step's output while carrying no secret.
	d := newDispatch("run-secret-leak", "step", "/bin/sh", "-c",
		`printf '%s' "$DHOLE_TEST_SECRET" | tr 'A-Za-z' 'N-ZA-Mn-za-m' | tee outputs/proof`)
	d.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	d.Step.Outputs = []*dholev1.Port{{Name: "proof"}}
	d.Secrets = []*dholev1.SecretRef{{
		Name:      "DHOLE_TEST_SECRET",
		Handle:    "handle-5",
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase(), "error %q", status.GetError())

	// The step really did hold the value: the output is its ROT13.
	require.Len(t, status.GetOutputs(), 1)
	r, err := h.cas.Get(ctx, tenantID, status.GetOutputs()[0].GetDigest())
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	require.Equal(t, rot13(secretValue), string(readAll(t, r)))

	require.NotContains(t, status.GetError(), secretValue)
	for _, o := range status.GetOutputs() {
		require.NotContains(t, o.GetKey(), secretValue)
		require.NotContains(t, o.GetPort(), secretValue)
	}

	// The authoritative log, which outlives the run and is what anybody reads
	// afterwards.
	logR, err := h.blobs.Read(ctx, tenantID, status.GetLogKey())
	require.NoError(t, err)
	defer func() { _ = logR.Close() }()
	require.NotContains(t, string(readAll(t, logR)), secretValue)

	// And the live copy, drained without blocking: chunks are best-effort, so
	// this asserts about what did arrive rather than waiting for a count.
	for {
		select {
		case c := <-chunks:
			require.NotContains(t, string(c.GetData()), secretValue,
				"live logs go to every viewer watching the run")
			continue
		default:
		}
		break
	}
}

// TestARefusedRedemptionFailsTheStepWithoutPuttingTheRefusalsCauseInTheLog
// closes the last path a value could take: the error return. A redeemer that
// failed with a message quoting what it was handed would put it in a status.
func TestARefusedRedemptionFailsTheStepWithoutPuttingTheRefusalsCauseInTheLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-secret-refused", "step")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-6",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  &fakeRedeemer{err: errors.New("the handle was refused")},
	})

	d := newDispatch("run-secret-refused", "step", "/bin/sh", "-c", "true")
	d.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	d.Secrets = []*dholev1.SecretRef{{
		Name:      "DHOLE_TEST_SECRET",
		Handle:    "handle-6",
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_FAILED, status.GetPhase())
	require.Contains(t, status.GetError(), "the handle was refused")
	require.NotContains(t, status.GetError(), "handle-6")
}

// rot13 is the step's proof-of-possession transform, mirrored here so the case
// can say what it expected.
func rot13(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return 'a' + (r-'a'+13)%26
		case r >= 'A' && r <= 'Z':
			return 'A' + (r-'A'+13)%26
		}
		return r
	}, s)
}

// TestAnEngineWithFewerSlotsThanCapabilitySetsStillServesEveryOne states the
// property that made the two cases above hang before it held. An engine binds
// one consumer per capability SUBSET it can serve, and each takes a slot before
// fetching. Held for the whole wait, a one-slot engine with two subsets fetched
// from the first forever: steps on the other subject sat in the work queue with
// a warm idle engine subscribed to them, and nothing anywhere reported a fault.
//
// One slot and two subsets — the empty one and {SECRETS} — is the smallest
// arrangement that shows it.
func TestAnEngineWithFewerSlotsThanCapabilitySetsStillServesEveryOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	plain := h.statuses(ctx, t, "run-turns", "plain")
	guarded := h.statuses(ctx, t, "run-turns", "guarded")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-7",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: process.New(),
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  &fakeRedeemer{value: secretValue},
	})

	// The capability set nothing was starving, and the one that was.
	h.publishDispatch(ctx, t, newDispatch("run-turns", "plain", "/bin/sh", "-c", "true"))

	g := newDispatch("run-turns", "guarded", "/bin/sh", "-c", "true")
	g.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	h.publishDispatch(ctx, t, g)

	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, plain).GetPhase())
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, awaitTerminal(ctx, t, guarded).GetPhase())
}

// strictExecutor refuses any capability it does not advertise, which is what
// the vm and containerd backends do and what process and kubernetes do not.
// The difference is why this bug survived every unit test and both local
// conformance runs: the two backends that check are the two nobody ran the
// suite against.
type strictExecutor struct {
	executor.Executor
	advertises []dholev1.Capability
}

func (strictExecutor) Capabilities() []dholev1.Capability { return nil }

func (e strictExecutor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	for _, want := range spec.Requirements.Capabilities {
		if want == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		if !slices.Contains(e.advertises, want) {
			return nil, fmt.Errorf("strict executor: %s: capability not advertised by this backend", want)
		}
	}
	return e.Executor.Acquire(ctx, spec)
}

// TestASandboxIsNotAskedForACapabilityTheAgentItselfSatisfies is the test whose
// absence let this ship. CAPABILITY_SECRETS describes the AGENT — it redeems a
// reference over the bus before any sandbox exists — and
// `advertisedCapabilities` says so in as many words. But the agent handed the
// executor the step's WHOLE capability set, secrets included, so a backend that
// checks what it was asked for refused a step it was perfectly able to run.
//
// Found on kw on 2026-09-11 by running the engine conformance suite against the
// vm executor on real KVM hardware: 10 passed, 1 failed, and the failure was
// "vm executor: CAPABILITY_SECRETS: capability not advertised by this backend
// (it advertises [CAPABILITY_PRIVILEGED])". The same step passes on the process
// and kubernetes backends, which never check — so every local run was green.
func TestASandboxIsNotAskedForACapabilityTheAgentItselfSatisfies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h := newHarness(t)
	statuses := h.statuses(ctx, t, "run-secrets-sandbox", "secretive")

	h.start(ctx, t, engine.Config{
		EngineID: "engine-secrets-sandbox",
		Tier:     tier,
		Bus:      h.engineBus,
		Executor: strictExecutor{Executor: process.New()},
		Blobs:    h.blobs,
		CAS:      h.cas,
		Slots:    1,
		Secrets:  &fakeRedeemer{value: secretValue},
	})

	d := newDispatch("run-secrets-sandbox", "secretive", "/bin/sh", "-c", "test -n \"$TOKEN\"")
	d.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	d.Secrets = []*dholev1.SecretRef{{
		Name: "TOKEN", Handle: "handle-1", ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}}
	h.publishDispatch(ctx, t, d)

	status := awaitTerminal(ctx, t, statuses)
	require.Equal(t, dholev1.Phase_PHASE_SUCCEEDED, status.GetPhase(),
		"the agent asked the sandbox for a capability only the agent can satisfy: %s", status.GetError())
}
