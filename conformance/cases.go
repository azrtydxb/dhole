package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/wire"
)

// This file is one case per obligation in docs/wire-contract.md. Each case is
// written against the DOCUMENT, not against internal/engine: a case that can
// only be passed by knowing how Dhole's own Go engine is built has stopped
// testing the contract and started testing the implementation.
//
// Where the document does not say enough for a stranger to implement — and
// there are several such places, listed below — the suite states the
// convention it assumes in the case's Obligation text, so an engine author
// reading a failure learns what the harness expected and can see that the
// expectation came from the harness rather than from the wire contract.
//
// # Conventions this suite invents, because the contract does not state them
//
//  1. HOW AN ENGINE IS CONFIGURED. The contract never says how an engine
//     learns its bus URL, its engine id, or its tier. The suite passes them in
//     the environment: DHOLE_NATS_URL, DHOLE_ENGINE_ID, DHOLE_ENGINE_TIER.
//  2. WHAT THE OBJECT STORE IS. JobDispatch.output_prefix and
//     JobStatus.log_key name objects in a store whose PROTOCOL still appears
//     nowhere in the contract. The suite uses a directory, passed as
//     DHOLE_BLOB_DIR. The SHAPE of a key is no longer the suite's invention:
//     the contract states that every object is tenant-scoped, so a key k for
//     tenant t resolves at <tenant>/<key>, and a content-addressed object at
//     <tenant>/<algo>/<first two hex>/<hex>.
//  3. HOW A SECRET IS REDEEMED. The contract says a handle is redeemed and
//     never says on what subject or with what message. The suite serves a
//     core-NATS request/reply on DHOLE_SECRET_SUBJECT: the request body is the
//     handle, the reply body is the value, and a reply beginning "ERR " is a
//     refusal.
//  4. HOW A FENCE IS COMPARED. Fence tokens are opaque strings with no stated
//     ordering, so no case asks an engine to decide which of two fences is
//     newer — only whether a fence EQUALS the one it holds.
//
// Every one of those is reported as a contract gap, not worked around
// silently.

// killedExitCode is the exit status the contract reserves for a step the
// engine killed, timeouts included.
const killedExitCode int32 = 137

// caseTimeout is the default bound on one case. A case that needs longer says
// so; nothing here may hang, because a conformance run that never returns is
// indistinguishable from an engine that never answers.
const caseTimeout = 30 * time.Second

// kase is one contract obligation and the check that proves an engine meets
// it. run returns nil when the engine complied, and otherwise an error whose
// text names what was expected, what arrived, and how long the suite waited —
// an engine author must be able to act on it without reading this file.
type kase struct {
	name       string
	obligation string
	timeout    time.Duration
	run        func(ctx context.Context, h *harness) error
}

// cases is the whole suite, in the order it runs. Order matters only in that
// registration comes first: everything after it needs an engine that has
// announced itself.
func cases() []kase {
	return []kase{
		registrationCase(),
		versionNegotiationCase(),
		dispatchSuccessCase(),
		nonZeroExitCase(),
		cancellationCase(),
		timeoutCase(),
		logThroughputCase(),
		binaryArtifactCase(),
		secretRedemptionCase(),
		leaseRenewalCase(),
		fenceRefusalCase(),
	}
}

// registrationCase: docs/wire-contract.md, "Message framing" and "Protocol
// version negotiation".
func registrationCase() kase {
	return kase{
		name: "registration",
		obligation: "An engine announces itself on engine.registration as an EngineMessage{registration} — framed, " +
			"not bare — carrying a non-empty engine_id, the protocol versions it speaks, its os, arch, tier and slots.",
		timeout: 20 * time.Second,
		run: func(ctx context.Context, h *harness) error {
			reg, err := h.awaitRegistration(ctx)
			if err != nil {
				return err
			}
			if reg.GetEngineId() == "" {
				return fmt.Errorf("the registration on %s carries an empty engine_id; a plane refuses that outright "+
					"rather than admitting an engine it cannot address", h.registrationSubject())
			}
			if _, err := wire.Negotiate(reg.GetProtocolVersions()); err != nil {
				return fmt.Errorf("protocol_versions %v: %v; this control plane speaks version %d and accepts one "+
					"version back, so advertise a version in that window", reg.GetProtocolVersions(), err, wire.ProtocolVersion)
			}
			if reg.GetTier() != h.tier {
				return fmt.Errorf("registration tier is %q, expected %q (the tier handed to the engine as "+
					"DHOLE_ENGINE_TIER); an engine that misreports its tier is matched against work it may not receive",
					reg.GetTier(), h.tier)
			}
			if reg.GetOs() == "" || reg.GetArch() == "" {
				return fmt.Errorf("registration carries os=%q arch=%q; the scheduler matches a step's platform "+
					"against exactly these fields, so an empty one makes every step unschedulable",
					reg.GetOs(), reg.GetArch())
			}
			if reg.GetSlots() == 0 {
				return fmt.Errorf("registration carries slots=0; an engine that will run no jobs is never given one")
			}
			if len(reg.GetCapabilities()) == 0 {
				return fmt.Errorf("registration advertises no capabilities; this suite dispatches a step requiring " +
					"CAPABILITY_SECRETS, which is only ever sent to an engine that advertises it")
			}
			return nil
		},
	}
}

// versionNegotiationCase: docs/wire-contract.md, "Protocol version
// negotiation" — the one case whose expected reply the contract spells out
// literally, down to the substring.
func versionNegotiationCase() kase {
	return kase{
		name: "version-negotiation",
		obligation: "A dispatch whose protocol_version the engine does not speak is answered with " +
			"JobStatus{PHASE_FAILED} whose error contains \"unsupported protocol\". Dropping it is not allowed: " +
			"silence is indistinguishable from a dead engine and the step hangs until its lease expires.",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("version")
			d.ProtocolVersion = 4242
			d.Command = []string{"sh", "-c", "exit 0"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_FAILED {
				return fmt.Errorf("expected phase PHASE_FAILED for a dispatch at protocol_version 4242, got %s "+
					"(error %q); an engine that RUNS a dispatch it cannot parse the version of is running work "+
					"whose meaning it has not agreed to", status.GetPhase(), status.GetError())
			}
			if !strings.Contains(strings.ToLower(status.GetError()), "unsupported protocol") {
				return fmt.Errorf("expected the error to contain \"unsupported protocol\", got %q; the contract "+
					"fixes that substring so an operator can grep a fleet for engines that need upgrading",
					status.GetError())
			}
			return h.checkFence(d, status)
		},
	}
}

// dispatchSuccessCase: docs/wire-contract.md, "The message flow of one
// attempt", steps 3 to 6.
func dispatchSuccessCase() kase {
	return kase{
		name: "dispatch-and-success",
		obligation: "An engine accepts a dispatch (PHASE_ACCEPTED), runs it, finishes the authoritative log BEFORE " +
			"the terminal status, and publishes PHASE_SUCCEEDED with exit_code 0, the fence echoed unchanged, and a " +
			"log_key naming the complete log.",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("success")
			d.Command = []string{"sh", "-c", "printf 'hello conformance\n'"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			accepted, err := watch.await(ctx, phaseIs(dholev1.Phase_PHASE_ACCEPTED), "JobStatus{PHASE_ACCEPTED}")
			if err != nil {
				return fmt.Errorf("%w; step 3 of the message flow: an engine says it has taken the work before it "+
					"runs it, so the plane can tell a slow step from an unclaimed one", err)
			}
			if err := h.checkFence(d, accepted); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED for a command exiting 0, got %s (exit_code %d, error %q)",
					status.GetPhase(), status.GetExitCode(), status.GetError())
			}
			if status.GetExitCode() != 0 {
				return fmt.Errorf("expected exit_code 0 on PHASE_SUCCEEDED, got %d", status.GetExitCode())
			}
			if err := h.checkFence(d, status); err != nil {
				return err
			}
			if status.GetLogKey() == "" {
				return fmt.Errorf("the terminal status carries no log_key; the LogChunks on %s are ephemeral and "+
					"best-effort, so without the authoritative object the run has no log at all",
					h.logsSubject(d))
			}
			log, err := h.readBlob(status.GetLogKey())
			if err != nil {
				return fmt.Errorf("reading the authoritative log named by log_key %q: %w; it must be COMPLETE in "+
					"the store before the terminal status is published, never after", status.GetLogKey(), err)
			}
			if !bytes.Contains(log, []byte("hello conformance")) {
				return fmt.Errorf("the authoritative log at %q does not contain the step's stdout "+
					"(%d bytes: %q)", status.GetLogKey(), len(log), truncate(log, 200))
			}
			if len(h.logChunks(d)) == 0 {
				return fmt.Errorf("no LogChunk arrived on %s; the live copy is best-effort but it is not optional — "+
					"a GUI watching this run would show nothing at all", h.logsSubject(d))
			}
			return nil
		},
	}
}

// nonZeroExitCase. The contract has terminal phases and an exit_code; it does
// not say in so many words that a non-zero exit is PHASE_FAILED, which is
// reported as a gap. The suite requires it because the alternative — reporting
// SUCCEEDED for a step that failed — would let a broken build be cached.
func nonZeroExitCase() kase {
	return kase{
		name: "non-zero-exit",
		obligation: "A step whose command exits non-zero is reported PHASE_FAILED with that exit code in " +
			"exit_code. (The contract states the terminal phases and the exit_code field but not this mapping; " +
			"the suite requires it because SUCCEEDED for a failed command would let a broken result be cached.)",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("exit")
			d.Command = []string{"sh", "-c", "printf 'going down\n' >&2; exit 7"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_FAILED {
				return fmt.Errorf("expected PHASE_FAILED for a command exiting 7, got %s (exit_code %d, error %q)",
					status.GetPhase(), status.GetExitCode(), status.GetError())
			}
			if status.GetExitCode() != 7 {
				return fmt.Errorf("expected exit_code 7, got %d; the exit code is how a retry policy tells a "+
					"step that failed from an engine that broke", status.GetExitCode())
			}
			if status.GetLogKey() == "" {
				return fmt.Errorf("a failed step reported no log_key; the log of a FAILURE is the one anybody " +
					"actually goes looking for")
			}
			log, err := h.readBlob(status.GetLogKey())
			if err != nil {
				return fmt.Errorf("reading the authoritative log of the failed attempt at %q: %w",
					status.GetLogKey(), err)
			}
			if !bytes.Contains(log, []byte("going down")) {
				return fmt.Errorf("the authoritative log at %q holds no stderr from the step (%d bytes: %q); "+
					"both streams belong in it", status.GetLogKey(), len(log), truncate(log, 200))
			}
			return nil
		},
	}
}

// cancellationCase: docs/wire-contract.md, "Control".
func cancellationCase() kase {
	return kase{
		name: "cancellation",
		obligation: "EngineControl{Cancel} on engine.control.<engine-id> stops the named in-flight job: the engine " +
			"terminates it and publishes JobStatus{PHASE_CANCELLED}, echoing the fence, within 2 seconds.",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("cancel")
			d.Command = []string{"sh", "-c", "printf 'started\n'; sleep 20"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			if _, err := watch.await(ctx, phaseIs(dholev1.Phase_PHASE_ACCEPTED), "JobStatus{PHASE_ACCEPTED}"); err != nil {
				return err
			}
			// The job has to be genuinely running before it is cancelled, or a
			// passing engine could be one that never started it.
			if err := h.awaitLogContains(ctx, d, "started", 10*time.Second); err != nil {
				return err
			}
			sent := time.Now()
			if err := h.cancel(ctx, d, d.GetFenceToken()); err != nil {
				return err
			}
			cancelDeadline, cancel := context.WithTimeout(ctx, cancellationBudget)
			defer cancel()
			status, err := watch.await(cancelDeadline, terminal, "a terminal JobStatus, PHASE_CANCELLED,")
			if err != nil {
				return fmt.Errorf("%w, %s after EngineControl{Cancel} was published on %s for run %s step %s "+
					"attempt %d; a cancelled step that reports nothing holds its lease until it expires",
					err, cancellationBudget, h.controlSubject(), d.GetRunId(), d.GetStepId(), d.GetAttempt())
			}
			if status.GetPhase() != dholev1.Phase_PHASE_CANCELLED {
				return fmt.Errorf("expected PHASE_CANCELLED after EngineControl{Cancel}, got %s (exit_code %d, "+
					"error %q); the contract names the phase, and a plane that sees SUCCEEDED or FAILED cannot "+
					"tell a step it killed from one that died on its own",
					status.GetPhase(), status.GetExitCode(), status.GetError())
			}
			if elapsed := time.Since(sent); elapsed > cancellationBudget {
				return fmt.Errorf("PHASE_CANCELLED arrived %s after the Cancel; the budget is %s",
					elapsed.Round(time.Millisecond), cancellationBudget)
			}
			return h.checkFence(d, status)
		},
	}
}

// cancellationBudget is the "within 2s" of the plan's checklist, plus nothing.
// A slower engine leaves a step apparently alive after an operator has killed
// it, which is what makes a rolling upgrade unsafe.
const cancellationBudget = 2 * time.Second

// timeoutCase: docs/wire-contract.md, "Step timeouts". The bound is
// Step.timeout_seconds, added in protocol version 3; it used to be
// DHOLE_STEP_TIMEOUT_SECONDS in JobDispatch.env, which was a harness
// convention two engines had each read differently.
func timeoutCase() kase {
	return kase{
		name: "step-timeout",
		obligation: "A step that outlives Step.timeout_seconds is killed by the engine and reported terminally — " +
			"a non-successful phase carrying exit_code 137 — rather than left running while the engine holds " +
			"the slot and renews the lease.",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("timeout")
			d.Command = []string{"sh", "-c", "printf 'sleeping\n'; sleep 30"}
			d.Step.TimeoutSeconds = 2
			watch := h.watchStatus(d)
			started := time.Now()
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			budget, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			status, err := watch.await(budget, terminal, "a terminal JobStatus")
			if err != nil {
				return fmt.Errorf("%w for a step with Step.timeout_seconds=2 running `sleep 30`; an engine "+
					"that does not enforce the timeout holds the slot for the full 30 seconds", err)
			}
			if status.GetPhase() == dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected a non-successful terminal phase for a step killed at its timeout, " +
					"got PHASE_SUCCEEDED; a timed-out step that reports success is a step whose output nobody wrote")
			}
			if elapsed := time.Since(started); elapsed > 15*time.Second {
				return fmt.Errorf("the terminal status arrived %s after dispatch for a 2 second timeout",
					elapsed.Round(time.Millisecond))
			}
			// One code for one thing: a step the ENGINE killed reports 137 on
			// every platform and every backend, so a plane can tell a step it
			// stopped from one that died on its own. A negative code — the
			// -1 a signalled process has instead of a status — is refused by
			// the same rule, and sign-extends to ten bytes on the wire.
			if status.GetExitCode() != killedExitCode {
				return fmt.Errorf("the timed-out step reported exit_code %d, expected %d; a step the engine "+
					"killed reports 137 whatever the platform did to it (docs/wire-contract.md, \"Exit codes\")",
					status.GetExitCode(), killedExitCode)
			}
			return h.checkFence(d, status)
		},
	}
}

// logThroughputCase: docs/wire-contract.md, "Logs". Ten megabytes, because a
// per-line publish that works on a chatty step falls over on a real build.
func logThroughputCase() kase {
	return kase{
		name: "log-throughput-10mb",
		obligation: "A step producing 10MB of output completes, its LogChunks carry a monotonic seq so a viewer " +
			"can see it missed some, and the authoritative object holds the WHOLE 10MB — the live copy may be " +
			"dropped, the durable one may not.",
		timeout: 90 * time.Second,
		run: func(ctx context.Context, h *harness) error {
			const want = 10 * 1024 * 1024
			d := h.newDispatch("logs")
			// 10240 lines of 1024 bytes: exactly 10MiB, in POSIX sh, with no
			// dd, yes or seq — the command has to be as portable as the engine.
			d.Command = []string{"sh", "-c", `i=0; while [ $i -lt 10240 ]; do printf '%01023d\n' $i; i=$((i+1)); done`}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED for the 10MB step, got %s (error %q); an engine that "+
					"buffers the log in one message or one publish fails here and nowhere else",
					status.GetPhase(), status.GetError())
			}
			log, err := h.readBlob(status.GetLogKey())
			if err != nil {
				return fmt.Errorf("reading the authoritative log at %q: %w", status.GetLogKey(), err)
			}
			if len(log) < want {
				return fmt.Errorf("the authoritative log at %q holds %d bytes, expected at least %d; the durable "+
					"copy is COMPLETE by contract — truncating it loses exactly the output somebody went looking for",
					status.GetLogKey(), len(log), want)
			}
			chunks := h.logChunks(d)
			if len(chunks) == 0 {
				return fmt.Errorf("no LogChunk arrived on %s during a 10MB step", h.logsSubject(d))
			}
			var last uint64
			for i, c := range chunks {
				if c.GetSeq() <= last && i > 0 {
					return fmt.Errorf("LogChunk seq went %d then %d; seq is monotonic per (run, step, attempt) "+
						"precisely so a viewer can tell a gap from a reordering", last, c.GetSeq())
				}
				if c.GetAttempt() != d.GetAttempt() {
					return fmt.Errorf("a LogChunk carries attempt %d, expected %d; chunks from two attempts of "+
						"one step are otherwise indistinguishable", c.GetAttempt(), d.GetAttempt())
				}
				last = c.GetSeq()
			}
			return nil
		},
	}
}

// binaryArtifactCase: docs/wire-contract.md's inputs and outputs, exercised
// with bytes that are not text — an engine that treats a payload as a string
// passes every case but this one.
func binaryArtifactCase() kase {
	return kase{
		name: "binary-artifact-round-trip",
		obligation: "An engine materialises every declared input, and stores every declared output, byte for " +
			"byte — including NUL bytes and invalid UTF-8 — reporting each in an OutputRef with its size and its " +
			"sha256 digest.",
		run: func(ctx context.Context, h *harness) error {
			payload := make([]byte, 64*1024)
			if _, err := rand.Read(payload); err != nil {
				return err
			}
			// Guarantee the bytes an engine that stringifies its payloads will
			// mangle, rather than trusting randomness to include them.
			copy(payload, []byte{0x00, 0xff, 0xfe, 0x00, 0x0d, 0x0a, 0x1a, 0x80})
			key := h.newDispatchPrefix("artifact") + "/input.bin"
			if err := h.writeBlob(key, payload); err != nil {
				return err
			}

			d := h.newDispatch("artifact")
			d.Inputs = []*dholev1.InputRef{{Port: "src", Key: key}}
			d.Step.Inputs = []*dholev1.Port{{Name: "src"}}
			d.Step.Outputs = []*dholev1.Port{{Name: "copy"}}
			d.Command = []string{"sh", "-c", "cat inputs/src > outputs/copy"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED, got %s (error %q); the step copies inputs/src to "+
					"outputs/copy, so a failure here is usually an input that was never materialised",
					status.GetPhase(), status.GetError())
			}
			if len(status.GetOutputs()) != 1 {
				return fmt.Errorf("expected exactly 1 OutputRef for the step's one declared output port %q, got %d; "+
					"an output nobody reported is an output the next step cannot find", "copy", len(status.GetOutputs()))
			}
			out := status.GetOutputs()[0]
			if out.GetPort() != "copy" {
				return fmt.Errorf("OutputRef names port %q, expected %q", out.GetPort(), "copy")
			}
			if out.GetSizeBytes() != uint64(len(payload)) {
				return fmt.Errorf("OutputRef reports size_bytes %d, expected %d", out.GetSizeBytes(), len(payload))
			}
			sum := sha256.Sum256(payload)
			wantHex := hex.EncodeToString(sum[:])
			if out.GetDigest().GetHex() != "" || out.GetDigest().GetAlgo() != "" {
				if out.GetDigest().GetAlgo() != "sha256" {
					return fmt.Errorf("OutputRef digest algo is %q, expected \"sha256\"", out.GetDigest().GetAlgo())
				}
				if out.GetDigest().GetHex() != wantHex {
					return fmt.Errorf("OutputRef digest is %s, expected %s — the stored bytes are not the bytes "+
						"the step wrote", out.GetDigest().GetHex(), wantHex)
				}
			}
			stored, err := h.readOutput(out)
			if err != nil {
				return fmt.Errorf("reading the stored output for port %q: %w", out.GetPort(), err)
			}
			if !bytes.Equal(stored, payload) {
				return fmt.Errorf("the stored output for port %q differs from the input it was copied from: "+
					"%d bytes stored, %d expected, first difference at offset %d; a binary artifact must survive "+
					"the round trip unchanged", out.GetPort(), len(stored), len(payload), firstDiff(stored, payload))
			}
			return nil
		},
	}
}

// secretRedemptionCase: docs/wire-contract.md, "Secrets". The whole case is
// the second half: redemption is easy, and NOT leaking the value is the part
// an engine gets wrong.
func secretRedemptionCase() kase {
	return kase{
		name: "secret-redemption",
		obligation: "An engine redeems a SecretRef handle for its value, binds it to the step, and never lets the " +
			"VALUE appear in a LogChunk, in the authoritative log, in an OutputRef, or in a JobStatus error. " +
			"(The redemption subject and reply shape are a harness convention: the contract does not define them.)",
		run: func(ctx context.Context, h *harness) error {
			const value = "correcthorsebatterystaple"
			handle := h.issueSecret(value)

			d := h.newDispatch("secret")
			d.Step.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
			d.Step.Outputs = []*dholev1.Port{{Name: "proof"}}
			d.Secrets = []*dholev1.SecretRef{{
				Name:      "DHOLE_TEST_SECRET",
				Handle:    handle,
				ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
			}}
			// The step proves it received the value without ever printing it:
			// it writes the value ROT13'd. A step that echoed the secret to
			// prove it had it would make the leak check untestable.
			d.Command = []string{"sh", "-c", `printf '%s' "$DHOLE_TEST_SECRET" | tr 'A-Za-z' 'N-ZA-Mn-za-m' > outputs/proof`}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED, got %s (error %q); the dispatch carries one SecretRef "+
					"and the step is dispatched to the capability set {CAPABILITY_SECRETS}",
					status.GetPhase(), status.GetError())
			}
			if n := h.redemptions(handle); n == 0 {
				return fmt.Errorf("the engine never redeemed handle %q on %s; a SecretRef carries no value, so a "+
					"step whose environment holds the handle instead of the secret sees a meaningless string",
					handle, h.secretSubject())
			}
			if len(status.GetOutputs()) != 1 {
				return fmt.Errorf("expected 1 OutputRef for port \"proof\", got %d", len(status.GetOutputs()))
			}
			proof, err := h.readOutput(status.GetOutputs()[0])
			if err != nil {
				return fmt.Errorf("reading the step's proof output: %w", err)
			}
			if got, want := string(proof), rot13(value); got != want {
				return fmt.Errorf("the step wrote %q where the ROT13 of the redeemed secret (%q) was expected: "+
					"the value bound to $DHOLE_TEST_SECRET was not the value behind the handle", got, want)
			}

			// The leak check, over every place a value could escape to.
			if strings.Contains(status.GetError(), value) {
				return fmt.Errorf("the JobStatus error contains the secret VALUE; a status is durable and archived, " +
					"so that is a secret at rest in the run history")
			}
			for _, o := range status.GetOutputs() {
				if strings.Contains(o.GetKey(), value) {
					return fmt.Errorf("an OutputRef key contains the secret value: %q", o.GetKey())
				}
			}
			for _, c := range h.logChunks(d) {
				if bytes.Contains(c.GetData(), []byte(value)) {
					return fmt.Errorf("LogChunk seq %d on %s contains the secret VALUE %q; live logs go to every "+
						"viewer watching the run", c.GetSeq(), h.logsSubject(d), value)
				}
			}
			log, err := h.readBlob(status.GetLogKey())
			if err != nil {
				return fmt.Errorf("reading the authoritative log at %q: %w", status.GetLogKey(), err)
			}
			if bytes.Contains(log, []byte(value)) {
				return fmt.Errorf("the authoritative log at %q contains the secret VALUE %q; that object outlives "+
					"the run and is what anybody reads afterwards", status.GetLogKey(), value)
			}
			return nil
		},
	}
}

// leaseRenewalCase: docs/wire-contract.md, "Heartbeats and orphans". This is
// the case that decides whether a long step survives: a lease that stops being
// renewed expires, and the step is re-dispatched under a new fence while it is
// still running.
func leaseRenewalCase() kase {
	return kase{
		name: "lease-renewal-during-a-long-step",
		obligation: "An engine publishes EngineMessage{heartbeat} on engine.heartbeat.<engine-id> every five " +
			"seconds THROUGHOUT a long step, each listing the job in in_flight with its run, step, attempt and the " +
			"fence echoed unchanged; a step that stops being declared is treated as orphaned and re-dispatched.",
		timeout: 45 * time.Second,
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("longstep")
			d.Command = []string{"sh", "-c", "printf 'started\n'; sleep 13"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			if err := h.awaitLogContains(ctx, d, "started", 15*time.Second); err != nil {
				return err
			}

			// Two heartbeats declaring the job, at the contract's interval,
			// while it runs. One would be satisfied by an engine that beats
			// once on acceptance and then goes quiet for the whole step.
			const wantBeats = 2
			deadline := time.Now().Add(engine.HeartbeatInterval*wantBeats + 6*time.Second)
			seen := 0
			var lastErr error
			for time.Now().Before(deadline) && seen < wantBeats {
				beat, err := h.nextHeartbeat(ctx, 8*time.Second)
				if err != nil {
					lastErr = err
					break
				}
				entry := findInFlight(beat, d)
				if entry == nil {
					continue
				}
				if entry.GetFenceToken() != d.GetFenceToken() {
					return fmt.Errorf("the in_flight entry for run %s step %s carries fence %q, expected the "+
						"dispatch's fence %q echoed unchanged; a heartbeat with the wrong fence renews nothing",
						d.GetRunId(), d.GetStepId(), entry.GetFenceToken(), d.GetFenceToken())
				}
				if entry.GetAttempt() != d.GetAttempt() {
					return fmt.Errorf("the in_flight entry carries attempt %d, expected %d",
						entry.GetAttempt(), d.GetAttempt())
				}
				seen++
			}
			if seen < wantBeats {
				return fmt.Errorf("saw %d of %d expected heartbeats declaring run %s step %s in in_flight while it "+
					"ran for 13s (interval is %s; last read: %v); a lease that stops being renewed expires and the "+
					"step is re-dispatched under a new fence while this engine is still running it",
					seen, wantBeats, d.GetRunId(), d.GetStepId(), engine.HeartbeatInterval, lastErr)
			}

			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED for the long step, got %s (error %q)",
					status.GetPhase(), status.GetError())
			}
			// And it stops declaring the job once it is done: an engine that
			// never releases holds a lease against work it has finished.
			released, err := h.awaitHeartbeatWithout(ctx, d, 12*time.Second)
			if err != nil {
				return err
			}
			if !released {
				return fmt.Errorf("run %s step %s is still listed in in_flight after its terminal status; an "+
					"engine that never releases a finished job keeps its lease alive against nothing",
					d.GetRunId(), d.GetStepId())
			}
			return nil
		},
	}
}

// fenceRefusalCase. The contract says an engine never invents, reuses or omits
// a fence, and that the plane discards a status whose fence is not current; it
// does NOT say what an engine does with a Cancel whose fence is not the one it
// holds. The suite requires the symmetric rule — refuse it — and the gap is
// reported. Only EQUALITY is asked of the engine: fence tokens are opaque
// strings with no stated ordering.
func fenceRefusalCase() kase {
	return kase{
		name: "fenced-out-attempt-refused",
		obligation: "An EngineControl{Cancel} whose fence_token is not the fence the engine holds for that job is " +
			"refused: the running attempt is untouched and reports its own terminal status under its own fence. " +
			"(The contract states the plane's half of this rule; the engine's half is the suite's reading of it.)",
		run: func(ctx context.Context, h *harness) error {
			d := h.newDispatch("fence")
			d.Command = []string{"sh", "-c", "printf 'started\n'; sleep 4; printf 'finished\n'"}
			watch := h.watchStatus(d)
			if err := h.dispatch(ctx, d); err != nil {
				return err
			}
			if err := h.awaitLogContains(ctx, d, "started", 10*time.Second); err != nil {
				return err
			}
			stale := d.GetFenceToken() + ".stale"
			if err := h.cancel(ctx, d, stale); err != nil {
				return err
			}
			status, err := watch.await(ctx, terminal, "a terminal JobStatus")
			if err != nil {
				return err
			}
			if status.GetPhase() == dholev1.Phase_PHASE_CANCELLED {
				return fmt.Errorf("the engine cancelled run %s step %s on an EngineControl{Cancel} carrying fence "+
					"%q, which is NOT the fence %q it was dispatched under; a superseded plane could then kill a "+
					"live attempt it no longer owns",
					d.GetRunId(), d.GetStepId(), stale, d.GetFenceToken())
			}
			if status.GetPhase() != dholev1.Phase_PHASE_SUCCEEDED {
				return fmt.Errorf("expected PHASE_SUCCEEDED for an attempt whose stale-fence Cancel was refused, "+
					"got %s (error %q)", status.GetPhase(), status.GetError())
			}
			return h.checkFence(d, status)
		},
	}
}

// terminal matches the statuses that end an attempt. PHASE_ACCEPTED and
// PHASE_RUNNING are progress, not an outcome.
func terminal(s *dholev1.JobStatus) bool {
	switch s.GetPhase() {
	case dholev1.Phase_PHASE_SUCCEEDED, dholev1.Phase_PHASE_FAILED, dholev1.Phase_PHASE_CANCELLED:
		return true
	case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
		return false
	default:
		return false
	}
}

func phaseIs(p dholev1.Phase) func(*dholev1.JobStatus) bool {
	return func(s *dholev1.JobStatus) bool { return s.GetPhase() == p }
}

// findInFlight returns the heartbeat's entry for this dispatch, or nil.
func findInFlight(beat *dholev1.EngineHeartbeat, d *dholev1.JobDispatch) *dholev1.InFlight {
	for _, f := range beat.GetInFlight() {
		if f.GetRunId() == d.GetRunId() && f.GetStepId() == d.GetStepId() {
			return f
		}
	}
	return nil
}

func rot13(s string) string {
	out := []byte(s)
	for i, c := range out {
		switch {
		case c >= 'a' && c <= 'z':
			out[i] = 'a' + (c-'a'+13)%26
		case c >= 'A' && c <= 'Z':
			out[i] = 'A' + (c-'A'+13)%26
		}
	}
	return string(out)
}

func firstDiff(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
