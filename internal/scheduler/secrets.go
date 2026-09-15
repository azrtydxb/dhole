package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// StepSecretUnavailable records that a step declared a secret it cannot be
// given, and why — naming the secret, never a value (ADR 0027).
//
// It is an event for the reason StepPolicyDenied is: nothing was dispatched,
// so no status will ever come back, and a refusal that were merely logged
// would leave the run looking slow. Stored values are a persistence contract:
// add types, never rename one.
const StepSecretUnavailable runstore.EventType = "STEP_SECRET_UNAVAILABLE"

// DefaultSecretTTL is how long a step's secret handles live.
//
// It bounds the time between the dispatch being written and the engine
// redeeming, which happens after the sandbox is acquired and the inputs are
// materialised and before the command starts. It is NOT the step's runtime:
// a handle is spent the moment it is redeemed, so a three-hour build needs
// its handle for the minutes before it starts, not for three hours. A
// dispatch that waits longer than this — a queue backed up, an image pull
// that takes a quarter of an hour — has its redemption refused, fails, and is
// retried under its effect class with fresh handles.
const DefaultSecretTTL = 10 * time.Minute

// StepSecrets resolves and issues the secrets a step declares. It is
// implemented by *secrets.StepIssuer; it is an interface so this package need
// not know where the values come from.
type StepSecrets interface {
	// Check reports, without issuing anything, why the step's declared
	// secrets cannot be issued for this tenant — or nil when they can.
	Check(ctx context.Context, tenantID string, step *dholev1.Step) error
	// Issue mints one handle per declaration for exactly this attempt.
	Issue(ctx context.Context, scope secrets.Scope, step *dholev1.Step, ttl time.Duration) ([]*dholev1.SecretRef, error)
	// Revoke forgets the unspent handles of exactly one attempt, when that
	// attempt ends (ADR 0030).
	Revoke(ctx context.Context, scope secrets.Scope)
	// RevokeRun forgets the unspent handles of every attempt of one run, when
	// the run ends (ADR 0031).
	RevokeRun(ctx context.Context, tenantID, runID string)
	// Discard forgets handles issued for a dispatch that never committed.
	Discard(ctx context.Context, refs []*dholev1.SecretRef)
}

// SecretUnavailable is the STEP_SECRET_UNAVAILABLE payload.
type SecretUnavailable struct {
	Reason string `json:"reason"`
}

// MarshalSecretUnavailable encodes the STEP_SECRET_UNAVAILABLE payload.
func MarshalSecretUnavailable(u SecretUnavailable) ([]byte, error) { return marshalPayload(u) }

// UnmarshalSecretUnavailable decodes the STEP_SECRET_UNAVAILABLE payload.
func UnmarshalSecretUnavailable(b []byte) (SecretUnavailable, error) {
	return unmarshalPayload[SecretUnavailable](b, string(StepSecretUnavailable))
}

// refuseSecrets decides, before a step takes a lease, a slot or an outbox row,
// whether the secrets it declares can be given to it. A step that cannot is
// refused for good: the refusal is recorded naming the secret, and the run
// fails. It is a deployment fault — a secret nobody configured — and retrying
// would only repeat it.
//
// It reports whether the step was refused.
func (s *Scheduler) refuseSecrets(
	ctx context.Context, tenantID, runID string, step *dholev1.Step,
) (bool, error) {
	if len(step.GetSecrets()) == 0 {
		return false, nil
	}
	var reason error
	if s.secrets == nil {
		names := make([]string, 0, len(step.GetSecrets()))
		for _, decl := range step.GetSecrets() {
			names = append(names, fmt.Sprintf("%q", decl.GetName()))
		}
		reason = fmt.Errorf("this control plane issues no step secrets, so step %q cannot be given %s",
			step.GetId(), strings.Join(names, ", "))
	} else {
		reason = s.secrets.Check(ctx, tenantID, step)
	}
	if reason == nil {
		return false, nil
	}
	return true, s.recordSecretRefusal(ctx, tenantID, runID, step, reason)
}

// builtinScheme is the plugin reference of a step the control plane runs
// itself. It is the public contract a pipeline author types, and internal/server
// owns the step types behind it; this package only needs to recognise it.
const builtinScheme = "builtin:"

// refuseBuiltinSecrets refuses a `builtin:` step that declares secrets, before
// it is served from cache, taken by a plane worker or armed as a gate. A step
// the plane hosts never has a JobDispatch, so its declared secrets would go
// nowhere and the author would believe a credential had been delivered. A
// builtin that needs one names it in its own configuration (ADR 0024, 0030).
//
// It reports whether the step was refused.
func (s *Scheduler) refuseBuiltinSecrets(
	ctx context.Context, tenantID, runID string, step *dholev1.Step,
) (bool, error) {
	if len(step.GetSecrets()) == 0 || !strings.HasPrefix(step.GetPluginRef(), builtinScheme) {
		return false, nil
	}
	return true, s.recordSecretRefusal(ctx, tenantID, runID, step, fmt.Errorf(
		"step %q is %s, which the control plane runs itself and never dispatches, so it cannot be "+
			"given step secrets; a builtin: step names a credential in its own configuration instead",
		step.GetId(), step.GetPluginRef()))
}

// recordSecretRefusal writes STEP_SECRET_UNAVAILABLE and fails the run. The
// store keeps one per step (migration 0026), so a pass that races another to
// the same refusal appends nothing.
func (s *Scheduler) recordSecretRefusal(
	ctx context.Context, tenantID, runID string, step *dholev1.Step, reason error,
) error {
	payload, err := MarshalSecretUnavailable(SecretUnavailable{Reason: reason.Error()})
	if err != nil {
		return err
	}
	if err := s.append(ctx, tenantID, runstore.Event{
		RunID:   runID,
		StepID:  step.GetId(),
		Attempt: state0Attempt,
		Type:    StepSecretUnavailable,
		Payload: payload,
	}); err != nil {
		return err
	}
	return s.fail(ctx, tenantID, runID, []string{step.GetId()})
}

// secretRefs issues the handles one attempt's dispatch carries.
func (s *Scheduler) secretRefs(
	ctx context.Context, tenantID, runID string, step *dholev1.Step, attempt uint32,
) ([]*dholev1.SecretRef, error) {
	if len(step.GetSecrets()) == 0 {
		return nil, nil
	}
	if s.secrets == nil {
		// refuseSecrets ran first and refused; reaching here is a caller bug,
		// and a dispatch without its declared secret is never the answer.
		return nil, errors.New("scheduler: a step declaring secrets reached dispatch on a plane that issues none")
	}
	return s.secrets.Issue(ctx, secrets.Scope{
		TenantID: tenantID, RunID: runID, StepID: step.GetId(), Attempt: attempt,
	}, step, DefaultSecretTTL)
}

// revokeSecrets forgets the unspent handles of one attempt that has ended —
// succeeded, failed, cancelled, or lost with its engine (ADR 0030). Handles are
// shared by every plane, so whichever plane processes the end revokes them
// (ADR 0031).
func (s *Scheduler) revokeSecrets(ctx context.Context, tenantID, runID, stepID string, attempt uint32) {
	if s.secrets == nil {
		return
	}
	s.secrets.Revoke(ctx, secrets.Scope{TenantID: tenantID, RunID: runID, StepID: stepID, Attempt: attempt})
}

// discardSecrets forgets the handles a dispatch minted and then did not commit.
// By handle rather than by attempt: the attempt number it was built under may
// be one another pass has already dispatched, whose handles are live.
func (s *Scheduler) discardSecrets(ctx context.Context, refs []*dholev1.SecretRef) {
	if s.secrets == nil || len(refs) == 0 {
		return
	}
	s.secrets.Discard(ctx, refs)
}

// revokeRunSecrets forgets the unspent handles of every attempt of a run the
// scheduler has just closed — failed for exhausted attempts, a policy denial or
// a secret refusal, or completed (ADR 0031). A run failed with a sibling's
// dispatch still queued otherwise left that dispatch's credential live until
// its expiry. A run closed by anything other than the scheduler is left to the
// plane's sweep.
func (s *Scheduler) revokeRunSecrets(ctx context.Context, tenantID, runID string) {
	if s.secrets == nil {
		return
	}
	s.secrets.RevokeRun(ctx, tenantID, runID)
}
