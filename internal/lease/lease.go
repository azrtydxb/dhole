// Package lease owns step leases and the fence tokens that make duplicate
// delivery safe.
//
// A dispatch carries the fence of the lease it was scheduled under, and the
// engine echoes that fence unchanged on every status it reports
// (docs/wire-contract.md, "Fence tokens"). An engine presumed dead can come
// back to life holding a step that has since been re-dispatched; its late
// report carries the old fence, Validate refuses it, and the newer attempt's
// result survives. Every at-most-once guarantee in Dhole rests on that refusal
// and on the fence strictly increasing.
//
// The fence is therefore never generated here. It is the NATS KV per-key
// revision: monotonic and assigned by the server, which is the only place two
// control planes can agree. A client-side counter would collide the moment a
// second plane started.
package lease

import (
	"context"
	"errors"
	"time"
)

// ErrFenced is returned for any token that is not the current lease: a
// superseded fence, an invented one, or one whose lease has been swept away.
// The caller's duty is to discard whatever the token was carrying.
var ErrFenced = errors.New("lease: fence superseded")

// ErrTenantRequired refuses an unscoped lease. Every stored record in Dhole
// carries a tenant scope, and a lease is a stored record.
var ErrTenantRequired = errors.New("tenant scope required")

// Token identifies one lease. Value names the leased step — tenant included,
// so a token from one tenant can never address another's lease — and Fence is
// the server-assigned revision that orders attempts against each other.
type Token struct {
	Value string
	Fence uint64
}

// Orphan is a step whose lease stopped being renewed. It carries everything the
// scheduler needs to re-dispatch under a fresh fence.
type Orphan struct {
	TenantID string
	RunID    string
	StepID   string
	Attempt  uint32
	// Fence is the fence the dead holder had. It is reported so the caller can
	// tell which attempt died; it is already invalid by the time Expire returns.
	Fence uint64
}

// Waiting is a step offered to a work queue that no holder has accepted yet.
// It is not an orphan and nothing about it has expired: it is reported so the
// scheduler can ask whether anything in the fleet is still able to take it.
type Waiting struct {
	TenantID string
	RunID    string
	StepID   string
	Attempt  uint32
	Fence    uint64
}

// Manager hands out leases and detects the ones that died.
//
// Claim and Offer always supersede: a step re-dispatched to a new engine takes
// a strictly higher fence, and the previous holder is fenced out from that
// moment. Renew deliberately leaves the fence alone — a holder must not
// invalidate its own dispatch token by proving it is alive.
//
// A lease's heartbeat deadline belongs to a HOLDER. Claim is for a caller that
// is the holder from the first instant (the plane running its own step), so
// the deadline starts at once. Offer is for a dispatch put on a work queue,
// where the attempt may wait any length of time for an engine slot: the fence
// exists from the start because it must travel inside the dispatch, but there
// is no deadline until the first Renew — which is an engine's ACCEPTED status
// or the first heartbeat naming the job. Timing out a dispatch nobody has
// taken was the kw defect: every step that had to wait for a slot was declared
// lost after one TTL, re-dispatched, and lost again.
type Manager interface {
	// Claim takes the lease on a step for one attempt, superseding any current
	// holder, and returns the token that must travel with the dispatch. The
	// claimer is the holder, so the ttl runs from now.
	Claim(ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error)
	// Offer takes the lease on a step for an attempt handed to a work queue,
	// superseding any current holder. It does not expire until it has been
	// accepted by a Renew; from then on ttl is its heartbeat window.
	Offer(ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error)
	// Renew extends the lease behind t without moving its fence, and accepts
	// it if it was an offer. A token that is no longer current is refused with
	// ErrFenced and extends nothing.
	Renew(ctx context.Context, t Token) error
	// Validate reports whether t is still the current lease. Anything else is
	// ErrFenced and its report must be discarded as stale.
	Validate(ctx context.Context, t Token) error
	// Expire claims and returns the leases that have passed their deadline. An
	// offer nobody has accepted has no deadline and is never returned. It is
	// safe to run on several control planes at once: each orphan is handed to
	// exactly one of them.
	Expire(ctx context.Context) ([]Orphan, error)
	// Unaccepted lists the current offers no holder has accepted yet. It
	// changes nothing.
	Unaccepted(ctx context.Context) ([]Waiting, error)
}

var _ Manager = (*KV)(nil)
