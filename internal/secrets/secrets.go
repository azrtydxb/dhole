// Package secrets is the redemption half of the rule that engines never
// receive secret values, only short-lived references they redeem.
//
// A JobDispatch is durable, replayable and archived, so a secret value inside
// one would be a secret at rest in the run history. What travels instead is a
// SecretRef: a name to bind the value to, an opaque handle, and an expiry. The
// handle becomes a value exactly once, at the moment the engine needs it, over
// the request/reply exchange described in docs/wire-contract.md under
// "Secrets".
//
// Two halves live here. The Broker is the control plane's: it issues handles
// and answers redemptions. The BusRedeemer is an engine's: it exchanges a
// handle for a value over the bus it already dialled. Neither writes a value
// anywhere — not to a log, not to a store, not into an error message.
package secrets

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// RefusalPrefix marks a reply that is a refusal rather than a value. It is
// part of the wire contract: a reply beginning with these bytes is an error
// whose text follows, and any other reply is the value itself.
//
// The prefix is the whole reason Issue refuses a value that starts with it.
// The alternative — a framed reply with a status field — would put the value
// inside a message with other fields, and every one of those is a place the
// value gets copied to. This shape keeps the value alone on the wire, and
// pays for it with one forbidden prefix, enforced where somebody can see it.
const RefusalPrefix = "ERR "

// redeemTimeout bounds one redemption request. An engine that waited forever
// on a plane that is not answering would hold a slot and a lease until the
// lease expired, and the step would be re-dispatched to an engine that would
// wait forever in exactly the same way.
const redeemTimeout = 10 * time.Second

// ErrNoRedeemer is what an engine with no redemption endpoint reports. It is a
// configuration fault, not a fault of the dispatch, and it is distinguishable
// so an operator reading a failed step knows which one to fix.
var ErrNoRedeemer = errors.New("secrets: this engine has no redemption endpoint")

// Broker issues single-use handles and redeems them.
//
// It holds handles in memory and nothing else. Persisting them would defeat
// the point: a handle is a bearer credential for its TTL, and a table of live
// handles in the run database is the secret-at-rest this whole design exists
// to avoid. A plane restart invalidates every outstanding handle, which is
// correct — the dispatches that carry them are re-issued under a new attempt.
type Broker struct {
	mu      sync.Mutex
	handles map[string]entry
}

type entry struct {
	tenantID string
	name     string
	value    string
	expires  time.Time
	// scope is the attempt a step's handle was issued for, and zero for one
	// the plane issued for itself. It is what revocation matches on.
	scope Scope
}

// NewBroker returns an empty broker.
func NewBroker() *Broker {
	return &Broker{handles: map[string]entry{}}
}

// Issue mints a handle for value, valid for ttl, and returns the SecretRef a
// dispatch carries. The value never leaves this process except as the reply to
// a redemption of this handle.
func (b *Broker) Issue(tenantID, name, value string, ttl time.Duration) (*dholev1.SecretRef, error) {
	return b.IssueFor(Scope{TenantID: tenantID}, name, value, ttl)
}

// IssueFor is Issue for one attempt of a step: the handle remembers the scope
// it was minted for, so the attempt's end can revoke it (ADR 0030). A scope
// carrying only a tenant is a handle no attempt owns, which is what the plane's
// own resolutions are.
func (b *Broker) IssueFor(scope Scope, name, value string, ttl time.Duration) (*dholev1.SecretRef, error) {
	tenantID := scope.TenantID
	switch {
	case tenantID == "":
		// No unscoped record, even while only one tenant exists.
		return nil, errors.New("secrets: a tenant is required to issue a handle")
	case name == "":
		return nil, errors.New("secrets: a binding name is required to issue a handle")
	case strings.HasPrefix(value, RefusalPrefix):
		// The refusal names neither the value nor the binding's contents: this
		// error is itself the closest thing to a leak in the package.
		return nil, fmt.Errorf("secrets: a value beginning %q cannot be issued: "+
			"a redemption reply with that prefix is a refusal, so the value would be read as an error",
			RefusalPrefix)
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("secrets: generating a handle: %w", err)
	}
	handle := hex.EncodeToString(raw)
	expires := time.Now().Add(ttl)

	b.mu.Lock()
	defer b.mu.Unlock()
	// Forget what can no longer be honoured. Only a redemption used to delete a
	// handle, and every step dispatch now mints some: a dispatch cancelled,
	// lost or refused before its engine redeemed would otherwise keep its value
	// in this process for as long as the plane runs.
	now := time.Now()
	for h, e := range b.handles {
		if now.After(e.expires) {
			delete(b.handles, h)
		}
	}
	b.handles[handle] = entry{tenantID: tenantID, name: name, value: value, expires: expires, scope: scope}
	return &dholev1.SecretRef{
		Name:      name,
		Handle:    handle,
		ExpiresAt: expires.Unix(),
	}, nil
}

// Redeem exchanges a handle for its value, once.
//
// Every refusal is the same three words regardless of which rule was broken —
// unknown, spent, expired — because the caller is on the other side of a bus
// and the difference tells an attacker which handles exist. The reason an
// operator needs is on the issuing side, in the plane's own log.
func (b *Broker) Redeem(handle string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.handles[handle]
	if !ok {
		return "", errRefused
	}
	// Taken whatever happens next: single-use means the handle is spent by
	// being presented, not by being answered.
	delete(b.handles, handle)
	if time.Now().After(e.expires) {
		return "", errRefused
	}
	return e.value, nil
}

// RevokeAttempt forgets every unspent handle issued for exactly this attempt,
// and reports how many. It is called when the attempt ends — whatever way it
// ends — so a handle its engine never redeemed stops being a live credential
// then rather than at its expiry (ADR 0030).
//
// Exact, and only exact: a scope missing any field matches nothing. Matching
// by step would reach the retry of the same step, which may already have been
// issued its own handles by the time the previous attempt's end is processed.
func (b *Broker) RevokeAttempt(scope Scope) int {
	if scope.TenantID == "" || scope.RunID == "" || scope.StepID == "" || scope.Attempt == 0 {
		return 0
	}
	return b.revoke(func(e entry) bool { return e.scope == scope })
}

// RevokeRun forgets every unspent handle issued for any attempt of one run. A
// cancelled run may still have a dispatch in a work queue that no engine holds
// and no Cancel can reach; revoking here is what stops the engine that later
// takes it from being handed the credential.
func (b *Broker) RevokeRun(tenantID, runID string) int {
	if tenantID == "" || runID == "" {
		return 0
	}
	return b.revoke(func(e entry) bool { return e.scope.TenantID == tenantID && e.scope.RunID == runID })
}

// RevokeHandles forgets the named handles if they are still unspent. It is for
// a dispatch that minted handles and then never committed: the attempt number
// it built them under may belong to another pass's dispatch that did, so
// revoking by scope there would take a live attempt's credential.
func (b *Broker) RevokeHandles(handles ...string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, h := range handles {
		if _, ok := b.handles[h]; ok {
			delete(b.handles, h)
			n++
		}
	}
	return n
}

func (b *Broker) revoke(match func(entry) bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for h, e := range b.handles {
		if match(e) {
			delete(b.handles, h)
			n++
		}
	}
	return n
}

// RedeemFor exchanges a handle for its value, once, on behalf of tenantID —
// the tenant named by the subject the request arrived on (ADR 0030).
//
// A handle issued for another tenant is refused with the one refusal every
// other rule gives, so the reply does not say that the handle exists; and it is
// spent, because single-use means spent by being PRESENTED. A handle that has
// reached another tenant has leaked, and leaving it redeemable would leave the
// leak live for the rest of its expiry.
func (b *Broker) RedeemFor(tenantID, handle string) (string, error) {
	if tenantID == "" {
		return "", errRefused
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.handles[handle]
	if !ok {
		return "", errRefused
	}
	delete(b.handles, handle)
	if e.tenantID != tenantID || time.Now().After(e.expires) {
		return "", errRefused
	}
	return e.value, nil
}

// errRefused is the one refusal. It names neither the handle nor the value: an
// engine puts this text in a JobStatus error, and a status is durable and
// archived.
var errRefused = errors.New("secrets: the handle was refused")

// Requester is the engine's half of the bus: one raw request, one raw reply.
type Requester interface {
	RequestRaw(ctx context.Context, subject string, body []byte) ([]byte, error)
}

// Responder is the plane's half.
type Responder interface {
	RespondRaw(ctx context.Context, subject string, fn func([]byte) []byte) (func(), error)
}

// SubjectResponder is the plane's half for a wildcard subject: the handler is
// told which subject each request arrived on.
type SubjectResponder interface {
	RespondRawSubject(ctx context.Context, subject string, fn func(subject string, body []byte) []byte) (func(), error)
}

// ServeTenants answers redemptions from b on every tenant's subject under base
// — `<base>.<tenant>` — until the returned function is called. The tenant is
// the token after base on the subject the request ARRIVED on, never anything
// the request carries, and a handle issued for any other tenant is refused
// (ADR 0030).
func ServeTenants(ctx context.Context, r SubjectResponder, b *Broker, base string) (func(), error) {
	prefix := base + "."
	return r.RespondRawSubject(ctx, prefix+"*", func(subject string, req []byte) []byte {
		tenantID, ok := strings.CutPrefix(subject, prefix)
		if !ok || tenantID == "" || strings.Contains(tenantID, ".") {
			return []byte(RefusalPrefix + errRefused.Error())
		}
		value, err := b.RedeemFor(tenantID, string(req))
		if err != nil {
			return []byte(RefusalPrefix + err.Error())
		}
		return []byte(value)
	})
}

// Serve answers redemptions from b on subject until the returned function is
// called, by handle alone.
//
// It is the DEPRECATED unscoped path (ADR 0030): an engine written before the
// redemption subject named its tenant requests on bare secret.redeem, and the
// plane keeps answering it while it accepts that engine's protocol version.
// The tenant is the handle's own, which is what it always was.
func Serve(ctx context.Context, r Responder, b *Broker, subject string) (func(), error) {
	return r.RespondRaw(ctx, subject, func(req []byte) []byte {
		value, err := b.Redeem(string(req))
		if err != nil {
			return []byte(RefusalPrefix + err.Error())
		}
		return []byte(value)
	})
}

// Redeemer exchanges a SecretRef for the value behind it.
//
// This interface is why CAPABILITY_SECRETS is not sourced from an executor.
// NETWORK, PRIVILEGED and HOST_MOUNT are isolation guarantees a sandbox
// backend either can or cannot make; redeeming a reference is something the
// AGENT does, over the bus it dialled, before any sandbox is involved. An
// engine advertises the capability when it holds one of these and refuses a
// dispatch carrying a secret when it does not.
//
// tenantID is the tenant the dispatch carrying ref belongs to, and it is what
// names the subject the redemption is asked on (ADR 0030).
type Redeemer interface {
	Redeem(ctx context.Context, tenantID string, ref *dholev1.SecretRef) (string, error)
}

// BusRedeemer redeems over the bus, on the subject the wire contract names.
type BusRedeemer struct {
	req     Requester
	subject string
}

var _ Redeemer = (*BusRedeemer)(nil)

// NewBusRedeemer returns a redeemer that requests on `<base>.<tenant>`, and on
// base itself only when nothing serves the scoped subject.
func NewBusRedeemer(req Requester, base string) *BusRedeemer {
	return &BusRedeemer{req: req, subject: base}
}

// Redeem sends the handle and returns the value.
//
// The error path is written for what it will be pasted into: a JobStatus, which
// is durable and archived. It names the BINDING — the environment variable the
// step expected — and never the handle and never the reply, because a reply
// that is not a refusal is the value itself.
//
// The request goes to the tenant's own subject. It falls back to the unscoped
// base ONLY when nothing at all serves the scoped one — a plane that predates
// ADR 0030 — so an engine may still be upgraded before its plane. A refusal is
// a reply, not an absence, and is never retried elsewhere; and a request the
// bus refuses for permissions times out rather than reporting no responder, so
// a credential cannot be talked into the unscoped path either.
func (r *BusRedeemer) Redeem(ctx context.Context, tenantID string, ref *dholev1.SecretRef) (string, error) {
	if r == nil || r.req == nil || r.subject == "" {
		return "", ErrNoRedeemer
	}
	if tenantID == "" {
		return "", fmt.Errorf("secrets: redeeming the reference bound to %q: the dispatch names no tenant",
			ref.GetName())
	}
	ctx, cancel := context.WithTimeout(ctx, redeemTimeout)
	defer cancel()

	reply, err := r.req.RequestRaw(ctx, r.subject+"."+tenantID, []byte(ref.GetHandle()))
	if errors.Is(err, bus.ErrNoResponders) {
		reply, err = r.req.RequestRaw(ctx, r.subject, []byte(ref.GetHandle()))
	}
	if err != nil {
		return "", fmt.Errorf("secrets: redeeming the reference bound to %q: %w", ref.GetName(), err)
	}
	if strings.HasPrefix(string(reply), RefusalPrefix) {
		return "", fmt.Errorf("secrets: the reference bound to %q was refused: %s",
			ref.GetName(), strings.TrimPrefix(string(reply), RefusalPrefix))
	}
	return string(reply), nil
}
