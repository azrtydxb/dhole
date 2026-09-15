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
// The prefix is the whole reason the step issuer refuses a value that starts
// with it, and a redemption refuses one that does.
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
// It holds no value, in memory or anywhere else (ADR 0031). A handle is
// recorded as a REFERENCE — the tenant, run, step and attempt it was issued
// for, which source it resolves from and the secret's name there — in a
// Handles store every plane shares, and the value is read from the source at
// the moment of redemption, by whichever plane answers. A table of live values
// would be the secret at rest that SecretRef exists to avoid; a table of
// references is not, and it is what lets any replica redeem, spend and revoke.
type Broker struct {
	handles Handles
	now     func() time.Time
	// onError hears a revocation that failed. The callers that revoke have
	// nothing to do about one — the expiry, and the sweep, are the backstop —
	// but an operator has to be able to see that it happened.
	onError func(error)

	mu      sync.RWMutex
	sources map[Kind]Source
}

// BrokerOption configures a Broker.
type BrokerOption func(*Broker)

// WithHandles gives the broker the store its records live in. A deployment of
// more than one plane must give every plane the same one — NewKVHandles over
// the shared bus. Without it the broker keeps records in its own memory, which
// is correct for exactly one plane.
func WithHandles(h Handles) BrokerOption {
	return func(b *Broker) {
		if h != nil {
			b.handles = h
		}
	}
}

// WithErrorHandler gives the broker somewhere to report a revocation that
// failed on a path that cannot return it.
func WithErrorHandler(fn func(error)) BrokerOption {
	return func(b *Broker) { b.onError = fn }
}

func (b *Broker) report(err error) {
	if err != nil && b.onError != nil {
		b.onError(err)
	}
}

// NewBroker returns a broker with no records.
func NewBroker(opts ...BrokerOption) *Broker {
	b := &Broker{handles: NewMemoryHandles(), now: time.Now, sources: map[Kind]Source{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// lend records the source handles of kind resolve from. The step issuer and
// the plane resolver lend theirs, so the broker resolves exactly what they
// were built with; a nil source lends nothing, so a caller that only revokes
// does not unset the one a caller that issues gave.
func (b *Broker) lend(kind Kind, src Source) {
	if b == nil || src == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sources[kind] = src
}

func (b *Broker) source(kind Kind) Source {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sources[kind]
}

// Issue mints a handle for ref, for scope, valid for ttl, and returns the
// SecretRef a dispatch carries. Nothing it records is the value.
//
// A scope carrying only a tenant is a handle no attempt owns, which is what
// the plane's own resolutions are.
func (b *Broker) Issue(ctx context.Context, scope Scope, ref Reference, ttl time.Duration) (*dholev1.SecretRef, error) {
	switch {
	case scope.TenantID == "":
		// No unscoped record, even while only one tenant exists.
		return nil, errors.New("secrets: a tenant is required to issue a handle")
	case ref.Binding == "":
		return nil, errors.New("secrets: a binding name is required to issue a handle")
	case ref.Secret == "":
		return nil, errors.New("secrets: a secret name is required to issue a handle")
	case ref.Source != SourceStep && ref.Source != SourcePlane:
		return nil, fmt.Errorf("secrets: a handle cannot resolve from source %q", ref.Source)
	case ttl <= 0:
		return nil, errors.New("secrets: a handle needs a positive expiry")
	case ttl > HandleRetention:
		return nil, fmt.Errorf("secrets: a handle cannot outlive the %s its record is kept for", HandleRetention)
	}

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("secrets: generating a handle: %w", err)
	}
	handle := hex.EncodeToString(raw)
	expires := b.now().Add(ttl)
	if err := b.handles.Put(ctx, Digest(handle), Record{
		TenantID:  scope.TenantID,
		RunID:     scope.RunID,
		StepID:    scope.StepID,
		Attempt:   scope.Attempt,
		Source:    ref.Source,
		Name:      ref.Secret,
		ExpiresAt: expires.UnixNano(),
	}); err != nil {
		return nil, err
	}
	return &dholev1.SecretRef{
		Name:      ref.Binding,
		Handle:    handle,
		ExpiresAt: expires.Unix(),
	}, nil
}

// Redeem exchanges a handle for its value, once, for whichever tenant it was
// issued to. It is the DEPRECATED unscoped path (ADR 0030).
//
// Every refusal is the same three words regardless of which rule was broken —
// unknown, spent, expired — because the caller is on the other side of a bus
// and the difference tells an attacker which handles exist. The reason an
// operator needs is on the issuing side, in the plane's own log.
func (b *Broker) Redeem(ctx context.Context, handle string) (string, error) {
	return b.redeem(ctx, "", handle)
}

// RedeemFor exchanges a handle for its value, once, on behalf of tenantID —
// the tenant named by the subject the request arrived on (ADR 0030).
//
// A handle issued for another tenant is refused with the one refusal every
// other rule gives, so the reply does not say that the handle exists; and it is
// spent, because single-use means spent by being PRESENTED. A handle that has
// reached another tenant has leaked, and leaving it redeemable would leave the
// leak live for the rest of its expiry.
func (b *Broker) RedeemFor(ctx context.Context, tenantID, handle string) (string, error) {
	if tenantID == "" {
		return "", errRefused
	}
	return b.redeem(ctx, tenantID, handle)
}

func (b *Broker) redeem(ctx context.Context, tenantID, handle string) (string, error) {
	if handle == "" {
		return "", errRefused
	}
	// Taken whatever happens next: single-use means the handle is spent by
	// being presented, not by being answered. The store's compare-and-set is
	// what makes that hold across planes (ADR 0031).
	r, ok, err := b.handles.Spend(ctx, Digest(handle))
	if err != nil || !ok {
		return "", errRefused
	}
	if (tenantID != "" && r.TenantID != tenantID) || b.now().UnixNano() > r.ExpiresAt {
		return "", errRefused
	}
	src := b.source(r.Source)
	if src == nil {
		return "", errRefused
	}
	value, err := src.Value(ctx, r.TenantID, r.Name)
	if err != nil || strings.HasPrefix(value, RefusalPrefix) {
		// A value that reads as a refusal cannot be carried by this reply
		// shape; the issuer refuses it where somebody can see why.
		return "", errRefused
	}
	return value, nil
}

// RevokeAttempt forgets every unspent handle issued for exactly this attempt,
// and reports how many. It is called when the attempt ends — whatever way it
// ends — so a handle its engine never redeemed stops being a live credential
// then rather than at its expiry (ADR 0030). Any plane may call it (ADR 0031).
//
// Exact, and only exact: a scope missing any field matches nothing. Matching
// by step would reach the retry of the same step, which may already have been
// issued its own handles by the time the previous attempt's end is processed.
func (b *Broker) RevokeAttempt(ctx context.Context, scope Scope) (int, error) {
	if scope.TenantID == "" || scope.RunID == "" || scope.StepID == "" || scope.Attempt == 0 {
		return 0, nil
	}
	return b.handles.Drop(ctx, Filter{
		TenantID: scope.TenantID, RunID: scope.RunID, StepID: scope.StepID, Attempt: scope.Attempt,
	})
}

// RevokeRun forgets every unspent handle issued for any attempt of one run: a
// run that ended, however it ended, and a cancelled one whose dispatch may
// still sit in a work queue that no engine holds and no Cancel can reach.
func (b *Broker) RevokeRun(ctx context.Context, tenantID, runID string) (int, error) {
	if tenantID == "" || runID == "" {
		return 0, nil
	}
	return b.handles.Drop(ctx, Filter{TenantID: tenantID, RunID: runID})
}

// RevokeHandles forgets the named handles if they are still unspent. It is for
// a dispatch that minted handles and then never committed: the attempt number
// it built them under may belong to another pass's dispatch that did, so
// revoking by scope there would take a live attempt's credential.
func (b *Broker) RevokeHandles(ctx context.Context, handles ...string) (int, error) {
	n := 0
	for _, h := range handles {
		if h == "" {
			continue
		}
		dropped, err := b.handles.Drop(ctx, Filter{Digest: Digest(h)})
		n += dropped
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// OpenRuns lists one tenant's runs that have not ended.
type OpenRuns func(ctx context.Context, tenantID string) ([]string, error)

// RevokeClosedRuns forgets every unspent handle whose run is no longer open,
// and reports how many (ADR 0031). It is the sweep beneath the revocations
// the scheduler and the API make when they end a run: a run closed by any
// other writer — an approval denied, a halting model step — is caught here.
//
// The handles are read BEFORE the index. A handle listed exists, so its run
// was created before the index is read; if that run is open, the index says
// so. Reading the index first would let a run created in between look closed.
// The plane's own handles belong to no run and are left to their expiry.
func (b *Broker) RevokeClosedRuns(ctx context.Context, open OpenRuns) (int, error) {
	records, err := b.handles.List(ctx)
	if err != nil {
		return 0, err
	}
	runs := map[string]map[string]bool{}
	for _, r := range records {
		if r.RunID == "" {
			continue
		}
		if runs[r.TenantID] == nil {
			runs[r.TenantID] = map[string]bool{}
		}
		runs[r.TenantID][r.RunID] = true
	}
	n := 0
	for tenantID, candidates := range runs {
		ids, err := open(ctx, tenantID)
		if err != nil {
			return n, fmt.Errorf("secrets: reading tenant %q's open runs: %w", tenantID, err)
		}
		for _, id := range ids {
			delete(candidates, id)
		}
		for runID := range candidates {
			dropped, err := b.RevokeRun(ctx, tenantID, runID)
			n += dropped
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// errRefused is the one refusal. It names neither the handle nor the value: an
// engine puts this text in a JobStatus error, and a status is durable and
// archived.
var errRefused = errors.New("secrets: the handle was refused")

// Requester is the engine's half of the bus: one raw request, one raw reply.
type Requester interface {
	RequestRaw(ctx context.Context, subject string, body []byte) ([]byte, error)
}

// RedeemQueue is the queue group every plane's redemption responder joins, so
// that exactly one plane answers each request (ADR 0031). Without it every
// replica answered and the engine took whichever reply came first.
const RedeemQueue = "dhole-secret-redeem"

// SubjectResponder is the plane's half: a queue-group responder on a subject
// that may be a wildcard, told which subject each request arrived on.
type SubjectResponder interface {
	RespondRawSubjectQueue(
		ctx context.Context, subject, queue string, fn func(subject string, body []byte) []byte,
	) (func(), error)
}

func refusal() []byte { return []byte(RefusalPrefix + errRefused.Error()) }

// answering bounds one redemption's work on the plane. It is NOT the context a
// responder was started under: that one bounds start-up, and a handler holding
// it would refuse every redemption once the plane had finished starting.
func answering(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), redeemTimeout)
}

func reply(value string, err error) []byte {
	if err != nil {
		return []byte(RefusalPrefix + err.Error())
	}
	return []byte(value)
}

// ServeTenants answers redemptions from b on every tenant's subject under base
// — `<base>.<tenant>` — until the returned function is called. The tenant is
// the token after base on the subject the request ARRIVED on, never anything
// the request carries, and a handle issued for any other tenant is refused
// (ADR 0030).
func ServeTenants(ctx context.Context, r SubjectResponder, b *Broker, base string) (func(), error) {
	prefix := base + "."
	return r.RespondRawSubjectQueue(ctx, prefix+"*", RedeemQueue, func(subject string, req []byte) []byte {
		tenantID, ok := strings.CutPrefix(subject, prefix)
		if !ok || tenantID == "" || strings.Contains(tenantID, ".") {
			return refusal()
		}
		rctx, cancel := answering(ctx)
		defer cancel()
		return reply(b.RedeemFor(rctx, tenantID, string(req)))
	})
}

// Serve answers redemptions from b on subject until the returned function is
// called, by handle alone.
//
// It is the DEPRECATED unscoped path (ADR 0030): an engine written before the
// redemption subject named its tenant requests on bare secret.redeem, and the
// plane keeps answering it while it accepts that engine's protocol version.
// The tenant is the handle's own, which is what it always was.
func Serve(ctx context.Context, r SubjectResponder, b *Broker, subject string) (func(), error) {
	return r.RespondRawSubjectQueue(ctx, subject, RedeemQueue, func(_ string, req []byte) []byte {
		rctx, cancel := answering(ctx)
		defer cancel()
		return reply(b.Redeem(rctx, string(req)))
	})
}

// ServeAccount answers one tenant's redemptions inside that tenant's own NATS
// account, over r — a connection holding the account's credential — until the
// returned function is called (ADR 0031).
//
// Inside an account the account IS the tenant (ADR 0014), so the tenant here
// is tenantID and never the token a request carries: a request on
// `<base>.<other>` in this account redeems as tenantID and a handle of <other>
// is refused. The legacy base is answered the same way, scoped by the account
// as ADR 0030 promised it would be.
func ServeAccount(ctx context.Context, r SubjectResponder, b *Broker, tenantID, base string) (func(), error) {
	if tenantID == "" {
		return nil, errors.New("secrets: serving an account's redemptions needs the tenant it belongs to")
	}
	answer := func(_ string, req []byte) []byte {
		rctx, cancel := answering(ctx)
		defer cancel()
		return reply(b.RedeemFor(rctx, tenantID, string(req)))
	}
	stopScoped, err := r.RespondRawSubjectQueue(ctx, base+".*", RedeemQueue, answer)
	if err != nil {
		return nil, err
	}
	stopLegacy, err := r.RespondRawSubjectQueue(ctx, base, RedeemQueue, answer)
	if err != nil {
		stopScoped()
		return nil, err
	}
	return func() { stopScoped(); stopLegacy() }, nil
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
