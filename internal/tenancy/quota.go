package tenancy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// QuotaName names one limit. It is a closed set of stable strings because it
// appears in the reason a person reads when their run was refused, and in the
// metric label an operator groups by; a rename breaks both.
type QuotaName string

// The limits a tenant runs under.
const (
	QuotaMaxConcurrentSteps QuotaName = "max_concurrent_steps"
	QuotaMaxRunsPerDay      QuotaName = "max_runs_per_day"
	QuotaMaxCASBytes        QuotaName = "max_cas_bytes"
)

// ErrQuotaExceeded is what every refusal wraps. Callers match it with
// errors.Is to tell "you are over your limit", which the customer can fix, from
// "the store is broken", which they cannot.
var ErrQuotaExceeded = errors.New("tenancy: quota exceeded")

// Quota is one tenant's limits.
type Quota struct {
	// MaxConcurrentSteps is how many of this tenant's steps may be in flight
	// across the whole fleet at once.
	MaxConcurrentSteps int
	// MaxRunsPerDay is how many runs the tenant may START in one UTC day.
	// Runs already admitted are never re-refused: see Enforcer.AdmitRun.
	MaxRunsPerDay int
	// MaxCASBytes is how many bytes of content-addressed storage the tenant
	// may hold.
	MaxCASBytes int64
}

// DefaultQuota is what a tenant nobody has made a decision about runs under.
//
// It is generous enough to be invisible to a homelab and finite enough that a
// runaway pipeline stops rather than filling a disk. There is no "unlimited"
// default: a limit nobody set is the shape in which a forgotten provisioning
// step becomes an unbounded bill.
func DefaultQuota() Quota {
	return Quota{
		MaxConcurrentSteps: 64,
		MaxRunsPerDay:      1000,
		MaxCASBytes:        64 << 30,
	}
}

// Validate refuses a quota that is not a limit. Zero is refused rather than
// read as "unlimited", because the two are indistinguishable to whoever typed
// it and only one of them is what they meant.
func (q Quota) Validate() error {
	switch {
	case q.MaxConcurrentSteps <= 0:
		return fmt.Errorf("tenancy: %s must be positive, got %d", QuotaMaxConcurrentSteps, q.MaxConcurrentSteps)
	case q.MaxRunsPerDay <= 0:
		return fmt.Errorf("tenancy: %s must be positive, got %d", QuotaMaxRunsPerDay, q.MaxRunsPerDay)
	case q.MaxCASBytes <= 0:
		return fmt.Errorf("tenancy: %s must be positive, got %d", QuotaMaxCASBytes, q.MaxCASBytes)
	}
	return nil
}

const upsertQuotaIfAbsent = `INSERT INTO quotas
	(tenant_id, max_concurrent_steps, max_runs_per_day, max_cas_bytes, updated_at)
	VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`

const replaceQuota = `UPDATE quotas SET max_concurrent_steps = ?, max_runs_per_day = ?,
	max_cas_bytes = ?, updated_at = ? WHERE tenant_id = ?`

const selectQuota = `SELECT max_concurrent_steps, max_runs_per_day, max_cas_bytes
	FROM quotas WHERE tenant_id = ?`

// ensureQuota writes the defaults for a tenant that has no quota row and
// leaves an existing row untouched. It is the half of provisioning that must
// not reset anything.
func (s *Store) ensureQuota(ctx context.Context, tenantID string, q Quota, at time.Time) error {
	if tenantID == "" {
		return fmt.Errorf("tenancy: seeding a quota: %w", runstore.ErrTenantRequired)
	}
	_, err := s.db.ExecContext(ctx, s.dialect.Rebind(upsertQuotaIfAbsent),
		tenantID, q.MaxConcurrentSteps, q.MaxRunsPerDay, q.MaxCASBytes,
		at.UTC().Format(runstore.TimeFormat))
	if err != nil {
		return fmt.Errorf("tenancy: seeding the quota for %q: %w", tenantID, err)
	}
	return nil
}

// SetQuota replaces a tenant's limits. It is the ONLY path that overwrites
// them: provisioning seeds and never resets.
func (s *Store) SetQuota(ctx context.Context, tenantID string, q Quota) error {
	if tenantID == "" {
		return fmt.Errorf("tenancy: setting a quota: %w", runstore.ErrTenantRequired)
	}
	if err := q.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC().Format(runstore.TimeFormat)
	res, err := s.db.ExecContext(ctx, s.dialect.Rebind(replaceQuota),
		q.MaxConcurrentSteps, q.MaxRunsPerDay, q.MaxCASBytes, now, tenantID)
	if err != nil {
		return fmt.Errorf("tenancy: setting the quota for %q: %w", tenantID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("tenancy: setting the quota for %q: %w", tenantID, err)
	}
	if affected == 0 {
		// No row yet: a quota set before provisioning is still a decision
		// somebody made, and dropping it silently would be the worst of both.
		return s.ensureQuota(ctx, tenantID, q, time.Now())
	}
	return nil
}

// Quota reads a tenant's limits, falling back to DefaultQuota for a tenant
// with no row.
//
// The fallback is deliberate and it is not "unlimited". A tenant that reached
// the scheduler without a quota row is a provisioning bug, and the safe answer
// to a provisioning bug is the default limit, not none.
func (s *Store) Quota(ctx context.Context, tenantID string) (Quota, error) {
	if tenantID == "" {
		return Quota{}, fmt.Errorf("tenancy: reading a quota: %w", runstore.ErrTenantRequired)
	}
	row := s.db.QueryRowContext(ctx, s.dialect.Rebind(selectQuota), tenantID)
	var q Quota
	switch err := row.Scan(&q.MaxConcurrentSteps, &q.MaxRunsPerDay, &q.MaxCASBytes); {
	case errors.Is(err, sql.ErrNoRows):
		return DefaultQuota(), nil
	case err != nil:
		return Quota{}, fmt.Errorf("tenancy: reading the quota for %q: %w", tenantID, err)
	}
	return q, nil
}

// Decision is the answer to "may this happen", and it is a VALUE rather than a
// bare bool because a refusal nobody can explain is an outage nobody can
// explain. It names the tenant, the quota, the limit and what was already
// used, and Reason says all four in a sentence that can be put in front of the
// person whose run stopped.
type Decision struct {
	TenantID  string
	Quota     QuotaName
	Limit     int64
	Observed  int64
	Requested int64
	Allowed   bool
	Reason    string
}

// Err is the refusal as an error, or nil when the decision allowed it. It
// exists so a caller can either branch on Allowed or propagate, without
// rebuilding the message and losing half of it.
func (d Decision) Err() error {
	if d.Allowed {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrQuotaExceeded, d.Reason)
}

// EnforcerConfig configures an Enforcer.
type EnforcerConfig struct {
	// Store holds the limits and the usage they are measured against.
	Store *Store
	// Observe is called with every REFUSAL, before it is returned. It is how a
	// quota becomes visible: without it the only trace of a refused run is an
	// error message somebody may or may not have logged. A nil observer is
	// allowed — the Decision still carries everything — but a deployment that
	// enforces quotas and reports none of it will be asked questions it cannot
	// answer.
	Observe func(context.Context, Decision)
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Enforcer answers admission questions against a tenant's quota.
//
// Every check reads the limit and the usage from the store rather than from a
// cached figure. Several control planes enforce the same quota, and a limit
// each plane kept its own count of would be multiplied by the number of planes
// — the same mistake scheduler.Budgets exists to avoid.
type Enforcer struct {
	store   *Store
	observe func(context.Context, Decision)
	now     func() time.Time
}

// NewEnforcer builds an Enforcer.
func NewEnforcer(cfg EnforcerConfig) (*Enforcer, error) {
	if cfg.Store == nil {
		return nil, errors.New("tenancy: an enforcer needs a store")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Enforcer{store: cfg.Store, observe: cfg.Observe, now: now}, nil
}

// allow builds an allowing decision.
func allow(tenantID string, name QuotaName, limit, observed, requested int64) Decision {
	return Decision{
		TenantID: tenantID, Quota: name, Limit: limit,
		Observed: observed, Requested: requested, Allowed: true,
	}
}

// refuse builds a refusal, reports it to the observer, and returns it. Every
// refusal in this package goes through here, so there is exactly one place
// where a quota decision can become silent.
func (e *Enforcer) refuse(ctx context.Context, tenantID string, name QuotaName, limit, observed, requested int64) Decision {
	d := Decision{
		TenantID: tenantID, Quota: name, Limit: limit,
		Observed: observed, Requested: requested, Allowed: false,
		Reason: fmt.Sprintf(
			"tenant %q is at its %s quota: limit %d, already used %d, requested %d",
			tenantID, name, limit, observed, requested),
	}
	if e.observe != nil {
		e.observe(ctx, d)
	}
	return d
}

// AdmitRun decides whether a run may start, and counts it if it may.
//
// Admission is recorded as usage, which gives the daily limit two properties a
// counter would not have. The number the limit is applied to is the same
// number that appears on the invoice, so a customer disputing one is disputing
// the other. And admission is IDEMPOTENT on the run id: a control plane that
// re-admits a run it already admitted — after a restart, or because a second
// plane picked the run up — passes, even when the tenant is now at its limit.
// Refusing a run that has already been counted would kill work mid-flight to
// enforce a limit that work was already charged against.
func (e *Enforcer) AdmitRun(ctx context.Context, tenantID, runID string) (Decision, error) {
	if tenantID == "" {
		return Decision{}, fmt.Errorf("tenancy: admitting a run: %w", runstore.ErrTenantRequired)
	}
	if runID == "" {
		return Decision{}, errors.New("tenancy: admitting a run: a run id is required")
	}
	q, err := e.store.Quota(ctx, tenantID)
	if err != nil {
		return Decision{}, err
	}
	now := e.now().UTC()

	// Already counted today: this run is in flight and stays in flight.
	counted, err := e.store.runStartedToday(ctx, tenantID, runID, now)
	if err != nil {
		return Decision{}, err
	}
	used, err := e.store.RunsStartedToday(ctx, tenantID, now)
	if err != nil {
		return Decision{}, err
	}
	limit := int64(q.MaxRunsPerDay)
	if counted {
		return allow(tenantID, QuotaMaxRunsPerDay, limit, int64(used), 0), nil
	}
	if int64(used) >= limit {
		return e.refuse(ctx, tenantID, QuotaMaxRunsPerDay, limit, int64(used), 1), nil
	}

	if _, err := e.store.RecordUsage(ctx, tenantID, Usage{
		Kind: KindRunStarted, RunID: runID, Quantity: 1, Billable: true, At: now,
	}); err != nil {
		return Decision{}, err
	}
	return allow(tenantID, QuotaMaxRunsPerDay, limit, int64(used)+1, 1), nil
}

// AdmitStep decides whether one more of this tenant's steps may go in flight,
// given how many already are.
//
// inFlight is passed in rather than counted here because the authority on it
// is the fleet's own bookkeeping — the leases and the scheduler's budgets —
// and a second count derived from the event log would disagree with it under
// exactly the conditions the limit matters.
func (e *Enforcer) AdmitStep(ctx context.Context, tenantID string, inFlight int) (Decision, error) {
	if tenantID == "" {
		return Decision{}, fmt.Errorf("tenancy: admitting a step: %w", runstore.ErrTenantRequired)
	}
	if inFlight < 0 {
		return Decision{}, fmt.Errorf("tenancy: in-flight steps cannot be negative, got %d", inFlight)
	}
	q, err := e.store.Quota(ctx, tenantID)
	if err != nil {
		return Decision{}, err
	}
	limit := int64(q.MaxConcurrentSteps)
	if int64(inFlight) >= limit {
		return e.refuse(ctx, tenantID, QuotaMaxConcurrentSteps, limit, int64(inFlight), 1), nil
	}
	return allow(tenantID, QuotaMaxConcurrentSteps, limit, int64(inFlight), 1), nil
}

// AdmitCASBytes decides whether size more bytes may be stored.
//
// The check is read-then-write and is therefore a soft cap under concurrency:
// two writers that both read the same total can both be admitted, and the
// tenant briefly exceeds its limit by one blob. That is the right trade here —
// the alternative is a lock on every artifact write — and it is a cap that
// slips, not one that fails open: the next write sees the real total.
func (e *Enforcer) AdmitCASBytes(ctx context.Context, tenantID string, size int64) (Decision, error) {
	if tenantID == "" {
		return Decision{}, fmt.Errorf("tenancy: admitting a blob: %w", runstore.ErrTenantRequired)
	}
	if size < 0 {
		return Decision{}, fmt.Errorf("tenancy: blob size cannot be negative, got %d", size)
	}
	q, err := e.store.Quota(ctx, tenantID)
	if err != nil {
		return Decision{}, err
	}
	used, err := e.store.CASBytes(ctx, tenantID)
	if err != nil {
		return Decision{}, err
	}
	if used+size > q.MaxCASBytes {
		return e.refuse(ctx, tenantID, QuotaMaxCASBytes, q.MaxCASBytes, used, size), nil
	}
	return allow(tenantID, QuotaMaxCASBytes, q.MaxCASBytes, used, size), nil
}

// casGate is a cas.Store that refuses a write which would take the tenant past
// its storage quota, and meters the ones it lets through.
type casGate struct {
	inner cas.Store
	enf   *Enforcer
}

// GuardCAS wraps a blob store so every Put is charged against the tenant's
// MaxCASBytes quota.
//
// A blob's size is not known before it is read, so the gate does not ask
// permission for a number it does not have: it reads through a limiter set to
// the tenant's remaining headroom, and the read FAILS the moment the blob
// would exceed it. The underlying store stages a Put in a temp file and
// publishes it with a rename, so a Put that fails part way leaves nothing
// visible — the quota blocks the write rather than complaining about bytes
// that already landed.
//
// A successful Put is metered under the blob's DIGEST, which means storing
// identical bytes twice is charged once. That is not a rounding decision: the
// store keeps one copy, so charging for two would be billing for storage
// nobody is using.
//
// The known cost of not knowing the size in advance: a tenant whose headroom
// is smaller than a blob it ALREADY holds is refused when it re-uploads that
// blob, even though storing it would consume nothing. The gate cannot know the
// digest before reading the bytes, and the alternative — write first, check
// after — is the failure this exists to prevent. It errs towards refusing, and
// never towards billing for something it promised to refuse.
func GuardCAS(inner cas.Store, enf *Enforcer) (cas.Store, error) {
	if inner == nil {
		return nil, errors.New("tenancy: guarding a blob store needs a blob store")
	}
	if enf == nil {
		return nil, errors.New("tenancy: guarding a blob store needs an enforcer")
	}
	return &casGate{inner: inner, enf: enf}, nil
}

// Put stores bytes, refusing anything that would exceed the tenant's quota.
func (g *casGate) Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenancy: storing a blob: %w", runstore.ErrTenantRequired)
	}
	// A zero-byte request answers the two questions the gate needs — the limit
	// and what is already used — without pretending to know the size.
	headroom, err := g.enf.AdmitCASBytes(ctx, tenantID, 0)
	if err != nil {
		return nil, err
	}
	if !headroom.Allowed {
		return nil, headroom.Err()
	}
	remaining := headroom.Limit - headroom.Observed

	counted := &quotaReader{inner: r, remaining: remaining}
	digest, err := g.inner.Put(ctx, tenantID, counted)
	if counted.exceeded {
		// The limiter tripped: nothing was published, and the refusal is built
		// from what the blob would have taken.
		return nil, g.enf.refuse(ctx, tenantID, QuotaMaxCASBytes,
			headroom.Limit, headroom.Observed, counted.read).Err()
	}
	if err != nil {
		return nil, err
	}

	if _, err := g.enf.store.RecordUsage(ctx, tenantID, Usage{
		Kind: KindCASBytes, StepID: digest.GetHex(), Quantity: counted.read,
		Billable: true, At: g.enf.now().UTC(),
	}); err != nil {
		return nil, err
	}
	return digest, nil
}

// Get opens a blob. Reading is not charged, so the gate only enforces the
// scope.
func (g *casGate) Get(ctx context.Context, tenantID string, d *dholev1.Digest) (io.ReadCloser, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenancy: reading a blob: %w", runstore.ErrTenantRequired)
	}
	return g.inner.Get(ctx, tenantID, d)
}

// Has reports whether the tenant holds a blob.
func (g *casGate) Has(ctx context.Context, tenantID string, d *dholev1.Digest) (bool, error) {
	if tenantID == "" {
		return false, fmt.Errorf("tenancy: checking a blob: %w", runstore.ErrTenantRequired)
	}
	return g.inner.Has(ctx, tenantID, d)
}

// errQuotaTripped stops the copy inside the underlying store. It never
// escapes: the gate recognises it by the reader's own flag and replaces it
// with the Decision's message, which names the quota and the limit.
var errQuotaTripped = errors.New("tenancy: blob exceeds the storage quota")

// quotaReader counts what it passes through and fails once more than
// `remaining` bytes have been read.
type quotaReader struct {
	inner     io.Reader
	remaining int64
	read      int64
	exceeded  bool
}

func (q *quotaReader) Read(p []byte) (int, error) {
	n, err := q.inner.Read(p)
	q.read += int64(n)
	if q.read > q.remaining {
		q.exceeded = true
		return n, errQuotaTripped
	}
	return n, err
}
