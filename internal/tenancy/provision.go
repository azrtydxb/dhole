// Package tenancy turns a tenant id into an account somebody can be held to:
// it provisions the tenant, holds the limits it runs under, and keeps the
// ledger it is billed from.
//
// internal/tenant is the tenancy BOUNDARY — what an id may be, how it travels,
// what it names on the bus. This package is what sits on top of that boundary:
// the register, the quotas and the meter. The split matters because the
// boundary is imported by every store in the tree and must stay tiny, while
// this is imported by the control plane and needs a database.
//
// Two rules run through all of it.
//
// Nothing here is unscoped. Every method takes an explicit tenant and refuses
// an empty one with "tenant scope required", the same words runstore uses, and
// a reflective sweep in the tests fails the build if a method added later
// forgets.
//
// Nothing here counts deliveries. Delivery in this system is at-least-once by
// construction (ADR 0005): the outbox redelivers, and a step lost with its
// engine is re-dispatched under a new fence. A meter that counted messages
// would bill a customer twice for one step, so usage is identified by the WORK
// — (tenant, kind, run, step, attempt) — and recording the same work twice is
// a no-op rather than a second charge.
package tenancy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/tenant"
)

// ErrUnknownTenant reports a tenant that has never been provisioned. It is an
// error rather than a zero Tenant so that a caller cannot mistake "no such
// tenant" for "a tenant with no limits".
var ErrUnknownTenant = errors.New("tenancy: unknown tenant")

// Tenant is one provisioned tenant.
//
// Credentials is the NATS client URL for the tenant's own account, and it is
// deliberately NOT a stored column: it carries a secret, and the account's
// password lives on the bus server rather than in the control plane's
// database. It is empty when the provisioner was built without a bus.
type Tenant struct {
	ID               string
	DisplayName      string
	NATSAccount      string
	CreatedAt        time.Time
	PlaneCredentials string
}

// Store is the tenancy tables — `tenants`, `quotas` and `usage_records` —
// reached the way every other store in this tree reaches its tables: a
// database/sql handle plus the dialect its statements must be rebound for.
//
// Statements are written once with `?` placeholders and rebound per dialect.
// A hand-copied Postgres variant of the same statement is two things that
// drift, and a drifted copy still runs.
type Store struct {
	db      *sql.DB
	dialect runstore.Dialect
}

// NewStore builds a Store over an already-migrated handle. Open it with
// runstore.OpenSQLite or runstore.OpenPostgres and pass the matching dialect:
// pgx rejects the `?` placeholders below outright, so a Postgres handle left
// on the zero dialect would fail on its first statement.
func NewStore(db *sql.DB, dialect runstore.Dialect) (*Store, error) {
	if db == nil {
		return nil, errors.New("tenancy: a database handle is required")
	}
	if dialect == "" {
		return nil, errors.New("tenancy: a dialect is required")
	}
	return &Store{db: db, dialect: dialect}, nil
}

const insertTenant = `INSERT INTO tenants (id, display_name, nats_account, created_at)
	VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`

const selectTenant = `SELECT id, display_name, nats_account, created_at
	FROM tenants WHERE id = ?`

// Tenant reads one tenant's register row.
func (s *Store) Tenant(ctx context.Context, tenantID string) (Tenant, error) {
	if tenantID == "" {
		return Tenant{}, fmt.Errorf("tenancy: reading a tenant: %w", runstore.ErrTenantRequired)
	}
	row := s.db.QueryRowContext(ctx, s.dialect.Rebind(selectTenant), tenantID)
	var t Tenant
	var createdAt string
	switch err := row.Scan(&t.ID, &t.DisplayName, &t.NATSAccount, &createdAt); {
	case errors.Is(err, sql.ErrNoRows):
		return Tenant{}, fmt.Errorf("%w: %q", ErrUnknownTenant, tenantID)
	case err != nil:
		return Tenant{}, fmt.Errorf("tenancy: reading tenant %q: %w", tenantID, err)
	}
	parsed, err := time.Parse(runstore.TimeFormat, createdAt)
	if err != nil {
		return Tenant{}, fmt.Errorf("tenancy: tenant %q has an unreadable created_at: %w", tenantID, err)
	}
	t.CreatedAt = parsed.UTC()
	return t, nil
}

// ProvisionerConfig configures a Provisioner.
type ProvisionerConfig struct {
	// Store is where the register and the quota rows live. Required.
	Store *Store
	// Bus is the embedded NATS server a tenant's account is created on. It is
	// optional: a control plane pointed at an external cluster provisions
	// accounts through that cluster's own operator tooling, and a tenant
	// provisioned without a bus simply has no Credentials yet.
	Bus *bus.Embedded
	// Defaults is the quota a NEW tenant starts on. Zero means DefaultQuota.
	// It is applied only on creation: see Provision.
	Defaults Quota
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Provisioner creates tenants. One tenant, one NATS account, one quota row.
type Provisioner struct {
	store    *Store
	bus      *bus.Embedded
	defaults Quota
	now      func() time.Time
}

// NewProvisioner builds a Provisioner.
func NewProvisioner(cfg ProvisionerConfig) (*Provisioner, error) {
	if cfg.Store == nil {
		return nil, errors.New("tenancy: a provisioner needs a store")
	}
	defaults := cfg.Defaults
	if defaults == (Quota{}) {
		defaults = DefaultQuota()
	}
	if err := defaults.Validate(); err != nil {
		return nil, err
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Provisioner{store: cfg.Store, bus: cfg.Bus, defaults: defaults, now: now}, nil
}

// Provision creates the tenant named id, or returns the one that is already
// there.
//
// It is IDEMPOTENT, and idempotent in the strong sense: a second call on an
// existing tenant returns what exists and changes nothing. Not its creation
// time, not its NATS account, and above all not its quotas — an operator who
// lowered a limit by hand must not have it silently restored to the default by
// a controller reconciling, or by somebody re-running the provisioning command
// because the first run's output scrolled away. The quota row and the register
// row are both written with ON CONFLICT DO NOTHING for exactly that reason.
//
// The id is validated by internal/tenant, which refuses anything that could
// become a subject wildcard or escape a storage path. It is refused, never
// sanitised: rewriting `a>` into `a` would provision a DIFFERENT tenant and
// look like it had worked.
func (p *Provisioner) Provision(ctx context.Context, id string) (Tenant, error) {
	if err := tenant.Validate(id); err != nil {
		return Tenant{}, fmt.Errorf("tenancy: provisioning: %w", err)
	}

	account := tenant.AccountName(id)
	created := p.now().UTC()
	_, err := p.store.db.ExecContext(ctx, p.store.dialect.Rebind(insertTenant),
		id, id, account, created.Format(runstore.TimeFormat))
	if err != nil {
		return Tenant{}, fmt.Errorf("tenancy: registering tenant %q: %w", id, err)
	}

	// Read back rather than return what was written: on the second call the
	// insert did nothing, and the row that matters is the one already there.
	t, err := p.store.Tenant(ctx, id)
	if err != nil {
		return Tenant{}, err
	}

	if err := p.store.ensureQuota(ctx, id, p.defaults, created); err != nil {
		return Tenant{}, err
	}

	if p.bus != nil {
		// tenant.ProvisionAccount is itself idempotent: an existing account
		// returns the credentials it already has, so engines holding the old
		// ones are not locked out by a re-provision.
		creds, err := tenant.ProvisionAccount(ctx, p.bus, id)
		if err != nil {
			return Tenant{}, fmt.Errorf("tenancy: provisioning tenant %q: %w", id, err)
		}
		t.PlaneCredentials = creds
	}
	return t, nil
}

// EngineCredentials is the credential an ENGINE of tier connects with, inside
// tenantID's account. It is NOT the credential Provision returns.
//
// The split is the point, and it is why there are two named accessors rather
// than one Tenant field. Tenant.PlaneCredentials is the tenant's ACCOUNT
// credential: it reaches the tenant's whole subject space, every tier of
// job.dispatch.> included, because the control plane publishes to every tier
// and declares every tier's stream. Handing that to an engine — which is what
// this package did until now — leaves an engine separated from other tiers by
// nothing but its own choice of filter subject, the arrangement [S-5] refuses.
// This one carries bus.TierPermissions and is refused another tier's work
// queue by the SERVER.
//
// THE TIER IS A PARAMETER, not something read from the engine's own
// configuration, and that is the whole boundary. An engine knows its tier as
// DHOLE_TIER, but a credential whose scope its holder chooses is not a
// boundary — it is a request the holder can revise. So the issuer names the
// tier, and DHOLE_TIER only decides which queue the engine tries to pull:
// an engine configured for a tier its credential does not carry gets a
// permissions error from the bus rather than another tier's work.
//
// Idempotent, for the same reason Provision is: an operator re-runs the
// command, a controller reconciles, and a second issue that minted a new
// password would lock out every engine already holding the old one.
func (p *Provisioner) EngineCredentials(ctx context.Context, tenantID, tier string) (string, error) {
	if err := tenant.Validate(tenantID); err != nil {
		return "", fmt.Errorf("tenancy: engine credentials: %w", err)
	}
	if p.bus == nil {
		return "", fmt.Errorf("tenancy: engine credentials for tenant %q: "+
			"this provisioner has no bus, so accounts are the external cluster's "+
			"operator tooling to issue", tenantID)
	}
	// Registered first. Provisioning an account as a side effect of asking for
	// an engine's credential would turn a typo in a tenant id into a live
	// account with a live credential, and the typo would look like it worked.
	if _, err := p.store.Tenant(ctx, tenantID); err != nil {
		return "", err
	}
	// The tier user lives INSIDE the tenant's account, so the account has to
	// exist. ProvisionAccount is idempotent and returns the existing
	// credential; that credential is deliberately dropped here and never
	// returned to a caller asking for an engine's.
	if _, err := tenant.ProvisionAccount(ctx, p.bus, tenantID); err != nil {
		return "", fmt.Errorf("tenancy: engine credentials for tenant %q: %w", tenantID, err)
	}
	creds, err := tenant.ProvisionTierUser(ctx, p.bus, tenantID, tier)
	if err != nil {
		return "", fmt.Errorf("tenancy: engine credentials for tenant %q tier %q: %w",
			tenantID, tier, err)
	}
	return creds, nil
}
