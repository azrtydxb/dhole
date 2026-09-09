// Package catalog is the durable registry of the types a tenant may use: step,
// trigger and engine types with their schemas, digests, effect classes and
// capabilities.
//
// It is deliberately NOT the runtime engine registry (ADR 0010). That one lives
// in NATS KV behind a heartbeat TTL and is meant to evaporate — an instance
// that stops heartbeating ages out, because a stale claim that a dead process
// is alive is worse than no claim at all. This one must survive everything: a
// control-plane restart that forgot what a step type is would strand every
// pipeline already saved. The two have opposite lifecycles, so they are two
// stores, and entries must not drift between them.
package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// timeFormat matches the run store: SQLite has no time type, and RFC3339 with
// nanoseconds in UTC sorts lexicographically in chronological order.
const timeFormat = time.RFC3339Nano

// ErrNotFound is returned when a well-formed reference names nothing this
// tenant has published. It is an error rather than a zero Entry on purpose: a
// zero Entry carries EFFECT_CLASS_UNSPECIFIED, which a caller that forgot to
// check would read as a declaration rather than as an absence.
var ErrNotFound = errors.New("catalog: no such entry")

// ErrVersionExists is returned when a namespace/name/version already holds a
// DIFFERENT manifest. A version is immutable: Task 35 resolves plugin versions
// into a per-revision lockfile and dispatch routes on the recorded digest, so a
// mutable version would make every lockfile already written point silently at
// different code, and every cache key folded over it a lie (ADR 0010). A
// byte-identical republish is a no-op instead, because publishing is retried
// and re-running the same deploy must not fail.
var ErrVersionExists = errors.New("catalog: version already published")

// Store is the durable catalog. Implementations are safe for concurrent use.
//
// Every method takes an explicit tenant and rejects an empty one with
// runstore.ErrTenantRequired ("tenant scope required"). There is no unscoped
// query in this system, even while only one tenant exists.
type Store interface {
	// Publish records a type. It is idempotent for a byte-identical manifest
	// and returns ErrVersionExists for one that differs.
	Publish(ctx context.Context, tenantID string, m Manifest) error

	// Resolve returns the entry a "namespace/name@version" reference names,
	// ErrMalformedRef if it does not parse, or ErrNotFound if nothing is
	// published under it.
	Resolve(ctx context.Context, tenantID, ref string) (Entry, error)

	// List returns every entry the tenant has published, in reference order.
	List(ctx context.Context, tenantID string) ([]Entry, error)

	// ResolveStep resolves a pipeline step against the plugin it names,
	// applying ADR 0002's defaulting rule: a step that declares no effect
	// class inherits its plugin's. An explicit override is honoured, but a
	// WIDENING one is reported in Entry.OverrideWarnings.
	ResolveStep(ctx context.Context, tenantID string, step *dholev1.Step) (Entry, error)

	// Close releases the store's resources.
	Close() error
}

// sqlStore is the catalog over the same database as the run event log.
//
// It shares that database rather than owning one, exactly as the cache does:
// the catalog DDL lives beside the run store's, is embedded in the same binary
// and applied by the same migration runner, so the schema has one definition.
// That database is SQLite for a developer and a homelab and Postgres for the
// tuned target, so the dialect travels with the handle.
type sqlStore struct {
	db      *sql.DB
	dialect runstore.Dialect
	// ownsDB is true only when this store opened the handle itself. A catalog
	// handed a shared handle must not close the definition store's and the
	// collector's database out from under them.
	ownsDB bool
}

var _ Store = (*sqlStore)(nil)

// New returns a catalog over an already-open handle speaking dialect. The
// handle is the caller's, and its schema is expected to carry the migrations
// the run store applies.
func New(db *sql.DB, dialect runstore.Dialect) Store {
	return &sqlStore{db: db, dialect: dialect}
}

// NewSQLite is the single-file convenience: it opens the SQLite database at
// path, applies the embedded migrations, and hands back a catalog that owns
// and closes that handle. It is a shorthand for New over runstore.OpenSQLite,
// not the only way in — a Postgres deployment passes its own handle to New.
func NewSQLite(path string) (Store, error) {
	db, err := runstore.OpenSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("catalog: open sqlite: %w", err)
	}
	return &sqlStore{db: db, dialect: runstore.DialectSQLite, ownsDB: true}, nil
}

// Close releases the database handle, and only if this store opened it. A
// handle passed to New belongs to whoever opened it.
func (s *sqlStore) Close() error {
	if !s.ownsDB {
		return nil
	}
	return s.db.Close()
}

// Publish validates the manifest and stores it, refusing a republish that would
// change an already-published version.
func (s *sqlStore) Publish(ctx context.Context, tenantID string, m Manifest) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	if err := m.validate(); err != nil {
		return err
	}

	row, err := encode(m)
	if err != nil {
		return err
	}

	// Immutability is enforced by reading what is there rather than by
	// upserting: an identical republish must pass, a differing one must be
	// refused, and only a comparison can tell those apart.
	switch existing, err := s.Resolve(ctx, tenantID, m.Ref()); {
	case err == nil:
		stored, err := encode(existing.Manifest)
		if err != nil {
			return err
		}
		if stored == row {
			return nil
		}
		return fmt.Errorf("%w: %s (a version is immutable; publish a new one)",
			ErrVersionExists, m.Ref())
	case errors.Is(err, ErrNotFound):
	default:
		return err
	}

	const q = `INSERT INTO catalog_entries
		(tenant_id, namespace, name, version, digest, kind, effect_class,
		 capabilities, engine_types, input_schema, output_schema, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q),
		tenantID, m.Namespace, m.Name, m.Version, row.digest, string(m.Kind),
		row.effectClass, row.capabilities, row.engineTypes,
		row.inputSchema, row.outputSchema,
		time.Now().UTC().Format(timeFormat),
	); err != nil {
		return fmt.Errorf("catalog: publish %s: %w", m.Ref(), err)
	}
	return nil
}

// selectColumns is the projection every read shares, in decode's order.
const selectColumns = `namespace, name, version, digest, kind, effect_class,
	capabilities, engine_types, input_schema, output_schema`

// Resolve returns the entry a reference names.
func (s *sqlStore) Resolve(ctx context.Context, tenantID, ref string) (Entry, error) {
	if tenantID == "" {
		return Entry{}, runstore.ErrTenantRequired
	}
	namespace, name, version, err := parseRef(ref)
	if err != nil {
		return Entry{}, err
	}

	q := `SELECT ` + selectColumns + ` FROM catalog_entries
		WHERE tenant_id = ? AND namespace = ? AND name = ? AND version = ?`
	var r row
	switch err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, namespace, name, version).Scan(
		&r.namespace, &r.name, &r.version, &r.digest, &r.kind, &r.effectClass,
		&r.capabilities, &r.engineTypes, &r.inputSchema, &r.outputSchema,
	); {
	case errors.Is(err, sql.ErrNoRows):
		return Entry{}, fmt.Errorf("%w: %s", ErrNotFound, ref)
	case err != nil:
		return Entry{}, fmt.Errorf("catalog: resolve %s: %w", ref, err)
	}

	m, err := r.decode()
	if err != nil {
		return Entry{}, err
	}
	return Entry{Manifest: m}, nil
}

// List returns the tenant's whole catalog.
func (s *sqlStore) List(ctx context.Context, tenantID string) ([]Entry, error) {
	if tenantID == "" {
		return nil, runstore.ErrTenantRequired
	}

	q := `SELECT ` + selectColumns + ` FROM catalog_entries
		WHERE tenant_id = ? ORDER BY namespace, name, version`
	rows, err := s.db.QueryContext(ctx, s.dialect.Rebind(q), tenantID)
	if err != nil {
		return nil, fmt.Errorf("catalog: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var entries []Entry
	for rows.Next() {
		var r row
		if err := rows.Scan(
			&r.namespace, &r.name, &r.version, &r.digest, &r.kind, &r.effectClass,
			&r.capabilities, &r.engineTypes, &r.inputSchema, &r.outputSchema,
		); err != nil {
			return nil, fmt.Errorf("catalog: list: %w", err)
		}
		m, err := r.decode()
		if err != nil {
			return nil, err
		}
		entries = append(entries, Entry{Manifest: m})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: list: %w", err)
	}
	return entries, nil
}

// ResolveStep resolves the plugin a step names and folds the step's own
// declaration into the entry it returns.
func (s *sqlStore) ResolveStep(
	ctx context.Context, tenantID string, step *dholev1.Step,
) (Entry, error) {
	if tenantID == "" {
		return Entry{}, runstore.ErrTenantRequired
	}
	entry, err := s.Resolve(ctx, tenantID, step.GetPluginRef())
	if err != nil {
		return Entry{}, err
	}

	declared := step.GetEffectClass()
	if declared == dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED {
		// ADR 0002: the plugin author is better placed to declare this than
		// the pipeline author is to retype it, so silence means inherit.
		return entry, nil
	}

	plugin := entry.EffectClass
	entry.EffectClass = declared
	// Only the weakening direction is reported; see widenWarning for why the
	// asymmetry is the point rather than an omission.
	if effectRank(declared) < effectRank(plugin) {
		entry.OverrideWarnings = append(entry.OverrideWarnings,
			widenWarning(step.GetId(), declared, plugin, entry.Ref()))
	}
	return entry, nil
}

// row is one stored catalog record, every column TEXT so the DDL is identical
// in both dialects. It is comparable, which is what makes "is this republish
// byte-identical?" a single equality.
type row struct {
	namespace    string
	name         string
	version      string
	digest       string
	kind         string
	effectClass  string
	capabilities string
	engineTypes  string
	inputSchema  string
	outputSchema string
}

// encode renders a manifest as the row that will be stored. Enum NAMES are
// stored rather than their numbers: the values are a persistence contract, and
// a name survives a renumbering that a wire tag would not.
func encode(m Manifest) (row, error) {
	caps := make([]string, 0, len(m.Capabilities))
	for _, c := range m.Capabilities {
		caps = append(caps, c.String())
	}
	capsJSON, err := json.Marshal(caps)
	if err != nil {
		return row{}, fmt.Errorf("catalog: encode capabilities: %w", err)
	}
	engines := m.EngineTypes
	if engines == nil {
		engines = []string{}
	}
	enginesJSON, err := json.Marshal(engines)
	if err != nil {
		return row{}, fmt.Errorf("catalog: encode engine types: %w", err)
	}
	return row{
		namespace:    m.Namespace,
		name:         m.Name,
		version:      m.Version,
		digest:       m.Digest.GetAlgo() + ":" + m.Digest.GetHex(),
		kind:         string(m.Kind),
		effectClass:  m.EffectClass.String(),
		capabilities: string(capsJSON),
		engineTypes:  string(enginesJSON),
		inputSchema:  string(m.InputSchema),
		outputSchema: string(m.OutputSchema),
	}, nil
}

// decode reverses encode. An unreadable row is an error rather than a
// best-effort manifest: a half-decoded declaration would be acted on as if the
// missing half had been declared.
func (r row) decode() (Manifest, error) {
	algo, hex, ok := strings.Cut(r.digest, ":")
	if !ok || algo == "" || hex == "" {
		return Manifest{}, fmt.Errorf("catalog: stored digest %q is not \"algo:hex\"", r.digest)
	}

	var capNames []string
	if err := json.Unmarshal([]byte(r.capabilities), &capNames); err != nil {
		return Manifest{}, fmt.Errorf("catalog: decode capabilities: %w", err)
	}
	var caps []dholev1.Capability
	for _, n := range capNames {
		v, ok := dholev1.Capability_value[n]
		if !ok {
			return Manifest{}, fmt.Errorf("catalog: unknown stored capability %q", n)
		}
		caps = append(caps, dholev1.Capability(v))
	}

	var engines []string
	if err := json.Unmarshal([]byte(r.engineTypes), &engines); err != nil {
		return Manifest{}, fmt.Errorf("catalog: decode engine types: %w", err)
	}
	if len(engines) == 0 {
		engines = nil
	}

	class, ok := dholev1.EffectClass_value[r.effectClass]
	if !ok {
		return Manifest{}, fmt.Errorf("catalog: unknown stored effect class %q", r.effectClass)
	}

	return Manifest{
		Namespace:    r.namespace,
		Name:         r.name,
		Version:      r.version,
		Digest:       &dholev1.Digest{Algo: algo, Hex: hex},
		Kind:         Kind(r.kind),
		EffectClass:  dholev1.EffectClass(class),
		Capabilities: caps,
		InputSchema:  []byte(r.inputSchema),
		OutputSchema: []byte(r.outputSchema),
		EngineTypes:  engines,
	}, nil
}
