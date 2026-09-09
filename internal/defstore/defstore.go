// Package defstore is the canonical store of pipeline definitions.
//
// The server database holds the definition; git, where a deployment has one,
// is a one-way mirror of it (ADR 0008). Every definition carries a revision
// identity — the hash of its canonical encoding — and an approval state,
// draft -> reviewed -> active, with the approver recorded, because DB-primary
// storage forfeits the review a forge would have given for free.
//
// A run pins an exact revision, and that revision is immutable for the whole
// life of the run however long it lasts: an edit saved mid-flight, an approval
// that moves to a newer revision, an approval revoked outright — none of them
// change what a running run reads back. Without that, what actually ran is
// unknowable after the fact.
//
// Every operation is tenant-scoped. There is no unscoped query, even while
// only one tenant exists: an empty tenant is a bug in the caller, never a
// wildcard.
package defstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// The errors this package returns.
var (
	// ErrTenantRequired is returned by every method given an empty tenant.
	ErrTenantRequired = errors.New("tenant scope required")

	// ErrNotFound is returned for a revision this tenant does not have —
	// including one that exists for another tenant, which must be
	// indistinguishable from one that does not exist at all.
	ErrNotFound = errors.New("revision not found")

	// ErrNoActiveRevision means the pipeline has never had a revision
	// approved. It is not an error in the store: a pipeline whose every
	// revision is still a draft has nothing that may run.
	ErrNoActiveRevision = errors.New("no active revision")

	// ErrSelfApproval refuses an approval by the author of the revision.
	// Approval here stands in for forge-native review, and a review the
	// author gave themselves records something that did not happen.
	ErrSelfApproval = errors.New("a revision may not be approved by its author")
)

// timeFormat is how a timestamp is stored. SQLite has no time type; RFC3339
// with nanoseconds in UTC sorts lexicographically in the same order it sorts
// chronologically.
const timeFormat = time.RFC3339Nano

// Store holds pipeline definitions and their revisions.
//
// Every method takes an explicit tenant and rejects an empty one with
// ErrTenantRequired.
type Store interface {
	// Save records p as a revision authored by author, and returns it. The
	// revision starts as a draft: saving never changes what is running.
	// Saving content identical to an existing revision returns that
	// revision unchanged, approval state and all, rather than creating a
	// second one or demoting the first.
	Save(ctx context.Context, tenantID string, p *dholev1.Pipeline, author string) (Revision, error)

	// Get returns the definition of exactly the revision asked for. It is
	// what a run calls with the revision it pinned, so it never follows the
	// pipeline's current or active revision instead.
	Get(ctx context.Context, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error)

	// Active returns the pipeline's approved revision, or ErrNoActiveRevision
	// if none has been approved.
	Active(ctx context.Context, tenantID, pipelineID string) (Revision, error)

	// Approve promotes a revision to active and records the approver. The
	// revision it supersedes steps back to reviewed. Approving the revision
	// that is already active is a no-op that keeps the original approver, so
	// a retried request is harmless.
	Approve(ctx context.Context, tenantID, revisionID, approver string) error

	// Revision returns one revision's metadata without its definition.
	Revision(ctx context.Context, tenantID, revisionID string) (Revision, error)
}

// SQLStore is the SQL implementation of Store, over the same database handle
// as the run store: definitions and run history are one transactional unit,
// so a run can never be recorded against a revision the store does not hold.
type SQLStore struct {
	db      *sql.DB
	dialect runstore.Dialect
	// resolver pins plugin references at save time. See lockfile.go for why
	// that moment, and no later one, is the only one that can be trusted.
	resolver plugins.Resolver
}

// Compile-time proof that the SQL implementation satisfies the interface later
// tasks consume.
var _ Store = (*SQLStore)(nil)

// New returns a definition store over a SQLite handle. It is the convenience
// form of NewWithDialect and nothing more: a Postgres deployment calls
// NewWithDialect, because pgx rejects the `?` placeholders these statements
// are written with.
func New(db *sql.DB, opts ...Option) *SQLStore {
	return NewWithDialect(db, runstore.DialectSQLite, opts...)
}

// NewWithDialect returns a definition store over db, which speaks dialect and
// whose schema is expected to carry the migrations the run store applies.
func NewWithDialect(db *sql.DB, dialect runstore.Dialect, opts ...Option) *SQLStore {
	s := &SQLStore{db: db, dialect: dialect}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Save records a revision of p, as a draft.
func (s *SQLStore) Save(ctx context.Context, tenantID string, p *dholev1.Pipeline, author string) (Revision, error) {
	if tenantID == "" {
		return Revision{}, ErrTenantRequired
	}
	if p == nil {
		return Revision{}, errors.New("save definition: nil pipeline")
	}

	// Resolution comes first, and outside the transaction: every plugin
	// reference is pinned before a single row is written, so a registry that
	// cannot answer for one of them leaves no half-pinned revision behind. A
	// revision that looks pinned and is not would never be noticed.
	//
	// A store built without a resolver pins nothing and stores an empty
	// lockfile. That is a real configuration — a deployment whose definitions
	// carry no plugin references, and the tests of everything here that is not
	// about plugins — but it is also the one way to get an unpinned revision,
	// so any deployment that serves plugin references MUST pass WithResolver.
	// An empty lockfile on a definition that names plugins is not a pinned
	// revision; it is a revision nobody pinned.
	lockfile := map[string]string{}
	if s.resolver != nil {
		var err error
		if lockfile, err = ResolveLockfile(ctx, tenantID, p, s.resolver); err != nil {
			return Revision{}, fmt.Errorf("save definition: %w", err)
		}
	}

	hash := ContentHash(p)
	rev := Revision{
		ID:          "rev_" + hash,
		PipelineID:  p.GetId(),
		ContentHash: hash,
		State:       StateDraft,
		Lockfile:    lockfile,
		Author:      author,
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Revision{}, fmt.Errorf("save definition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(timeFormat)
	const upsertPipeline = `INSERT INTO pipelines (tenant_id, id, created_at)
		VALUES (?, ?, ?) ON CONFLICT DO NOTHING`
	if _, err := tx.ExecContext(ctx, s.dialect.Rebind(upsertPipeline), tenantID, rev.PipelineID, now); err != nil {
		return Revision{}, fmt.Errorf("save definition: %w", err)
	}

	// The definition stored is the canonical encoding the hash was taken
	// over, so what a run reads back cannot drift from what was hashed.
	definition := canonicalBytes(p)
	// encoding/json sorts map keys, so the stored bytes do not depend on the
	// iteration order of the map they came from.
	encodedLockfile, err := json.Marshal(rev.Lockfile)
	if err != nil {
		return Revision{}, fmt.Errorf("save definition: encode lockfile: %w", err)
	}
	const insertRevision = `INSERT INTO revisions
		(tenant_id, id, pipeline_id, content_hash, state, lockfile, definition,
		 author, approver, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?)
		ON CONFLICT DO NOTHING`
	if _, err := tx.ExecContext(ctx, s.dialect.Rebind(insertRevision),
		tenantID, rev.ID, rev.PipelineID, rev.ContentHash, string(rev.State),
		string(encodedLockfile), definition, author, now); err != nil {
		return Revision{}, fmt.Errorf("save definition: %w", err)
	}

	// Read the row back rather than returning what was offered: an identical
	// save is the revision that already exists, with the state and the author
	// it already had.
	stored, err := revisionRow(ctx, tx, s.dialect, tenantID, rev.ID)
	if err != nil {
		return Revision{}, fmt.Errorf("save definition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Revision{}, fmt.Errorf("save definition: %w", err)
	}
	return stored, nil
}

// Get returns the definition of the revision asked for, and of no other.
func (s *SQLStore) Get(ctx context.Context, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	const q = `SELECT definition FROM revisions
		WHERE tenant_id = ? AND pipeline_id = ? AND id = ?`
	var definition []byte
	switch err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, pipelineID, revisionID).Scan(&definition); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, fmt.Errorf("get definition: %w", err)
	}

	// The stored bytes are the canonical encoding, which carries the domain
	// prefix the hash was taken over.
	p := &dholev1.Pipeline{}
	if err := proto.Unmarshal(definition[len(hashDomain):], p); err != nil {
		return nil, fmt.Errorf("get definition: decode revision %s: %w", revisionID, err)
	}
	return p, nil
}

// Active returns the pipeline's approved revision.
func (s *SQLStore) Active(ctx context.Context, tenantID, pipelineID string) (Revision, error) {
	if tenantID == "" {
		return Revision{}, ErrTenantRequired
	}
	const q = `SELECT id, pipeline_id, content_hash, state, lockfile, author, approver
		FROM revisions
		WHERE tenant_id = ? AND pipeline_id = ? AND state = ?`
	row := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, pipelineID, string(StateActive))
	rev, err := scanRevision(row)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Revision{}, ErrNoActiveRevision
	case err != nil:
		return Revision{}, fmt.Errorf("active revision: %w", err)
	}
	return rev, nil
}

// Revision returns one revision's metadata.
func (s *SQLStore) Revision(ctx context.Context, tenantID, revisionID string) (Revision, error) {
	if tenantID == "" {
		return Revision{}, ErrTenantRequired
	}
	rev, err := revisionRow(ctx, s.db, s.dialect, tenantID, revisionID)
	if err != nil {
		return Revision{}, err
	}
	return rev, nil
}

// Approve promotes a revision and demotes the one it supersedes.
func (s *SQLStore) Approve(ctx context.Context, tenantID, revisionID, approver string) error {
	if tenantID == "" {
		return ErrTenantRequired
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("approve revision: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rev, err := revisionRow(ctx, tx, s.dialect, tenantID, revisionID)
	if err != nil {
		return err
	}
	if rev.State == StateActive {
		// Already what runs: a retried approval must not rewrite the person
		// who actually reviewed it.
		return nil
	}
	if approver == rev.Author {
		return fmt.Errorf("%w: %s", ErrSelfApproval, approver)
	}

	// At most one active revision per pipeline. The superseded one keeps its
	// approver and steps back to reviewed rather than being deleted, because
	// history is what makes an approval auditable.
	const demote = `UPDATE revisions SET state = ?
		WHERE tenant_id = ? AND pipeline_id = ? AND state = ?`
	if _, err := tx.ExecContext(ctx, s.dialect.Rebind(demote),
		string(StateReviewed), tenantID, rev.PipelineID, string(StateActive)); err != nil {
		return fmt.Errorf("approve revision: %w", err)
	}
	const promote = `UPDATE revisions SET state = ?, approver = ?
		WHERE tenant_id = ? AND id = ?`
	if _, err := tx.ExecContext(ctx, s.dialect.Rebind(promote),
		string(StateActive), approver, tenantID, revisionID); err != nil {
		return fmt.Errorf("approve revision: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("approve revision: %w", err)
	}
	return nil
}

// querier is the part of *sql.DB and *sql.Tx this package reads through, so a
// lookup reads the same way inside and outside a transaction.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// revisionRow reads one revision, scoped to its tenant.
func revisionRow(
	ctx context.Context, q querier, dialect runstore.Dialect, tenantID, revisionID string,
) (Revision, error) {
	const query = `SELECT id, pipeline_id, content_hash, state, lockfile, author, approver
		FROM revisions WHERE tenant_id = ? AND id = ?`
	rev, err := scanRevision(q.QueryRowContext(ctx, dialect.Rebind(query), tenantID, revisionID))
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Revision{}, ErrNotFound
	case err != nil:
		return Revision{}, fmt.Errorf("read revision: %w", err)
	}
	return rev, nil
}

// scanRevision reads one revisions row into a Revision.
func scanRevision(row *sql.Row) (Revision, error) {
	var (
		rev      Revision
		state    string
		lockfile string
	)
	if err := row.Scan(&rev.ID, &rev.PipelineID, &rev.ContentHash, &state,
		&lockfile, &rev.Author, &rev.Approver); err != nil {
		return Revision{}, err
	}
	rev.State = State(state)
	// A caller reading a lockfile back gets a map to range over, never nil.
	rev.Lockfile = map[string]string{}
	if err := json.Unmarshal([]byte(lockfile), &rev.Lockfile); err != nil {
		return Revision{}, fmt.Errorf("decode lockfile of revision %s: %w", rev.ID, err)
	}
	return rev, nil
}
