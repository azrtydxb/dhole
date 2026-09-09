package defstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// ErrHeadMoved refuses a head move whose starting point is no longer the
// pipeline's head. It is the "compare" half of compare-and-set, and it is what
// makes base_revision a real check between two control planes rather than
// only within one.
var ErrHeadMoved = errors.New("the pipeline's editing head has moved")

// SQLHeads is the stored editing head: which revision of a pipeline the next
// edit must be based on.
//
// It is stored rather than remembered because revisions are content-addressed
// and carry no parent, so the head is a fact about the pipeline that exists
// nowhere else. A head held in one process's memory is an optimistic
// concurrency check for that process alone: a second plane finds no head it
// knows of, accepts an edit against the same base, and one of the two edits
// vanishes with nothing recording that it was ever made.
//
// Every move is a compare-and-set. The caller names the head it read, and a
// move from a head that is no longer current writes nothing and says so — a
// last-write-wins UPSERT here would store the head correctly and still lose
// the edit, which is the failure this type exists to prevent.
type SQLHeads struct {
	db      *sql.DB
	dialect runstore.Dialect
}

// NewHeads returns the stored head table over db, which speaks dialect and
// whose schema carries the migrations the run store applies.
func NewHeads(db *sql.DB, dialect runstore.Dialect) *SQLHeads {
	return &SQLHeads{db: db, dialect: dialect}
}

// Head returns the pipeline's head revision, and whether one is recorded. A
// pipeline nobody has edited through the API has no head, which is not an
// error: it is the state every pipeline starts in.
func (h *SQLHeads) Head(ctx context.Context, tenantID, pipelineID string) (string, bool, error) {
	if tenantID == "" {
		return "", false, ErrTenantRequired
	}
	const q = `SELECT revision_id FROM pipeline_heads
		WHERE tenant_id = ? AND pipeline_id = ?`
	var revisionID string
	switch err := h.db.QueryRowContext(ctx, h.dialect.Rebind(q), tenantID, pipelineID).
		Scan(&revisionID); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("read head: %w", err)
	}
	return revisionID, true, nil
}

// CompareAndSetHead moves the pipeline's head from `from` to `to`, and does
// nothing at all if the head is not `from`.
//
// An empty `from` means "there is no head yet", which is an INSERT that loses
// to whoever inserted first rather than one that overwrites them: a second
// plane seeding the same pipeline is exactly the race this method exists for.
func (h *SQLHeads) CompareAndSetHead(ctx context.Context, tenantID, pipelineID, from, to string) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	if to == "" {
		return errors.New("set head: a revision id is required")
	}
	now := time.Now().UTC().Format(timeFormat)

	var (
		result sql.Result
		err    error
	)
	if from == "" {
		const insert = `INSERT INTO pipeline_heads (tenant_id, pipeline_id, revision_id, updated_at)
			VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING`
		result, err = h.db.ExecContext(ctx, h.dialect.Rebind(insert),
			tenantID, pipelineID, to, now)
	} else {
		const update = `UPDATE pipeline_heads SET revision_id = ?, updated_at = ?
			WHERE tenant_id = ? AND pipeline_id = ? AND revision_id = ?`
		result, err = h.db.ExecContext(ctx, h.dialect.Rebind(update),
			to, now, tenantID, pipelineID, from)
	}
	if err != nil {
		return fmt.Errorf("set head: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set head: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: pipeline %q is no longer at %s", ErrHeadMoved, pipelineID, from)
	}
	return nil
}
