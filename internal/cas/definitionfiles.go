package cas

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// definitionFileTimeFormat is the shape every timestamp in this database is
// written in.
const definitionFileTimeFormat = time.RFC3339Nano

// DefinitionFiles is the content-addressed store that the files a DEFINITION
// carries are uploaded through: the store itself, plus the one row that lets
// the collector ever see the bytes again.
//
// It exists because those two acts were separate and only one of them
// happened. A step output is Put and then Referenced, so blob_refs holds a row
// and the collector enumerates it. A definition file was only ever Put:
// nothing wrote a row, nothing enumerated the digest, and the bytes were
// retained for ever against the quota they are charged to (ADR 0023, whose
// Consequences promise the collector reclaims them).
//
// It is a wrapper rather than a change to the store because exactly one
// upload path carries definition files. Recording every blob any producer Puts
// would make a step output that has not been Referenced YET a candidate, and
// the window between the two is real.
//
// It satisfies the narrow interface internal/api uses for uploads (Put and
// Has) and, where it wraps a Store, the whole of Store.
type DefinitionFiles struct {
	store   Store
	db      *sql.DB
	dialect runstore.Dialect
	now     func() time.Time
}

// NewDefinitionFiles wraps store so an upload is recorded as something the
// collector may one day examine. db is the same handle blob_refs lives in, and
// dialect must match it — pgx rejects the `?` placeholders this statement is
// written with, so a Postgres handle left on the zero value would record
// nothing.
//
// Pass the QUOTA-GUARDED store, not the raw one: the bytes of a definition
// file count against max_cas_bytes like every other stored byte.
func NewDefinitionFiles(store Store, db *sql.DB, dialect runstore.Dialect) *DefinitionFiles {
	return &DefinitionFiles{store: store, db: db, dialect: dialect, now: time.Now}
}

// Put stores the bytes and records them as a definition file of this tenant.
//
// The row is written AFTER the bytes are stored, because the row's meaning is
// "these bytes exist and nobody has to have named them yet": a row written
// first and a Put that then failed would be a candidate for bytes that were
// never stored, which the delete loop tolerates but which no sweep should have
// to.
//
// A failed insert fails the upload even though the bytes are stored. Returning
// the digest anyway is what the defect looked like — bytes in the store that
// no sweep will ever consider — and the caller's remedy is to upload again,
// which in a content-addressed store restores exactly the same object and
// retries the row.
func (d *DefinitionFiles) Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error) {
	digest, err := d.store.Put(ctx, tenantID, r)
	if err != nil {
		return nil, err
	}
	if err := d.record(ctx, tenantID, digest); err != nil {
		return nil, err
	}
	return digest, nil
}

// Get opens the blob, so a DefinitionFiles can stand in for the Store it
// wraps.
func (d *DefinitionFiles) Get(ctx context.Context, tenantID string, dig *dholev1.Digest) (io.ReadCloser, error) {
	return d.store.Get(ctx, tenantID, dig)
}

// Has reports whether the tenant has these bytes.
func (d *DefinitionFiles) Has(ctx context.Context, tenantID string, dig *dholev1.Digest) (bool, error) {
	return d.store.Has(ctx, tenantID, dig)
}

// record marks the digest as a definition file uploaded now.
//
// The timestamp is REFRESHED when the row already exists. A candidate younger
// than the collector's retain window is never collected, because an upload is
// a separate call from the edit that declares the file and in between the
// bytes are carried by no revision at all. Keeping the first upload's time
// would leave bytes somebody re-uploaded a second ago looking arbitrarily old,
// and a sweep landing in that window deletes the file the next save is about
// to name.
func (d *DefinitionFiles) record(ctx context.Context, tenantID string, dig *dholev1.Digest) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	text, err := digestText(dig)
	if err != nil {
		return err
	}
	at := d.now().UTC().Format(definitionFileTimeFormat)
	const q = `INSERT INTO definition_blobs (tenant_id, digest, uploaded_at)
		VALUES (?, ?, ?)
		ON CONFLICT (tenant_id, digest) DO UPDATE SET uploaded_at = ?`
	if _, err := d.db.ExecContext(ctx, d.dialect.Rebind(q), tenantID, text, at, at); err != nil {
		return fmt.Errorf("cas: record definition file: %w", err)
	}
	return nil
}
