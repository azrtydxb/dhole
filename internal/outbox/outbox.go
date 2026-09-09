// Package outbox bridges the run store and the bus.
//
// The bus is NOT the source of truth (ADR 0005). If a run event were committed
// and its dispatch published as two separate acts, a crash between them would
// lose the step silently: nothing on the bus, nothing owed in the store, and
// no amount of retrying recovers what was never recorded. This package makes
// the event and the INTENT TO PUBLISH one transaction. Publishing is then a
// retryable side effect, and a bus outage can only make a message late.
//
// The guarantee is at-least-once, honestly. A crash between handing a message
// to NATS and marking its row sent republishes that message on the next drain,
// because the alternative — marking it sent first — turns the same crash into
// silent loss, and losing a step is worse than repeating one. Consumers must
// therefore deduplicate: a dispatch carries (run, step, attempt) and the fence
// token it was issued under, which is exactly enough for an engine or the
// control plane to recognise a delivery it has already handled.
//
// Ordering is NOT guaranteed, not even per tenant. Rows are claimed and
// published oldest-first by a single drainer, but two control planes drain
// concurrently by design: on Postgres SKIP LOCKED hands the second drainer
// later rows while the first still holds earlier ones, and a failed publish
// leaves its row to be retried after rows enqueued behind it. Anything that
// needs an order reads it from the run event log's sequence, which is the
// only thing in this system that carries one.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
)

const (
	// drainInterval is how often the background drainer looks for owed rows.
	// It is also the retry interval while the bus is down, which is why the
	// drainer must never loop on failure: one attempt per tick is a retry,
	// a loop is a hot spin that outlasts the outage.
	drainInterval = 200 * time.Millisecond

	// drainBatch bounds one claim. It matters most on SQLite, where the
	// claim is a database-wide write lock held for the whole batch: an
	// unbounded drain would stall every other writer for as long as the
	// backlog takes to publish.
	drainBatch = 100
)

// Outbox is the bridge. It is safe for concurrent use, and safe to run in
// several control planes at once: a drainer claims rows before publishing
// them, so two planes do not routinely publish the same row twice.
type Outbox struct {
	store   runstore.Store
	bus     bus.Bus
	onError func(error)
}

// Option tunes an Outbox.
type Option func(*Outbox)

// WithErrorHandler is called with every error the background drainer
// swallows. Without one those errors are invisible: the row stays owed and is
// retried, which is correct but says nothing about a bus that has been down
// for an hour.
func WithErrorHandler(fn func(error)) Option {
	return func(o *Outbox) { o.onError = fn }
}

// New builds an Outbox over a run store and a bus.
func New(store runstore.Store, b bus.Bus, opts ...Option) *Outbox {
	o := &Outbox{store: store, bus: b}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Enqueue records the intent to publish msg on subject, INSIDE the caller's
// transaction. It does not publish anything: the message goes out on a later
// Drain, and only if tx commits.
//
// tx must be the same transaction that wrote the run event this message
// reports. Passing a different one — or writing the event outside a
// transaction at all — reintroduces the two-act commit this package exists to
// prevent.
func (o *Outbox) Enqueue(ctx context.Context, tx runstore.Tx, tenantID, subject string, msg proto.Message) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	if subject == "" {
		return errors.New("outbox: subject required")
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox: marshal for %q: %w", subject, err)
	}

	switch tx.Dialect() {
	case runstore.DialectPostgres:
		const q = `INSERT INTO outbox (tenant_id, subject, payload, created_at, sent_at)
			VALUES ($1, $2, $3, $4, NULL)`
		return tx.Exec(ctx, q, tenantID, subject, payload, time.Now().UTC())
	case runstore.DialectSQLite:
		const q = `INSERT INTO outbox (tenant_id, subject, payload, created_at, sent_at)
			VALUES (?, ?, ?, ?, NULL)`
		return tx.Exec(ctx, q, tenantID, subject, payload, time.Now().UTC().Format(runstore.TimeFormat))
	default:
		return fmt.Errorf("outbox: unknown store dialect %q", tx.Dialect())
	}
}

// Drain claims a batch of owed rows, publishes each, and marks the published
// ones sent. It returns how many reached the bus.
//
// A publish that fails stops the batch and returns the error with the count
// that did get out. The rows that did not are left UNSENT — that is the whole
// point: a failed publish stays owed and is retried, so the bus being down
// delays a step rather than losing it.
func (o *Outbox) Drain(ctx context.Context) (int, error) {
	var (
		published  int
		publishErr error
	)
	err := o.store.WithTx(ctx, func(tx runstore.Tx) error {
		rows, err := o.claim(ctx, tx)
		if err != nil {
			return err
		}
		sent := make([]int64, 0, len(rows))
		for _, row := range rows {
			if err := o.bus.Publish(ctx, row.subject, rawMessage(row.payload)); err != nil {
				publishErr = fmt.Errorf("outbox: publish %q: %w", row.subject, err)
				break
			}
			sent = append(sent, row.id)
		}
		// Marked sent only AFTER the bus accepted them. The reverse order
		// would turn a crash here into a step that no one ever hears about:
		// nothing owed, nothing published, nothing to retry.
		//
		// If ctx expired during a publish this write fails too and the whole
		// claim rolls back, so rows that DID go out are published again on
		// the next drain. That is the at-least-once cost, taken deliberately:
		// the alternative is stamping rows sent under a dead context.
		if err := o.markSent(ctx, tx, sent); err != nil {
			return err
		}
		published = len(sent)
		// The transaction commits even when a publish failed, so the rows
		// that did go out are not republished; the ones that did not are
		// still owed.
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, publishErr
}

// Run drains on a ticker until ctx is done, returning ctx's error. It is the
// control plane's background bridge: an Enqueue is a promise, and this is what
// keeps it.
func (o *Outbox) Run(ctx context.Context) error {
	ticker := time.NewTicker(drainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			o.drainBacklog(ctx)
		}
	}
}

// drainBacklog empties whatever is owed, one claim at a time, and gives up
// until the next tick the moment anything fails. Retrying inside the tick
// would spin as hard as the machine allows for as long as the bus is down.
func (o *Outbox) drainBacklog(ctx context.Context) {
	for {
		n, err := o.Drain(ctx)
		if err != nil {
			if o.onError != nil && ctx.Err() == nil {
				o.onError(err)
			}
			return
		}
		// A short batch means the backlog is empty; a full one means there
		// is more waiting and no reason to sleep on it.
		if n < drainBatch || ctx.Err() != nil {
			return
		}
	}
}

// owed is one claimed row.
type owed struct {
	id      int64
	subject string
	payload []byte
}

// claim takes ownership of a batch of unsent rows for the life of tx.
//
// Two control planes drain concurrently by design, and a claim is what stops
// them publishing the same row as a matter of course. Postgres claims with
// FOR UPDATE SKIP LOCKED: the second drainer walks straight past the locked
// rows to later ones instead of blocking or duplicating. SQLite has no such
// clause, so the claim is the database-wide write lock its store takes when a
// transaction begins (`_txlock=immediate`): a second drainer cannot even read
// these rows until the first commits. Both are claims; only the granularity
// differs, and SQLite is the development and homelab store where it does not.
func (o *Outbox) claim(ctx context.Context, tx runstore.Tx) ([]owed, error) {
	var q string
	switch tx.Dialect() {
	case runstore.DialectPostgres:
		q = `SELECT id, subject, payload FROM outbox
			WHERE sent_at IS NULL ORDER BY id LIMIT $1
			FOR UPDATE SKIP LOCKED`
	case runstore.DialectSQLite:
		q = `SELECT id, subject, payload FROM outbox
			WHERE sent_at IS NULL ORDER BY id LIMIT ?`
	default:
		return nil, fmt.Errorf("outbox: unknown store dialect %q", tx.Dialect())
	}

	rows, err := tx.Query(ctx, q, drainBatch)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	// The whole batch is read and the rows closed before anything else runs
	// on this transaction: both drivers hold one connection per transaction
	// and cannot interleave a second statement with an open result set.
	defer func() { _ = rows.Close() }()

	var claimed []owed
	for rows.Next() {
		var row owed
		if err := rows.Scan(&row.id, &row.subject, &row.payload); err != nil {
			return nil, fmt.Errorf("outbox: claim: %w", err)
		}
		claimed = append(claimed, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	return claimed, nil
}

// markSent stamps the rows the bus accepted. Anything not in ids stays owed.
func (o *Outbox) markSent(ctx context.Context, tx runstore.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, id := range ids {
		var err error
		switch tx.Dialect() {
		case runstore.DialectPostgres:
			err = tx.Exec(ctx, `UPDATE outbox SET sent_at = $1 WHERE id = $2`, now, id)
		case runstore.DialectSQLite:
			err = tx.Exec(ctx, `UPDATE outbox SET sent_at = ? WHERE id = ?`,
				now.Format(runstore.TimeFormat), id)
		default:
			err = fmt.Errorf("unknown store dialect %q", tx.Dialect())
		}
		if err != nil {
			return fmt.Errorf("outbox: mark sent: %w", err)
		}
	}
	return nil
}

// rawMessage republishes stored bytes byte-for-byte without knowing their
// type. The outbox stores a marshalled message and must hand the bus the same
// bytes back; the bus takes a proto.Message and marshals it, so the payload is
// carried as the unknown fields of an empty message. Unknown fields are
// emitted verbatim and Empty has no known fields of its own, so marshalling
// this reproduces the stored bytes exactly.
//
// The alternative — a type registry mapping subjects to message types — buys
// nothing here: the outbox never reads a payload, only forwards it.
func rawMessage(payload []byte) proto.Message {
	msg := &emptypb.Empty{}
	msg.ProtoReflect().SetUnknown(protoreflect.RawFields(payload))
	return msg
}
