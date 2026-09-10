package tenancy

import (
	"context"
	"fmt"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Reclaimer credits a tenant back for storage the collector reclaimed. It
// satisfies cas.Collector.
//
// It exists because CAS usage only ever grew: the collector deleted blobs and
// wrote nothing that offset them, so a tenant who reclaimed a terabyte was
// still charged for it and was eventually refused every write against a store
// that was nearly empty.
type Reclaimer struct {
	store *Store
	now   func() time.Time
}

// NewReclaimer returns a Reclaimer writing into store.
func NewReclaimer(store *Store) *Reclaimer {
	return &Reclaimer{store: store, now: time.Now}
}

// Collected credits back the bytes that were charged for this digest.
//
// The quantity comes from the LEDGER rather than from the caller, because the
// ledger is what will be netted against: crediting a number the collector
// happened to know would let the two disagree, and a credit that does not
// match its charge is worse than none — it leaves a balance nobody can explain.
//
// A digest that was never charged for is not an error. Blobs predate the
// metering, and a sweep that tidies one up has nothing to credit.
//
// It is idempotent by the ledger's own key: the credit is recorded under the
// same digest as the charge, so a second sweep over the same blob is refused
// as a duplicate rather than crediting it twice.
func (r *Reclaimer) Collected(ctx context.Context, tenantID string, d *dholev1.Digest) error {
	if r == nil || r.store == nil {
		return nil
	}
	hex := d.GetHex()
	if hex == "" {
		return fmt.Errorf("tenancy: crediting a collected blob: the digest has no hex")
	}

	charged, err := r.store.ChargedCASBytes(ctx, tenantID, hex)
	if err != nil {
		return err
	}
	if charged == 0 {
		return nil
	}

	_, err = r.store.RecordUsage(ctx, tenantID, Usage{
		Kind:     KindCASBytesReleased,
		StepID:   hex,
		Quantity: charged,
		At:       r.now().UTC(),
	})
	return err
}
