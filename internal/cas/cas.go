// Package cas is the content-addressed store: blobs named by the hash of their
// bytes, scoped per tenant.
//
// A step declares its inputs and outputs and never inherits ambient filesystem
// state (ADR 0001), so the identity of every artifact must be the bytes
// themselves. The same bytes are the same object, always — which is what makes
// the cache correct by construction rather than by bookkeeping.
package cas

import (
	"context"
	"errors"
	"io"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrNotFound reports that no blob with the given digest is stored for the
// tenant. Callers match it with errors.Is to tell a cache miss apart from a
// broken store.
var ErrNotFound = errors.New("cas: blob not found")

// ErrInvalidTenant reports a tenant id that cannot be used as a storage scope —
// one that is empty or would escape its own subtree. Such an id is refused, not
// sanitised: quietly rewriting it would address a different tenant's blobs.
var ErrInvalidTenant = errors.New("cas: invalid tenant id")

// Store holds immutable blobs keyed by the digest of their content. Every
// operation is tenant-scoped; there is no unscoped read or write.
//
// Digests travel as *dholev1.Digest rather than by value: the generated
// protobuf message embeds a MessageState, so copying one is a vet copylocks
// error and the gate refuses it.
type Store interface {
	// Put stores the bytes read from r and returns their digest. Putting bytes
	// that are already stored is cheap and leaves the stored object untouched.
	Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error)

	// Get opens the blob with digest d. It returns an error satisfying
	// errors.Is(err, ErrNotFound) when the tenant has no such blob.
	Get(ctx context.Context, tenantID string, d *dholev1.Digest) (io.ReadCloser, error)

	// Has reports whether the tenant has a blob with digest d.
	Has(ctx context.Context, tenantID string, d *dholev1.Digest) (bool, error)
}
