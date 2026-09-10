package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
)

// Blobs is the object storage a content-addressed store can be built on.
// blobstore.Store satisfies it.
type Blobs interface {
	Write(ctx context.Context, tenantID, key string, r io.Reader) error
	Read(ctx context.Context, tenantID, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, tenantID, key string) error
}

// NewOverBlobs returns a Store that keeps its objects in b.
//
// It exists because the filesystem store was the only one, and a filesystem is
// exactly what a distributed deployment does not share: an engine wrote a
// step's output to its own pod's disk, and the control plane — and every other
// engine — looked for it on theirs and did not find it. Pointing both at one
// bucket makes the content-addressed store actually shared, which is what
// "the same bytes are the same object" needs in order to be true across
// processes rather than within one.
//
// Layering over blobstore rather than reaching for S3 directly means the
// backend is chosen once, in one place, and a deployment cannot end up with
// its logs in a bucket and its artifacts on a disk.
func NewOverBlobs(b Blobs) Store { return &overBlobs{blobs: b} }

type overBlobs struct{ blobs Blobs }

var (
	_ Store   = (*overBlobs)(nil)
	_ Deleter = (*overBlobs)(nil)
)

// Put buffers to a temporary file while hashing.
//
// The buffer is unavoidable and deliberately not in memory: the key an object
// is written under is the hash of its bytes, so the last byte must be read
// before the first can be stored, and a step's output is not required to fit
// in RAM.
func (o *overBlobs) Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validTenant(tenantID); err != nil {
		return nil, err
	}

	tmp, err := os.CreateTemp("", "dhole-cas-put-*")
	if err != nil {
		return nil, fmt.Errorf("cas: create staging file: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hasher), r); err != nil {
		return nil, fmt.Errorf("cas: stage blob: %w", err)
	}
	digest := &dholev1.Digest{Algo: algoSHA256, Hex: hex.EncodeToString(hasher.Sum(nil))}

	key, err := blobKey(digest)
	if err != nil {
		return nil, err
	}

	// Objects are immutable, so bytes already stored need no rewrite. The
	// check is a saved upload of a possibly large object, not a correctness
	// requirement: writing the same bytes to the same key again would be
	// harmless, just wasteful.
	if has, err := o.Has(ctx, tenantID, digest); err != nil {
		return nil, err
	} else if has {
		return digest, nil
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cas: rewind staged blob: %w", err)
	}
	if err := o.blobs.Write(ctx, tenantID, key, tmp); err != nil {
		return nil, fmt.Errorf("cas: store blob: %w", err)
	}
	return digest, nil
}

func (o *overBlobs) Get(ctx context.Context, tenantID string, d *dholev1.Digest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validTenant(tenantID); err != nil {
		return nil, err
	}
	key, err := blobKey(d)
	if err != nil {
		return nil, err
	}
	rc, err := o.blobs.Read(ctx, tenantID, key)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, fmt.Errorf("cas: get %s: %w", d.GetHex(), ErrNotFound)
		}
		return nil, fmt.Errorf("cas: get %s: %w", d.GetHex(), err)
	}
	return rc, nil
}

// Has opens the object and closes it again. The blob store has no cheaper
// existence check, and inventing one on the interface for this alone would put
// a method on every backend to save a round trip on a path that is already
// making one.
func (o *overBlobs) Has(ctx context.Context, tenantID string, d *dholev1.Digest) (bool, error) {
	rc, err := o.Get(ctx, tenantID, d)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, rc.Close()
}

// Delete implements Deleter, which is what lets the reference-counting
// collector reclaim a blob that nothing points at any more.
func (o *overBlobs) Delete(ctx context.Context, tenantID string, d *dholev1.Digest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validTenant(tenantID); err != nil {
		return err
	}
	key, err := blobKey(d)
	if err != nil {
		return err
	}
	if err := o.blobs.Delete(ctx, tenantID, key); err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return fmt.Errorf("cas: delete %s: %w", d.GetHex(), ErrNotFound)
		}
		return fmt.Errorf("cas: delete %s: %w", d.GetHex(), err)
	}
	return nil
}

// blobKey is the object's location within a tenant. It mirrors the filesystem
// store's layout — algo/first-two/full-hex — so the two backends can be read
// by the same eye, and so a bucket does not end up with a million objects
// directly under one prefix.
func blobKey(d *dholev1.Digest) (string, error) {
	algo, hexDigest := d.GetAlgo(), d.GetHex()
	if algo != algoSHA256 {
		return "", fmt.Errorf("cas: unsupported digest algorithm %q", algo)
	}
	if len(hexDigest) != sha256.Size*2 || !isLowerHex(hexDigest) {
		return "", fmt.Errorf("cas: malformed %s digest %q", algo, hexDigest)
	}
	return algo + "/" + hexDigest[:2] + "/" + hexDigest, nil
}

// validTenant refuses an id that could address another tenant's objects. The
// blob store scopes structurally and would refuse these too, but the error a
// caller gets should name the store it actually called.
func validTenant(tenantID string) error {
	switch {
	case tenantID == "", tenantID == ".", tenantID == "..",
		strings.ContainsAny(tenantID, `/\`),
		strings.ContainsRune(tenantID, 0):
		return fmt.Errorf("%w: %q", ErrInvalidTenant, tenantID)
	}
	return nil
}
