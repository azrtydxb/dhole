package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// algoSHA256 is the only algorithm this store writes. It is recorded in the
// digest and in the path so a future algorithm can be added beside it rather
// than replacing objects already stored.
const algoSHA256 = "sha256"

const (
	dirPerm  os.FileMode = 0o700
	filePerm os.FileMode = 0o600
)

// filesystem lays blobs out as <root>/<tenant>/<algo>/<hex[:2]>/<hex>. The
// two-character fanout keeps any one directory small enough for the filesystem
// to stay fast once a busy tenant has accumulated a lot of artifacts.
type filesystem struct {
	root string
}

// NewFilesystem returns a Store keeping blobs under root. The root is created
// lazily, on the first Put that has somewhere valid to write.
func NewFilesystem(root string) Store {
	return &filesystem{root: root}
}

// Put streams r to a temp file in the tenant's shard directory, hashing as it
// goes — build artifacts can be large, so the blob is never held in memory —
// then makes it visible with a single atomic rename. A Put that fails part way
// removes its temp file and leaves nothing visible.
func (f *filesystem) Put(ctx context.Context, tenantID string, r io.Reader) (*dholev1.Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tenantDir, err := f.tenantDir(tenantID)
	if err != nil {
		return nil, err
	}

	// The temp file lives in the tenant's own tree so the rename stays within
	// one filesystem and so a crash cannot strand bytes in another tenant's
	// space.
	stagingDir := filepath.Join(tenantDir, "tmp")
	if err := os.MkdirAll(stagingDir, dirPerm); err != nil {
		return nil, fmt.Errorf("cas: prepare staging dir: %w", err)
	}
	tmp, err := os.CreateTemp(stagingDir, "put-*")
	if err != nil {
		return nil, fmt.Errorf("cas: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	hasher := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, hasher), r); err != nil {
		return nil, fmt.Errorf("cas: write blob: %w", err)
	}
	// Durability before visibility: the rename must not be able to publish a
	// name whose contents are still only in the page cache.
	if err := tmp.Sync(); err != nil {
		return nil, fmt.Errorf("cas: sync blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("cas: close blob: %w", err)
	}
	if err := os.Chmod(tmpName, filePerm); err != nil {
		return nil, fmt.Errorf("cas: set blob permissions: %w", err)
	}

	digest := &dholev1.Digest{Algo: algoSHA256, Hex: hex.EncodeToString(hasher.Sum(nil))}
	blobPath := filepath.Join(tenantDir, algoSHA256, digest.GetHex()[:2], digest.GetHex())
	if err := os.MkdirAll(filepath.Dir(blobPath), dirPerm); err != nil {
		return nil, fmt.Errorf("cas: prepare blob dir: %w", err)
	}

	// Objects are immutable, so bytes already stored need no rewrite: dropping
	// the temp file is both the cheap path and the one that cannot corrupt an
	// object another reader is streaming.
	if _, err := os.Stat(blobPath); err == nil {
		return digest, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("cas: stat blob: %w", err)
	}

	if err := os.Rename(tmpName, blobPath); err != nil {
		return nil, fmt.Errorf("cas: publish blob: %w", err)
	}
	committed = true
	return digest, nil
}

func (f *filesystem) Get(ctx context.Context, tenantID string, d *dholev1.Digest) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := f.blobPath(tenantID, d)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path) //nolint:gosec // path is built from a validated tenant and a hex digest.
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s:%s", ErrNotFound, d.GetAlgo(), d.GetHex())
	}
	if err != nil {
		return nil, fmt.Errorf("cas: open blob: %w", err)
	}
	return file, nil
}

func (f *filesystem) Has(ctx context.Context, tenantID string, d *dholev1.Digest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := f.blobPath(tenantID, d)
	if err != nil {
		return false, err
	}
	switch _, err := os.Stat(path); {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("cas: stat blob: %w", err)
	}
}

// blobPath resolves the on-disk location of a digest, rejecting anything that
// is not a plain tenant segment and a well-formed digest.
func (f *filesystem) blobPath(tenantID string, d *dholev1.Digest) (string, error) {
	tenantDir, err := f.tenantDir(tenantID)
	if err != nil {
		return "", err
	}
	algo, hexDigest := d.GetAlgo(), d.GetHex()
	if algo != algoSHA256 {
		return "", fmt.Errorf("cas: unsupported digest algorithm %q", algo)
	}
	if len(hexDigest) != sha256.Size*2 || !isLowerHex(hexDigest) {
		return "", fmt.Errorf("cas: malformed %s digest %q", algo, hexDigest)
	}
	return filepath.Join(tenantDir, algo, hexDigest[:2], hexDigest), nil
}

// tenantDir maps a tenant id to its subtree. The id is a single path segment;
// anything that could climb out of the root — a separator, a dot segment — is
// refused rather than rewritten, because a rewritten id addresses somebody
// else's blobs.
func (f *filesystem) tenantDir(tenantID string) (string, error) {
	switch {
	case tenantID == "", tenantID == "." || tenantID == "..",
		strings.ContainsAny(tenantID, `/\`),
		strings.ContainsRune(tenantID, 0):
		return "", fmt.Errorf("%w: %q", ErrInvalidTenant, tenantID)
	}
	return filepath.Join(f.root, tenantID), nil
}

func isLowerHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// Delete removes the blob with digest d. It is the only removal path in the
// store and exists for the collector alone: a blob is immutable and shared by
// content, so nothing that merely finished with one may delete it.
//
// A blob that is already gone reports ErrNotFound rather than nil, so a
// collector can distinguish work already done from work it did.
func (f *filesystem) Delete(ctx context.Context, tenantID string, d *dholev1.Digest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := f.blobPath(tenantID, d)
	if err != nil {
		return err
	}
	switch err := os.Remove(path); {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %s:%s", ErrNotFound, d.GetAlgo(), d.GetHex())
	case err != nil:
		return fmt.Errorf("cas: delete blob: %w", err)
	}
	return nil
}

// Compile-time proof that the filesystem store is the collector's Deleter.
var _ Deleter = (*filesystem)(nil)
