package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// Filesystem stores blobs as files under a root directory. It is the
// development and homelab backend: no object store to run, and the bytes are
// inspectable with cat when a run misbehaves.
type Filesystem struct {
	root string
}

// NewFilesystem returns a Store rooted at root. The directory is created on
// first write rather than here, so constructing a store is not a side effect.
func NewFilesystem(root string) *Filesystem {
	return &Filesystem{root: root}
}

var _ Store = (*Filesystem)(nil)

func (f *Filesystem) path(tenantID, key string) (string, error) {
	rel, err := objectPath(tenantID, key)
	if err != nil {
		return "", err
	}
	if err := validTenant(tenantID); err != nil {
		return "", err
	}
	return filepath.Join(f.root, filepath.FromSlash(rel)), nil
}

// Write stores r at the tenant-scoped path. It writes to a temporary file and
// renames, so a reader never observes a half-written log, and every failure
// names the key: a log lost quietly is worse than a run that fails loudly.
func (f *Filesystem) Write(_ context.Context, tenantID, key string, r io.Reader) (err error) {
	full, err := f.path(tenantID, key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(full)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, err)
	}

	tmp, err := os.CreateTemp(dir, ".dhole-blob-*")
	if err != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, cerr := io.Copy(tmp, r); cerr != nil {
		_ = tmp.Close()
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, cerr)
	}
	if cerr := tmp.Close(); cerr != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, cerr)
	}
	if cerr := os.Chmod(tmpName, 0o600); cerr != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, cerr)
	}
	if cerr := os.Rename(tmpName, full); cerr != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, cerr)
	}
	return nil
}

func (f *Filesystem) Read(_ context.Context, tenantID, key string) (io.ReadCloser, error) {
	full, err := f.path(tenantID, key)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(full) //nolint:gosec // the path is built from a validated tenant and key.
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("blobstore: read %q for tenant %q: %w", key, tenantID, ErrNotFound)
		}
		return nil, fmt.Errorf("blobstore: read %q for tenant %q: %w", key, tenantID, err)
	}
	return file, nil
}

// URL returns a file:// link. There is nothing to pre-sign on a local
// filesystem, so ttl is accepted and ignored: the caller's contract is "a link
// good for at most ttl", and a file URL is only reachable by a process that
// already has the disk.
func (f *Filesystem) URL(_ context.Context, tenantID, key string, _ time.Duration) (string, error) {
	full, err := f.path(tenantID, key)
	if err != nil {
		return "", err
	}
	abs, err := filepath.Abs(full)
	if err != nil {
		return "", fmt.Errorf("blobstore: url %q for tenant %q: %w", key, tenantID, err)
	}
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}).String(), nil
}

// Delete removes the blob under the key.
//
// A key that was already gone reports ErrNotFound rather than nil: the caller
// is a collector, and "I removed it" and "somebody else had already removed
// it" are different answers to the question it is asking.
func (f *Filesystem) Delete(_ context.Context, tenantID, key string) error {
	full, err := f.path(tenantID, key)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("blobstore: delete %q for tenant %q: %w", key, tenantID, ErrNotFound)
		}
		return fmt.Errorf("blobstore: delete %q for tenant %q: %w", key, tenantID, err)
	}
	return nil
}
