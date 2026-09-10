// Package blobstore holds bytes addressed by a key the control plane chose:
// step logs and large artifacts. It is the counterpart to internal/cas, which
// addresses bytes by their content hash; a log is appended to and named before
// its content exists, so it cannot be content-addressed.
package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

// ErrNotFound is returned by Read when no blob is stored under the key. Callers
// distinguish "the log has not been written yet" from "the store is broken",
// so this must be matchable with errors.Is rather than inferred from a message.
var ErrNotFound = errors.New("blobstore: blob not found")

// Store is the object storage the control plane and the GUI share.
//
// Tenant scoping is structural: tenantID is part of the stored location, not a
// filter applied afterwards, so one tenant's key cannot name another tenant's
// blob even if the key is guessed exactly.
type Store interface {
	// Write stores r under the key, replacing anything already there.
	Write(ctx context.Context, tenantID, key string, r io.Reader) error
	// Read opens the blob. It returns an error matching ErrNotFound when the
	// key holds nothing. The caller closes the reader.
	Read(ctx context.Context, tenantID, key string) (io.ReadCloser, error)
	// URL returns a link the GUI can follow directly for at most ttl, so a
	// large log streams from the store instead of through the control plane.
	URL(ctx context.Context, tenantID, key string, ttl time.Duration) (string, error)
	// Delete removes the blob under the key. It returns an error matching
	// ErrNotFound when the key held nothing, so a collector can tell work it
	// did from work already done.
	//
	// It is on the interface because the content-addressed store can be built
	// over this one (cas.NewOverBlobs), and a CAS that cannot delete is a CAS
	// whose reference counting decides what to collect and then cannot.
	Delete(ctx context.Context, tenantID, key string) error
}

// ErrInvalidKey reports a key or tenant the store refuses to interpret.
var ErrInvalidKey = errors.New("blobstore: invalid key")

// objectPath joins the tenant and key into the single tenant-scoped location
// both backends store under. It rejects rather than normalises: silently
// cleaning "../" would resolve a key into a different tenant's namespace, which
// is a cross-tenant read dressed up as a convenience.
func objectPath(tenantID, key string) (string, error) {
	if err := validSegment("tenant", tenantID); err != nil {
		return "", err
	}
	if err := validSegment("key", key); err != nil {
		return "", err
	}
	if strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("%w: key %q is absolute", ErrInvalidKey, key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == ".." {
			return "", fmt.Errorf("%w: key %q traverses upward", ErrInvalidKey, key)
		}
		if part == "" {
			return "", fmt.Errorf("%w: key %q has an empty path segment", ErrInvalidKey, key)
		}
	}
	return path.Join(tenantID, key), nil
}

func validSegment(what, value string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is empty", ErrInvalidKey, what)
	}
	if strings.ContainsRune(value, 0) || strings.Contains(value, `\`) {
		return fmt.Errorf("%w: %s %q contains a forbidden character", ErrInvalidKey, what, value)
	}
	return nil
}

func validTenant(tenantID string) error {
	if err := validSegment("tenant", tenantID); err != nil {
		return err
	}
	if tenantID == ".." || tenantID == "." || strings.Contains(tenantID, "/") {
		return fmt.Errorf("%w: tenant %q is not a single path segment", ErrInvalidKey, tenantID)
	}
	return nil
}
