package blobstore_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/azrtydxb/dhole/internal/blobstore"
)

// The warning about an unshared object store used to print the CALLER's
// fallback directory rather than the one the store opened, so an engine
// configured with DHOLE_BLOB_DIR warned about a path it was not using. A
// diagnostic that names the wrong thing is worse than none: it sends whoever
// reads it to the wrong directory.
func TestTheStoreReportsWhereItActuallyIs(t *testing.T) {
	chosen := t.TempDir()
	fallback := filepath.Join(t.TempDir(), "not-this-one")

	t.Setenv("DHOLE_OBJECT_STORE", "")
	t.Setenv("DHOLE_BLOB_DIR", chosen)

	_, shared, where, err := blobstore.FromEnvDescribed(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if where != chosen {
		t.Errorf("the store reports %q but opened %q", where, chosen)
	}
	if shared {
		t.Error("a filesystem store called itself shared")
	}
}

// With nothing configured it is the fallback, and it must say so rather than
// reporting an empty string.
func TestWithNothingConfiguredTheStoreNamesTheFallback(t *testing.T) {
	fallback := t.TempDir()
	t.Setenv("DHOLE_OBJECT_STORE", "")
	t.Setenv("DHOLE_BLOB_DIR", "")

	_, _, where, err := blobstore.FromEnvDescribed(fallback)
	if err != nil {
		t.Fatal(err)
	}
	if where != fallback {
		t.Errorf("the store reports %q, want the fallback %q", where, fallback)
	}
}

// An S3 store is shared, and names its bucket rather than a local path.
func TestAnS3StoreIsSharedAndNamesItsBucket(t *testing.T) {
	t.Setenv("DHOLE_OBJECT_STORE", "s3")
	t.Setenv("DHOLE_S3_BUCKET", "dhole-artifacts")
	t.Setenv("DHOLE_S3_REGION", "us-east-1")

	_, shared, where, err := blobstore.FromEnvDescribed(t.TempDir())
	if err != nil {
		t.Skipf("no ambient AWS configuration to build an S3 client here: %v", err)
	}
	if !shared {
		t.Error("an S3 store did not call itself shared, so nothing would warn correctly")
	}
	if !strings.Contains(where, "dhole-artifacts") {
		t.Errorf("the store reports %q and does not name its bucket", where)
	}
}
