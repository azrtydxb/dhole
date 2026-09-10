package blobstore

import (
	"fmt"
	"os"
	"strings"
)

// Kind names an object store backend in configuration.
const (
	KindFilesystem = "filesystem"
	KindS3         = "s3"
)

// Kinds is what FromEnv accepts, in the order an error lists them.
var Kinds = []string{KindFilesystem, KindS3}

// FromEnv builds the object store this process should use, and reports whether
// it is one other processes can also reach.
//
// Both binaries call this, deliberately, and with the same variable names: a
// deployment whose engines wrote artifacts to one store while the control
// plane read another is not a misconfiguration anyone would notice — every
// step succeeds, and the logs and outputs are simply not there. That happened
// on a real cluster, where the engine wrote a step's log to its own pod's
// emptyDir and the API looked for it on the control plane's.
//
// The filesystem default is kept because it is what a single binary and a
// laptop want, and because a default that needed a bucket would make the
// simple case impossible. The shared flag is how a caller that needs more can
// say so.
func FromEnv(fallbackDir string) (store Store, shared bool, err error) {
	store, shared, _, err = FromEnvDescribed(fallbackDir)
	return store, shared, err
}

// FromEnvDescribed is FromEnv, also returning where the store actually is.
//
// The description exists because the warning about an unshared store used to
// print the caller's FALLBACK directory rather than the directory the store
// opened — so an engine told to use DHOLE_BLOB_DIR warned about a path it was
// not using, which is a worse diagnostic than none.
func FromEnvDescribed(fallbackDir string) (store Store, shared bool, where string, err error) {
	kind := strings.TrimSpace(os.Getenv("DHOLE_OBJECT_STORE"))
	if kind == "" {
		kind = KindFilesystem
	}
	switch kind {
	case KindFilesystem:
		dir := fallbackDir
		if v := strings.TrimSpace(os.Getenv("DHOLE_BLOB_DIR")); v != "" {
			dir = v
		}
		if dir == "" {
			return nil, false, "", fmt.Errorf(
				"blobstore: the %s store needs a directory: set DHOLE_BLOB_DIR", KindFilesystem)
		}
		return NewFilesystem(dir), false, dir, nil
	case KindS3:
		bucket := strings.TrimSpace(os.Getenv("DHOLE_S3_BUCKET"))
		if bucket == "" {
			return nil, false, "", fmt.Errorf(
				"blobstore: the %s store needs a bucket: set DHOLE_S3_BUCKET", KindS3)
		}
		s3, err := NewS3(S3Config{
			Endpoint:        os.Getenv("DHOLE_S3_ENDPOINT"),
			Region:          os.Getenv("DHOLE_S3_REGION"),
			Bucket:          bucket,
			AccessKeyID:     os.Getenv("DHOLE_S3_ACCESS_KEY_ID"),
			SecretAccessKey: os.Getenv("DHOLE_S3_SECRET_ACCESS_KEY"),
			SessionToken:    os.Getenv("DHOLE_S3_SESSION_TOKEN"),
		})
		if err != nil {
			return nil, false, "", err
		}
		return s3, true, "s3://" + bucket, nil
	default:
		return nil, false, "", fmt.Errorf("DHOLE_OBJECT_STORE must be one of %s, got %q",
			strings.Join(Kinds, ", "), kind)
	}
}
