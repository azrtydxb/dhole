package blobstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/blobstore"
)

// blobStoreContract is the behaviour every Store implementation owes its
// callers, run against each of them. Step logs are written by the engine and
// read back by the GUI through whichever backend the deployment configured; if
// the filesystem store and the S3 store disagree about round-tripping bytes,
// about tenant scoping, or about what a missing key looks like, the reader
// breaks on exactly one of the two deployments — the one nobody tested.
func blobStoreContract(t *testing.T, s blobstore.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("delete removes the blob and says whether it did", func(t *testing.T) {
		key := "runs/r9/steps/s1/log"
		require.NoError(t, s.Write(ctx, "tenant-a", key, bytes.NewReader([]byte("collect me"))))

		require.NoError(t, s.Delete(ctx, "tenant-a", key))

		_, err := s.Read(ctx, "tenant-a", key)
		require.ErrorIs(t, err, blobstore.ErrNotFound)

		// The collector needs to tell work it did from work already done, and
		// S3's DeleteObject reports success for a key that was never there —
		// so this is the assertion that keeps the two backends honest with
		// each other rather than one of them merely being convenient.
		require.ErrorIs(t, s.Delete(ctx, "tenant-a", key), blobstore.ErrNotFound)
	})

	t.Run("one tenant cannot delete another's blob", func(t *testing.T) {
		key := "runs/r10/steps/s1/log"
		require.NoError(t, s.Write(ctx, "tenant-a", key, bytes.NewReader([]byte("mine"))))

		require.Error(t, s.Delete(ctx, "tenant-b", key),
			"a key guessed exactly still must not cross tenants")

		rc, err := s.Read(ctx, "tenant-a", key)
		require.NoError(t, err, "the blob was deleted by another tenant")
		require.NoError(t, rc.Close())
	})

	t.Run("write then read round-trips the bytes", func(t *testing.T) {
		payload := []byte("step 3 log line\nsecond line\n\x00binary\xff")
		key := "runs/r1/steps/s3/log"
		require.NoError(t, s.Write(ctx, "tenant-a", key, bytes.NewReader(payload)))

		rc, err := s.Read(ctx, "tenant-a", key)
		require.NoError(t, err)
		t.Cleanup(func() { _ = rc.Close() })

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		require.Equal(t, payload, got)
	})

	t.Run("reading an absent key returns ErrNotFound", func(t *testing.T) {
		_, err := s.Read(ctx, "tenant-a", "runs/r1/steps/never-written/log")
		require.ErrorIs(t, err, blobstore.ErrNotFound)
	})

	t.Run("the same key in two tenants holds different bytes", func(t *testing.T) {
		// Tenant scoping is structural: the tenant is part of the stored
		// location, so one tenant cannot read another's blob even by guessing
		// the exact key. A store that merely filtered would leak here.
		key := "runs/shared-id/steps/1/log"
		require.NoError(t, s.Write(ctx, "tenant-one", key, strings.NewReader("one")))
		require.NoError(t, s.Write(ctx, "tenant-two", key, strings.NewReader("two")))

		for tenant, want := range map[string]string{"tenant-one": "one", "tenant-two": "two"} {
			rc, err := s.Read(ctx, tenant, key)
			require.NoError(t, err)
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			require.NoError(t, rc.Close())
			require.Equal(t, want, string(got))
		}

		_, err := s.Read(ctx, "tenant-three", key)
		require.ErrorIs(t, err, blobstore.ErrNotFound)
	})

	t.Run("a traversing or absolute key is rejected, not normalised", func(t *testing.T) {
		// Silently cleaning "../" would let one tenant's key land in another
		// tenant's namespace, so these are refused outright.
		for _, key := range []string{"../escape", "runs/../../escape", "/absolute", "", ".."} {
			require.Error(t, s.Write(ctx, "tenant-a", key, strings.NewReader("x")), "Write(%q)", key)
			_, err := s.Read(ctx, "tenant-a", key)
			require.Error(t, err, "Read(%q)", key)
			_, err = s.URL(ctx, "tenant-a", key, time.Minute)
			require.Error(t, err, "URL(%q)", key)
		}
	})

	t.Run("URL returns a link carrying the tenant-scoped key", func(t *testing.T) {
		key := "runs/r9/steps/1/log"
		require.NoError(t, s.Write(ctx, "tenant-a", key, strings.NewReader("hello")))

		link, err := s.URL(ctx, "tenant-a", key, 5*time.Minute)
		require.NoError(t, err)
		parsed, err := url.Parse(link)
		require.NoError(t, err)
		require.NotEmpty(t, parsed.Scheme)
		require.Contains(t, parsed.Path, "tenant-a")
		require.Contains(t, parsed.Path, key)
	})
}

func TestFilesystemBlobStoreContract(t *testing.T) {
	blobStoreContract(t, blobstore.NewFilesystem(t.TempDir()))
}

// TestS3BlobStoreContract runs the identical contract against MinIO, which is
// the S3 API the homelab deployment actually talks to. Without a real server
// the path-style addressing and the NoSuchKey-to-ErrNotFound mapping are
// untested guesses.
func TestS3BlobStoreContract(t *testing.T) {
	endpoint := os.Getenv("DHOLE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("DHOLE_TEST_S3_ENDPOINT not set")
	}

	cfg := blobstore.S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1",
		Bucket:          fmt.Sprintf("dhole-test-%d", time.Now().UnixNano()),
		AccessKeyID:     envOr("DHOLE_TEST_S3_ACCESS_KEY", "dholetest"),
		SecretAccessKey: envOr("DHOLE_TEST_S3_SECRET_KEY", "dholetestsecret"),
	}
	createTestBucket(t, cfg)

	store, err := blobstore.NewS3(cfg)
	require.NoError(t, err)
	blobStoreContract(t, store)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func createTestBucket(t *testing.T, cfg blobstore.S3Config) {
	t.Helper()
	ctx := context.Background()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)})
	var exists *s3types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &exists) {
		require.NoError(t, err)
	}
}

// TestWriteFailureIsReportedNotSwallowed pins the loud failure. A step log that
// cannot be stored must surface as an error naming the key: a run that reports
// success while its log silently went nowhere is a worse outcome than a run
// that fails, because the evidence needed to debug it is gone.
func TestWriteFailureIsReportedNotSwallowed(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o500)) // read and traverse, no create
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) })

	store := blobstore.NewFilesystem(root)
	err := store.Write(context.Background(), "tenant-a", "runs/r1/steps/1/log", strings.NewReader("log"))

	require.Error(t, err)
	require.Contains(t, err.Error(), "runs/r1/steps/1/log")
}
