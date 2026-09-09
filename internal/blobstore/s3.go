package blobstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config describes the bucket the tuned deployment stores blobs in. Endpoint
// is empty for AWS itself and set for MinIO or any other S3-compatible server;
// leaving the credentials empty falls back to the ambient AWS chain (instance
// role, environment, shared config), which is how a cloud deployment avoids
// carrying static keys.
type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// S3 stores blobs in an S3-compatible bucket.
type S3 struct {
	client   *s3.Client
	presign  *s3.PresignClient
	bucket   string
	endpoint string
}

var _ Store = (*S3)(nil)

// NewS3 builds a Store over the configured bucket. Addressing is always
// path-style (bucket in the path, not the hostname): virtual-host addressing
// requires wildcard DNS for the endpoint, which MinIO on a homelab IP does not
// have.
func NewS3(cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("blobstore: S3Config.Bucket is required")
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("blobstore: load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = true
	})
	return &S3{
		client:   client,
		presign:  s3.NewPresignClient(client),
		bucket:   cfg.Bucket,
		endpoint: cfg.Endpoint,
	}, nil
}

func (s *S3) Write(ctx context.Context, tenantID, key string, r io.Reader) error {
	object, err := objectPath(tenantID, key)
	if err != nil {
		return err
	}
	// PutObject needs a seekable body to sign; buffering here keeps the
	// interface an io.Reader for callers that stream from a pipe.
	body, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, err)
	}
	if _, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object),
		Body:   bytes.NewReader(body),
	}); err != nil {
		return fmt.Errorf("blobstore: write %q for tenant %q: %w", key, tenantID, err)
	}
	return nil
}

func (s *S3) Read(ctx context.Context, tenantID, key string) (io.ReadCloser, error) {
	object, err := objectPath(tenantID, key)
	if err != nil {
		return nil, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		var missing *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &missing) || errors.As(err, &notFound) {
			return nil, fmt.Errorf("blobstore: read %q for tenant %q: %w", key, tenantID, ErrNotFound)
		}
		return nil, fmt.Errorf("blobstore: read %q for tenant %q: %w", key, tenantID, err)
	}
	return out.Body, nil
}

// URL pre-signs a GET valid for ttl, so the GUI streams a multi-megabyte log
// straight from the store rather than through the control plane.
func (s *S3) URL(ctx context.Context, tenantID, key string, ttl time.Duration) (string, error) {
	object, err := objectPath(tenantID, key)
	if err != nil {
		return "", err
	}
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(object),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("blobstore: url %q for tenant %q: %w", key, tenantID, err)
	}
	return req.URL, nil
}
