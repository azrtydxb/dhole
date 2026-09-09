package plugins

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
)

// algoSHA256 is the only digest algorithm this package writes. Recording it
// beside the hex leaves room for a second algorithm to be added rather than to
// replace what is already pinned.
const algoSHA256 = "sha256"

// ociBackend resolves and fetches artifacts from OCI registries.
//
// The division of labour is the whole point: resolve may read a tag, fetch may
// not. A registry tag is a mutable pointer, so the only reference that can be
// dispatched twice with the same result is one addressed by digest.
type ociBackend struct {
	insecureHosts map[string]struct{}
}

// resolve reads the reference — tag included, this being save time — and
// returns the manifest digest it points at right now.
func (o *ociBackend) resolve(ctx context.Context, ref, rest string) (Artifact, error) {
	parsed, err := o.parse(ref, rest)
	if err != nil {
		return Artifact{}, err
	}

	desc, err := remote.Get(parsed, remote.WithContext(ctx))
	if err != nil {
		return Artifact{}, o.wrap(ref, "resolve", err)
	}

	return Artifact{
		Ref:       ref,
		Digest:    newDigest(desc.Digest.Hex),
		Scheme:    SchemeOCI,
		MediaType: string(desc.MediaType),
	}, nil
}

// fetch opens the artifact's payload, addressed by the digest resolution
// recorded. Any tag on a.Ref is discarded before the registry is contacted:
// this is the line that makes a saved revision immune to a moved tag.
func (o *ociBackend) fetch(ctx context.Context, a Artifact) (io.ReadCloser, error) {
	_, rest, err := splitScheme(a.Ref)
	if err != nil {
		return nil, err
	}
	repo, err := o.repository(a.Ref, rest)
	if err != nil {
		return nil, err
	}

	pinned := repo.Digest(algoSHA256 + ":" + a.Digest.GetHex())
	img, err := remote.Image(pinned, remote.WithContext(ctx))
	if err != nil {
		return nil, o.wrap(a.Ref, "fetch", err)
	}

	layers, err := img.Layers()
	if err != nil {
		return nil, o.wrap(a.Ref, "fetch", err)
	}
	if len(layers) == 0 {
		return nil, fmt.Errorf("plugins: oci artifact %s@%s:%s carries no layers", repo.Name(), algoSHA256, a.Digest.GetHex())
	}

	// The plugin payload is the topmost layer: a plugin image is built by
	// appending its artifact onto whatever base it needed, so the last layer is
	// the one the plugin author added.
	rc, err := layers[len(layers)-1].Uncompressed()
	if err != nil {
		return nil, o.wrap(a.Ref, "fetch", err)
	}
	return rc, nil
}

// parse turns the scheme-stripped remainder into a registry reference,
// applying the insecure allowance only to hosts the operator named.
func (o *ociBackend) parse(ref, rest string) (name.Reference, error) {
	parsed, err := name.ParseReference(rest, o.nameOptions(rest)...)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %s: %w", ErrMalformedRef, ref, refForm, err)
	}
	return parsed, nil
}

// repository extracts just the repository from a reference, dropping whatever
// tag or digest it carried. fetch uses it to rebuild a digest-pinned reference.
func (o *ociBackend) repository(ref, rest string) (name.Repository, error) {
	parsed, err := o.parse(ref, rest)
	if err != nil {
		return name.Repository{}, err
	}
	return parsed.Context(), nil
}

// nameOptions marks the reference insecure when its registry host was
// configured as such. The host has to be read off the reference before it is
// parsed, so this does the cheap prefix split itself.
func (o *ociBackend) nameOptions(rest string) []name.Option {
	host, _, _ := strings.Cut(rest, "/")
	if _, ok := o.insecureHosts[host]; ok {
		return []name.Option{name.Insecure}
	}
	return nil
}

// wrap distinguishes a missing artifact from a registry we could not talk to.
// An operator whose plugin was never mirrored and an operator whose registry is
// down have different problems, and an error that blurs them sends both of them
// looking in the wrong place.
func (o *ociBackend) wrap(ref, op string, err error) error {
	var terr *transport.Error
	if errors.As(err, &terr) {
		for _, d := range terr.Errors {
			switch d.Code {
			case transport.ManifestUnknownErrorCode, transport.BlobUnknownErrorCode, transport.NameUnknownErrorCode:
				return fmt.Errorf("%w: oci %s %q: %w", ErrNotFound, op, ref, err)
			}
		}
		if terr.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: oci %s %q: %w", ErrNotFound, op, ref, err)
		}
	}
	return fmt.Errorf("plugins: oci %s %q: %w", op, ref, err)
}
