package containerd

import (
	"fmt"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/azrtydxb/dhole/internal/executor"
)

// resolveDigest turns an image reference into the digest of the manifest it
// currently points at, in `repository@sha256:...` form.
//
// It asks the REGISTRY rather than the local containerd image store, and that
// is the whole point: the store answers with whatever was pulled last, so a
// tag whose content has since been replaced would keep reporting the identity
// of an environment that no longer exists — which is precisely the stale cache
// hit ADR 0021 exists to prevent. A reference that already carries a digest is
// returned as it stands; nothing needs resolving and no network call is made.
//
// An image that cannot be resolved yields ErrNoStableIdentity and an empty
// string. Inventing one would be worse than having none: internal/cache
// declines to cache a step whose environment has no identity, and that is the
// safe direction to be wrong in.
func resolveDigest(image string, insecure map[string]struct{}) (string, error) {
	noIdentity := func(err error) (string, error) {
		return "", fmt.Errorf("containerd executor: %q has no resolvable digest: %w: %w",
			image, err, executor.ErrNoStableIdentity)
	}
	ref, err := name.ParseReference(image)
	if err != nil {
		return noIdentity(err)
	}
	if digest, ok := ref.(name.Digest); ok {
		return digest.Name(), nil
	}
	// Plain HTTP is re-parsed rather than assumed, and only for hosts the
	// operator named: a registry contacted over HTTP that was expected to be
	// HTTPS hands its manifests to whoever is on the path.
	if _, ok := insecure[ref.Context().RegistryStr()]; ok {
		if ref, err = name.ParseReference(image, name.Insecure); err != nil {
			return noIdentity(err)
		}
	}
	desc, err := remote.Head(ref)
	if err != nil {
		return noIdentity(err)
	}
	return ref.Context().Digest(desc.Digest.String()).Name(), nil
}
