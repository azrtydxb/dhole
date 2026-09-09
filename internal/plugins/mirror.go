package plugins

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// The cosign convention this file reads.
//
// A signature for an artifact with manifest digest sha256:<hex> is published in
// the SAME repository under the tag `sha256-<hex>.sig`. Each layer of that
// artifact is one signature: the layer's bytes are the simple-signing payload,
// and its annotations carry the base64 signature and the signer's PEM
// certificate. Reassembling those three into the bundle cosign writes with
// `--bundle` is all this file does — the checking itself belongs to
// signature.go and cosign.go, and is deliberately not repeated here.
const (
	cosignSignatureAnnotation   = "dev.cosignproject.cosign/signature"
	cosignCertificateAnnotation = "dev.sigstore.cosign/certificate"
	cosignSignatureTagSuffix    = ".sig"
)

// registryMirror copies upstream artifacts into a repository prefix on a
// registry this deployment controls.
//
// A registry rather than the CAS, and a byte-identical copy rather than a
// re-packing, because the manifest digest has to survive the copy. That digest
// is what the upstream's signature was made over and what a saved revision
// pinned; a mirror that changed it would leave every signature unverifiable and
// every lockfile pointing at an artifact that no longer exists under that name.
type registryMirror struct {
	// prefix is "<registry>[/<path>]" — where mirrored repositories are rooted.
	prefix string
	// insecureHosts are the registry hosts reachable over plain HTTP, for the
	// test registry and for deployments terminating TLS elsewhere.
	insecureHosts map[string]struct{}
}

// ref is the local reference an artifact is mirrored to.
//
// The tenant is folded in as a hash rather than as its literal id: a tenant id
// is free text and a repository path is not, and sanitising the id into the
// path would let two different tenants sanitise to the SAME path — which is
// exactly the cross-tenant read this system has no other way to make.
func (m *registryMirror) ref(tenantID, namespace, pluginName, tag string) string {
	sum := sha256.Sum256([]byte(tenantID))
	return fmt.Sprintf("%s/t-%s/%s/%s:%s", m.prefix, hex.EncodeToString(sum[:])[:16], namespace, pluginName, tag)
}

// copy pulls the upstream artifact and writes it, unchanged, to dst. It returns
// the artifact as the mirror now serves it: a reference naming the LOCAL
// registry, carrying the digest the upstream published.
func (m *registryMirror) copy(ctx context.Context, srcRef, dstRef string) (Artifact, error) {
	src, err := m.parseTag(srcRef)
	if err != nil {
		return Artifact{}, err
	}
	dst, err := m.parseTag(dstRef)
	if err != nil {
		return Artifact{}, err
	}

	desc, err := remote.Get(src, remote.WithContext(ctx))
	if err != nil {
		return Artifact{}, fmt.Errorf("plugins: read upstream artifact %q: %w", srcRef, err)
	}
	img, err := desc.Image()
	if err != nil {
		return Artifact{}, fmt.Errorf("plugins: read upstream artifact %q: %w", srcRef, err)
	}
	if err := remote.Write(dst, img, remote.WithContext(ctx)); err != nil {
		return Artifact{}, fmt.Errorf("plugins: write mirrored artifact %q: %w", dstRef, err)
	}

	// The digest is re-read from what was written rather than carried over from
	// the source: if the copy were ever not byte-identical, the mirror must say
	// what it actually holds instead of repeating the upstream's claim.
	written, err := img.Digest()
	if err != nil {
		return Artifact{}, fmt.Errorf("plugins: digest mirrored artifact %q: %w", dstRef, err)
	}
	if written.Hex != desc.Digest.Hex {
		return Artifact{}, fmt.Errorf("plugins: mirrored copy of %q has digest %s, upstream published %s",
			srcRef, written.String(), desc.Digest.String())
	}

	return Artifact{
		Ref:       "oci://" + dstRef,
		Digest:    newDigest(written.Hex),
		Scheme:    SchemeOCI,
		MediaType: string(desc.MediaType),
	}, nil
}

// head reads what the upstream currently publishes at ref, without pulling any
// content: the digest is enough to decide whether the mirror already holds it
// and to look up the signature made over it.
func (m *registryMirror) head(ctx context.Context, ref string) (*dholev1.Digest, string, error) {
	parsed, err := m.parseTag(ref)
	if err != nil {
		return nil, "", err
	}
	desc, err := remote.Head(parsed, remote.WithContext(ctx))
	if err != nil {
		return nil, "", fmt.Errorf("plugins: read upstream artifact %q: %w", ref, err)
	}
	return newDigest(desc.Digest.Hex), string(desc.MediaType), nil
}

// holds reports whether the mirror already serves exactly this artifact at this
// coordinate. It is what makes a second sync of an unchanged upstream cheap and
// what keeps the count Sync returns meaningful.
func (m *registryMirror) holds(ctx context.Context, dstRef string, d *dholev1.Digest) (bool, error) {
	ref, err := m.parseTag(dstRef)
	if err != nil {
		return false, err
	}
	desc, err := remote.Head(ref, remote.WithContext(ctx))
	if err != nil {
		// A miss and an unreachable mirror are both "cannot serve it from
		// here", and both lead to the same action: mirror it again.
		return false, nil //nolint:nilerr // a mirror that cannot answer has not answered "yes"
	}
	return desc.Digest.Hex == d.GetHex(), nil
}

// repositories lists the repositories an upstream publishes under its prefix,
// as plugin names. Cosign signature artifacts are not plugins and are skipped
// here rather than being discovered and refused later.
func (m *registryMirror) repositories(ctx context.Context, registryHost, prefix string) ([]string, error) {
	reg, err := name.NewRegistry(registryHost, m.nameOptions(registryHost)...)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrMalformedRef, registryHost, err)
	}
	repos, err := remote.Catalog(ctx, reg, remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("plugins: list repositories on upstream %q: %w", registryHost, err)
	}

	var out []string
	for _, repo := range repos {
		switch {
		case prefix == "":
		case repo == prefix:
			continue
		case strings.HasPrefix(repo, prefix+"/"):
			repo = strings.TrimPrefix(repo, prefix+"/")
		default:
			continue
		}
		if repo == "" {
			continue
		}
		out = append(out, repo)
	}
	return out, nil
}

// tags lists the tags of one upstream repository, minus cosign's signature
// tags, which name signatures rather than versions of the plugin.
func (m *registryMirror) tags(ctx context.Context, repoRef string) ([]string, error) {
	repo, err := name.NewRepository(repoRef, m.nameOptions(repoRef)...)
	if err != nil {
		return nil, fmt.Errorf("%w %q: %w", ErrMalformedRef, repoRef, err)
	}
	all, err := remote.List(repo, remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("plugins: list tags of %q: %w", repoRef, err)
	}
	out := make([]string, 0, len(all))
	for _, tag := range all {
		if strings.HasPrefix(tag, algoSHA256+"-") && strings.HasSuffix(tag, cosignSignatureTagSuffix) {
			continue
		}
		out = append(out, tag)
	}
	return out, nil
}

// signatures reads the signature bundles the upstream publishes for d.
//
// Nothing is checked here. Each bundle is handed to the signature store, which
// verifies it against d and records it — verification lives in one place, and a
// second implementation of it in the sync path would be a second thing to get
// wrong.
func (m *registryMirror) signatures(ctx context.Context, repoRef string, d *dholev1.Digest) ([]Signature, error) {
	sigRef := fmt.Sprintf("%s:%s-%s%s", repoRef, algoSHA256, d.GetHex(), cosignSignatureTagSuffix)
	ref, err := m.parseTag(sigRef)
	if err != nil {
		return nil, err
	}

	desc, err := remote.Get(ref, remote.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("plugins: read signatures for %q: %w", repoRef, err)
	}
	img, err := desc.Image()
	if err != nil {
		return nil, fmt.Errorf("plugins: read signatures for %q: %w", repoRef, err)
	}
	manifest, err := img.Manifest()
	if err != nil {
		return nil, fmt.Errorf("plugins: read signatures for %q: %w", repoRef, err)
	}

	var out []Signature
	for _, layer := range manifest.Layers {
		bundle, err := m.bundle(img, layer)
		if err != nil {
			return nil, err
		}
		sig, err := SignatureFromCosignBundle(d, bundle)
		if err != nil {
			return nil, err
		}
		out = append(out, sig)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("plugins: signature artifact for %q carries no signatures", repoRef)
	}
	return out, nil
}

// bundle reassembles the cosign bundle from one signature layer: the payload is
// the layer's bytes, the signature and certificate its annotations.
func (m *registryMirror) bundle(img v1.Image, layer v1.Descriptor) ([]byte, error) {
	signature := layer.Annotations[cosignSignatureAnnotation]
	certPEM := layer.Annotations[cosignCertificateAnnotation]
	if signature == "" || certPEM == "" {
		return nil, fmt.Errorf("%w: signature layer carries no %s or %s annotation",
			ErrMalformedSignature, cosignSignatureAnnotation, cosignCertificateAnnotation)
	}

	l, err := img.LayerByDigest(layer.Digest)
	if err != nil {
		return nil, fmt.Errorf("%w: signature layer %s is not in its own artifact: %w",
			ErrMalformedSignature, layer.Digest.String(), err)
	}
	rc, err := l.Uncompressed()
	if err != nil {
		return nil, fmt.Errorf("%w: signature layer %s cannot be read: %w",
			ErrMalformedSignature, layer.Digest.String(), err)
	}
	defer func() { _ = rc.Close() }()

	payload, err := readLimited(rc)
	if err != nil {
		return nil, fmt.Errorf("%w: signature layer %s cannot be read: %w",
			ErrMalformedSignature, layer.Digest.String(), err)
	}

	return json.Marshal(map[string]string{
		"base64Signature": signature,
		"cert":            base64.StdEncoding.EncodeToString([]byte(certPEM)),
		"payload":         base64.StdEncoding.EncodeToString(payload),
	})
}

// parseTag turns "host/repo:tag" into a reference, applying the insecure
// allowance only to hosts the operator named.
func (m *registryMirror) parseTag(ref string) (name.Tag, error) {
	tag, err := name.NewTag(ref, m.nameOptions(ref)...)
	if err != nil {
		return name.Tag{}, fmt.Errorf("%w %q: %w", ErrMalformedRef, ref, err)
	}
	return tag, nil
}

// nameOptions marks a reference insecure when its registry host was configured
// as such.
func (m *registryMirror) nameOptions(ref string) []name.Option {
	host, _, _ := strings.Cut(ref, "/")
	if _, ok := m.insecureHosts[host]; ok {
		return []name.Option{name.Insecure}
	}
	return nil
}
