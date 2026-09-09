package plugins

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// WHY THIS PACKAGE MIRRORS AT ALL.
//
// An upstream a tenant depends on at DISPATCH time is two dependencies wearing
// one coat. It is an availability dependency: someone else's registry being
// down stops steps here. And it is a supply-chain dependency: the bytes a step
// runs are whatever that registry serves at the moment the step starts, which
// is nobody's decision and nobody's review.
//
// Mirroring moves both to sync time. A sync is an operation a human runs and
// whose refusal a human reads; a dispatch is a machine event at 3am. So the
// rule this file exists to enforce is blunt: dispatch reads the local mirror
// and NOTHING else. There is no fallback to the upstream on a mirror miss —
// such a fallback would restore both dependencies precisely on the path that
// was supposed to be free of them, and it would do so silently, because a
// fallback that works looks exactly like a mirror that worked.

// MirrorPolicy says when an upstream's artifacts may be copied locally.
//
// Both policies mirror BEFORE dispatch; they differ only in whether save-time
// resolution is allowed to pull something a sync has not already seen.
type MirrorPolicy string

const (
	// MirrorAlways mirrors only what a sync brought in. Resolving a plugin no
	// sync has mirrored is refused rather than fetched: an operator asked for
	// this upstream's content to arrive through one reviewed door.
	MirrorAlways MirrorPolicy = "always"

	// MirrorOnDemand additionally lets save-time resolution pull an artifact
	// the mirror does not yet hold. Saving a definition is a human action with
	// a human reading its error, so an upstream reached there is a different
	// risk from one reached at dispatch.
	MirrorOnDemand MirrorPolicy = "on_demand"
)

// defaultTag is the version a reference without one names. It is spelled out
// rather than meaning "whatever is newest": resolution pins a digest once, and
// "newest" evaluated twice is two different artifacts.
const defaultTag = "latest"

// reservedNamespaces may not be claimed by a federated upstream.
//
// A tenant's own plugins live under `local`. If an upstream could claim that
// namespace, adding a federation would silently reroute references that already
// resolved to this tenant's own code — the one substitution nothing downstream
// could notice.
var reservedNamespaces = []string{"local"}

// namespacePattern is what a namespace may look like. It doubles as the
// repository-path rule of the mirror registry, so a namespace that registers is
// a namespace that can actually be mirrored rather than one that fails on first
// sync.
var namespacePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// maxSignaturePayload caps a signature layer read from an upstream. The
// upstream is not trusted to be well behaved, and an unbounded read from it is
// an unbounded allocation here.
const maxSignaturePayload = 1 << 20

var (
	// ErrNamespaceExists reports a namespace this tenant has already claimed
	// for a different upstream.
	ErrNamespaceExists = errors.New("plugins: upstream namespace already registered")

	// ErrReservedNamespace reports a namespace a federated upstream may not
	// claim because local plugins are addressed under it.
	ErrReservedNamespace = errors.New("plugins: reserved upstream namespace")

	// ErrUnknownUpstream reports a namespace this tenant has not registered.
	ErrUnknownUpstream = errors.New("plugins: no such upstream")

	// ErrMalformedUpstream reports a registration that could never work:
	// no namespace, no URL, an unusable scheme, no policy, or no allowed
	// signer. Each is refused at registration rather than at the first sync,
	// because an upstream that registers cleanly and then never syncs looks to
	// an operator like an upstream that is merely empty.
	ErrMalformedUpstream = errors.New("plugins: malformed upstream")

	// ErrNotMirrored reports a plugin that is not held locally. It is the
	// dispatch-time refusal, and it names the plugin AND the upstream it would
	// have come from: an operator reading it at 3am must not have to guess
	// which of a dozen federations is the one that is down.
	ErrNotMirrored = errors.New("plugins: plugin is not mirrored locally")
)

// Upstream is one federation this tenant pulls plugins from.
type Upstream struct {
	// Namespace is what this upstream's plugins are addressed under —
	// `a/docker-build` rather than `docker-build`. Two federations both
	// publish `docker-build`; without the namespace, the second registration
	// silently shadows the first.
	Namespace string
	// URL is "oci://<registry>[/<repository prefix>]".
	URL string
	// AllowedIdentities are the signers this tenant accepts FOR THIS UPSTREAM,
	// each "<issuer> <identity>". The list is per-upstream on purpose: a signer
	// vouched for inside one federation is not thereby vouched for inside
	// another, and pooling the lists would mean adding one upstream widened
	// the trust of every other.
	AllowedIdentities []string
	// MirrorPolicy is when this upstream's artifacts may be copied locally.
	MirrorPolicy MirrorPolicy
}

// Upstreams is the tenant's federation registry and the mirror built from it.
type Upstreams interface {
	// Add registers an upstream under its namespace. Re-adding an IDENTICAL
	// registration is a no-op; changing one is refused with
	// ErrNamespaceExists.
	Add(ctx context.Context, tenantID string, u Upstream) error

	// Sync mirrors the namespace's upstream content locally and returns how
	// many artifacts THIS CALL newly mirrored. An artifact the mirror already
	// holds at the same digest is not re-copied and not counted, so a second
	// sync of an unchanged upstream returns 0 — which is what makes the count
	// readable as "what changed" rather than "how much exists".
	//
	// An artifact whose signature this upstream's allowed signers do not cover
	// is not mirrored, is not counted, and is reported in the returned error.
	// One bad artifact does not abandon the rest of the namespace.
	Sync(ctx context.Context, tenantID, namespace string) (int, error)

	// Resolve pins "<namespace>/<name>[:<tag>]" to the artifact the LOCAL
	// mirror serves. Under MirrorOnDemand it may mirror an artifact it does not
	// yet hold; under MirrorAlways it refuses with ErrNotMirrored.
	Resolve(ctx context.Context, tenantID, ref string) (Artifact, error)

	// Upstream returns the registration this tenant holds under namespace, or
	// ErrUnknownUpstream. It is read at DISPATCH time as well as at sync
	// time: a policy decision about which upstream an artifact came from, and
	// about whose signature over it this tenant accepts, is answered from
	// this registration and from nowhere else (ADR 0012).
	Upstream(ctx context.Context, tenantID, namespace string) (Upstream, error)

	// Dispatch opens a mirrored plugin's bytes after re-verifying its
	// signature against its upstream's allowed signers. It NEVER contacts the
	// upstream.
	Dispatch(ctx context.Context, tenantID, ref string) (io.ReadCloser, error)

	// Close releases resources this store owns.
	Close() error
}

// sqlUpstreams keeps registrations and the mirror index in the same database as
// the run event log, exactly as the catalog and the signature store do.
type sqlUpstreams struct {
	db      *sql.DB
	dialect runstore.Dialect
	sigs    Signatures
	mirror  *registryMirror
	oci     *ociBackend
}

var _ Upstreams = (*sqlUpstreams)(nil)

// UpstreamOption configures the federation store.
type UpstreamOption func(*sqlUpstreams)

// WithInsecureUpstreams permits plain HTTP to the named registry hosts, for
// upstreams and for the mirror itself. It exists for the test registry and for
// deployments terminating TLS elsewhere; a host not named here is contacted
// over HTTPS.
func WithInsecureUpstreams(hosts ...string) UpstreamOption {
	return func(s *sqlUpstreams) {
		for _, h := range hosts {
			if h == "" {
				continue
			}
			s.mirror.insecureHosts[h] = struct{}{}
			s.oci.insecureHosts[h] = struct{}{}
		}
	}
}

// NewUpstreams returns the federation store over an already-open handle.
//
// mirrorPrefix is "<registry>[/<path>]" on a registry this deployment
// controls — where upstream artifacts are copied to and the only place
// dispatch reads from. sigs is Task 33's signature store: signatures found
// upstream are recorded and verified through it, never re-verified here.
func NewUpstreams(db *sql.DB, dialect runstore.Dialect, sigs Signatures, mirrorPrefix string, opts ...UpstreamOption) Upstreams {
	s := &sqlUpstreams{
		db:      db,
		dialect: dialect,
		sigs:    sigs,
		mirror:  &registryMirror{prefix: strings.TrimSuffix(mirrorPrefix, "/"), insecureHosts: map[string]struct{}{}},
		oci:     &ociBackend{insecureHosts: map[string]struct{}{}},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Close releases nothing: the handle belongs to whoever opened it. The method
// exists so a caller holding the interface can close it uniformly with the
// other stores in this package.
func (s *sqlUpstreams) Close() error { return nil }

// Add registers an upstream, or accepts an identical re-registration.
//
// A DIFFERENT registration under a claimed namespace is refused rather than
// applied. Repointing a namespace at another registry, or widening whose
// signatures it accepts, changes what code this tenant will execute — the kind
// of change that must be made deliberately, not by re-applying a config file
// somebody edited. Deleting the registration first is the deliberate act.
func (s *sqlUpstreams) Add(ctx context.Context, tenantID string, u Upstream) error {
	if err := requireTenant(tenantID); err != nil {
		return err
	}
	if err := u.validate(); err != nil {
		return err
	}

	existing, err := s.upstream(ctx, tenantID, u.Namespace)
	switch {
	case err == nil && existing.equal(u):
		return nil
	case err == nil:
		return fmt.Errorf("%w: %q already points at %q; remove it before repointing it at %q",
			ErrNamespaceExists, u.Namespace, existing.URL, u.URL)
	case !errors.Is(err, ErrUnknownUpstream):
		return err
	}

	allowed, err := json.Marshal(u.AllowedIdentities)
	if err != nil {
		return fmt.Errorf("plugins: encode allowed identities for %q: %w", u.Namespace, err)
	}
	const q = `INSERT INTO plugin_upstreams
		(tenant_id, namespace, url, allowed_identities, mirror_policy, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q),
		tenantID, u.Namespace, u.URL, string(allowed), string(u.MirrorPolicy),
		time.Now().UTC().Format(signatureTimeFormat),
	); err != nil {
		return fmt.Errorf("plugins: register upstream %q: %w", u.Namespace, err)
	}
	return nil
}

// Sync mirrors everything the namespace's upstream publishes.
func (s *sqlUpstreams) Sync(ctx context.Context, tenantID, namespace string) (int, error) {
	if err := requireTenant(tenantID); err != nil {
		return 0, err
	}
	u, err := s.upstream(ctx, tenantID, namespace)
	if err != nil {
		return 0, err
	}
	host, prefix, err := u.registry()
	if err != nil {
		return 0, err
	}

	names, err := s.mirror.repositories(ctx, host, prefix)
	if err != nil {
		return 0, fmt.Errorf("plugins: sync upstream %q (%s): %w", namespace, u.URL, err)
	}

	var (
		mirrored int
		problems []error
	)
	for _, pluginName := range names {
		repoRef := joinRepo(host, prefix, pluginName)
		tags, err := s.mirror.tags(ctx, repoRef)
		if err != nil {
			problems = append(problems, err)
			continue
		}
		for _, tag := range tags {
			// A per-artifact failure is collected, not fatal: one plugin whose
			// signer this upstream does not cover must not stop the rest of a
			// federation from being mirrored.
			fresh, err := s.mirrorOne(ctx, tenantID, u, pluginName, tag)
			if err != nil {
				problems = append(problems, err)
				continue
			}
			if fresh {
				mirrored++
			}
		}
	}
	return mirrored, errors.Join(problems...)
}

// mirrorOne brings one upstream artifact in, and reports whether this call is
// what newly mirrored it.
//
// The order matters: the signature is recorded and checked BEFORE the bytes are
// written to the mirror. A copy made first and vetted afterwards would leave
// unvouched-for content sitting in the registry that dispatch reads from, which
// is the one place it must never be.
func (s *sqlUpstreams) mirrorOne(ctx context.Context, tenantID string, u Upstream, pluginName, tag string) (bool, error) {
	coord := u.Namespace + "/" + pluginName + ":" + tag
	repoRef := joinRepo(u.mustHost(), u.mustPrefix(), pluginName)
	srcRef := repoRef + ":" + tag
	dstRef := s.mirror.ref(tenantID, u.Namespace, pluginName, tag)

	d, mediaType, err := s.mirror.head(ctx, srcRef)
	if err != nil {
		return false, fmt.Errorf("plugins: mirroring %q from upstream %s: %w", coord, u.URL, err)
	}

	// Already held at exactly this digest, and recorded: this sync mirrors
	// nothing new, which is what makes running it twice safe and its count
	// meaningful.
	if held, err := s.mirrored(ctx, tenantID, u.Namespace, pluginName, tag); err == nil && held.Digest.GetHex() == d.GetHex() {
		if ok, err := s.mirror.holds(ctx, dstRef, d); err == nil && ok {
			return false, nil
		}
	}

	sigs, err := s.mirror.signatures(ctx, repoRef, d)
	if err != nil {
		return false, fmt.Errorf("plugins: refusing to mirror %q from upstream %s: %w", coord, u.URL, err)
	}
	for _, sig := range sigs {
		if err := s.sigs.Record(ctx, tenantID, d, sig); err != nil {
			return false, fmt.Errorf("plugins: refusing to mirror %q from upstream %s: %w", coord, u.URL, err)
		}
	}
	// Per-upstream trust: THIS upstream's allowed list, never a pool of every
	// upstream's. The signature may be perfectly genuine and still not be one
	// this federation vouches for.
	if err := s.sigs.Verify(ctx, tenantID, d, u.AllowedIdentities); err != nil {
		return false, fmt.Errorf("plugins: refusing to mirror %q from upstream %s: %w", coord, u.URL, err)
	}

	local, err := s.mirror.copy(ctx, srcRef, dstRef)
	if err != nil {
		return false, fmt.Errorf("plugins: mirroring %q from upstream %s: %w", coord, u.URL, err)
	}
	if mediaType != "" {
		local.MediaType = mediaType
	}
	if err := s.record(ctx, tenantID, u.Namespace, pluginName, tag, local, srcRef); err != nil {
		return false, err
	}
	return true, nil
}

// Resolve pins a federated reference to the artifact the local mirror serves.
func (s *sqlUpstreams) Resolve(ctx context.Context, tenantID, ref string) (Artifact, error) {
	if err := requireTenant(tenantID); err != nil {
		return Artifact{}, err
	}
	namespace, pluginName, tag, err := splitPluginRef(ref)
	if err != nil {
		return Artifact{}, err
	}
	u, err := s.upstream(ctx, tenantID, namespace)
	if err != nil {
		return Artifact{}, err
	}

	held, err := s.mirrored(ctx, tenantID, namespace, pluginName, tag)
	if err == nil {
		return held, nil
	}
	if !errors.Is(err, ErrNotMirrored) {
		return Artifact{}, err
	}
	if u.MirrorPolicy != MirrorOnDemand {
		return Artifact{}, s.notMirrored(u, pluginName, tag)
	}

	// Save time, and only save time: a human is reading this error.
	if _, err := s.mirrorOne(ctx, tenantID, u, pluginName, tag); err != nil {
		return Artifact{}, err
	}
	return s.mirrored(ctx, tenantID, namespace, pluginName, tag)
}

// Dispatch opens a mirrored plugin's bytes.
//
// Two refusals guard it, and neither may be relaxed into a retry against the
// upstream. A plugin that is not mirrored is refused — no fallback fetch. And
// the signature is re-verified on every call against the upstream's allowed
// signers, because a signature withdrawn after a key compromise must stop
// dispatch now, not at the next sync.
func (s *sqlUpstreams) Dispatch(ctx context.Context, tenantID, ref string) (io.ReadCloser, error) {
	if err := requireTenant(tenantID); err != nil {
		return nil, err
	}
	namespace, pluginName, tag, err := splitPluginRef(ref)
	if err != nil {
		return nil, err
	}
	u, err := s.upstream(ctx, tenantID, namespace)
	if err != nil {
		return nil, err
	}

	a, err := s.mirrored(ctx, tenantID, namespace, pluginName, tag)
	if err != nil {
		if errors.Is(err, ErrNotMirrored) {
			return nil, s.notMirrored(u, pluginName, tag)
		}
		return nil, err
	}
	if err := s.sigs.Verify(ctx, tenantID, a.Digest, u.AllowedIdentities); err != nil {
		return nil, fmt.Errorf("plugins: refusing to dispatch %q from upstream %s: %w",
			u.Namespace+"/"+pluginName+":"+tag, u.URL, err)
	}
	return s.oci.fetch(ctx, a)
}

// notMirrored is the 3am diagnostic. It names the plugin and the upstream it
// would have come from, because the operator's next action — go and look at
// that registry, or run a sync against it — depends on knowing WHICH one.
func (s *sqlUpstreams) notMirrored(u Upstream, pluginName, tag string) error {
	return fmt.Errorf("%w: %s/%s:%s was never mirrored from upstream %q (%s, policy %s); run a sync against that upstream",
		ErrNotMirrored, u.Namespace, pluginName, tag, u.Namespace, u.URL, u.MirrorPolicy)
}

// record writes the mirror index row. The coordinate is the key, so re-syncing
// a moved tag updates in place rather than accumulating rows.
func (s *sqlUpstreams) record(ctx context.Context, tenantID, namespace, pluginName, tag string, a Artifact, srcRef string) error {
	text, err := digestText(a.Digest)
	if err != nil {
		return err
	}
	const q = `INSERT INTO plugin_mirrors
		(tenant_id, namespace, name, tag, digest, media_type, local_ref, source_ref, mirrored_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, namespace, name, tag)
		DO UPDATE SET digest = excluded.digest, media_type = excluded.media_type,
			local_ref = excluded.local_ref, source_ref = excluded.source_ref,
			mirrored_at = excluded.mirrored_at`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q),
		tenantID, namespace, pluginName, tag, text, a.MediaType, a.Ref, srcRef,
		time.Now().UTC().Format(signatureTimeFormat),
	); err != nil {
		return fmt.Errorf("plugins: record mirrored %s/%s:%s: %w", namespace, pluginName, tag, err)
	}
	return nil
}

// mirrored reads what the local mirror holds for a coordinate.
func (s *sqlUpstreams) mirrored(ctx context.Context, tenantID, namespace, pluginName, tag string) (Artifact, error) {
	const q = `SELECT digest, media_type, local_ref FROM plugin_mirrors
		WHERE tenant_id = ? AND namespace = ? AND name = ? AND tag = ?`
	var digest, mediaType, localRef string
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, namespace, pluginName, tag).
		Scan(&digest, &mediaType, &localRef)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Artifact{}, fmt.Errorf("%w: %s/%s:%s", ErrNotMirrored, namespace, pluginName, tag)
	case err != nil:
		return Artifact{}, fmt.Errorf("plugins: read mirror index for %s/%s:%s: %w", namespace, pluginName, tag, err)
	}

	_, hex, found := strings.Cut(digest, ":")
	if !found {
		return Artifact{}, fmt.Errorf("plugins: mirror index for %s/%s:%s holds %q, which is not an <algo>:<hex> digest",
			namespace, pluginName, tag, digest)
	}
	return Artifact{Ref: localRef, Digest: newDigest(hex), Scheme: SchemeOCI, MediaType: mediaType}, nil
}

// Upstream returns one registration. It is the exported half of upstream, and
// it refuses an empty tenant rather than reading across tenants: a
// registration is a trust decision and there is no unscoped one.
func (s *sqlUpstreams) Upstream(ctx context.Context, tenantID, namespace string) (Upstream, error) {
	if err := requireTenant(tenantID); err != nil {
		return Upstream{}, err
	}
	return s.upstream(ctx, tenantID, namespace)
}

// upstream reads one registration, scoped to its tenant.
func (s *sqlUpstreams) upstream(ctx context.Context, tenantID, namespace string) (Upstream, error) {
	const q = `SELECT url, allowed_identities, mirror_policy FROM plugin_upstreams
		WHERE tenant_id = ? AND namespace = ?`
	var url, allowed, policy string
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, namespace).Scan(&url, &allowed, &policy)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Upstream{}, fmt.Errorf("%w: this tenant has registered no upstream under %q", ErrUnknownUpstream, namespace)
	case err != nil:
		return Upstream{}, fmt.Errorf("plugins: read upstream %q: %w", namespace, err)
	}

	u := Upstream{Namespace: namespace, URL: url, MirrorPolicy: MirrorPolicy(policy)}
	if err := json.Unmarshal([]byte(allowed), &u.AllowedIdentities); err != nil {
		return Upstream{}, fmt.Errorf("plugins: read allowed identities of upstream %q: %w", namespace, err)
	}
	return u, nil
}

// validate refuses a registration that could never work, or that would work in
// a way nobody asked for.
func (u Upstream) validate() error {
	switch {
	case strings.TrimSpace(u.Namespace) == "":
		return fmt.Errorf("%w: an upstream must be registered under a namespace, or its plugins collide with every other upstream's", ErrMalformedUpstream)
	case !namespacePattern.MatchString(u.Namespace):
		return fmt.Errorf("%w: namespace %q must match %s", ErrMalformedUpstream, u.Namespace, namespacePattern)
	case slices.Contains(reservedNamespaces, u.Namespace):
		return fmt.Errorf("%w: %q addresses this tenant's own plugins; a federated upstream must not be able to shadow them",
			ErrReservedNamespace, u.Namespace)
	case strings.TrimSpace(u.URL) == "":
		return fmt.Errorf("%w: upstream %q names no registry", ErrMalformedUpstream, u.Namespace)
	}

	scheme, rest, err := splitScheme(u.URL)
	if err != nil {
		return fmt.Errorf("%w: upstream %q: %w", ErrMalformedUpstream, u.Namespace, err)
	}
	if scheme != SchemeOCI || strings.TrimSpace(rest) == "" {
		return fmt.Errorf(`%w: upstream %q must be "oci://<registry>[/<repository prefix>]", not %q`,
			ErrMalformedUpstream, u.Namespace, u.URL)
	}

	switch u.MirrorPolicy {
	case MirrorAlways, MirrorOnDemand:
	default:
		return fmt.Errorf("%w: upstream %q has mirror policy %q, expected %q or %q",
			ErrMalformedUpstream, u.Namespace, u.MirrorPolicy, MirrorAlways, MirrorOnDemand)
	}

	// An upstream with no allowed signer would mirror nothing, since
	// verification fails closed on an empty list. Refusing it here turns a
	// silent no-op into a message at the moment the mistake is made.
	if _, err := parseAllowed(u.AllowedIdentities); err != nil {
		return fmt.Errorf("%w: upstream %q: %w", ErrMalformedUpstream, u.Namespace, err)
	}
	return nil
}

// equal reports whether two registrations say the same thing. It is what makes
// a repeated `Add` idempotent without letting a CHANGED registration through.
func (u Upstream) equal(other Upstream) bool {
	return u.Namespace == other.Namespace &&
		u.URL == other.URL &&
		u.MirrorPolicy == other.MirrorPolicy &&
		slices.Equal(u.AllowedIdentities, other.AllowedIdentities)
}

// registry splits the URL into the registry host and the repository prefix.
func (u Upstream) registry() (host, prefix string, err error) {
	_, rest, err := splitScheme(u.URL)
	if err != nil {
		return "", "", fmt.Errorf("%w: upstream %q: %w", ErrMalformedUpstream, u.Namespace, err)
	}
	host, prefix, _ = strings.Cut(rest, "/")
	return host, strings.Trim(prefix, "/"), nil
}

// mustHost and mustPrefix are for the paths that already validated the URL.
func (u Upstream) mustHost() string   { host, _, _ := u.registry(); return host }
func (u Upstream) mustPrefix() string { _, prefix, _ := u.registry(); return prefix }

// joinRepo builds "host/prefix/name", tolerating an empty prefix.
func joinRepo(host, prefix, pluginName string) string {
	if prefix == "" {
		return host + "/" + pluginName
	}
	return host + "/" + prefix + "/" + pluginName
}

// splitPluginRef parses "<namespace>/<name>[:<tag>]".
//
// A reference with no namespace is refused rather than searched for across
// upstreams: with two federations publishing `docker-build`, a bare name names
// two different artifacts and picking either would be a guess.
func splitPluginRef(ref string) (namespace, pluginName, tag string, err error) {
	rest := ref
	tag = defaultTag
	if i := strings.LastIndex(rest, ":"); i >= 0 {
		tag, rest = rest[i+1:], rest[:i]
	}
	namespace, pluginName, found := strings.Cut(rest, "/")
	if !found || namespace == "" || pluginName == "" || tag == "" {
		return "", "", "", fmt.Errorf(`%w %q: expected "<namespace>/<name>" or "<namespace>/<name>:<tag>"`, ErrMalformedRef, ref)
	}
	return namespace, pluginName, tag, nil
}

// readLimited reads at most maxSignaturePayload bytes, and refuses more rather
// than truncating: a truncated signature payload would be checked as if it were
// the whole document.
func readLimited(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxSignaturePayload+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxSignaturePayload {
		return nil, fmt.Errorf("payload exceeds %d bytes", maxSignaturePayload)
	}
	return b, nil
}
