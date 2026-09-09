package plugins_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// digestText renders a digest the way a revision's lockfile records it. The
// scheduler hands the provenance source exactly this text.
func digestText(d *dholev1.Digest) string {
	return d.GetAlgo() + ":" + d.GetHex()
}

// newProvenance builds the provenance source over a real signature store and a
// real upstream registry sharing one database — the wiring a control plane
// has. Nothing here is faked: a fake signature store that answered "signed" to
// everything would prove only that the fake was called.
func newProvenance(t *testing.T) (*plugins.Provenance, plugins.Signatures, plugins.Upstreams) {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sigs := plugins.NewSignatures(db, runstore.DialectSQLite)
	ups := plugins.NewUpstreams(db, runstore.DialectSQLite, sigs, "registry.internal/mirror")
	t.Cleanup(func() { require.NoError(t, ups.Close()) })
	return plugins.NewProvenance(sigs, ups), sigs, ups
}

// TestProvenanceReportsSignedOnlyForAnAcceptedSignature is the fact the
// dispatch-time policy decision turns on, and the reason that decision cannot
// be made at definition-save time: nothing has been pinned, mirrored or
// verified when a definition is saved.
//
// Three artifacts under ONE upstream, so the answer cannot come from the
// registration alone: one signed by an allowed principal, one signed by
// somebody nobody named, and one nothing vouched for at all.
func TestProvenanceReportsSignedOnlyForAnAcceptedSignature(t *testing.T) {
	ctx := context.Background()
	prov, sigs, ups := newProvenance(t)
	tenant := uniqueTenant(t)

	require.NoError(t, ups.Add(ctx, tenant, plugins.Upstream{
		Namespace:         "acme",
		URL:               "oci://registry.acme.example/plugins",
		AllowedIdentities: allowOnly(testIssuer, testIdentity),
		MirrorPolicy:      plugins.MirrorAlways,
	}))

	vouched := digestOf("the plugin the release bot signed")
	require.NoError(t, sigs.Record(ctx, tenant, vouched, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	stranger := digestOf("the plugin somebody else signed")
	require.NoError(t, sigs.Record(ctx, tenant, stranger, plugins.Signature{
		Identity: "someone-else@evil.example",
		Issuer:   testIssuer,
		Payload:  []byte("attested by nobody this tenant named"),
		Source:   plugins.SourceManual,
	}))

	unsigned := digestOf("the plugin nobody vouched for")

	for _, tc := range []struct {
		name   string
		digest string
		signed bool
	}{
		{"an allowed signer vouched for these bytes", digestText(vouched), true},
		{"signed, but by a principal this tenant never named", digestText(stranger), false},
		{"no signature is recorded for these bytes at all", digestText(unsigned), false},
		{"the lockfile pinned nothing", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signed, upstream, err := prov.Provenance(ctx, tenant, "acme/build:v1", tc.digest)
			require.NoError(t, err)
			require.Equal(t, tc.signed, signed)
			require.Equal(t, "oci://registry.acme.example/plugins", upstream,
				"the upstream is the registration's, whatever the signature says")
		})
	}
}

// TestProvenanceOfANonFederatedReferenceVouchesForNothing. An inline command
// or a scheme-addressed artifact belongs to no upstream, so there is no
// allowed-signer list to judge it by. The answer is "not signed" rather than
// an error, because a tier that requires signatures must refuse it and a tier
// that does not must be able to run it.
func TestProvenanceOfANonFederatedReferenceVouchesForNothing(t *testing.T) {
	ctx := context.Background()
	prov, _, _ := newProvenance(t)
	tenant := uniqueTenant(t)

	for _, ref := range []string{
		"cmd://echo",
		"oci://registry.example/plugins/build:v1",
		"unregistered/build:v1",
	} {
		signed, upstream, err := prov.Provenance(ctx, tenant, ref, digestText(digestOf("bytes")))
		require.NoError(t, err, ref)
		require.False(t, signed, "%s belongs to no upstream, so nothing vouched for it", ref)
		require.Empty(t, upstream, "%s came from no registered upstream", ref)
	}
}

// TestProvenanceIsTenantScoped. One tenant's registration and one tenant's
// signature records answer for that tenant alone: there is no unscoped read
// here, and an empty tenant is a caller bug rather than a wildcard.
func TestProvenanceIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	prov, sigs, ups := newProvenance(t)
	owner, other := uniqueTenant(t), uniqueTenant(t)

	require.NoError(t, ups.Add(ctx, owner, plugins.Upstream{
		Namespace:         "acme",
		URL:               "oci://registry.acme.example/plugins",
		AllowedIdentities: allowOnly(testIssuer, testIdentity),
		MirrorPolicy:      plugins.MirrorAlways,
	}))
	d := digestOf("the plugin the release bot signed")
	require.NoError(t, sigs.Record(ctx, owner, d, plugins.Signature{
		Identity: testIdentity, Issuer: testIssuer,
		Payload: []byte("attested by the release pipeline"), Source: plugins.SourceManual,
	}))

	signed, upstream, err := prov.Provenance(ctx, owner, "acme/build:v1", digestText(d))
	require.NoError(t, err)
	require.True(t, signed)
	require.Equal(t, "oci://registry.acme.example/plugins", upstream)

	signed, upstream, err = prov.Provenance(ctx, other, "acme/build:v1", digestText(d))
	require.NoError(t, err)
	require.False(t, signed, "another tenant's signature vouches for nothing here")
	require.Empty(t, upstream, "another tenant's registration is not this tenant's upstream")

	_, _, err = prov.Provenance(ctx, "", "acme/build:v1", digestText(d))
	require.ErrorIs(t, err, plugins.ErrTenantRequired)
}
