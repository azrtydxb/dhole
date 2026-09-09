package plugins_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
)

// newSignatures opens a signature store over a fresh SQLite file, as the
// catalog does: the DDL is embedded with the run store's and applied by the
// same migration runner, so the schema has one definition.
func newSignatures(t *testing.T) plugins.Signatures {
	t.Helper()
	s, err := plugins.NewSignatures(filepath.Join(t.TempDir(), "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// digestOf is the digest of some bytes, so a test's digest is a real one
// rather than a literal nobody can trace back to content.
func digestOf(payload string) *dholev1.Digest {
	sum := sha256.Sum256([]byte(payload))
	return plugins.NewDigest(hex.EncodeToString(sum[:]))
}

const (
	testIdentity = "release-bot@corp.example"
	testIssuer   = "https://token.actions.githubusercontent.com"
)

// allowOnly builds the allowed-signer list for one principal. A principal is
// an issuer AND an identity: the same identity string from a different issuer
// is a different principal, so both halves are named.
func allowOnly(issuer, identity string) []string {
	return []string{issuer + " " + identity}
}

// TestVerificationIsUniformAcrossSchemes is the reason a signature record is
// detached and keyed on the digest rather than on the reference.
//
// A signature is about BYTES. Keying it on the digest is what makes
// verification identical for an `oci://` and a `cas://` artifact, and what
// stops a moved tag from carrying a signature over to different content. If
// verification took the reference, the two schemes would need two code paths
// and the OCI one would be verifying a name.
func TestVerificationIsUniformAcrossSchemes(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)

	// One artifact's bytes, distributed both ways: mirrored into the tenant's
	// CAS and pushed to a registry. Same bytes, same digest, one signature.
	d := digestOf("plugin payload signed once")
	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	oci := plugins.Artifact{
		Ref:    "oci://registry.example/dhole/plugin@sha256:" + d.GetHex(),
		Digest: d,
		Scheme: plugins.SchemeOCI,
	}
	cas := plugins.Artifact{
		Ref:    fmt.Sprintf("cas://%s:%s", d.GetAlgo(), d.GetHex()),
		Digest: d,
		Scheme: plugins.SchemeCAS,
	}

	for _, a := range []plugins.Artifact{oci, cas} {
		t.Run(string(a.Scheme), func(t *testing.T) {
			// Identical call, identical arguments: nothing here can branch on
			// the scheme, because the scheme is never passed.
			require.NoError(t, sigs.Verify(ctx, testTenant, a.Digest,
				allowOnly(testIssuer, testIdentity)))
		})
	}
}

// recordingMarker is the catalog seam: verification failure has to be visible
// to the layer that decides what a tenant may run, not only to the caller that
// happened to dispatch.
type recordingMarker struct {
	tenants []string
	digests []string
	reasons []string
}

func (m *recordingMarker) MarkUntrusted(_ context.Context, tenantID string, d *dholev1.Digest, reason string) error {
	m.tenants = append(m.tenants, tenantID)
	m.digests = append(m.digests, d.GetHex())
	m.reasons = append(m.reasons, reason)
	return nil
}

// TestUnsignedPluginIsNotDispatchedRegardlessOfCachedResolution is the whole
// point of re-checking trust at dispatch.
//
// Task 32 resolves a reference to a digest exactly once and everything
// afterwards routes on that recorded digest. It would be natural — and wrong —
// to treat that successful resolution as standing evidence that the artifact
// is trustworthy. It is not: it is evidence about WHERE the bytes are, not
// about who vouched for them. A signature can be withdrawn after a key
// compromise or a bad release, and the artifact that was fine an hour ago must
// stop dispatching immediately. Trust is re-checked, never remembered.
func TestUnsignedPluginIsNotDispatchedRegardlessOfCachedResolution(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	r, store := newResolver(t)

	payload := "a plugin that was signed, then unsigned"
	d, err := store.Put(ctx, testTenant, strings.NewReader(payload))
	require.NoError(t, err)

	// Resolution succeeds and is cached, exactly as a saved revision would
	// hold it.
	resolved, err := r.Resolve(ctx, testTenant, fmt.Sprintf("cas://%s:%s", d.GetAlgo(), d.GetHex()))
	require.NoError(t, err)

	allowed := allowOnly(testIssuer, testIdentity)
	require.NoError(t, sigs.Record(ctx, testTenant, resolved.Digest, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	marker := &recordingMarker{}
	dispatcher := plugins.NewDispatcher(r, sigs, allowed, plugins.WithTrustMarker(marker))

	body, err := dispatcher.Dispatch(ctx, testTenant, resolved)
	require.NoError(t, err)
	require.Equal(t, payload, string(readAll(t, body)))

	// The signature is withdrawn. Nothing about the resolution changed: the
	// digest is the same, the bytes are still in the CAS, the artifact value
	// the caller holds is byte-for-byte the one that just dispatched.
	require.NoError(t, sigs.Remove(ctx, testTenant, resolved.Digest))

	_, err = dispatcher.Dispatch(ctx, testTenant, resolved)
	require.Error(t, err)
	require.ErrorIs(t, err, plugins.ErrVerificationFailed)
	require.Contains(t, err.Error(), "signature verification failed")

	// And the catalog entry is marked untrusted, so the refusal is durable
	// rather than a per-caller surprise.
	require.Equal(t, []string{testTenant}, marker.tenants)
	require.Equal(t, []string{resolved.Digest.GetHex()}, marker.digests)
	require.Contains(t, marker.reasons[0], "signature verification failed")
}

// TestSignatureFromDisallowedIdentityIsRejected: a stored signature is not
// authorisation. It says who signed; the allowed list says whose word this
// tenant accepts. A record from anyone else is evidence of nothing.
func TestSignatureFromDisallowedIdentityIsRejected(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("signed by someone we do not trust")

	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: "intern@corp.example",
		Issuer:   testIssuer,
		Payload:  []byte("signed, but not by a release principal"),
		Source:   plugins.SourceManual,
	}))

	err := sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity))
	require.ErrorIs(t, err, plugins.ErrVerificationFailed)
}

// TestEmptyAllowedListAllowsNothing pins the direction Verify fails in. An
// empty list is the shape a misconfigured tier arrives in — a missing policy
// key, an unmarshalled nil — and reading it as "no restriction" turns the one
// deployment that forgot to configure trust into the one that trusts
// everybody.
func TestEmptyAllowedListAllowsNothing(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("perfectly well signed")

	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	for _, allowed := range [][]string{nil, {}} {
		err := sigs.Verify(ctx, testTenant, d, allowed)
		require.ErrorIs(t, err, plugins.ErrVerificationFailed)
		require.Contains(t, err.Error(), "no allowed signer")
	}
}

// TestIdentityComparisonIsExactNotAPrefix. A substring or prefix comparison is
// the classic way an identity check is defeated: "evil-release-bot@corp.example"
// contains "release-bot@corp.example", and an attacker who can obtain a
// certificate for a name of their choosing chooses that one.
func TestIdentityComparisonIsExactNotAPrefix(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	allowed := allowOnly(testIssuer, testIdentity)

	for _, identity := range []string{
		"evil-" + testIdentity,     // allowed identity is a SUFFIX of this
		testIdentity + ".attacker", // allowed identity is a PREFIX of this
		"x" + testIdentity + "x",   // allowed identity is a SUBSTRING of this
		"RELEASE-BOT@CORP.EXAMPLE", // case is not equality either
		" " + testIdentity,         // nor is a padded copy
	} {
		t.Run(identity, func(t *testing.T) {
			d := digestOf("artifact signed by " + identity)
			require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
				Identity: identity,
				Issuer:   testIssuer,
				Payload:  []byte("attested"),
				Source:   plugins.SourceManual,
			}))
			require.ErrorIs(t, sigs.Verify(ctx, testTenant, d, allowed),
				plugins.ErrVerificationFailed)
		})
	}
}

// TestIssuerIsCheckedNotJustIdentity. "alice@corp.example" attested by the
// corporate OIDC provider and "alice@corp.example" attested by a public issuer
// anyone can obtain a token from are two different principals that happen to
// share a string. Checking only the identity means anyone who can get any
// issuer to assert that address can sign for the release bot.
func TestIssuerIsCheckedNotJustIdentity(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("same name, different issuer")

	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   "https://issuer.attacker.example",
		Payload:  []byte("attested by an issuer we never named"),
		Source:   plugins.SourceManual,
	}))

	require.ErrorIs(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)),
		plugins.ErrVerificationFailed)
	// And it verifies once the issuer actually named is the one allowed, so
	// the test above failed for the issuer and not for some other reason.
	require.NoError(t, sigs.Verify(ctx, testTenant, d,
		allowOnly("https://issuer.attacker.example", testIdentity)))
}

// TestAllowedEntryWithoutAnIssuerIsRefused. An entry naming only an identity
// would admit that identity from ANY issuer, which is exactly the check the
// test above exists to keep. A policy that cannot express "from anywhere" is
// the point; a malformed entry is a configuration error and denies rather than
// being skipped, because a silently-ignored entry is a rule the operator
// believes is in force.
func TestAllowedEntryWithoutAnIssuerIsRefused(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("well signed, badly configured")

	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	err := sigs.Verify(ctx, testTenant, d, []string{testIdentity})
	require.ErrorIs(t, err, plugins.ErrVerificationFailed)
	require.Contains(t, err.Error(), "malformed allowed signer")
}

// TestOneTenantsSignatureDoesNotSatisfyAnother. The digest is global — two
// tenants mirroring the same upstream plugin hold the same bytes — so the
// digest alone cannot be the key. Only the tenant scope keeps one tenant's
// decision to trust a signer from becoming another tenant's.
func TestOneTenantsSignatureDoesNotSatisfyAnother(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("the same upstream plugin, mirrored twice")
	allowed := allowOnly(testIssuer, testIdentity)

	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))

	require.NoError(t, sigs.Verify(ctx, testTenant, d, allowed))
	require.ErrorIs(t, sigs.Verify(ctx, "tenant-b", d, allowed), plugins.ErrVerificationFailed)
}

// TestSignaturesRejectAnEmptyTenant. There is no unscoped read in this system,
// even while only one tenant exists: an empty tenant is a caller bug, never a
// wildcard that reads across every tenant's records.
func TestSignaturesRejectAnEmptyTenant(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("unscoped")
	sig := plugins.Signature{Identity: testIdentity, Issuer: testIssuer, Payload: []byte("x"), Source: plugins.SourceManual}

	for _, tenant := range []string{"", "   "} {
		require.ErrorIs(t, sigs.Record(ctx, tenant, d, sig), plugins.ErrTenantRequired)
		require.ErrorIs(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)), plugins.ErrTenantRequired)
		require.ErrorIs(t, sigs.Remove(ctx, tenant, d), plugins.ErrTenantRequired)
		require.ErrorContains(t, sigs.Verify(ctx, tenant, d, nil), "tenant scope required")
	}
}

// TestStorageFailureDeniesRatherThanAdmits. A store that cannot answer has not
// said the artifact is signed; it has said nothing. Returning nil on a query
// error would make a database outage into a global authorisation bypass, which
// is the worst possible failure mode for a check that only ever runs on the
// path to executing someone else's code.
func TestStorageFailureDeniesRatherThanAdmits(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dhole.db")
	sigs, err := plugins.NewSignatures(path)
	require.NoError(t, err)

	d := digestOf("stored, then unreachable")
	require.NoError(t, sigs.Record(ctx, testTenant, d, plugins.Signature{
		Identity: testIdentity,
		Issuer:   testIssuer,
		Payload:  []byte("attested by the release pipeline"),
		Source:   plugins.SourceManual,
	}))
	require.NoError(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)))

	// The store goes away underneath a verification that had been passing.
	require.NoError(t, sigs.Close())

	err = sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity))
	require.Error(t, err)
	require.ErrorIs(t, err, plugins.ErrVerificationFailed)
}

// --- cosign ---------------------------------------------------------------
//
// The fixtures below build a Fulcio-SHAPED certificate: a short-lived leaf
// carrying the signer identity in a SAN and the OIDC issuer in Fulcio's
// 1.3.6.1.4.1.57264.1.8 extension. It is self-signed, because what this
// package verifies locally is the leaf's own claims and its signature over the
// payload — see the debt note in cosign.go for what is deliberately NOT
// verified here.

// oidFulcioIssuerV2 is Fulcio's OIDC issuer extension, a DER UTF8String.
var oidFulcioIssuerV2 = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57264, 1, 8}

// utf8String hand-encodes a short DER UTF8String, which is what Fulcio puts in
// the issuer extension.
func utf8String(t *testing.T, s string) []byte {
	t.Helper()
	require.Less(t, len(s), 128, "fixture only encodes short-form DER lengths")
	return append([]byte{0x0c, byte(len(s))}, s...)
}

// fulcioLikeCert returns a leaf certificate and its key.
func fulcioLikeCert(t *testing.T, identity, issuer string, notBefore, notAfter time.Time) (*ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:   big.NewInt(1),
		Subject:        pkix.Name{},
		NotBefore:      notBefore,
		NotAfter:       notAfter,
		KeyUsage:       x509.KeyUsageDigitalSignature,
		ExtKeyUsage:    []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		EmailAddresses: []string{identity},
		ExtraExtensions: []pkix.Extension{
			{Id: oidFulcioIssuerV2, Value: utf8String(t, issuer)},
		},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// simpleSigningPayload is the document cosign signs: it names the digest, which
// is what binds a signature to specific bytes rather than to a reference.
func simpleSigningPayload(t *testing.T, d *dholev1.Digest) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"critical": map[string]any{
			"identity": map[string]any{"docker-reference": "registry.example/dhole/plugin"},
			"image":    map[string]any{"docker-manifest-digest": d.GetAlgo() + ":" + d.GetHex()},
			"type":     "cosign container image signature",
		},
		"optional": map[string]any{},
	})
	require.NoError(t, err)
	return b
}

// cosignBundle assembles the JSON cosign writes with `--bundle`.
func cosignBundle(t *testing.T, key *ecdsa.PrivateKey, certPEM, payload []byte) []byte {
	t.Helper()
	sum := sha256.Sum256(payload)
	sig, err := ecdsa.SignASN1(rand.Reader, key, sum[:])
	require.NoError(t, err)
	b, err := json.Marshal(map[string]any{
		"base64Signature": base64.StdEncoding.EncodeToString(sig),
		"cert":            base64.StdEncoding.EncodeToString(certPEM),
		"payload":         base64.StdEncoding.EncodeToString(payload),
	})
	require.NoError(t, err)
	return b
}

// signedBundleFor is the happy path fixture: a valid bundle over d.
func signedBundleFor(t *testing.T, d *dholev1.Digest, identity, issuer string) []byte {
	t.Helper()
	now := time.Now()
	key, certPEM := fulcioLikeCert(t, identity, issuer, now.Add(-time.Hour), now.Add(time.Hour))
	return cosignBundle(t, key, certPEM, simpleSigningPayload(t, d))
}

// TestCosignBundleIsRecordedFromItsCertificate: the identity and issuer stored
// on a record come from the certificate, not from whatever the caller claims.
// A caller-supplied identity would make the whole record self-asserted.
func TestCosignBundleIsRecordedFromItsCertificate(t *testing.T) {
	ctx := context.Background()
	sigs := newSignatures(t)
	d := digestOf("cosign-signed plugin")

	sig, err := plugins.SignatureFromCosignBundle(d, signedBundleFor(t, d, testIdentity, testIssuer))
	require.NoError(t, err)
	require.Equal(t, testIdentity, sig.Identity)
	require.Equal(t, testIssuer, sig.Issuer)
	require.Equal(t, plugins.SourceCosign, sig.Source)

	require.NoError(t, sigs.Record(ctx, testTenant, d, sig))
	require.NoError(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)))
}

// TestCosignSignatureForAnotherDigestIsRejected. Without the digest binding, a
// perfectly valid signature over some OTHER artifact could be filed against
// this digest and would verify — which would defeat the entire premise that a
// signature is about bytes.
func TestCosignSignatureForAnotherDigestIsRejected(t *testing.T) {
	other := digestOf("a different, legitimately signed artifact")
	target := digestOf("the artifact an attacker wants dispatched")

	_, err := plugins.SignatureFromCosignBundle(target, signedBundleFor(t, other, testIdentity, testIssuer))
	require.Error(t, err)
	require.Contains(t, err.Error(), "signed digest")
}

// TestTamperedCosignBundleIsRejected: the certificate says who, the signature
// says over what. Reading the identity without checking the signature would
// accept any bundle carrying a genuine certificate.
func TestTamperedCosignBundleIsRejected(t *testing.T) {
	d := digestOf("tampered")
	now := time.Now()
	key, certPEM := fulcioLikeCert(t, testIdentity, testIssuer, now.Add(-time.Hour), now.Add(time.Hour))

	// A signature made over different bytes than the payload carries.
	bundle := cosignBundle(t, key, certPEM, simpleSigningPayload(t, digestOf("something else")))
	var raw map[string]any
	require.NoError(t, json.Unmarshal(bundle, &raw))
	raw["payload"] = base64.StdEncoding.EncodeToString(simpleSigningPayload(t, d))
	swapped, err := json.Marshal(raw)
	require.NoError(t, err)

	_, err = plugins.SignatureFromCosignBundle(d, swapped)
	require.Error(t, err)
	require.Contains(t, err.Error(), "signature does not verify")
}

// TestExpiredCosignCertificateIsRejected. Fulcio certificates are deliberately
// short-lived; accepting one outside its validity window discards that.
func TestExpiredCosignCertificateIsRejected(t *testing.T) {
	d := digestOf("signed long ago")
	past := time.Now().Add(-48 * time.Hour)
	key, certPEM := fulcioLikeCert(t, testIdentity, testIssuer, past, past.Add(time.Hour))

	_, err := plugins.SignatureFromCosignBundle(d, cosignBundle(t, key, certPEM, simpleSigningPayload(t, d)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside its validity")
}

// TestCosignBundleWithoutAnIssuerExtensionIsRejected: a certificate that names
// no issuer cannot be checked against an allowed principal, and defaulting one
// would invent a claim the CA never made.
func TestCosignBundleWithoutAnIssuerExtensionIsRejected(t *testing.T) {
	d := digestOf("no issuer")
	now := time.Now()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:   big.NewInt(2),
		NotBefore:      now.Add(-time.Hour),
		NotAfter:       now.Add(time.Hour),
		EmailAddresses: []string{testIdentity},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	_, err = plugins.SignatureFromCosignBundle(d, cosignBundle(t, key, certPEM, simpleSigningPayload(t, d)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "issuer")
}

// insertRawSignature writes a row the exported API would refuse, which is how
// a corrupted or hand-edited database — or a future writer with a bug — would
// look to Verify.
func insertRawSignature(t *testing.T, path, tenantID string, d *dholev1.Digest, identity, issuer, source string, payload []byte) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	_, err = db.Exec(`INSERT INTO artifact_signatures
		(tenant_id, digest, identity, issuer, payload, source, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		tenantID, d.GetAlgo()+":"+d.GetHex(), identity, issuer, payload, source,
		time.Now().UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
}

// TestStoredRecordWithAnUnknownSourceDenies. A source this build does not
// understand is a record whose payload it cannot check. Treating "I do not
// know how to verify this" as "verified" is how a forward-compatibility
// shortcut becomes a bypass: write the row with source "future", and the check
// is skipped.
func TestStoredRecordWithAnUnknownSourceDenies(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dhole.db")
	sigs, err := plugins.NewSignatures(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sigs.Close() })

	d := digestOf("recorded by a newer build")
	insertRawSignature(t, path, testTenant, d, testIdentity, testIssuer, "sigstore-v3", []byte("opaque"))

	require.ErrorIs(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)),
		plugins.ErrVerificationFailed)
}

// TestStoredCosignRecordWithAMalformedPayloadDenies. The columns are an index,
// not the evidence. A cosign record is re-verified from its bundle on every
// read, so a row whose identity column says the right thing and whose payload
// is garbage must not pass — otherwise anyone who can write a row can grant
// themselves any identity.
func TestStoredCosignRecordWithAMalformedPayloadDenies(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dhole.db")
	sigs, err := plugins.NewSignatures(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sigs.Close() })

	d := digestOf("a row someone wrote by hand")
	insertRawSignature(t, path, testTenant, d, testIdentity, testIssuer, "cosign", []byte("{not a bundle"))

	require.ErrorIs(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)),
		plugins.ErrVerificationFailed)
}

// TestStoredCosignRecordMustMatchItsOwnCertificate: the same attack from the
// other side — a valid bundle from one signer filed under another signer's
// identity column.
func TestStoredCosignRecordMustMatchItsOwnCertificate(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dhole.db")
	sigs, err := plugins.NewSignatures(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sigs.Close() })

	d := digestOf("filed under the wrong name")
	bundle := signedBundleFor(t, d, "intern@corp.example", testIssuer)
	insertRawSignature(t, path, testTenant, d, testIdentity, testIssuer, "cosign", bundle)

	require.ErrorIs(t, sigs.Verify(ctx, testTenant, d, allowOnly(testIssuer, testIdentity)),
		plugins.ErrVerificationFailed)
}
