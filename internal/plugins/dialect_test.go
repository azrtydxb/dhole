package plugins_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// uniqueTenant keeps every dialect run in its own tenant. SQLite gets a fresh
// file per test; Postgres is shared and persistent, so an assertion that only
// holds because an earlier run's rows were absent is not an assertion.
var (
	tenantSeq atomic.Uint64
	tenantRun = strconv.FormatInt(time.Now().UnixNano(), 36)
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' || r == ':' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%s-%d", name, tenantRun, tenantSeq.Add(1))
}

// postgresSignatures opens the signature store over the live Postgres, or
// skips with a reason. An integration test that silently degrades to nothing
// reports green while testing nothing.
func postgresSignatures(t *testing.T) plugins.Signatures {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := plugins.NewSignatures(db, runstore.DialectPostgres)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// sqliteSignatures opens the signature store over a fresh SQLite file, through
// the same dialect-explicit constructor Postgres uses.
func sqliteSignatures(t *testing.T) plugins.Signatures {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := plugins.NewSignatures(db, runstore.DialectSQLite)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

// TestSQLiteSatisfiesSignatureContract and its Postgres twin hold both
// dialects to one contract.
//
// This is a fail-closed security check: every path that is not an explicit
// match is a denial. A store that cannot even reach its table on Postgres
// therefore denies EVERYTHING there — no plugin dispatches — while every
// SQLite suite stays green. The inverse mistake, a Verify that reads "I could
// not ask" as "signed", would be an authorisation bypass. Both are why this
// contract has to run where the deployment runs.
func TestSQLiteSatisfiesSignatureContract(t *testing.T) {
	signatureContract(t, sqliteSignatures(t))
}

func TestPostgresSatisfiesSignatureContract(t *testing.T) {
	signatureContract(t, postgresSignatures(t))
}

// signatureContract is the behaviour the signature store must show on both
// dialects.
func signatureContract(t *testing.T, sigs plugins.Signatures) {
	t.Helper()

	manual := func(payload string) plugins.Signature {
		return plugins.Signature{
			Identity: testIdentity,
			Issuer:   testIssuer,
			Payload:  []byte(payload),
			Source:   plugins.SourceManual,
		}
	}

	t.Run("ARecordedSignatureVerifies", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("a plugin someone vouched for")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("attested by the release pipeline")))
		require.NoError(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)))
	})

	t.Run("ThePayloadRoundTripsByteForByte", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("bytes, not text")

		// The payload column is BLOB on SQLite and BYTEA on Postgres, and a
		// cosign bundle is base64 and DER. Verification re-parses these exact
		// bytes on every read, so an encoding round-trip anywhere in the
		// driver turns a valid signature into a denial — or, with a laxer
		// parser, into a signature over something else.
		raw := []byte{0x00, 0x01, 0xff, 0xfe, '\n', '\'', '"', '?', 0x80, 0x00}
		require.NoError(t, sigs.Record(ctx, tenant, d, plugins.Signature{
			Identity: testIdentity, Issuer: testIssuer,
			Payload: raw, Source: plugins.SourceManual,
		}))

		// A manual record verifies only if its payload is non-empty, so the
		// check above proves the bytes survived at all; the cosign case below
		// proves they survived exactly.
		require.NoError(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)))
	})

	t.Run("ACosignBundleSurvivesTheRoundTripAndReVerifies", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("a cosign-signed plugin, on both dialects")

		sig, err := plugins.SignatureFromCosignBundle(d, signedBundleFor(t, d, testIdentity, testIssuer))
		require.NoError(t, err)
		require.NoError(t, sigs.Record(ctx, tenant, d, sig))

		// Verify re-parses the stored bundle and re-checks its certificate: it
		// passes only if the bytes came back exactly as they went in.
		require.NoError(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)))
	})

	t.Run("ReRecordingTheSameSignerIsIdempotent", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("recorded twice")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("first attestation")))
		// ON CONFLICT ... DO UPDATE: a retried publish refreshes the row
		// rather than fanning out records or failing.
		require.NoError(t, sigs.Record(ctx, tenant, d, manual("second attestation")))
		require.NoError(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)))
	})

	t.Run("AnUnrecordedDigestDenies", func(t *testing.T) {
		ctx := context.Background()
		require.ErrorIs(t, sigs.Verify(ctx, uniqueTenant(t),
			digestOf("nobody signed this"), allowOnly(testIssuer, testIdentity)),
			plugins.ErrVerificationFailed)
	})

	t.Run("ASignerNobodyAllowedDenies", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("signed, but not by anyone we trust")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("attested by a stranger")))
		require.ErrorIs(t,
			sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, "someone-else@corp.example")),
			plugins.ErrVerificationFailed)
	})

	t.Run("TheSameIdentityFromAnotherIssuerDenies", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("right name, wrong issuer")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("attested")))
		require.ErrorIs(t,
			sigs.Verify(ctx, tenant, d, allowOnly("https://evil.example", testIdentity)),
			plugins.ErrVerificationFailed,
			"the same identity string from a different issuer is a different principal")
	})

	t.Run("AnEmptyAllowedListAllowsNothing", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("signed, with nothing configured")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("attested")))
		require.ErrorIs(t, sigs.Verify(ctx, tenant, d, nil), plugins.ErrVerificationFailed,
			"an unconfigured tier is the one that must trust nobody")
	})

	t.Run("RecordsAreScopedToTheirTenant", func(t *testing.T) {
		ctx := context.Background()
		mine, theirs := uniqueTenant(t), uniqueTenant(t)
		d := digestOf("the same upstream plugin, two tenants")

		require.NoError(t, sigs.Record(ctx, mine, d, manual("attested")))
		require.ErrorIs(t, sigs.Verify(ctx, theirs, d, allowOnly(testIssuer, testIdentity)),
			plugins.ErrVerificationFailed,
			"one tenant's decision to trust a signer is not another's")
	})

	t.Run("RemoveWithdrawsTheRecord", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		d := digestOf("signed, then revoked")

		require.NoError(t, sigs.Record(ctx, tenant, d, manual("attested")))
		require.NoError(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)))

		// Revocation after a key compromise: the artifact that dispatched a
		// minute ago must stop dispatching now.
		require.NoError(t, sigs.Remove(ctx, tenant, d))
		require.ErrorIs(t, sigs.Verify(ctx, tenant, d, allowOnly(testIssuer, testIdentity)),
			plugins.ErrVerificationFailed)
	})

	t.Run("RemoveIsScopedToItsTenant", func(t *testing.T) {
		ctx := context.Background()
		mine, theirs := uniqueTenant(t), uniqueTenant(t)
		d := digestOf("revoked by the wrong tenant")

		require.NoError(t, sigs.Record(ctx, mine, d, manual("attested")))
		require.NoError(t, sigs.Record(ctx, theirs, d, manual("attested")))
		require.NoError(t, sigs.Remove(ctx, theirs, d))

		require.NoError(t, sigs.Verify(ctx, mine, d, allowOnly(testIssuer, testIdentity)),
			"a revocation in one tenant must not withdraw another tenant's record")
	})

	t.Run("AnUnscopedCallIsRefused", func(t *testing.T) {
		ctx := context.Background()
		d := digestOf("unscoped")
		require.ErrorIs(t, sigs.Record(ctx, "", d, manual("attested")), plugins.ErrTenantRequired)
		require.ErrorIs(t, sigs.Remove(ctx, "", d), plugins.ErrTenantRequired)
		require.ErrorContains(t, sigs.Verify(ctx, "", d, allowOnly(testIssuer, testIdentity)),
			"tenant scope required")
	})

	t.Run("AnIncompleteDigestIsRefused", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		require.Error(t, sigs.Record(ctx, tenant, nil, manual("attested")))
		require.ErrorIs(t, sigs.Verify(ctx, tenant, &dholev1.Digest{Algo: "sha256"},
			allowOnly(testIssuer, testIdentity)), plugins.ErrVerificationFailed)
	})
}

// TestPostgresDispatcherRefusesUnsignedArtifacts is the fail-closed path end to
// end on the tuned target: the Dispatcher verifies on EVERY call, and an
// artifact whose signature this tenant does not hold is never fetched.
func TestPostgresDispatcherRefusesUnsignedArtifacts(t *testing.T) {
	dispatcherContract(t, postgresSignatures(t))
}

func TestSQLiteDispatcherRefusesUnsignedArtifacts(t *testing.T) {
	dispatcherContract(t, sqliteSignatures(t))
}

func dispatcherContract(t *testing.T, sigs plugins.Signatures) {
	t.Helper()
	ctx := context.Background()
	tenant := uniqueTenant(t)

	r, store := newResolver(t)
	d, err := store.Put(ctx, tenant, strings.NewReader("the plugin bytes themselves"))
	require.NoError(t, err)

	// Resolution pins the reference to a digest exactly once, as a saved
	// revision would hold it. It is deliberately NOT evidence of trust.
	artifact, err := r.Resolve(ctx, tenant, fmt.Sprintf("cas://%s:%s", d.GetAlgo(), d.GetHex()))
	require.NoError(t, err)

	dispatcher := plugins.NewDispatcher(r, sigs, allowOnly(testIssuer, testIdentity))

	_, err = dispatcher.Dispatch(ctx, tenant, artifact)
	require.ErrorIs(t, err, plugins.ErrVerificationFailed,
		"an unsigned artifact must not be fetched, on either dialect")

	require.NoError(t, sigs.Record(ctx, tenant, d, plugins.Signature{
		Identity: testIdentity, Issuer: testIssuer,
		Payload: []byte("attested by the release pipeline"), Source: plugins.SourceManual,
	}))
	rc, err := dispatcher.Dispatch(ctx, tenant, artifact)
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	// Trust is re-checked, never remembered.
	require.NoError(t, sigs.Remove(ctx, tenant, d))
	_, err = dispatcher.Dispatch(ctx, tenant, artifact)
	require.ErrorIs(t, err, plugins.ErrVerificationFailed,
		"a withdrawn signature must stop dispatch immediately")
}
