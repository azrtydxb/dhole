package identity_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/identity"
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

// postgresIdentityStore opens the identity store over the live Postgres, or
// skips with a reason. An integration test that silently degrades to nothing
// reports green while testing nothing.
func postgresIdentityStore(t *testing.T) identity.Store {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return identity.NewSQLStoreWithDialect(db, runstore.DialectPostgres)
}

// sqliteIdentityStore opens the identity store over a fresh SQLite file,
// through the same dialect-explicit constructor Postgres uses.
func sqliteIdentityStore(t *testing.T) identity.Store {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return identity.NewSQLStoreWithDialect(db, runstore.DialectSQLite)
}

// TestSQLiteSatisfiesIdentityStoreContract and its Postgres twin hold both
// dialects to one contract. Principals and service tokens are the credential
// database: a store that writes on the development file and refuses on the
// tuned target locks every operator out of the deployment that matters, and a
// password check that only ever ran on SQLite has never run where it counts.
func TestSQLiteSatisfiesIdentityStoreContract(t *testing.T) {
	identityStoreContract(t, sqliteIdentityStore(t))
}

func TestPostgresSatisfiesIdentityStoreContract(t *testing.T) {
	identityStoreContract(t, postgresIdentityStore(t))
}

// identityStoreContract is the behaviour the store must show on both dialects.
func identityStoreContract(t *testing.T, store identity.Store) {
	t.Helper()

	t.Run("APrincipalRoundTrips", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)

		require.NoError(t, store.PutPrincipal(ctx, identity.StoredPrincipal{
			TenantID:       tenant,
			Subject:        "ada",
			Kind:           identity.PrincipalUser,
			CredentialHash: "$argon2id$stored-verbatim",
		}))

		got, err := store.PrincipalCredential(ctx, tenant, "ada")
		require.NoError(t, err)
		require.Equal(t, tenant, got.TenantID)
		require.Equal(t, "ada", got.Subject)
		require.Equal(t, identity.PrincipalUser, got.Kind)
		require.Equal(t, "$argon2id$stored-verbatim", got.CredentialHash)
	})

	t.Run("PutPrincipalReplacesTheCredential", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)

		require.NoError(t, store.PutPrincipal(ctx, identity.StoredPrincipal{
			TenantID: tenant, Subject: "ada",
			Kind: identity.PrincipalUser, CredentialHash: "old",
		}))
		// A password change is an upsert on (tenant, subject). If the ON
		// CONFLICT clause does not fire, the rotation silently leaves the old
		// credential in force.
		require.NoError(t, store.PutPrincipal(ctx, identity.StoredPrincipal{
			TenantID: tenant, Subject: "ada",
			Kind: identity.PrincipalUser, CredentialHash: "new",
		}))

		got, err := store.PrincipalCredential(ctx, tenant, "ada")
		require.NoError(t, err)
		require.Equal(t, "new", got.CredentialHash)
	})

	t.Run("EnsurePrincipalCreatesOneAndThenLeavesItAlone", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)

		// The mint's own write: a subject a token was issued to becomes a
		// principal of the tenant, because everything that asks whether a
		// caller is one reads this table and not `tokens`.
		require.NoError(t, store.EnsurePrincipal(ctx, identity.StoredPrincipal{
			TenantID: tenant, Subject: "runner", Kind: identity.PrincipalService,
		}))
		got, err := store.PrincipalCredential(ctx, tenant, "runner")
		require.NoError(t, err)
		require.Equal(t, identity.PrincipalService, got.Kind)

		// And a second one must NOT be an upsert. If the ON CONFLICT clause
		// updated instead of doing nothing, issuing a token to a person would
		// wipe the password they log in with.
		require.NoError(t, store.PutPrincipal(ctx, identity.StoredPrincipal{
			TenantID: tenant, Subject: "runner",
			Kind: identity.PrincipalUser, CredentialHash: "$argon2id$theirs",
		}))
		require.NoError(t, store.EnsurePrincipal(ctx, identity.StoredPrincipal{
			TenantID: tenant, Subject: "runner", Kind: identity.PrincipalService,
		}))
		got, err = store.PrincipalCredential(ctx, tenant, "runner")
		require.NoError(t, err)
		require.Equal(t, "$argon2id$theirs", got.CredentialHash,
			"ensuring a principal overwrote the credential of one that existed")
		require.Equal(t, identity.PrincipalUser, got.Kind)
	})

	t.Run("AnUnknownPrincipalIsNotFound", func(t *testing.T) {
		ctx := context.Background()
		_, err := store.PrincipalCredential(ctx, uniqueTenant(t), "nobody")
		require.ErrorIs(t, err, identity.ErrNotFound)
	})

	t.Run("PrincipalsAreScopedToTheirTenant", func(t *testing.T) {
		ctx := context.Background()
		mine, theirs := uniqueTenant(t), uniqueTenant(t)

		require.NoError(t, store.PutPrincipal(ctx, identity.StoredPrincipal{
			TenantID: mine, Subject: "ada",
			Kind: identity.PrincipalUser, CredentialHash: "mine",
		}))
		_, err := store.PrincipalCredential(ctx, theirs, "ada")
		require.ErrorIs(t, err, identity.ErrNotFound,
			"the same subject name in another tenant is another principal")
	})

	t.Run("ATokenRoundTripsWithItsScopesAndExpiry", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		expires := time.Now().Add(time.Hour).UTC().Truncate(time.Nanosecond)

		require.NoError(t, store.PutToken(ctx, identity.StoredToken{
			TenantID: tenant, Subject: "runner",
			Hash: "hash-a", Scopes: []string{"runs:write", "runs:read"},
			ExpiresAt: expires,
		}))

		got, err := store.Token(ctx, tenant, "hash-a")
		require.NoError(t, err)
		require.Equal(t, "runner", got.Subject)
		require.Equal(t, "hash-a", got.Hash)
		require.Equal(t, []string{"runs:write", "runs:read"}, got.Scopes)
		require.WithinDuration(t, expires, got.ExpiresAt, time.Millisecond,
			"expiry decides whether a credential still authenticates: it has to survive the round trip")
	})

	t.Run("ReIssuingTheSameTokenHashIsIdempotent", func(t *testing.T) {
		ctx := context.Background()
		tenant := uniqueTenant(t)
		expires := time.Now().Add(time.Hour).UTC()

		tok := identity.StoredToken{
			TenantID: tenant, Subject: "runner",
			Hash: "hash-b", Scopes: []string{"runs:read"}, ExpiresAt: expires,
		}
		require.NoError(t, store.PutToken(ctx, tok))
		// ON CONFLICT DO NOTHING: a retried write must not fail and must not
		// widen the token that is already out in the world.
		tok.Scopes = []string{"runs:read", "admin"}
		require.NoError(t, store.PutToken(ctx, tok))

		got, err := store.Token(ctx, tenant, "hash-b")
		require.NoError(t, err)
		require.Equal(t, []string{"runs:read"}, got.Scopes,
			"a re-presented hash must not silently escalate the scopes of a live token")
	})

	t.Run("AnUnknownTokenIsNotFound", func(t *testing.T) {
		ctx := context.Background()
		_, err := store.Token(ctx, uniqueTenant(t), "no-such-hash")
		require.ErrorIs(t, err, identity.ErrNotFound)
	})

	t.Run("TokensAreScopedToTheirTenant", func(t *testing.T) {
		ctx := context.Background()
		mine, theirs := uniqueTenant(t), uniqueTenant(t)

		require.NoError(t, store.PutToken(ctx, identity.StoredToken{
			TenantID: mine, Subject: "runner", Hash: "shared-hash",
			Scopes: []string{"runs:read"}, ExpiresAt: time.Now().Add(time.Hour).UTC(),
		}))
		_, err := store.Token(ctx, theirs, "shared-hash")
		require.ErrorIs(t, err, identity.ErrNotFound,
			"a token hash is not a bearer credential across tenants")
	})

	t.Run("AnUnscopedCallIsRefused", func(t *testing.T) {
		ctx := context.Background()
		require.ErrorIs(t, store.PutPrincipal(ctx, identity.StoredPrincipal{Subject: "ada"}),
			identity.ErrTenantRequired)
		_, err := store.PrincipalCredential(ctx, "", "ada")
		require.ErrorIs(t, err, identity.ErrTenantRequired)
		require.ErrorIs(t, store.PutToken(ctx, identity.StoredToken{Subject: "runner"}),
			identity.ErrTenantRequired)
		_, err = store.Token(ctx, "", "hash")
		require.ErrorIs(t, err, identity.ErrTenantRequired)
	})
}

// TestSQLiteLocalProviderAuthenticates and its Postgres twin run the security
// path — argon2id verification and service-token authentication — over the
// real store on both dialects. A password check that passes on SQLite and
// cannot even reach its row on Postgres is the worst outcome available here:
// it is not a weaker check, it is no check, in the deployment that matters.
func TestSQLiteLocalProviderAuthenticates(t *testing.T) {
	localProviderContract(t, sqliteIdentityStore(t))
}

func TestPostgresLocalProviderAuthenticates(t *testing.T) {
	localProviderContract(t, postgresIdentityStore(t))
}

func localProviderContract(t *testing.T, store identity.Store) {
	t.Helper()
	ctx := context.Background()
	local := identity.NewLocal(store)

	t.Run("APasswordAuthenticatesAndAWrongOneDoesNot", func(t *testing.T) {
		tenant := uniqueTenant(t)
		require.NoError(t, local.CreateUser(ctx, tenant, "ada", "correct horse battery staple"))

		p, err := local.Authenticate(ctx,
			identity.PasswordCredential(tenant, "ada", "correct horse battery staple"))
		require.NoError(t, err)
		require.Equal(t, "ada", p.Subject)
		require.Equal(t, tenant, p.TenantID)
		require.Equal(t, identity.PrincipalUser, p.Kind)

		_, err = local.Authenticate(ctx, identity.PasswordCredential(tenant, "ada", "wrong"))
		require.ErrorIs(t, err, identity.ErrUnauthenticated)

		// An unknown subject must be indistinguishable from a wrong password.
		_, err = local.Authenticate(ctx, identity.PasswordCredential(tenant, "nobody", "wrong"))
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
	})

	t.Run("AServiceTokenAuthenticatesWithItsScopes", func(t *testing.T) {
		tenant := uniqueTenant(t)
		token, err := local.IssueToken(ctx, identity.Principal{
			TenantID: tenant, Subject: "runner", Scopes: []string{"runs:write"},
		}, time.Hour)
		require.NoError(t, err)

		p, err := local.Authenticate(ctx, token)
		require.NoError(t, err)
		require.Equal(t, "runner", p.Subject)
		require.Equal(t, tenant, p.TenantID)
		require.Equal(t, []string{"runs:write"}, p.Scopes)
		require.Equal(t, identity.PrincipalService, p.Kind)
	})

	t.Run("AnExpiredTokenIsRefused", func(t *testing.T) {
		tenant := uniqueTenant(t)
		token, err := local.IssueToken(ctx, identity.Principal{
			TenantID: tenant, Subject: "runner",
		}, -time.Second)
		require.NoError(t, err)

		_, err = local.Authenticate(ctx, token)
		require.ErrorIs(t, err, identity.ErrExpired)
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
	})

	t.Run("AForgedTokenIsRefused", func(t *testing.T) {
		tenant := uniqueTenant(t)
		issued, err := local.IssueToken(ctx, identity.Principal{
			TenantID: tenant, Subject: "runner",
		}, time.Hour)
		require.NoError(t, err)

		// Same tenant, same shape, one byte of the secret changed.
		forged := []byte(issued)
		if forged[len(forged)-1] == 'a' {
			forged[len(forged)-1] = 'b'
		} else {
			forged[len(forged)-1] = 'a'
		}
		_, err = local.Authenticate(ctx, string(forged))
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
	})
}

// TestSQLiteBackfillsPrincipalsForTokensAlreadyIssued and its Postgres twin
// hold migration 0022 to both dialects.
//
// IssueToken establishes the principal from now on, which does nothing for a
// deployment whose tokens were minted before it did. Those tokens are in
// people's hands and in CI configuration: their holders must not have to
// reissue them to be recognised as approvers, and "your token works for
// everything except the thing you are trying to do" is the hardest possible
// failure to diagnose from outside.
func TestSQLiteBackfillsPrincipalsForTokensAlreadyIssued(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.db")
	backfillContract(t, runstore.DialectSQLite, func() *sql.DB {
		db, err := runstore.OpenSQLite(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	})
}

func TestPostgresBackfillsPrincipalsForTokensAlreadyIssued(t *testing.T) {
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	backfillContract(t, runstore.DialectPostgres, func() *sql.DB {
		db, err := runstore.OpenPostgres(context.Background(), dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	})
}

// backfillContract writes the row a pre-fix deployment has — a token whose
// subject is in no `principals` — and then does what a restart does: opens the
// database again, which re-runs every migration.
func backfillContract(t *testing.T, dialect runstore.Dialect, open func() *sql.DB) {
	t.Helper()
	ctx := context.Background()
	tenant := uniqueTenant(t)

	db := open()
	store := identity.NewSQLStoreWithDialect(db, dialect)

	// Written through SQL rather than through PutToken, because PutToken is
	// not what a pre-fix deployment used: this is the row IssueToken left
	// behind when it wrote `tokens` and nothing else.
	_, err := db.ExecContext(ctx, dialect.Rebind(
		`INSERT INTO tokens (tenant_id, subject, token_hash, scopes, expires_at)
		 VALUES (?, ?, ?, ?, ?)`),
		tenant, "legacy-runner", "0000000000000000000000000000000000000000000000000000000000000000",
		"[]", time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)

	_, err = store.PrincipalCredential(ctx, tenant, "legacy-runner")
	require.ErrorIs(t, err, identity.ErrNotFound,
		"the fixture is wrong: this test needs a token whose subject is not yet a principal")

	// The plane restarts.
	open()

	got, err := store.PrincipalCredential(ctx, tenant, "legacy-runner")
	require.NoError(t, err,
		"a token issued before the fix still resolves to nobody the approval subsystem knows")
	require.Equal(t, identity.PrincipalService, got.Kind)
	require.Empty(t, got.CredentialHash,
		"the backfill invented a credential for a principal that authenticates by token")
}
