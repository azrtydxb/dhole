package identity_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/azrtydxb/dhole/internal/identity"
)

// migrationPath is the one schema file this package owns. The test applies the
// real migration rather than a hand-written copy, so a schema that does not
// match the code fails here instead of in production.
const migrationPath = "../runstore/migrations/0004_identity.sql"

// newStore opens a throwaway SQLite database with the identity schema applied.
func newStore(t *testing.T) (identity.Store, *sql.DB) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "identity.db")
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	schema, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), string(schema))
	require.NoError(t, err)

	return identity.NewSQLStore(db), db
}

func newLocal(t *testing.T) (*identity.Local, identity.Store, *sql.DB) {
	t.Helper()
	store, db := newStore(t)
	return identity.NewLocal(store), store, db
}

// tokenSecret is the part of a presented token that actually authenticates:
// everything after the last underscore.
func tokenSecret(token string) string {
	return token[strings.LastIndex(token, "_")+1:]
}

// source reads one of this package's files. The two properties below cannot be
// observed from behaviour — swapping subtle.ConstantTimeCompare for == or
// crypto/rand for math/rand leaves every other test green while destroying the
// guarantee — so they are checked at the source, which is cheap and exact.
func source(t *testing.T, file string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	require.NoError(t, err)
	return string(src)
}

// requireSource asserts without dumping the whole file into the failure.
func requireSource(t *testing.T, ok bool, msg string) {
	t.Helper()
	if !ok {
		t.Fatal(msg)
	}
}

// TestServiceTokenAuthenticatesWithScopes is the core of the built-in
// provider: a token issued to a service comes back carrying exactly the
// scopes and tenant it was issued for.
func TestServiceTokenAuthenticatesWithScopes(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	issued := identity.Principal{
		Subject:  "ci-runner",
		TenantID: "acme",
		Scopes:   []string{"pipelines:write"},
		Kind:     identity.PrincipalService,
	}
	token, err := local.IssueToken(ctx, issued, time.Hour)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	got, err := local.Authenticate(ctx, token)
	require.NoError(t, err)
	require.Equal(t, "ci-runner", got.Subject)
	require.Equal(t, "acme", got.TenantID)
	require.Equal(t, []string{"pipelines:write"}, got.Scopes)
	require.Equal(t, identity.PrincipalService, got.Kind)
}

// TestExpiredTokenIsRejected: expiry is enforced at authentication time, not
// only by a cleanup job that may never have run.
func TestExpiredTokenIsRejected(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	token, err := local.IssueToken(ctx, identity.Principal{
		Subject:  "ci-runner",
		TenantID: "acme",
		Scopes:   []string{"pipelines:write"},
		Kind:     identity.PrincipalService,
	}, -1*time.Second)
	require.NoError(t, err)

	_, err = local.Authenticate(ctx, token)
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrExpired)
}

// TestPasswordsAreStoredAsArgon2idNotPlaintext: a stolen database must not be
// a list of passwords.
func TestPasswordsAreStoredAsArgon2idNotPlaintext(t *testing.T) {
	ctx := context.Background()
	local, store, _ := newLocal(t)

	const password = "correct-horse-battery-staple"
	require.NoError(t, local.CreateUser(ctx, "acme", "alice", password))

	stored, err := store.PrincipalCredential(ctx, "acme", "alice")
	require.NoError(t, err)
	require.NotContains(t, stored.CredentialHash, password)
	require.True(t, strings.HasPrefix(stored.CredentialHash, "$argon2id$"),
		"stored credential %q must be a PHC argon2id encoding", stored.CredentialHash)

	// The encoding carries its own parameters and salt so they can be raised
	// later without invalidating hashes already stored.
	require.Contains(t, stored.CredentialHash, "$v=19$")
	require.Contains(t, stored.CredentialHash, "m=")
	require.Contains(t, stored.CredentialHash, "t=")
	require.Contains(t, stored.CredentialHash, "p=")
	require.Len(t, strings.Split(stored.CredentialHash, "$"), 6)
}

// TestPasswordCredentialAuthenticates proves the argon2id encoding round-trips.
func TestPasswordCredentialAuthenticates(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	require.NoError(t, local.CreateUser(ctx, "acme", "alice", "correct-horse"))

	got, err := local.Authenticate(ctx, identity.PasswordCredential("acme", "alice", "correct-horse"))
	require.NoError(t, err)
	require.Equal(t, "alice", got.Subject)
	require.Equal(t, "acme", got.TenantID)
	require.Equal(t, identity.PrincipalUser, got.Kind)
}

// TestAuthenticationFailureDoesNotRevealWhichPartFailed: an unknown subject
// and a wrong password must be indistinguishable, or the endpoint becomes a
// user-enumeration oracle.
func TestAuthenticationFailureDoesNotRevealWhichPartFailed(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	require.NoError(t, local.CreateUser(ctx, "acme", "alice", "correct-horse"))

	_, wrongPassword := local.Authenticate(ctx, identity.PasswordCredential("acme", "alice", "wrong"))
	_, unknownSubject := local.Authenticate(ctx, identity.PasswordCredential("acme", "mallory", "wrong"))

	require.Error(t, wrongPassword)
	require.Error(t, unknownSubject)
	require.Equal(t, wrongPassword.Error(), unknownSubject.Error())
	require.ErrorIs(t, wrongPassword, identity.ErrUnauthenticated)
	require.ErrorIs(t, unknownSubject, identity.ErrUnauthenticated)
	require.NotContains(t, wrongPassword.Error(), "alice")
	require.NotContains(t, unknownSubject.Error(), "mallory")
}

// TestTokensAreStoredOnlyAsHashes: a tokens table that can be read back is a
// plaintext credential database.
func TestTokensAreStoredOnlyAsHashes(t *testing.T) {
	ctx := context.Background()
	local, _, db := newLocal(t)

	token, err := local.IssueToken(ctx, identity.Principal{
		Subject: "ci-runner", TenantID: "acme",
		Scopes: []string{"pipelines:write"}, Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	secret := tokenSecret(token)
	require.NotEmpty(t, secret)

	// Read every column of every row back as text and look for the secret.
	rows, err := db.QueryContext(ctx,
		`SELECT tenant_id || '|' || subject || '|' || token_hash || '|' || scopes || '|' || expires_at FROM tokens`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var seen int
	for rows.Next() {
		var row string
		require.NoError(t, rows.Scan(&row))
		seen++
		require.NotContains(t, row, secret, "the raw token secret must never be stored")
		require.NotContains(t, row, token, "the raw token must never be stored")
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 1, seen)

	// The stored form must still be able to authenticate the token, so this is
	// a hash and not simply a dropped column.
	p, err := local.Authenticate(ctx, token)
	require.NoError(t, err)
	require.Equal(t, "ci-runner", p.Subject)
}

// TestTokenOfAnotherTenantIsRejected: every lookup is tenant-scoped, so a
// token whose tenant does not hold it cannot authenticate.
func TestTokenOfAnotherTenantIsRejected(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	token, err := local.IssueToken(ctx, identity.Principal{
		Subject: "ci-runner", TenantID: "acme",
		Scopes: []string{"pipelines:write"}, Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	// Same secret, another tenant's namespace.
	forged := strings.Replace(token, "acme", "evilcorp", 1)
	require.NotEqual(t, token, forged)
	_, err = local.Authenticate(ctx, forged)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

// TestUnknownTokenIsRejected: a well-shaped token nobody issued is refused.
func TestUnknownTokenIsRejected(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	_, err := local.Authenticate(ctx,
		"dht_acme_"+strings.Repeat("ab", 32))
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

// TestEmptyTenantIsRejected: there is no unscoped identity operation, in line
// with the rest of the system.
func TestEmptyTenantIsRejected(t *testing.T) {
	ctx := context.Background()
	local, store, _ := newLocal(t)

	_, err := local.IssueToken(ctx, identity.Principal{
		Subject: "ci-runner", Kind: identity.PrincipalService,
	}, time.Hour)
	require.ErrorContains(t, err, "tenant scope required")

	err = local.CreateUser(ctx, "", "alice", "correct-horse")
	require.ErrorContains(t, err, "tenant scope required")

	_, err = local.Authenticate(ctx, identity.PasswordCredential("", "alice", "correct-horse"))
	require.ErrorContains(t, err, "tenant scope required")

	_, err = store.PrincipalCredential(ctx, "", "alice")
	require.ErrorContains(t, err, "tenant scope required")

	_, err = store.Token(ctx, "", "somehash")
	require.ErrorContains(t, err, "tenant scope required")
}

// recordingStore is a Store that enforces nothing, so a tenant check has to be
// the provider's own. Without it, the SQL store's guard masks a missing guard
// in Local and the two cannot be told apart.
type recordingStore struct{ called bool }

func (r *recordingStore) PutPrincipal(context.Context, identity.StoredPrincipal) error {
	r.called = true
	return nil
}

func (r *recordingStore) EnsurePrincipal(context.Context, identity.StoredPrincipal) error {
	r.called = true
	return nil
}

func (r *recordingStore) PrincipalCredential(context.Context, string, string) (identity.StoredPrincipal, error) {
	r.called = true
	return identity.StoredPrincipal{}, identity.ErrNotFound
}

func (r *recordingStore) PutToken(context.Context, identity.StoredToken) error {
	r.called = true
	return nil
}

func (r *recordingStore) Token(context.Context, string, string) (identity.StoredToken, error) {
	r.called = true
	return identity.StoredToken{}, identity.ErrNotFound
}

// TestEmptyTenantIsRejectedBeforeTheStoreIsTouched: the provider refuses an
// unscoped operation itself. Relying on the store to notice would leave any
// other Store implementation unprotected.
func TestEmptyTenantIsRejectedBeforeTheStoreIsTouched(t *testing.T) {
	ctx := context.Background()

	for name, call := range map[string]func(*identity.Local) error{
		"IssueToken": func(l *identity.Local) error {
			_, err := l.IssueToken(ctx, identity.Principal{
				Subject: "ci-runner", Kind: identity.PrincipalService,
			}, time.Hour)
			return err
		},
		"CreateUser": func(l *identity.Local) error {
			return l.CreateUser(ctx, "", "alice", "correct-horse")
		},
		"Authenticate/password": func(l *identity.Local) error {
			_, err := l.Authenticate(ctx, identity.PasswordCredential("", "alice", "correct-horse"))
			return err
		},
		"Authenticate/token": func(l *identity.Local) error {
			_, err := l.Authenticate(ctx, "dht__"+strings.Repeat("ab", 32))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := &recordingStore{}
			err := call(identity.NewLocal(store))
			require.ErrorIs(t, err, identity.ErrTenantRequired)
			require.ErrorContains(t, err, "tenant scope required")
			require.False(t, store.called, "the store must not be reached for an unscoped operation")
		})
	}
}

// TestUnrecognisedCredentialIsNotClaimed: the local provider must say "not
// mine" rather than "denied" for a shape it does not handle, so Task 24's
// chain can hand the credential to the next provider.
func TestUnrecognisedCredentialIsNotClaimed(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	for _, credential := range []string{
		"",
		"eyJhbGciOiJSUzI1NiJ9.e30.sig",
		"Bearer something",
	} {
		_, err := local.Authenticate(ctx, credential)
		require.ErrorIs(t, err, identity.ErrCredentialFormat, "credential %q", credential)
	}
}

// TestIssuedTokensAreDistinct: 32 bytes of real entropy per token. A seeded or
// weak generator collides or repeats across providers.
func TestIssuedTokensAreDistinct(t *testing.T) {
	ctx := context.Background()
	local, _, _ := newLocal(t)

	seen := make(map[string]bool, 256)
	for range 256 {
		token, err := local.IssueToken(ctx, identity.Principal{
			Subject: "ci-runner", TenantID: "acme", Kind: identity.PrincipalService,
		}, time.Hour)
		require.NoError(t, err)
		secret := tokenSecret(token)
		require.Len(t, secret, 64, "32 random bytes, hex encoded")
		require.False(t, seen[secret], "token secret repeated: %q", secret)
		seen[secret] = true
	}

	// A second provider, freshly constructed, must not replay the first
	// provider's sequence — as it would with a package-level seeded PRNG.
	other, _, _ := newLocal(t)
	token, err := other.IssueToken(ctx, identity.Principal{
		Subject: "ci-runner", TenantID: "acme", Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)
	require.False(t, seen[tokenSecret(token)])
}

func TestSecretsAreComparedInConstantTime(t *testing.T) {
	src := source(t, "token.go")
	requireSource(t, strings.Contains(src, "crypto/subtle"),
		"token.go must import crypto/subtle")
	requireSource(t, strings.Contains(src, "subtle.ConstantTimeCompare"),
		"token.go must compare secret material with subtle.ConstantTimeCompare, never ==")
}

func TestRandomnessComesFromCryptoRand(t *testing.T) {
	for _, file := range []string{"identity.go", "local.go", "token.go"} {
		src := source(t, file)
		requireSource(t, !strings.Contains(src, `"math/rand"`) &&
			!strings.Contains(src, `"math/rand/v2"`),
			file+" must not use math/rand: token secrets and salts come from crypto/rand")
	}
	requireSource(t, strings.Contains(source(t, "token.go"), `"crypto/rand"`),
		"token.go must draw its randomness from crypto/rand")
}

// TestCryptoRandFailureIsFatal: a token is never issued from degraded
// randomness. There is no weaker source to reach for.
func TestCryptoRandFailureIsFatal(t *testing.T) {
	src := source(t, "token.go")
	requireSource(t, strings.Contains(src, "rand.Read"), "token.go must read from crypto/rand")
	requireSource(t, !strings.Contains(src, "fallback"),
		"a crypto/rand failure is fatal to the operation; there is no fallback")
}

// TestProviderInterfaceIsSatisfied pins the shape Task 24's chain consumes.
func TestProviderInterfaceIsSatisfied(t *testing.T) {
	store, _ := newStore(t)
	var p identity.Provider = identity.NewLocal(store)
	require.NotNil(t, p)
	_, err := p.Authenticate(context.Background(), "not-a-dhole-credential")
	require.True(t, errors.Is(err, identity.ErrCredentialFormat))
}

// TestAnIssuedTokenMakesItsSubjectAPrincipalOfTheTenant: a credential's
// identity and an approver's identity used to live in different tables.
// IssueToken wrote `tokens`, every "is this a principal of this tenant" check
// reads `principals`, and a token minted the only way `dhole token issue`
// offers therefore authenticated every API call and was then refused by
// approval.Decide as "not a principal of tenant". One credential, two answers
// about who holds it.
func TestAnIssuedTokenMakesItsSubjectAPrincipalOfTheTenant(t *testing.T) {
	ctx := context.Background()
	local, store, _ := newLocal(t)

	_, err := local.IssueToken(ctx, identity.Principal{
		TenantID: "acme", Subject: "runner", Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	stored, err := store.PrincipalCredential(ctx, "acme", "runner")
	require.NoError(t, err, "the subject a token was issued to is not a principal of its tenant")
	require.Equal(t, identity.PrincipalService, stored.Kind)
	require.Empty(t, stored.CredentialHash,
		"a principal that authenticates by token was given a password hash")

	// And only within its own tenant: the same subject elsewhere is a
	// different person, and a token cannot establish them there.
	_, err = store.PrincipalCredential(ctx, "other", "runner")
	require.ErrorIs(t, err, identity.ErrNotFound)
}

// TestIssuingATokenToAPersonDoesNotDestroyTheirPassword: establishing the
// principal at the mint must not be an upsert. PutPrincipal replaces
// credential_hash, so issuing a service token to a human subject would have
// silently wiped the password they log in with — a fix for one identity
// problem creating a worse one.
func TestIssuingATokenToAPersonDoesNotDestroyTheirPassword(t *testing.T) {
	ctx := context.Background()
	local, store, _ := newLocal(t)

	require.NoError(t, local.CreateUser(ctx, "acme", "ada", "correct horse"))
	before, err := store.PrincipalCredential(ctx, "acme", "ada")
	require.NoError(t, err)

	_, err = local.IssueToken(ctx, identity.Principal{
		TenantID: "acme", Subject: "ada", Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	after, err := store.PrincipalCredential(ctx, "acme", "ada")
	require.NoError(t, err)
	require.Equal(t, before.CredentialHash, after.CredentialHash,
		"issuing a token overwrote the principal's password")
	require.Equal(t, identity.PrincipalUser, after.Kind,
		"issuing a token demoted a person to a service")

	// The password still works, which is the property the hash comparison
	// above is only evidence for.
	who, err := local.Authenticate(ctx, identity.PasswordCredential("acme", "ada", "correct horse"))
	require.NoError(t, err)
	require.Equal(t, "ada", who.Subject)
}
