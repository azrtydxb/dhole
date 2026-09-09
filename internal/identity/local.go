package identity

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// Argon2id parameters.
//
// These are RFC 9106's second recommended configuration: 64 MiB of memory,
// three passes, four lanes. The memory cost is the one that matters — it is
// what denies a GPU or ASIC attacker the parallelism that makes a stolen
// password database cheap to crack — and 64 MiB is affordable on a control
// plane that authenticates interactively rather than in a hot loop. The salt
// is 16 bytes because that is the size below which precomputation starts to
// pay off, and the derived key is 32 bytes to match the 256-bit security level
// the rest of the system works at.
//
// They are not frozen: every stored hash carries the parameters and the salt
// it was produced with, so raising them later re-verifies old hashes correctly
// and only new writes get the new cost.
const (
	argon2Time    uint32 = 3
	argon2Memory  uint32 = 64 * 1024 // KiB
	argon2Threads uint8  = 4
	argon2KeyLen  uint32 = 32
	argon2SaltLen        = 16

	// The range of derived-key lengths a stored hash may declare: 16 bytes is
	// the floor below which the hash itself is guessable, 64 the ceiling
	// beyond which no configuration of this system has any reason to go.
	argon2MinKeyLen = 16
	argon2MaxKeyLen = 64

	// argon2Version is the algorithm version the PHC string records. argon2.IDKey
	// implements 0x13 (19).
	argon2Version = argon2.Version
)

// passwordPrefix marks a local username/password credential. Task 24 chains
// this provider with OIDC, and the chain picks a provider by credential shape,
// so a local credential has to be recognisable without being tried.
const passwordPrefix = "local:"

// PasswordCredential builds the credential string a caller presents for a
// local password login. Tenant and subject may not contain a colon; the
// password may, since it is whatever remains.
func PasswordCredential(tenantID, subject, password string) string {
	return passwordPrefix + tenantID + ":" + subject + ":" + password
}

// Local is the built-in identity provider: local passwords and service tokens,
// backed by the run store's database. It is the provider that has to keep
// working when an external IdP is unreachable, which is why it depends on
// nothing but the store.
type Local struct {
	store Store
}

// Compile-time proof of the shape Task 24's chain consumes.
var _ Provider = (*Local)(nil)

// NewLocal returns the built-in provider over store.
func NewLocal(store Store) *Local {
	return &Local{store: store}
}

// CreateUser registers a human principal with a password credential. The
// password is never stored; only its argon2id hash is.
func (l *Local) CreateUser(ctx context.Context, tenantID, subject, password string) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	if subject == "" {
		return errors.New("subject required")
	}
	if strings.ContainsRune(tenantID, ':') || strings.ContainsRune(subject, ':') {
		return errors.New("tenant and subject must not contain a colon")
	}
	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	return l.store.PutPrincipal(ctx, StoredPrincipal{
		TenantID:       tenantID,
		Subject:        subject,
		Kind:           PrincipalUser,
		CredentialHash: hash,
	})
}

// IssueToken mints a service token for p, valid for ttl, and returns it. The
// returned string is the only copy that will ever exist: the store keeps its
// hash, so a token cannot be recovered from a database, only replaced.
func (l *Local) IssueToken(ctx context.Context, p Principal, ttl time.Duration) (string, error) {
	if p.TenantID == "" {
		return "", ErrTenantRequired
	}
	if p.Subject == "" {
		return "", errors.New("subject required")
	}
	secret, err := newTokenSecret()
	if err != nil {
		return "", err
	}
	if err := l.store.PutToken(ctx, StoredToken{
		TenantID:  p.TenantID,
		Subject:   p.Subject,
		Hash:      hashToken(secret),
		Scopes:    p.Scopes,
		ExpiresAt: time.Now().Add(ttl).UTC(),
	}); err != nil {
		return "", err
	}
	return formatToken(p.TenantID, secret), nil
}

// Authenticate resolves a service token or a local password credential.
//
// A credential of neither shape yields ErrCredentialFormat — "not mine" — so a
// chain can hand it on. Anything this provider does own and refuses yields the
// same ErrUnauthenticated regardless of why, except expiry.
func (l *Local) Authenticate(ctx context.Context, credential string) (Principal, error) {
	if tenantID, secret, ok := parseToken(credential); ok {
		return l.authenticateToken(ctx, tenantID, secret)
	}
	if rest, ok := strings.CutPrefix(credential, passwordPrefix); ok {
		return l.authenticatePassword(ctx, rest)
	}
	return Principal{}, ErrCredentialFormat
}

func (l *Local) authenticateToken(ctx context.Context, tenantID, secret string) (Principal, error) {
	if tenantID == "" {
		return Principal{}, ErrTenantRequired
	}
	computed := hashToken(secret)

	// The indexed lookup narrows to at most one row; the constant-time
	// comparison below is what actually decides, so a partial match learns an
	// attacker nothing from timing.
	stored, err := l.store.Token(ctx, tenantID, computed)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Principal{}, ErrUnauthenticated
		}
		return Principal{}, err
	}
	if !secretsEqual([]byte(stored.Hash), []byte(computed)) {
		return Principal{}, ErrUnauthenticated
	}
	if !time.Now().Before(stored.ExpiresAt) {
		return Principal{}, ErrExpired
	}
	return Principal{
		Subject:  stored.Subject,
		TenantID: stored.TenantID,
		Scopes:   stored.Scopes,
		Kind:     PrincipalService,
	}, nil
}

func (l *Local) authenticatePassword(ctx context.Context, rest string) (Principal, error) {
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) != 3 {
		return Principal{}, ErrCredentialFormat
	}
	tenantID, subject, password := parts[0], parts[1], parts[2]
	if tenantID == "" {
		return Principal{}, ErrTenantRequired
	}

	stored, err := l.store.PrincipalCredential(ctx, tenantID, subject)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// Verify against a decoy of the same cost before refusing. Without
			// it, an unknown subject returns in microseconds and a known one
			// in tens of milliseconds, and the difference is a user list.
			_, _ = verifyPassword(decoyHash(), password)
			return Principal{}, ErrUnauthenticated
		}
		return Principal{}, err
	}

	ok, err := verifyPassword(stored.CredentialHash, password)
	if err != nil || !ok {
		// A malformed stored hash is an operator problem, not something to
		// report to whoever is trying to log in.
		return Principal{}, ErrUnauthenticated
	}
	return Principal{
		Subject:  stored.Subject,
		TenantID: stored.TenantID,
		Kind:     stored.Kind,
	}, nil
}

// hashPassword derives a PHC-encoded argon2id string:
//
//	$argon2id$v=19$m=65536,t=3,p=4$<salt>$<key>
//
// The parameters and salt travel with the hash so they can be changed without
// invalidating what is already stored.
func hashPassword(password string) (string, error) {
	salt := make([]byte, argon2SaltLen)
	if _, err := randRead(salt); err != nil {
		return "", fmt.Errorf("generate password salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2Version, argon2Memory, argon2Time, argon2Threads,
		enc.EncodeToString(salt), enc.EncodeToString(key)), nil
}

// verifyPassword recomputes the key with the parameters recorded in the stored
// encoding — not with the current constants — so raising the cost later does
// not lock existing principals out.
func verifyPassword(encoded, password string) (bool, error) {
	fields := strings.Split(encoded, "$")
	if len(fields) != 6 || fields[0] != "" || fields[1] != "argon2id" {
		return false, errors.New("stored credential is not an argon2id PHC string")
	}
	var version int
	if _, err := fmt.Sscanf(fields[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("stored credential version: %w", err)
	}
	if version != argon2Version {
		return false, fmt.Errorf("unsupported argon2 version %d", version)
	}
	var (
		memory  uint32
		timeCst uint32
		threads uint8
	)
	if _, err := fmt.Sscanf(fields[3], "m=%d,t=%d,p=%d", &memory, &timeCst, &threads); err != nil {
		return false, fmt.Errorf("stored credential parameters: %w", err)
	}
	enc := base64.RawStdEncoding
	salt, err := enc.DecodeString(fields[4])
	if err != nil {
		return false, fmt.Errorf("stored credential salt: %w", err)
	}
	want, err := enc.DecodeString(fields[5])
	if err != nil {
		return false, fmt.Errorf("stored credential key: %w", err)
	}
	// The derived key is recomputed at the length recorded in the stored hash,
	// which is what lets argon2KeyLen change later. The bound is not a format
	// nicety: it keeps a corrupt or hostile row from asking argon2 for a
	// gigabyte of output, and it makes the width conversion below provably safe.
	if len(want) < argon2MinKeyLen || len(want) > argon2MaxKeyLen {
		return false, errors.New("stored credential key length out of range")
	}
	// G115: len(want) is bounded to [argon2MinKeyLen, argon2MaxKeyLen] by the
	// check immediately above, so this conversion cannot overflow. gosec does
	// not follow the guard; the guard, not this directive, is what makes it
	// safe, and the directive is scoped to this one line.
	keyLen := uint32(len(want)) //nolint:gosec // bounded above; see comment
	got := argon2.IDKey([]byte(password), salt, timeCst, memory, threads, keyLen)
	return secretsEqual(got, want), nil
}

// decoyHash is a real argon2id hash of a value nobody holds, used to give an
// unknown subject the same cost as a known one. It is derived once; deriving it
// per attempt would make the unknown-subject path the slower of the two.
var decoyHash = sync.OnceValue(func() string {
	// A failure here would only weaken the timing defence, never admit anyone,
	// so an unusable decoy degrades to a hash that simply never verifies.
	encoded, err := hashPassword("decoy: no principal holds this password")
	if err != nil {
		return "$argon2id$"
	}
	return encoded
})

// -- SQL-backed store -------------------------------------------------------

// SQLStore persists principals and tokens in the same database as the run
// store, whose migration runner applies 0004_identity.sql. The statements
// below are written once, with `?`, and rebound for the dialect in hand:
// pgx rejects `?` outright, and a second per-dialect copy of each statement
// would be free to drift.
type SQLStore struct {
	db      *sql.DB
	dialect runstore.Dialect
}

var _ Store = (*SQLStore)(nil)

// NewSQLStore returns a Store over an already-migrated SQLite handle. It is
// the convenience form of NewSQLStoreWithDialect and nothing more: a Postgres
// deployment calls NewSQLStoreWithDialect, because pgx rejects the `?`
// placeholders these statements are written with.
func NewSQLStore(db *sql.DB) *SQLStore {
	return NewSQLStoreWithDialect(db, runstore.DialectSQLite)
}

// NewSQLStoreWithDialect returns a Store over an already-migrated handle
// speaking dialect. The credential database is the one place where "works only
// on the development store" locks every operator out of the deployment that
// matters, so the dialect is explicit rather than assumed.
func NewSQLStoreWithDialect(db *sql.DB, dialect runstore.Dialect) *SQLStore {
	return &SQLStore{db: db, dialect: dialect}
}

// timeFormat matches the run store's: RFC3339 with nanoseconds in UTC sorts
// lexicographically in the same order it sorts chronologically.
const timeFormat = time.RFC3339Nano

// PutPrincipal writes or replaces a principal.
func (s *SQLStore) PutPrincipal(ctx context.Context, p StoredPrincipal) error {
	if p.TenantID == "" {
		return ErrTenantRequired
	}
	const q = `INSERT INTO principals (tenant_id, subject, kind, credential_hash)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (tenant_id, subject) DO UPDATE SET
			kind = excluded.kind, credential_hash = excluded.credential_hash`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q), p.TenantID, p.Subject, string(p.Kind), p.CredentialHash); err != nil {
		return fmt.Errorf("put principal: %w", err)
	}
	return nil
}

// PrincipalCredential returns one principal, or ErrNotFound.
func (s *SQLStore) PrincipalCredential(ctx context.Context, tenantID, subject string) (StoredPrincipal, error) {
	if tenantID == "" {
		return StoredPrincipal{}, ErrTenantRequired
	}
	const q = `SELECT kind, credential_hash FROM principals
		WHERE tenant_id = ? AND subject = ?`
	p := StoredPrincipal{TenantID: tenantID, Subject: subject}
	var kind string
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, subject).Scan(&kind, &p.CredentialHash)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return StoredPrincipal{}, ErrNotFound
	case err != nil:
		return StoredPrincipal{}, fmt.Errorf("read principal: %w", err)
	}
	p.Kind = Kind(kind)
	return p, nil
}

// PutToken records an issued token by its hash.
func (s *SQLStore) PutToken(ctx context.Context, t StoredToken) error {
	if t.TenantID == "" {
		return ErrTenantRequired
	}
	scopes, err := json.Marshal(t.Scopes)
	if err != nil {
		return fmt.Errorf("encode token scopes: %w", err)
	}
	const q = `INSERT INTO tokens (tenant_id, subject, token_hash, scopes, expires_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (tenant_id, token_hash) DO NOTHING`
	if _, err := s.db.ExecContext(ctx, s.dialect.Rebind(q),
		t.TenantID, t.Subject, t.Hash, string(scopes), t.ExpiresAt.UTC().Format(timeFormat)); err != nil {
		return fmt.Errorf("put token: %w", err)
	}
	return nil
}

// Token returns the token with that hash within the tenant, or ErrNotFound.
func (s *SQLStore) Token(ctx context.Context, tenantID, tokenHash string) (StoredToken, error) {
	if tenantID == "" {
		return StoredToken{}, ErrTenantRequired
	}
	const q = `SELECT subject, token_hash, scopes, expires_at FROM tokens
		WHERE tenant_id = ? AND token_hash = ?`
	t := StoredToken{TenantID: tenantID}
	var (
		scopes  string
		expires string
	)
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(q), tenantID, tokenHash).Scan(&t.Subject, &t.Hash, &scopes, &expires)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return StoredToken{}, ErrNotFound
	case err != nil:
		return StoredToken{}, fmt.Errorf("read token: %w", err)
	}
	if err := json.Unmarshal([]byte(scopes), &t.Scopes); err != nil {
		return StoredToken{}, fmt.Errorf("decode token scopes: %w", err)
	}
	if t.ExpiresAt, err = time.Parse(timeFormat, expires); err != nil {
		return StoredToken{}, fmt.Errorf("parse token expiry: %w", err)
	}
	return t, nil
}
