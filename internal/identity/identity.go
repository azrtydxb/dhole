// Package identity authenticates callers of the control plane.
//
// A Provider turns an opaque credential string into a Principal. The
// credential is deliberately untyped: the built-in provider here handles
// service tokens and local passwords, OIDC federation handles id tokens, and
// a chain of the two decides by shape which provider owns a given credential.
// A provider that does not recognise a credential returns ErrCredentialFormat
// so the chain can try the next one; a provider that does recognise it and
// refuses returns ErrUnauthenticated, which the chain must not paper over.
//
// Every operation is tenant-scoped. There is no unscoped identity lookup, even
// while only one tenant exists: an empty tenant is a bug in the caller, never
// a wildcard.
package identity

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Kind separates a human principal from a machine one. The values are stored
// verbatim, so they are a persistence contract: add new ones, never rename an
// existing one.
type Kind string

// The kinds of principal the system authenticates.
const (
	PrincipalUser    Kind = "user"
	PrincipalService Kind = "service"
)

// Principal is an authenticated caller. It is what authorisation decisions are
// made about, so it carries the tenant it belongs to rather than leaving the
// caller to supply one.
type Principal struct {
	Subject  string
	TenantID string
	Scopes   []string
	Kind     Kind
}

// Provider authenticates a credential.
//
// Authenticate returns ErrCredentialFormat when the credential is not one this
// provider issues — that is "not mine", not "denied" — and an error wrapping
// ErrUnauthenticated when it is one and fails. It must not distinguish, to the
// caller, between an unknown subject and a bad secret.
type Provider interface {
	Authenticate(ctx context.Context, credential string) (Principal, error)
}

// The errors every provider shares.
var (
	// ErrTenantRequired is returned by every operation given an empty tenant.
	ErrTenantRequired = errors.New("tenant scope required")

	// ErrUnauthenticated is the single, uniform refusal. Unknown subject and
	// wrong secret both produce exactly this, because a caller who can tell
	// them apart has a user-enumeration oracle.
	ErrUnauthenticated = errors.New("authentication failed")

	// ErrExpired is a refusal that is safe to distinguish: the caller learns
	// only that a credential it already held has aged out, which it needs to
	// know in order to renew rather than retry. It wraps ErrUnauthenticated
	// so a caller checking only for refusal still sees one.
	ErrExpired = fmt.Errorf("%w: credential expired", ErrUnauthenticated)

	// ErrCredentialFormat means this provider does not issue credentials of
	// that shape. A chain treats it as "ask the next provider".
	ErrCredentialFormat = errors.New("unrecognised credential format")

	// ErrNotFound is returned by a Store for a record that does not exist. It
	// never reaches a caller of Authenticate: providers collapse it into
	// ErrUnauthenticated.
	ErrNotFound = errors.New("identity record not found")
)

// StoredPrincipal is a principals row.
type StoredPrincipal struct {
	TenantID string
	Subject  string
	Kind     Kind
	// CredentialHash is a PHC-encoded argon2id string, or empty for a
	// principal that has no password credential.
	CredentialHash string
}

// StoredToken is a tokens row. It holds the hash of a service token, never the
// token itself.
type StoredToken struct {
	TenantID  string
	Subject   string
	Hash      string
	Scopes    []string
	ExpiresAt time.Time
}

// Store persists principals and service tokens. Every method takes an explicit
// tenant and rejects an empty one with ErrTenantRequired. Implementations are
// safe for concurrent use.
type Store interface {
	// PutPrincipal writes or replaces a principal.
	PutPrincipal(ctx context.Context, p StoredPrincipal) error

	// EnsurePrincipal records a principal only if the tenant does not
	// already have one with that subject, and leaves an existing row exactly
	// as it stands.
	//
	// It is separate from PutPrincipal because the upsert there replaces
	// credential_hash, and issuing a service token to a subject who also has
	// a password would then wipe that password. "Establish this identity if
	// it does not exist" and "set this identity's credential" are two
	// different intentions and only one of them is safe to perform on a
	// principal somebody else created.
	EnsurePrincipal(ctx context.Context, p StoredPrincipal) error

	// PrincipalCredential returns one principal, or ErrNotFound.
	PrincipalCredential(ctx context.Context, tenantID, subject string) (StoredPrincipal, error)

	// PutToken records an issued token by its hash.
	PutToken(ctx context.Context, t StoredToken) error

	// Token returns the token with that hash, or ErrNotFound. Expiry is the
	// provider's decision, not the store's: the row is returned either way.
	Token(ctx context.Context, tenantID, tokenHash string) (StoredToken, error)
}
