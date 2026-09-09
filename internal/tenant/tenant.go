// Package tenant is the tenancy boundary: the single place that says what a
// tenant id may be, how it travels through a call, and what it names on the
// bus.
//
// Every store in this tree already refuses an empty tenant on its own. This
// package exists for the property no single store can state: that the SYSTEM
// is scoped, that a tenant id cannot be forged into a wildcard, and that a
// context which never carried a tenant cannot be mistaken for one that carries
// none. Tenancy is in the data model from the first commit (ADR 0014), and a
// hole opened here is one that has to be found in production.
package tenant

import (
	"context"
	"errors"
	"fmt"
)

// ErrNoTenant reports a context with no tenant in it. The message matches
// runstore.ErrTenantRequired deliberately: the same failure should read the
// same way wherever it surfaces.
var ErrNoTenant = errors.New("tenant scope required")

// ErrInvalidTenant reports an id that cannot be used as a scope. Such an id is
// REFUSED, never sanitised: quietly rewriting `a>` into `a` would hand the
// caller a different tenant's data and look like it worked.
var ErrInvalidTenant = errors.New("tenant: invalid tenant id")

// maxIDLen bounds an id so it cannot overflow a subject token or an account
// name in any downstream system.
const maxIDLen = 63

// accountPrefix namespaces a tenant's NATS account. It is a fixed prefix and
// the id is appended whole, so the mapping id -> account name is injective:
// two tenants can never resolve to one account.
const accountPrefix = "tenant-"

// contextKey is unexported so nothing outside this package can put a tenant
// into a context without going through WithTenant.
type contextKey struct{}

// WithTenant returns a context carrying id.
//
// It does NOT validate: validation happens in FromContext, so there is exactly
// one place where a tenant becomes usable and no path that skips it. An id
// this package would refuse can be put into a context; it can never be taken
// back out.
func WithTenant(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext returns the tenant the context is scoped to.
//
// A context with no tenant is an ERROR, never ("", nil). That distinction is
// the whole point: an empty string with a nil error is how an unscoped query
// gets written by accident — the caller passes it on, the store's own guard is
// the only thing left, and any store that forgets one reads every tenant's
// rows.
func FromContext(ctx context.Context) (string, error) {
	id, ok := ctx.Value(contextKey{}).(string)
	if !ok {
		return "", ErrNoTenant
	}
	if err := Validate(id); err != nil {
		return "", err
	}
	return id, nil
}

// Validate reports whether id may be used as a tenant scope.
//
// The character set is narrow on purpose. A `*` or a `>` in an id becomes a
// wildcard the moment the id reaches a NATS subject; a `.` splits one subject
// token into two; a `/` or a `\` escapes a storage path; and the account-name
// separator would let one tenant name another's account. None of these are
// hypothetical — they are the id an attacker chooses.
func Validate(id string) error {
	if id == "" {
		return fmt.Errorf("%w: %w", ErrInvalidTenant, ErrNoTenant)
	}
	if len(id) > maxIDLen {
		return fmt.Errorf("%w: longer than %d characters", ErrInvalidTenant, maxIDLen)
	}
	for i, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case (r == '-' || r == '_') && i > 0:
		default:
			return fmt.Errorf("%w: %q contains %q, which is not [a-z0-9] or a non-leading '-' or '_'",
				ErrInvalidTenant, id, r)
		}
	}
	return nil
}

// AccountName is the NATS account one tenant's traffic lives in. Accounts are
// how tenancy reaches the bus (ADR 0014): an account is its own subject
// namespace, so the same subject string in two accounts is two different
// subjects and isolation is the server's job rather than the control plane's
// good manners.
//
// It returns the empty string for an id Validate refuses, which is not a usable
// account name anywhere: ProvisionAccount returns the reason instead.
func AccountName(tenantID string) string {
	if Validate(tenantID) != nil {
		return ""
	}
	return accountPrefix + tenantID
}
