package identity

import (
	"context"
	"errors"
)

// chain tries providers in order, and stops at the first one that owns the
// credential.
type chain struct {
	providers []Provider
}

var _ Provider = (*chain)(nil)

// Chain composes providers into one, selecting by credential shape.
//
// The forwarding rule is the whole security property of this package, and it
// is deliberately narrow: only ErrCredentialFormat — "this is not a credential
// I issue" — moves to the next provider. An ErrUnauthenticated is a provider
// that recognised the credential and refused it, and retrying that elsewhere
// would let a rejected federated token be re-presented to a weaker provider.
// Any other error is a failure to reach a decision at all — an unreachable
// IdP, a database that will not answer — and passing it on would turn an
// outage into a downgrade: exactly what an attacker who can partition the
// network would arrange. Both stop the chain.
//
// The cost of that strictness is the reason the chain exists: while the IdP is
// down, an OIDC credential fails, but a local service token is a shape the
// OIDC provider never claims, so automation keeps running.
func Chain(providers ...Provider) Provider {
	return &chain{providers: providers}
}

// Authenticate returns the first successful result, or the first refusal.
//
// With nothing left to try the answer is ErrCredentialFormat: no provider
// claimed the credential, so the caller learns the shape was unrecognised
// rather than that some account was denied.
func (c *chain) Authenticate(ctx context.Context, credential string) (Principal, error) {
	for _, p := range c.providers {
		principal, err := p.Authenticate(ctx, credential)
		switch {
		case err == nil:
			return principal, nil
		case errors.Is(err, ErrCredentialFormat):
			continue
		default:
			return Principal{}, err
		}
	}
	return Principal{}, ErrCredentialFormat
}
