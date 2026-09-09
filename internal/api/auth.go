package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"

	"github.com/azrtydxb/dhole/internal/identity"
)

// bearerScheme is the only authorization scheme this API accepts. It is
// compared case-insensitively because RFC 7235 says the scheme is.
const bearerScheme = "bearer"

// errNoCredential is the refusal for a call that presented nothing. It is
// deliberately indistinguishable, to the caller, from a credential that was
// presented and refused: a caller who can tell the two apart learns which
// tokens exist.
var errNoCredential = errors.New(
	"api: an Authorization: Bearer credential is required; this API has no unauthenticated call")

// principal authenticates one request and returns the caller it belongs to.
//
// Two refusals happen here and nowhere else, and both are CodeUnauthenticated:
//
//   - No credential at all. This is checked BEFORE the provider is consulted,
//     so that a provider which happened to accept an empty string could not
//     turn the absence of a credential into an anonymous session.
//   - A principal with no tenant. Every stored record and every bus subject in
//     this system carries a tenant scope; a caller that cannot be scoped has
//     nothing it may be shown, so an empty tenant is refused here rather than
//     travelling down to a store as a wildcard.
//
// The tenant a call is served under comes from this Principal and from
// nothing else. No request message carries a tenant field, because a tenant a
// caller can type is a tenant a caller can change.
func (s *Server) principal(ctx context.Context, header http.Header) (identity.Principal, error) {
	credential, err := bearerCredential(header)
	if err != nil {
		return identity.Principal{}, connect.NewError(connect.CodeUnauthenticated, err)
	}

	p, err := s.auth.Authenticate(ctx, credential)
	if err != nil {
		// Every failure — unknown subject, bad secret, a credential of a
		// shape no provider owns — collapses to the same refusal.
		return identity.Principal{}, connect.NewError(connect.CodeUnauthenticated,
			errors.New("api: authentication failed"))
	}
	if p.TenantID == "" {
		return identity.Principal{}, connect.NewError(connect.CodeUnauthenticated,
			errors.New("api: the credential resolves to no tenant, and there is no unscoped call"))
	}
	return p, nil
}

// bearerCredential pulls the credential out of an Authorization header.
func bearerCredential(header http.Header) (string, error) {
	raw := strings.TrimSpace(header.Get("Authorization"))
	if raw == "" {
		return "", errNoCredential
	}
	scheme, credential, found := strings.Cut(raw, " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", errNoCredential
	}
	credential = strings.TrimSpace(credential)
	if credential == "" {
		return "", errNoCredential
	}
	return credential, nil
}
