package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// OIDCConfig configures federation against one OpenID Connect provider.
type OIDCConfig struct {
	// IssuerURL is the issuer as it appears in the `iss` claim. Discovery
	// happens at <IssuerURL>/.well-known/openid-configuration and the
	// document's own issuer must match, so a redirect cannot move the trust
	// anchor.
	IssuerURL string

	// Audience is the client id this control plane is registered as, and the
	// value an id token's `aud` must carry. It is mandatory: an IdP typically
	// serves many relying parties, and a verifier that skips the audience
	// check accepts every token any of them was issued.
	Audience string

	// TenantID places every principal from this issuer in one tenant.
	// Mutually exclusive with TenantClaim; exactly one is required.
	TenantID string

	// TenantClaim names a claim of the *verified* payload to read the tenant
	// from — for an IdP that federates several tenants and is trusted to
	// assert which. It is only ever read after signature, issuer, audience
	// and expiry have all passed.
	TenantClaim string

	// HTTPClient fetches discovery and JWKS. Optional; a client with a
	// bounded timeout is used otherwise, because an IdP that accepts a
	// connection and never answers must not pin a request forever.
	HTTPClient *http.Client
}

// oidcSigningAlgs is the set of signatures this verifier will accept.
//
// It is pinned here rather than taken from the discovery document on purpose.
// `none` is the classic bypass, and a symmetric alg is the subtler one: an
// issuer that advertises HS256 publishes the verification secret in its own
// JWKS, so anyone who can read the JWKS can mint tokens. Only asymmetric
// algorithms appear below. go-oidc filters its own discovered algorithm list
// the same way, so this is defence in depth rather than the only guard — which
// is the right weight for a check whose absence is invisible until it is
// exploited.
var oidcSigningAlgs = []string{oidc.RS256, oidc.RS384, oidc.RS512, oidc.ES256, oidc.ES384, oidc.ES512, oidc.PS256, oidc.PS384, oidc.PS512}

// oidcProvider verifies id tokens from one issuer.
type oidcProvider struct {
	cfg    OIDCConfig
	client *http.Client
	// go-oidc uses a context as a configuration bag rather than for
	// cancellation, and keeps it for background key fetches, so this one is
	// long-lived by design and carries only the HTTP client.
	baseCtx context.Context

	mu       sync.Mutex
	verifier *oidc.IDTokenVerifier
}

var _ Provider = (*oidcProvider)(nil)

// NewOIDC returns a Provider federating to cfg's issuer.
//
// It performs no network I/O: discovery is deferred to the first
// authentication and retried after a failure, so a control plane can start
// while its IdP is down — with local credentials working and federated ones
// failing, which is the correct behaviour, rather than refusing to boot.
func NewOIDC(cfg OIDCConfig) (Provider, error) {
	if cfg.IssuerURL == "" {
		return nil, errors.New("oidc: issuer URL required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("oidc: audience required")
	}
	switch {
	case cfg.TenantID == "" && cfg.TenantClaim == "":
		return nil, fmt.Errorf("oidc: %w: set TenantID or TenantClaim", ErrTenantRequired)
	case cfg.TenantID != "" && cfg.TenantClaim != "":
		return nil, errors.New("oidc: set exactly one of TenantID and TenantClaim")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &oidcProvider{
		cfg:    cfg,
		client: client,
		// go-oidc uses a context as a configuration bag for its HTTP client,
		// and keeps it for background key fetches, so it must outlive any one
		// request.
		baseCtx: oidc.ClientContext(context.Background(), client),
	}, nil
}

// Authenticate verifies an id token and returns the principal it names.
//
// A credential that is not a compact JWS is not this provider's to refuse:
// that yields ErrCredentialFormat so a chain can hand it to the local
// provider. Everything that is one and fails yields ErrUnauthenticated, and a
// failure to reach the IdP yields neither — it is a transport error, so the
// chain stops instead of downgrading.
func (p *oidcProvider) Authenticate(ctx context.Context, credential string) (Principal, error) {
	if !looksLikeJWT(credential) {
		return Principal{}, ErrCredentialFormat
	}

	verifier, err := p.verifierFor(ctx)
	if err != nil {
		return Principal{}, err
	}

	// Verify checks the signature against the cached JWKS (refetching when the
	// key id is unknown), the issuer, the audience and the expiry. Nothing
	// below reads the token except through the payload this returned.
	idToken, err := verifier.Verify(ctx, credential)
	if err != nil {
		var expired *oidc.TokenExpiredError
		if errors.As(err, &expired) {
			return Principal{}, ErrExpired
		}
		// The reason travels with the refusal here, unlike in the local
		// provider: an id token is self-contained, so naming the failed check
		// tells an attacker only what they already know about the token they
		// presented, and tells an operator which of four checks to look at.
		return Principal{}, fmt.Errorf("%w: oidc: %s", ErrUnauthenticated, err)
	}

	var claims struct {
		Scope string `json:"scope"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: oidc: unreadable claims", ErrUnauthenticated)
	}

	tenantID, err := p.tenantFor(idToken)
	if err != nil {
		return Principal{}, err
	}

	return Principal{
		Subject:  idToken.Subject,
		TenantID: tenantID,
		Scopes:   strings.Fields(claims.Scope),
		Kind:     PrincipalUser,
	}, nil
}

// tenantFor resolves the tenant from configuration, or from a claim of the
// already-verified token. There is no third source: a tenant read from an
// unverified segment of the credential would let the caller choose the scope
// every authorisation decision is then made in.
func (p *oidcProvider) tenantFor(idToken *oidc.IDToken) (string, error) {
	if p.cfg.TenantID != "" {
		return p.cfg.TenantID, nil
	}
	var claims map[string]json.RawMessage
	if err := idToken.Claims(&claims); err != nil {
		return "", fmt.Errorf("%w: oidc: unreadable claims", ErrUnauthenticated)
	}
	raw, ok := claims[p.cfg.TenantClaim]
	if !ok {
		return "", fmt.Errorf("oidc: %w: claim %q absent", ErrTenantRequired, p.cfg.TenantClaim)
	}
	var tenantID string
	if err := json.Unmarshal(raw, &tenantID); err != nil || tenantID == "" {
		return "", fmt.Errorf("oidc: %w: claim %q is not a tenant id", ErrTenantRequired, p.cfg.TenantClaim)
	}
	return tenantID, nil
}

// verifierFor returns the verifier, running discovery once and caching it.
//
// A discovery failure is not cached: the next request tries again, so an IdP
// that comes back does not need the control plane restarted. The lock is held
// across the fetch so a burst of requests produces one discovery, not one per
// caller.
func (p *oidcProvider) verifierFor(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.verifier != nil {
		return p.verifier, nil
	}
	// The request's deadline bounds the discovery call; the client stored in
	// baseCtx is what go-oidc keeps for later key fetches.
	discoveryCtx := oidc.ClientContext(ctx, p.client)
	provider, err := oidc.NewProvider(discoveryCtx, p.cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc: discovery for %s: %w", p.cfg.IssuerURL, err)
	}
	p.verifier = provider.VerifierContext(p.baseCtx, &oidc.Config{
		ClientID:             p.cfg.Audience,
		SupportedSigningAlgs: oidcSigningAlgs,
	})
	return p.verifier, nil
}

// looksLikeJWT reports whether credential is a compact JWS: three
// dot-separated segments whose first decodes to a JSON header naming an
// algorithm. It is a shape test and nothing more — an unsigned token and a
// forged one both look like this, and both are refused after verification,
// not here. What matters is that it never claims a service token or a local
// password credential, and never disclaims a token an attacker means this
// provider to skip.
func looksLikeJWT(credential string) bool {
	parts := strings.Split(credential, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return false
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var parsed struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(header, &parsed); err != nil {
		return false
	}
	return parsed.Alg != ""
}
