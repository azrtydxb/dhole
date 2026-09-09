package identity_test

import (
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/identity"
)

// testCtx bounds every test that touches the network. A verifier that hangs on
// an unreachable IdP is the failure mode this task exists to prevent, so a
// test that would hang forever is a test that hides it.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// mockIDP is an OpenID provider: a discovery document, a JWKS, and the private
// key the tests sign with. Nothing about it is shared with the code under
// test except over HTTP, so the code has to do real discovery and real
// signature verification to pass.
type mockIDP struct {
	server *httptest.Server
	key    *rsa.PrivateKey
	keyID  string
}

func newMockIDP(t *testing.T) *mockIDP {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	idp := &mockIDP{key: key, keyID: "test-key-1"}
	mux := http.NewServeMux()
	idp.server = httptest.NewServer(mux)
	t.Cleanup(idp.server.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                idp.issuer(),
			"authorization_endpoint":                idp.issuer() + "/auth",
			"token_endpoint":                        idp.issuer() + "/token",
			"jwks_uri":                              idp.issuer() + "/jwks",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		pub := key.Public().(*rsa.PublicKey)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{{
				"kty": "RSA",
				"alg": "RS256",
				"use": "sig",
				"kid": idp.keyID,
				"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			}},
		})
	})
	return idp
}

func (m *mockIDP) issuer() string { return m.server.URL }

func (m *mockIDP) stop() { m.server.Close() }

// signWith produces a compact JWS over claims. The key is a parameter because
// half the point of these tests is signing with the wrong one.
func signWith(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()

	header, err := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid})
	require.NoError(t, err)
	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	enc := base64.RawURLEncoding
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	return signing + "." + enc.EncodeToString(sig)
}

// idToken signs a well-formed id token for this IdP, with the given claims
// merged over the defaults.
func (m *mockIDP) idToken(t *testing.T, claims map[string]any) string {
	t.Helper()

	all := map[string]any{
		"iss": m.issuer(),
		"sub": "alice@example.com",
		"aud": "dhole-control-plane",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	for k, v := range claims {
		all[k] = v
	}
	return signWith(t, m.key, m.keyID, all)
}

func TestOIDCAndServiceTokenWithIdPDown(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	local, _, _ := newLocal(t)

	oidcProvider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	chain := identity.Chain(oidcProvider, local)

	// A federated login while the IdP is reachable.
	principal, err := chain.Authenticate(ctx, idp.idToken(t, nil))
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", principal.Subject)
	require.Equal(t, "acme", principal.TenantID)
	require.Equal(t, identity.PrincipalUser, principal.Kind)

	// A service token issued for automation that must survive the outage.
	serviceToken, err := local.IssueToken(ctx, identity.Principal{
		TenantID: "acme",
		Subject:  "ci-runner",
		Scopes:   []string{"runs:write"},
	}, time.Hour)
	require.NoError(t, err)

	idp.stop()

	got, err := chain.Authenticate(ctx, serviceToken)
	require.NoError(t, err, "a local service token must keep working while the IdP is down")
	require.Equal(t, "ci-runner", got.Subject)
	require.Equal(t, "acme", got.TenantID)
	require.Equal(t, identity.PrincipalService, got.Kind)
}

func TestOIDCTokenWithWrongAudienceIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	// A token the same IdP signed for a different relying party. Accepting it
	// would let any application sharing this IdP mint control-plane sessions.
	_, err = provider.Authenticate(ctx, idp.idToken(t, map[string]any{"aud": "some-other-app"}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "audience")
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
	require.NotErrorIs(t, err, identity.ErrCredentialFormat,
		"a token this provider owns and refuses must never be offered to the next provider")
}

func TestOIDCFailureDoesNotFallBackToWeakerAuth(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	token := idp.idToken(t, nil)

	local, _, _ := newLocal(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)
	chain := identity.Chain(provider, local)

	// The IdP goes away before this provider ever reached it: nothing is
	// cached and nothing can be verified.
	idp.stop()

	_, err = chain.Authenticate(ctx, token)
	require.Error(t, err, "an outage must not become a downgrade")
	require.NotErrorIs(t, err, identity.ErrCredentialFormat,
		"a transport failure must stop the chain, not be reported as an unknown credential shape")
}

func TestOIDCTokenSignedByUnknownKeyIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	// Every claim is correct; only the signing key is the attacker's. A
	// verifier that decodes instead of verifying passes every other test in
	// this file and fails only this one.
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	forged := signWith(t, attacker, idp.keyID, map[string]any{
		"iss": idp.issuer(),
		"sub": "alice@example.com",
		"aud": "dhole-control-plane",
		"exp": time.Now().Add(time.Hour).Unix(),
	})

	_, err = provider.Authenticate(ctx, forged)
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

func TestOIDCTokenWithAlgNoneIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	enc := base64.RawURLEncoding
	header, err := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	require.NoError(t, err)
	payload, err := json.Marshal(map[string]any{
		"iss": idp.issuer(),
		"sub": "alice@example.com",
		"aud": "dhole-control-plane",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(payload) + "."

	_, err = provider.Authenticate(ctx, unsigned)
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

func TestOIDCExpiredTokenIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	expired := idp.idToken(t, map[string]any{
		"exp": time.Now().Add(-time.Hour).Unix(),
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
	})

	_, err = provider.Authenticate(ctx, expired)
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

func TestOIDCTokenFromWrongIssuerIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	// Signed by the key this provider trusts, but claiming another issuer.
	_, err = provider.Authenticate(ctx, idp.idToken(t, map[string]any{"iss": "https://evil.example.com"}))
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

func TestOIDCTenantIsNotTakenFromAnUnverifiedClaim(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)

	// Configured tenant: a tenant claim in the token is decoration and must be
	// ignored, or a federated user picks their own tenant.
	static, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	principal, err := static.Authenticate(ctx, idp.idToken(t, map[string]any{"tenant": "victim-corp"}))
	require.NoError(t, err)
	require.Equal(t, "acme", principal.TenantID)

	// Claim-derived tenant: only from the verified payload of a token that
	// passed signature, issuer, audience and expiry.
	claimed, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL:   idp.issuer(),
		Audience:    "dhole-control-plane",
		TenantClaim: "tenant",
	})
	require.NoError(t, err)

	principal, err = claimed.Authenticate(ctx, idp.idToken(t, map[string]any{"tenant": "acme"}))
	require.NoError(t, err)
	require.Equal(t, "acme", principal.TenantID)

	// A token that passes verification but carries no tenant claim cannot be
	// placed in a tenant at all, and an unscoped principal is never issued.
	_, err = claimed.Authenticate(ctx, idp.idToken(t, nil))
	require.Error(t, err)
	require.ErrorIs(t, err, identity.ErrTenantRequired)

	// A forged token carrying the right tenant claim is still forged: the
	// tenant is read from the payload only after verification.
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	forged := signWith(t, attacker, idp.keyID, map[string]any{
		"iss":    idp.issuer(),
		"sub":    "mallory",
		"aud":    "dhole-control-plane",
		"tenant": "acme",
		"exp":    time.Now().Add(time.Hour).Unix(),
	})
	_, err = claimed.Authenticate(ctx, forged)
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}

func TestOIDCConfigIsValidatedUpFront(t *testing.T) {
	t.Parallel()

	_, err := identity.NewOIDC(identity.OIDCConfig{Audience: "a", TenantID: "acme"})
	require.Error(t, err, "an issuer is not optional")

	_, err = identity.NewOIDC(identity.OIDCConfig{IssuerURL: "https://idp.example.com", TenantID: "acme"})
	require.Error(t, err, "an unconfigured audience would accept tokens minted for any relying party")

	_, err = identity.NewOIDC(identity.OIDCConfig{IssuerURL: "https://idp.example.com", Audience: "a"})
	require.ErrorIs(t, err, identity.ErrTenantRequired)

	_, err = identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: "https://idp.example.com", Audience: "a", TenantID: "acme", TenantClaim: "tenant",
	})
	require.Error(t, err, "two sources for the tenant is an ambiguity, not a fallback")
}

// stubProvider is one canned answer, so the chain's forwarding rules can be
// tested without an IdP or a database.
type stubProvider struct {
	principal identity.Principal
	err       error
	calls     *int
}

func (s stubProvider) Authenticate(context.Context, string) (identity.Principal, error) {
	*s.calls++
	return s.principal, s.err
}

func TestChainPassesOnOnlyUnrecognisedCredentials(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	accepted := identity.Principal{Subject: "s", TenantID: "acme", Kind: identity.PrincipalUser}

	t.Run("format error moves to the next provider", func(t *testing.T) {
		t.Parallel()
		first, second := 0, 0
		chain := identity.Chain(
			stubProvider{err: identity.ErrCredentialFormat, calls: &first},
			stubProvider{principal: accepted, calls: &second},
		)
		got, err := chain.Authenticate(ctx, "whatever")
		require.NoError(t, err)
		require.Equal(t, accepted, got)
		require.Equal(t, 1, first)
		require.Equal(t, 1, second)
	})

	t.Run("refusal stops the chain", func(t *testing.T) {
		t.Parallel()
		first, second := 0, 0
		chain := identity.Chain(
			stubProvider{err: identity.ErrUnauthenticated, calls: &first},
			stubProvider{principal: accepted, calls: &second},
		)
		_, err := chain.Authenticate(ctx, "whatever")
		require.ErrorIs(t, err, identity.ErrUnauthenticated)
		require.Equal(t, 0, second, "a credential a provider owns and refuses must not be retried elsewhere")
	})

	t.Run("transport failure stops the chain", func(t *testing.T) {
		t.Parallel()
		first, second := 0, 0
		boom := errors.New("dial tcp: connection refused")
		chain := identity.Chain(
			stubProvider{err: boom, calls: &first},
			stubProvider{principal: accepted, calls: &second},
		)
		_, err := chain.Authenticate(ctx, "whatever")
		require.ErrorIs(t, err, boom)
		require.Equal(t, 0, second, "an IdP outage must not downgrade to the next provider")
	})

	t.Run("nobody owns it", func(t *testing.T) {
		t.Parallel()
		first := 0
		chain := identity.Chain(stubProvider{err: identity.ErrCredentialFormat, calls: &first})
		_, err := chain.Authenticate(ctx, "whatever")
		require.ErrorIs(t, err, identity.ErrCredentialFormat)
	})
}

func TestOIDCIgnoresCredentialsItDoesNotOwn(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	idp := newMockIDP(t)
	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: idp.issuer(),
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	for _, credential := range []string{
		"",
		"dht_acme_" + strings.Repeat("a", 64),
		identity.PasswordCredential("acme", "alice", "hunter2"),
		"not.a.jwt.at.all.really",
	} {
		_, err := provider.Authenticate(ctx, credential)
		require.ErrorIs(t, err, identity.ErrCredentialFormat, "credential %q", credential)
	}
}

// TestOIDCSymmetricAlgorithmFromDiscoveryIsRejected covers the algorithm
// allowlist, which no other test in this file exercises: it is a guard against
// what the *issuer* says, not against what the token says.
//
// An issuer whose discovery document advertises a symmetric algorithm, and
// whose JWKS therefore publishes a shared secret, hands that secret to
// everyone who can read the JWKS — which is everyone. A verifier that takes
// its accepted algorithms from discovery will then verify a token anyone could
// have minted. Only asymmetric algorithms are accepted, whatever the issuer
// advertises.
func TestOIDCSymmetricAlgorithmFromDiscoveryIsRejected(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)

	secret := []byte("a symmetric key published in a public JWKS")

	var issuer string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	issuer = server.URL

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":                                issuer,
			"jwks_uri":                              issuer + "/jwks",
			"response_types_supported":              []string{"code"},
			"subject_types_supported":               []string{"public"},
			"id_token_signing_alg_values_supported": []string{"HS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]string{{
				"kty": "oct",
				"alg": "HS256",
				"use": "sig",
				"kid": "shared",
				"k":   base64.RawURLEncoding.EncodeToString(secret),
			}},
		})
	})

	provider, err := identity.NewOIDC(identity.OIDCConfig{
		IssuerURL: issuer,
		Audience:  "dhole-control-plane",
		TenantID:  "acme",
	})
	require.NoError(t, err)

	enc := base64.RawURLEncoding
	header, err := json.Marshal(map[string]any{"alg": "HS256", "typ": "JWT", "kid": "shared"})
	require.NoError(t, err)
	payload, err := json.Marshal(map[string]any{
		"iss": issuer,
		"sub": "mallory",
		"aud": "dhole-control-plane",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	require.NoError(t, err)
	signing := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, err = mac.Write([]byte(signing))
	require.NoError(t, err)
	token := signing + "." + enc.EncodeToString(mac.Sum(nil))

	_, err = provider.Authenticate(ctx, token)
	require.Error(t, err, "a token signed with a secret the JWKS publishes must never authenticate anyone")
	require.ErrorIs(t, err, identity.ErrUnauthenticated)
}
