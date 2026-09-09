package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strings"
)

// A service token is presented as `dht_<tenant>_<secret>`. The tenant travels
// in the token because every lookup in this system is tenant-scoped and the
// Provider interface takes no separate tenant argument; the secret is the only
// part that authenticates, and it is hex, so the last underscore separates the
// two even for a tenant id containing one.
const (
	tokenPrefix = "dht_"

	// tokenBytes is the entropy of a service token. 32 bytes is beyond any
	// feasible guessing attack, which is what lets the stored form be a plain
	// SHA-256 rather than a password hash: there is no low-entropy secret
	// here for an offline attacker to enumerate.
	tokenBytes = 32
)

// newTokenSecret returns a fresh token secret as hex.
//
// The bytes come from crypto/rand and nowhere else. If the system cannot
// produce randomness the operation fails: a token issued from a degraded
// source is worse than no token, so there is no weaker source to fall back to
// and no error to swallow here.
func newTokenSecret() (string, error) {
	buf := make([]byte, tokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate service token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// hashToken is the only representation of a token that is ever stored.
func hashToken(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// formatToken assembles the string handed to the caller. It exists exactly
// once, here, and is never persisted.
func formatToken(tenantID, secret string) string {
	return tokenPrefix + tenantID + "_" + secret
}

// parseToken splits a presented token. A credential that is not shaped like a
// service token is not this provider's to refuse, so the caller turns a false
// result into ErrCredentialFormat rather than ErrUnauthenticated.
func parseToken(credential string) (tenantID, secret string, ok bool) {
	rest, found := strings.CutPrefix(credential, tokenPrefix)
	if !found {
		return "", "", false
	}
	i := strings.LastIndex(rest, "_")
	if i < 0 {
		return "", "", false
	}
	tenantID, secret = rest[:i], rest[i+1:]
	if len(secret) != hex.EncodedLen(tokenBytes) {
		return "", "", false
	}
	if _, err := hex.DecodeString(secret); err != nil {
		return "", "", false
	}
	return tenantID, secret, true
}

// secretsEqual compares secret material without leaking, through the time it
// takes, how many leading bytes matched. Every comparison of a hash, a token
// or a derived key in this package goes through it; `==` on a string returns
// on the first differing byte and is a timing oracle.
func secretsEqual(a, b []byte) bool {
	return subtle.ConstantTimeCompare(a, b) == 1
}

// randRead fills buf from crypto/rand. Every random byte this package produces
// — token secrets and password salts alike — comes through here, so there is
// one place to audit the source of randomness.
func randRead(buf []byte) (int, error) {
	return rand.Read(buf)
}
