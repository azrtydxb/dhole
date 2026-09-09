package plugins

import (
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// WHAT THIS FILE VERIFIES, AND WHAT IT DOES NOT.
//
// Read this before trusting a record whose Source is SourceCosign. An
// unimplemented check that looks implemented is worse than an obviously
// missing one, so the ceiling is stated here rather than implied.
//
// Verified, locally and offline:
//   - the bundle parses and carries a leaf certificate, a payload and a
//     signature;
//   - the leaf certificate parses as X.509 and the current time is inside its
//     validity window;
//   - the leaf asserts exactly one SAN identity (email or URI) and carries a
//     Fulcio OIDC-issuer extension, so identity and issuer are read from the
//     CERTIFICATE and never from the caller;
//   - the signature verifies over the payload bytes under the leaf's public
//     key;
//   - the payload is a simple-signing document whose
//     critical.image.docker-manifest-digest is EXACTLY the digest being
//     recorded, which is what binds the signature to these bytes rather than
//     to some other artifact the same signer legitimately signed.
//
// NOT verified, deliberately:
//   - the certificate CHAIN. The leaf is not checked against a Fulcio root or
//     any intermediate, so a self-signed certificate asserting any identity
//     and any issuer is accepted here. This is the big one: on its own, this
//     file establishes "the holder of this key said so", not "Fulcio issued
//     this identity". The allowed-signer list narrows WHICH identities are
//     accepted, but an attacker who can write a signature record can mint a
//     certificate for an allowed identity.
//   - the signed certificate timestamp (SCT), so certificate-transparency
//     inclusion is unproven;
//   - the Rekor transparency log: neither the inclusion proof nor the signed
//     entry timestamp is checked, so there is no proof the signature existed
//     at a point in time and no detection of a key used after compromise. The
//     bundle's rekorBundle field is stored with the payload and ignored;
//   - revocation, and key-based (non-keyless) cosign signatures.
//
// debt: chain, SCT and Rekor verification need sigstore/cosign and its
// transitive tree, plus network or a pinned trust root bundle, which is a
// dependency decision this task is not the place to make. Revisit when the
// registry work of Task 34 lands a trust-root distribution mechanism, or
// sooner if signature records become writable by anything other than the
// control plane itself.

// Fulcio's OIDC issuer extensions. 1.8 is the current form, a DER UTF8String;
// 1.1 is the deprecated form carrying the raw string. Both are read because a
// record signed a year ago carries the old one.
var (
	oidFulcioIssuerV2 = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57264, 1, 8}
	oidFulcioIssuerV1 = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 57264, 1, 1}
)

// ErrMalformedSignature reports evidence that is not a signature bundle this
// build can check: unparseable JSON, a broken certificate, a signature that
// does not verify, or a payload naming different bytes. It is distinct from
// ErrVerificationFailed because it says the EVIDENCE is broken rather than
// that the signer was not allowed — but both deny.
var ErrMalformedSignature = errors.New("plugins: malformed signature")

// cosignBundle is the JSON cosign writes with `--bundle`, plus the payload it
// signed. Only the fields this build verifies are named; rekorBundle is
// deliberately absent from the struct because nothing here checks it, and a
// field that is parsed but unused reads like a field that is verified.
type cosignBundle struct {
	Base64Signature string `json:"base64Signature"`
	Cert            string `json:"cert"`
	Payload         string `json:"payload"`
}

// simpleSigningPayload is the document cosign signs. Only the digest binding
// matters here: it is what makes a signature a statement about bytes.
type simpleSigningPayload struct {
	Critical struct {
		Image struct {
			DockerManifestDigest string `json:"docker-manifest-digest"`
		} `json:"image"`
	} `json:"critical"`
}

// SignatureFromCosignBundle turns a cosign bundle into a record for d.
//
// The identity and issuer on the returned Signature come from the
// certificate, never from the caller: a caller-supplied identity would make
// the whole record self-asserted, and the store would be recording a claim
// rather than evidence.
func SignatureFromCosignBundle(d *dholev1.Digest, bundle []byte) (Signature, error) {
	identity, issuer, err := verifyCosignBundle(d, bundle)
	if err != nil {
		return Signature{}, err
	}
	return Signature{
		Identity: identity,
		Issuer:   issuer,
		Payload:  bundle,
		Source:   SourceCosign,
	}, nil
}

// verifyCosignBundle performs every check listed at the top of this file and
// returns the identity and issuer the certificate asserted.
//
// It is called on every Verify, not only on Record. The stored identity and
// issuer columns are an INDEX, not the evidence: re-deriving them from the
// bundle is what stops anyone who can write a row from granting themselves an
// allowed identity.
func verifyCosignBundle(d *dholev1.Digest, bundle []byte) (identity, issuer string, err error) {
	text, err := digestText(d)
	if err != nil {
		return "", "", err
	}

	var b cosignBundle
	if err := json.Unmarshal(bundle, &b); err != nil {
		return "", "", fmt.Errorf("%w: not a cosign bundle: %w", ErrMalformedSignature, err)
	}

	payload, err := base64.StdEncoding.DecodeString(b.Payload)
	if err != nil || len(payload) == 0 {
		return "", "", fmt.Errorf("%w: bundle carries no decodable signed payload", ErrMalformedSignature)
	}
	sig, err := base64.StdEncoding.DecodeString(b.Base64Signature)
	if err != nil || len(sig) == 0 {
		return "", "", fmt.Errorf("%w: bundle carries no decodable signature", ErrMalformedSignature)
	}
	certPEM, err := base64.StdEncoding.DecodeString(b.Cert)
	if err != nil || len(certPEM) == 0 {
		return "", "", fmt.Errorf("%w: bundle carries no decodable certificate", ErrMalformedSignature)
	}

	leaf, err := parseLeaf(certPEM)
	if err != nil {
		return "", "", err
	}

	// Fulcio certificates are deliberately short-lived. Accepting one outside
	// its window discards the only expiry this file can enforce.
	now := time.Now()
	if now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) {
		return "", "", fmt.Errorf("%w: certificate is outside its validity window (%s to %s)",
			ErrMalformedSignature, leaf.NotBefore.UTC().Format(time.RFC3339), leaf.NotAfter.UTC().Format(time.RFC3339))
	}

	identity, err = leafIdentity(leaf)
	if err != nil {
		return "", "", err
	}
	issuer, err = leafIssuer(leaf)
	if err != nil {
		return "", "", err
	}

	algo, err := signatureAlgorithm(leaf)
	if err != nil {
		return "", "", err
	}
	// The certificate says WHO; this says over WHAT. Reading the identity
	// without checking the signature would accept any bundle that carried a
	// genuine certificate stapled to somebody else's bytes.
	if err := leaf.CheckSignature(algo, payload, sig); err != nil {
		return "", "", fmt.Errorf("%w: signature does not verify under the certificate's key: %w",
			ErrMalformedSignature, err)
	}

	var signed simpleSigningPayload
	if err := json.Unmarshal(payload, &signed); err != nil {
		return "", "", fmt.Errorf("%w: signed payload is not a simple-signing document: %w",
			ErrMalformedSignature, err)
	}
	// Without this, a valid signature over some OTHER artifact could be filed
	// against this digest and would verify — defeating the entire premise
	// that a signature is about bytes.
	if signed.Critical.Image.DockerManifestDigest != text {
		return "", "", fmt.Errorf("%w: signed digest %q is not the digest being recorded (%s)",
			ErrMalformedSignature, signed.Critical.Image.DockerManifestDigest, text)
	}

	return identity, issuer, nil
}

// parseLeaf reads the first PEM certificate block, which is the leaf. The rest
// of the chain is not parsed because nothing here verifies a chain; see the
// debt note above.
func parseLeaf(certPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%w: certificate is not a PEM CERTIFICATE block", ErrMalformedSignature)
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: certificate does not parse: %w", ErrMalformedSignature, err)
	}
	return leaf, nil
}

// leafIdentity reads the one SAN the signer is known by. More than one is
// refused rather than resolved by picking the first: which name a record is
// filed under would then depend on SAN ordering, and an attacker who can add
// a second SAN chooses which check the certificate passes.
func leafIdentity(leaf *x509.Certificate) (string, error) {
	names := make([]string, 0, len(leaf.EmailAddresses)+len(leaf.URIs))
	names = append(names, leaf.EmailAddresses...)
	for _, u := range leaf.URIs {
		names = append(names, u.String())
	}
	switch len(names) {
	case 1:
		return names[0], nil
	case 0:
		return "", fmt.Errorf("%w: certificate asserts no subject alternative name to identify the signer by", ErrMalformedSignature)
	default:
		return "", fmt.Errorf("%w: certificate asserts %d subject alternative names (%s); a signer must be one identity",
			ErrMalformedSignature, len(names), strings.Join(names, ", "))
	}
}

// leafIssuer reads the OIDC issuer Fulcio recorded. There is no default: an
// invented issuer would be a claim the CA never made, and the issuer is half
// of what identifies a principal.
func leafIssuer(leaf *x509.Certificate) (string, error) {
	for _, ext := range leaf.Extensions {
		switch {
		case ext.Id.Equal(oidFulcioIssuerV2):
			var raw asn1.RawValue
			if _, err := asn1.Unmarshal(ext.Value, &raw); err != nil || raw.Tag != asn1.TagUTF8String {
				return "", fmt.Errorf("%w: OIDC issuer extension is not a DER UTF8String", ErrMalformedSignature)
			}
			if len(raw.Bytes) == 0 {
				return "", fmt.Errorf("%w: OIDC issuer extension is empty", ErrMalformedSignature)
			}
			return string(raw.Bytes), nil
		case ext.Id.Equal(oidFulcioIssuerV1):
			if len(ext.Value) == 0 {
				return "", fmt.Errorf("%w: OIDC issuer extension is empty", ErrMalformedSignature)
			}
			return string(ext.Value), nil
		}
	}
	return "", fmt.Errorf("%w: certificate carries no OIDC issuer extension", ErrMalformedSignature)
}

// signatureAlgorithm pairs the leaf's key with SHA-256, which is what cosign
// signs with. An unrecognised key type is refused rather than guessed at.
func signatureAlgorithm(leaf *x509.Certificate) (x509.SignatureAlgorithm, error) {
	switch leaf.PublicKeyAlgorithm {
	case x509.ECDSA:
		return x509.ECDSAWithSHA256, nil
	case x509.RSA:
		return x509.SHA256WithRSA, nil
	case x509.Ed25519:
		return x509.PureEd25519, nil
	default:
		return x509.UnknownSignatureAlgorithm, fmt.Errorf("%w: unsupported certificate key algorithm %q",
			ErrMalformedSignature, leaf.PublicKeyAlgorithm)
	}
}
