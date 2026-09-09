package docs

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The environment this test needs to be able to run at all. It verifies a
// PUBLISHED image, so there is nothing it can assert on a machine where no
// release exists — and rather than assert something weaker, it says exactly
// what is missing and skips.
//
// `.github/workflows/release.yml` sets all three immediately after publishing,
// which is the run that matters: the release verifies from the workflow that
// made it, before anybody is told it exists.
const (
	envImage    = "DHOLE_RELEASE_IMAGE"
	envIdentity = "DHOLE_RELEASE_IDENTITY_REGEXP"
	envIssuer   = "DHOLE_RELEASE_OIDC_ISSUER"
)

// The defaults for the two claims. They are what the release workflow's OIDC
// identity is, so pointing the test at an image is enough on a normal release.
const (
	defaultIdentity = `^https://github\.com/azrtydxb/dhole/\.github/workflows/release\.yml@`
	defaultIssuer   = "https://token.actions.githubusercontent.com"
)

// TestReleaseArtifactsAreSignedAndHaveSBOM verifies a published image's keyless
// signature and requires an SPDX SBOM attestation on it.
//
// The identity is checked, not merely the signature. "This image is signed" is
// close to meaningless on its own — anyone can sign anything — so what is
// asserted is the claim that matters: GitHub Actions, running THIS workflow, in
// THIS repository, produced this digest. That is a claim no leaked key forges,
// because there is no key.
//
// The attestation is decoded rather than trusted. `cosign verify-attestation`
// exiting 0 says a signed attestation of the requested type exists; it does not
// say the predicate inside is an SBOM. An empty document would satisfy the
// exit code and nothing else, so the predicate is parsed and held to being an
// actual SPDX document.
func TestReleaseArtifactsAreSignedAndHaveSBOM(t *testing.T) {
	image := os.Getenv(envImage)
	if image == "" {
		t.Skipf("no published image to verify: set %s to a released image "+
			"reference, e.g. ghcr.io/azrtydxb/dhole:1.2.3. "+
			"The release workflow sets it after publishing; there is nothing to "+
			"verify on a checkout that has never cut a release", envImage)
	}
	if _, err := exec.LookPath("cosign"); err != nil {
		t.Skipf("cosign is not installed, so %s cannot be verified: %v", image, err)
	}

	identity := envOr(envIdentity, defaultIdentity)
	issuer := envOr(envIssuer, defaultIssuer)

	t.Run("signature", func(t *testing.T) {
		out, err := cosign(t, "verify", image,
			"--certificate-identity-regexp", identity,
			"--certificate-oidc-issuer", issuer)
		require.NoError(t, err,
			"cosign verify %s did not accept the signature. Either the image is "+
				"unsigned, or it was signed by an identity other than %s at %s:\n%s",
			image, identity, issuer, out)
	})

	t.Run("spdx attestation", func(t *testing.T) {
		out, err := cosign(t, "verify-attestation", image,
			"--type", "spdxjson",
			"--certificate-identity-regexp", identity,
			"--certificate-oidc-issuer", issuer)
		require.NoError(t, err,
			"cosign verify-attestation %s found no SPDX attestation signed by %s:\n%s",
			image, identity, out)

		predicate := spdxPredicate(t, out)
		require.NotEmpty(t, predicate["spdxVersion"],
			"the attestation on %s carries no spdxVersion, so whatever it is, it is "+
				"not an SPDX document: %v", image, keysOf(predicate))
		packages, _ := predicate["packages"].([]any)
		require.NotEmpty(t, packages,
			"the SBOM attested on %s lists no packages. An empty SBOM satisfies "+
				"`cosign verify-attestation` and tells a reader nothing", image)
	})
}

// cosign runs one cosign invocation and returns its combined output.
func cosign(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("cosign", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// spdxPredicate pulls the SPDX document out of cosign's output.
//
// verify-attestation writes its human-readable verification notes to stderr and
// the DSSE envelopes to stdout, one JSON object per line; the envelope's
// payload is a base64 in-toto statement whose `predicate` is the document. The
// scan is line-by-line because the notes and the envelopes arrive interleaved
// in the combined output this is given.
func spdxPredicate(t *testing.T, out string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var envelope struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil || envelope.Payload == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(envelope.Payload)
		if err != nil {
			continue
		}
		var statement struct {
			PredicateType string         `json:"predicateType"`
			Predicate     map[string]any `json:"predicate"`
		}
		if err := json.Unmarshal(decoded, &statement); err != nil {
			continue
		}
		require.Contains(t, statement.PredicateType, "spdx",
			"the attestation's predicate type is %q, which is not SPDX",
			statement.PredicateType)
		return statement.Predicate
	}
	require.FailNow(t, "cosign verified an attestation but printed no DSSE envelope "+
		"this test could decode; the assertion about the SBOM's content cannot be made:\n"+out)
	return nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
