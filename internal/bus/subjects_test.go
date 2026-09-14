package bus_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
)

// TestSubjectsMatchDocumentedContract pins the builders to the subject table in
// docs/wire-contract.md. The document is the contract an engine in any language
// reads; if the code and the document disagree, the code is wrong.
func TestSubjectsMatchDocumentedContract(t *testing.T) {
	require.Equal(t, "job.dispatch.untrusted.abc", bus.SubjectDispatch("untrusted", "abc"))
	require.Equal(t, "job.dispatch.trusted.deadbeef", bus.SubjectDispatch("trusted", "deadbeef"))
	require.Equal(t, "job.dispatch.trusted.deadbeef.vm",
		bus.SubjectDispatchKind("trusted", "deadbeef", "vm"))
	require.Equal(t, "job.status.run-1.build", bus.SubjectStatus("run-1", "build"))
	require.Equal(t, "job.logs.run-1.build", bus.SubjectLogs("run-1", "build"))
	require.Equal(t, "engine.control.e1", bus.SubjectEngineControl("e1"))
	require.Equal(t, "engine.heartbeat.e1", bus.SubjectEngineHeartbeat("e1"))
	require.Equal(t, "engine.registration", bus.SubjectEngineRegistration())
}

// TestTierWildcardsScopeToOneTier is what the bus permissions are written
// against: an engine subscribes to its own tier's dispatch wildcard and to
// nothing else.
func TestTierWildcardsScopeToOneTier(t *testing.T) {
	require.Equal(t, "job.dispatch.untrusted.>", bus.SubjectDispatchWildcard("untrusted"))
	require.Equal(t, "job.dispatch.trusted.>", bus.SubjectDispatchWildcard("trusted"))
	// `>` and not `*`: a kind-targeted dispatch carries one token more, and
	// under `*` an engine was refused its own tier's kind subjects by the
	// server. The tier token is fixed either way, which is the boundary.
	require.True(t, strings.HasPrefix(
		bus.SubjectDispatchKind("trusted", "deadbeef", "vm"), "job.dispatch.trusted."))
}

// TestSecretRedemptionSubjectIsPinned holds the one subject an engine in
// another language has to spell exactly right to redeem anything.
func TestSecretRedemptionSubjectIsPinned(t *testing.T) {
	require.Equal(t, "secret.redeem.acme", bus.SubjectSecretRedeemFor("acme"))
	require.Equal(t, "secret.redeem.*", bus.SubjectSecretRedeemAny())
	// Deprecated, and still served while the plane accepts protocol version
	// 3: an engine written before ADR 0029 redeems here.
	require.Equal(t, "secret.redeem", bus.SubjectSecretRedeem())
}

// TestATierCredentialNamesExactlyOneTenant: the tenant is spelled into the
// redemption subject a tier credential may request on, so a tenant that is not
// one token would widen the permission instead of naming it — `*` would let an
// engine redeem on every tenant's subject (ADR 0029).
func TestATierCredentialNamesExactlyOneTenant(t *testing.T) {
	for _, bad := range []string{"", "*", ">", "a.b", "a b"} {
		_, err := bus.TierPermissions(bad, "untrusted")
		require.Error(t, err, "a tier credential was built for the tenant %q", bad)
		_, err = bus.StartEmbeddedWithTiers(t.TempDir(), bad, []string{"untrusted"})
		require.Error(t, err, "a tiered server was started for the tenant %q", bad)
	}

	perms, err := bus.TierPermissions("acme", "untrusted")
	require.NoError(t, err)
	require.Contains(t, perms.Publish.Allow, bus.SubjectSecretRedeemFor("acme"))
	require.NotContains(t, perms.Publish.Allow, bus.SubjectSecretRedeemAny(),
		"a tier credential may request on every tenant's redemption subject")
	require.NotContains(t, perms.Subscribe.Allow, bus.SubjectSecretRedeemAny())
	require.NotContains(t, perms.Subscribe.Allow, bus.SubjectSecretRedeemFor("acme"))
}
