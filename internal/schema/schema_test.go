package schema_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// TestEffectClassEnumValues pins the wire numbers of EffectClass. The enum is
// a public contract: a stored run event or an engine on the previous protocol
// version decodes these numbers, so renumbering them silently reinterprets
// every persisted effect class.
func TestEffectClassEnumValues(t *testing.T) {
	require.Equal(t, 1, int(dholev1.EffectClass_EFFECT_CLASS_PURE))
	require.Equal(t, 0, int(dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED))
	require.Equal(t, 2, int(dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT))
	require.Equal(t, 3, int(dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE))
	require.Len(t, dholev1.EffectClass_name, 4)
}
