package wire_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/wire"
)

// TestNegotiateAcceptsCurrentAndPrevious pins the compatibility window. The
// wire schema is a public contract: the control plane must keep talking to an
// engine one version behind, so a fleet can be upgraded without a flag day.
func TestNegotiateAcceptsCurrentAndPrevious(t *testing.T) {
	v, err := wire.NegotiateAgainst(1, []uint32{1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), v)

	_, err = wire.NegotiateAgainst(1, []uint32{0})
	require.ErrorContains(t, err, "unsupported protocol")

	// With the control plane at 2, an engine still speaking 1 is accepted.
	v, err = wire.NegotiateAgainst(2, []uint32{1})
	require.NoError(t, err)
	require.Equal(t, uint32(1), v)

	// Two behind is refused: there is no window that wide.
	_, err = wire.NegotiateAgainst(3, []uint32{1})
	require.ErrorContains(t, err, "unsupported protocol")
}

// TestNegotiatePicksTheHighestMutuallySupported guards against an engine
// being pinned to an old version merely because it listed it first.
func TestNegotiatePicksTheHighestMutuallySupported(t *testing.T) {
	v, err := wire.NegotiateAgainst(2, []uint32{1, 2})
	require.NoError(t, err)
	require.Equal(t, uint32(2), v)

	// Never negotiate above what this control plane speaks.
	v, err = wire.NegotiateAgainst(1, []uint32{1, 2, 99})
	require.NoError(t, err)
	require.Equal(t, uint32(1), v)
}

// TestNegotiateUsesTheCurrentProtocolVersion ties the exported entry point to
// the constant, so bumping ProtocolVersion moves the window with it.
func TestNegotiateUsesTheCurrentProtocolVersion(t *testing.T) {
	v, err := wire.Negotiate([]uint32{wire.ProtocolVersion})
	require.NoError(t, err)
	require.Equal(t, wire.ProtocolVersion, v)

	_, err = wire.Negotiate([]uint32{})
	require.ErrorContains(t, err, "unsupported protocol")
}
