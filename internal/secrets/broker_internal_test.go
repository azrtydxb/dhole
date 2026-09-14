package secrets

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAnExpiredHandleNobodyRedeemedIsForgotten: a step issues a handle on every
// dispatch, and a dispatch that is cancelled, lost or refused never redeems
// it. Only redemption used to delete one, so a long-lived plane kept every
// such value in memory for the rest of its life — long after the handle could
// be honoured.
func TestAnExpiredHandleNobodyRedeemedIsForgotten(t *testing.T) {
	b := NewBroker()
	_, err := b.Issue("acme", "REGISTRY_PASSWORD", "stale", time.Nanosecond)
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)

	_, err = b.Issue("acme", "REGISTRY_PASSWORD", "fresh", time.Minute)
	require.NoError(t, err)

	b.mu.Lock()
	defer b.mu.Unlock()
	require.Len(t, b.handles, 1, "an expired, unredeemed handle is still held")
}
