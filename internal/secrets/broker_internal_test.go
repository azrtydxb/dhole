package secrets

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAnExpiredHandleNobodyRedeemedIsForgotten: a step issues a handle on every
// dispatch, and a dispatch that is cancelled, lost or refused never redeems
// it. Only redemption used to delete one, so a long-lived plane kept every
// such record in memory for the rest of its life — long after the handle
// could be honoured. (The shared bucket forgets by its retention.)
func TestAnExpiredHandleNobodyRedeemedIsForgotten(t *testing.T) {
	ctx := context.Background()
	handles := NewMemoryHandles()
	b := NewBroker(WithHandles(handles))
	ref := Reference{Source: SourceStep, Secret: "harbor-robot", Binding: "REGISTRY_PASSWORD"}
	_, err := b.Issue(ctx, Scope{TenantID: "acme"}, ref, time.Nanosecond)
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)

	_, err = b.Issue(ctx, Scope{TenantID: "acme"}, ref, time.Minute)
	require.NoError(t, err)

	live, err := handles.List(ctx)
	require.NoError(t, err)
	require.Len(t, live, 1, "an expired, unredeemed handle is still held")
}
