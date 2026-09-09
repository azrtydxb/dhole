package engine_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/engine"
)

// TestCapsHashMatchesTheDocumentedAlgorithm reimplements docs/wire-contract.md's
// "Computing <caps>" steps literally and requires the result to equal
// engine.CapsHash.
//
// The document is the only thing a third-party engine author has, and getting
// this value wrong fails silently — the engine subscribes to a subject nothing
// publishes on. So the prose and the code are pinned to each other here rather
// than trusted to stay in step.
func TestCapsHashMatchesTheDocumentedAlgorithm(t *testing.T) {
	fromDoc := func(caps []dholev1.Capability) string {
		// 1. drop UNSPECIFIED, 2. de-duplicate and sort by enum number
		seen := map[int64]bool{}
		var nums []int64
		for _, c := range caps {
			if c == dholev1.Capability_CAPABILITY_UNSPECIFIED || seen[int64(c)] {
				continue
			}
			seen[int64(c)] = true
			nums = append(nums, int64(c))
		}
		for i := range nums {
			for j := i + 1; j < len(nums); j++ {
				if nums[j] < nums[i] {
					nums[i], nums[j] = nums[j], nums[i]
				}
			}
		}
		// 3. decimal number + "\n" each, 4. SHA-256, 5. first 16 hex chars
		sum := sha256.New()
		for _, n := range nums {
			fmt.Fprintf(sum, "%d\n", n)
		}
		return hex.EncodeToString(sum.Sum(nil))[:16]
	}

	for _, caps := range [][]dholev1.Capability{
		{},
		{dholev1.Capability_CAPABILITY_UNSPECIFIED},
		{dholev1.Capability_CAPABILITY_NETWORK},
		{dholev1.Capability_CAPABILITY_PRIVILEGED, dholev1.Capability_CAPABILITY_NETWORK},
		{dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_NETWORK,
			dholev1.Capability_CAPABILITY_SECRETS, dholev1.Capability_CAPABILITY_HOST_MOUNT},
	} {
		require.Equal(t, engine.CapsHash(caps), fromDoc(caps),
			"docs/wire-contract.md's algorithm disagrees with engine.CapsHash for %v", caps)
	}

	// The document states this literal value; an engine author will copy it.
	require.Equal(t, "e3b0c44298fc1c14", engine.CapsHash(nil),
		"the empty-set hash printed in docs/wire-contract.md is wrong")
}
