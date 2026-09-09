package engine

import (
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// TestLeaseScopeFromWireCoversEveryScope pins the mapping from the declared
// scope to the one the executor is asked for.
//
// The engine used to hardcode LeaseStep because the wire carried no scope at
// all, which meant a step asking for a pooled or service sandbox silently got
// a fresh one. That is the safe direction to fail, but it is still the wrong
// answer, and nothing would have reported it.
func TestLeaseScopeFromWireCoversEveryScope(t *testing.T) {
	for wire, want := range map[dholev1.LeaseScope]executor.LeaseScope{
		dholev1.LeaseScope_LEASE_SCOPE_STEP:     executor.LeaseStep,
		dholev1.LeaseScope_LEASE_SCOPE_JOB:      executor.LeaseJob,
		dholev1.LeaseScope_LEASE_SCOPE_PIPELINE: executor.LeasePipeline,
		dholev1.LeaseScope_LEASE_SCOPE_POOL:     executor.LeasePool,
		dholev1.LeaseScope_LEASE_SCOPE_SERVICE:  executor.LeaseService,
	} {
		require.Equal(t, want, leaseScopeFrom(wire), "scope %s", wire)
	}
}

// TestUnspecifiedLeaseScopeIsTheStepScope fixes the default deliberately.
// Every scope but LeaseStep carries state from a previous occupant that no
// cache key can see, so an unset field must never select one of them — a
// definition that forgot to say would otherwise become uncacheable, or worse,
// be cached while reusing state.
func TestUnspecifiedLeaseScopeIsTheStepScope(t *testing.T) {
	require.Equal(t, executor.LeaseStep,
		leaseScopeFrom(dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED))
}

// TestEveryWireLeaseScopeIsMapped fails when a scope is added to the proto and
// not to the mapping, rather than letting the new scope quietly become
// LeaseStep.
func TestEveryWireLeaseScopeIsMapped(t *testing.T) {
	for value, name := range dholev1.LeaseScope_name {
		scope := dholev1.LeaseScope(value)
		// These two are the step scope by definition, not by falling through.
		if scope == dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED ||
			scope == dholev1.LeaseScope_LEASE_SCOPE_STEP {
			continue
		}
		require.NotEqual(t, executor.LeaseStep, leaseScopeFrom(scope),
			"wire scope %s falls through to LeaseStep — add it to leaseScopeFrom", name)
	}
}
