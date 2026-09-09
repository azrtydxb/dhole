package engine

import (
	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// leaseScopeFrom maps the scope a step declares on the wire to the one the
// executor is asked for.
//
// An unspecified scope is the step scope. That is the only default that is
// safe in both directions: every other scope deliberately reuses a previous
// occupant's state, which no cache key can see, so a definition that simply
// did not say must not silently land in one.
func leaseScopeFrom(s dholev1.LeaseScope) executor.LeaseScope {
	switch s {
	case dholev1.LeaseScope_LEASE_SCOPE_JOB:
		return executor.LeaseJob
	case dholev1.LeaseScope_LEASE_SCOPE_PIPELINE:
		return executor.LeasePipeline
	case dholev1.LeaseScope_LEASE_SCOPE_POOL:
		return executor.LeasePool
	case dholev1.LeaseScope_LEASE_SCOPE_SERVICE:
		return executor.LeaseService
	case dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED, dholev1.LeaseScope_LEASE_SCOPE_STEP:
		return executor.LeaseStep
	default:
		return executor.LeaseStep
	}
}
