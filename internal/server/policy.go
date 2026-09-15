package server

import (
	"errors"
	"fmt"

	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/taint"
)

// THE DISPATCH POLICY THIS PLANE RUNS (ADR 0032).
//
// The scheduler has put every ready step, and each secret it declares, to a
// tier policy since ADR 0012 and ADR 0030 — when it is given one. `dhole serve`
// gave it none, so nothing was evaluated and nothing was recorded. It is always
// given one now: the operator's, or the built-in default, and beneath either
// the floor no configuration relaxes.

// validPolicy refuses a policy that cannot work, where the plane is configured.
// A rule that does not compile would deny every step at evaluation; a policy
// with no rules denies every step by definition. Neither is an outage an
// operator should discover by running a pipeline.
func validPolicy(p *policy.TierPolicy) error {
	if p == nil {
		return nil
	}
	if len(p.Rules) == 0 {
		return errors.New("server: the dispatch policy has no rules, and a tier with no rules refuses every step")
	}
	if err := policy.Validate(*p); err != nil {
		return fmt.Errorf("server: dispatch policy: %w", err)
	}
	return nil
}

// dispatchPolicy is what the scheduler is wired with: a CEL engine over this
// plane's tier, the configured policy (or the default) beneath the floor, every
// decision audited to policy_audit in the run database; and the provenance
// source the dispatch half of policy reads `signed` and `upstream` from.
//
// It returns the source as well, so SetPolicy can replace what the tier holds
// while the plane runs.
func (s *Server) dispatchPolicy(in *infra) (policy.Engine, scheduler.Provenances, *policy.StaticSource, error) {
	tierPolicy := policy.Default()
	if s.cfg.Policy != nil {
		tierPolicy = *s.cfg.Policy
	}
	source := policy.NewStaticSource()
	if err := source.Set(DefaultTier, tierPolicy); err != nil {
		return nil, nil, nil, fmt.Errorf("server: dispatch policy: %w", err)
	}
	engine, err := policy.New(policy.WithFloor(source, taint.DispatchFloor()),
		policy.NewSQLAuditWithDialect(in.db, in.dialect))
	if err != nil {
		return nil, nil, nil, err
	}
	// Signatures and upstreams live in the run database, as the catalog does.
	// Nothing in this plane mirrors, so the upstream registry is read for its
	// registrations alone and given no mirror prefix.
	sigs := plugins.NewSignatures(in.db, in.dialect)
	provenance := plugins.NewProvenance(sigs, plugins.NewUpstreams(in.db, in.dialect, sigs, ""))
	return engine, provenance, source, nil
}

// SetPolicy replaces the dispatch policy of a running plane (ADR 0032). The
// next decision is made under it; a decision already made is not revisited.
//
// A policy that cannot work is refused and the one in force stays: a reload
// that turned a typo into a plane refusing every step would make editing policy
// the riskiest thing an operator does. The caller owns the revision, and it must
// change whenever the rules do — the engine caches compiled programs under it.
func (s *Server) SetPolicy(p policy.TierPolicy) error {
	if err := validPolicy(&p); err != nil {
		return err
	}
	s.mu.Lock()
	source := s.policySource
	s.mu.Unlock()
	if source == nil {
		return errors.New("server: not started")
	}
	return source.Set(DefaultTier, p)
}
