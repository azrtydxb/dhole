package scheduler

import (
	"fmt"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/registry"
)

// Match returns the instances that may take a step with these requirements,
// in the order they were given.
//
// It is pure on purpose: no bus, no store, no registry lookup. Matching is the
// decision that most needs exhaustive testing — every capability, platform and
// lifecycle combination — and a function that needed a running NATS to answer
// would be tested once, at the happy path, and then trusted.
//
// It does NOT filter on the engine TYPE an instance advertises, even though
// registry.Instance now carries it. The engine types a step is compatible with
// are declared by its plugin's manifest (catalog.Entry.EngineTypes), and
// neither caller — Scheduler.dispatch nor api.Plan — resolves a manifest to
// build executor.Requirements. A filter only one of the two could populate
// would make the planner's answer disagree with the dispatcher's, which is the
// one property both sides are written to preserve. The type belongs in
// Requirements together with the manifest lookup that fills it, in one change
// that moves both callers; until then a step needing a container engine and
// matched to a process-only one fails at the far end, visibly, rather than
// being reported as unschedulable by a planner and dispatched anyway.
//
// The filter is a conjunction and it never widens: an empty OS or Arch in the
// requirements means the step does not care, but an empty one on the INSTANCE
// means the instance never said, which is not the same as "anything" and is
// not matched. An engine that cannot honestly grant a capability does not
// advertise it, so the capability check is a subset test against what it did.
func Match(req executor.Requirements, engines []registry.Instance) []registry.Instance {
	var out []registry.Instance
	for _, e := range engines {
		if !eligible(req, e) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// eligible is the whole rule for one instance.
func eligible(req executor.Requirements, e registry.Instance) bool {
	// Only ready takes new work. Draining is alive and finishing what it
	// holds, which is exactly why it must be handed nothing more.
	if e.State != registry.StateReady {
		return false
	}
	if e.Slots <= 0 {
		return false
	}
	if !platformMatches(req.OS, e.OS) || !platformMatches(req.Arch, e.Arch) {
		return false
	}
	for _, c := range req.Capabilities {
		if c == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		if !advertises(e, c) {
			return false
		}
	}
	return true
}

// platformMatches compares one axis of the platform. A requirement that is
// empty accepts anything; an instance that is empty is accepted only by such a
// requirement, because an unstated platform is unknown rather than universal.
func platformMatches(required, offered string) bool {
	return required == "" || required == offered
}

func advertises(e registry.Instance, c dholev1.Capability) bool {
	for _, have := range e.Capabilities {
		if have == c {
			return true
		}
	}
	return false
}

// Explain says, in one sentence, why no instance matched. It is the payload of
// the unschedulable event, and it exists because the alternative failure mode
// is the worst one a scheduler has: a step that is never dispatched, never
// fails, and never explains itself, so the run looks slow instead of stuck.
//
// The diagnosis is ordered from the coarsest cause to the finest, so the
// operator is told the first thing that would have to change rather than the
// last.
func Explain(req executor.Requirements, engines []registry.Instance) string {
	if len(engines) == 0 {
		return "no engine is registered"
	}

	var ready []registry.Instance
	draining := 0
	for _, e := range engines {
		switch e.State {
		case registry.StateReady:
			ready = append(ready, e)
		case registry.StateDraining:
			draining++
		case registry.StateRegistering, registry.StateGone:
		}
	}
	if len(ready) == 0 {
		if draining > 0 {
			return "every registered engine is draining"
		}
		return "no registered engine is ready"
	}

	var onPlatform []registry.Instance
	for _, e := range ready {
		if platformMatches(req.OS, e.OS) && platformMatches(req.Arch, e.Arch) {
			onPlatform = append(onPlatform, e)
		}
	}
	if len(onPlatform) == 0 {
		return fmt.Sprintf("no ready engine runs on %s", platformName(req.OS, req.Arch))
	}

	for _, c := range req.Capabilities {
		if c == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		matched := false
		for _, e := range onPlatform {
			if advertises(e, c) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Sprintf("no engine advertises capability %s", shortCapability(c))
		}
	}

	// Everything above is satisfied by somebody, so what is left is that no
	// single instance satisfies all of it at once, or that none has a slot.
	return fmt.Sprintf("no single engine satisfies %s together with %s",
		platformName(req.OS, req.Arch), capabilityList(req.Capabilities))
}

// platformName renders the platform axes for a person to read.
func platformName(os, arch string) string {
	switch {
	case os == "" && arch == "":
		return "any platform"
	case os == "":
		return "arch " + arch
	case arch == "":
		return "os " + os
	default:
		return os + "/" + arch
	}
}

// capabilityList renders required capabilities in the same short form the
// unschedulable reason uses.
func capabilityList(caps []dholev1.Capability) string {
	names := make([]string, 0, len(caps))
	for _, c := range caps {
		if c == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		names = append(names, shortCapability(c))
	}
	if len(names) == 0 {
		return "no capabilities"
	}
	return "capabilities " + strings.Join(names, ", ")
}

// shortCapability is the enum name without its prefix: PRIVILEGED rather than
// CAPABILITY_PRIVILEGED, which is what the person reading a stuck run wants.
func shortCapability(c dholev1.Capability) string {
	return strings.TrimPrefix(c.String(), "CAPABILITY_")
}
