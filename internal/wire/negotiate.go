// Package wire holds the engine protocol version and the rule for agreeing on
// one. The wire schema is a public contract: within a major version changes are
// additive only, and the control plane must keep talking to engines one version
// behind so a fleet upgrades gradually rather than in a flag day.
package wire

import "fmt"

// ProtocolVersion is the version this build of the control plane speaks.
//
// Version 2 added EngineRegistration.environment_identity (ADR 0021). An
// engine speaking version 1 registers without one, which is accepted — the
// window below is what makes a fleet upgrade gradually — and its tier caches
// nothing until it is upgraded.
//
// Version 3 added Step.timeout_seconds. It is a bump rather than a silent
// addition because the field changes what an engine must DO, not what it may
// report: a plane cannot see whether a timeout was enforced, and two engines
// both calling themselves version 2 while one kills a runaway step and the
// other holds a slot for its full runtime is exactly what a version number
// exists to prevent. An engine that negotiated 2 still receives work — the
// window is what makes a rolling upgrade possible — and runs it unbounded.
const ProtocolVersion uint32 = 3

// SupportedWindow is how many versions back the control plane accepts. One
// means "current and previous"; widening it is a deliberate decision, because
// every extra version is a shape the control plane must keep handling.
const SupportedWindow uint32 = 1

// Negotiate picks the version this control plane and an engine will speak.
func Negotiate(engineVersions []uint32) (uint32, error) {
	return NegotiateAgainst(ProtocolVersion, engineVersions)
}

// NegotiateAgainst is Negotiate with the control plane's version supplied
// explicitly, so the compatibility window can be exercised at versions this
// build does not happen to be pinned at.
//
// It returns the highest version both sides speak. It never returns a version
// above the control plane's own — an engine advertising a future version is
// talked down, not deferred to.
func NegotiateAgainst(ours uint32, engineVersions []uint32) (uint32, error) {
	oldest := oldestAccepted(ours)

	best, found := uint32(0), false
	for _, v := range engineVersions {
		if v < oldest || v > ours {
			continue
		}
		if !found || v > best {
			best, found = v, true
		}
	}
	if !found {
		return 0, fmt.Errorf("unsupported protocol: engine speaks %v, this control plane accepts %d..%d",
			engineVersions, oldest, ours)
	}
	return best, nil
}

// oldestAccepted is the floor of the compatibility window, clamped so a
// control plane at version 1 does not accept version 0.
func oldestAccepted(ours uint32) uint32 {
	if ours <= SupportedWindow {
		return 1
	}
	return ours - SupportedWindow
}
