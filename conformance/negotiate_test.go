package conformance

import (
	"testing"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/wire"
)

// The suite is the executable half of docs/wire-contract.md, and the contract
// supports N-1: a real control plane negotiates per engine and dispatches at
// what the two agreed. The harness dispatched at its OWN maximum instead, so
// an engine one version behind — which production accepts — failed every case
// after registration with "unsupported protocol version".
//
// This is asserted here rather than only through the end-to-end run because
// that run catches it only while a version gap happens to exist. Close the gap
// by teaching the reference engine the new version, and the bug comes back
// invisible until the next bump.
func TestTheHarnessDispatchesAtTheVersionTheEngineAgreedTo(t *testing.T) {
	if wire.SupportedWindow == 0 {
		t.Skip("this build supports no older version, so there is no gap to test")
	}
	older := wire.ProtocolVersion - 1

	h := &harness{}
	h.regs = append(h.regs, &dholev1.EngineRegistration{
		EngineId:         "one-version-behind",
		ProtocolVersions: []uint32{older},
	})

	// Asserted through newDispatch, not through negotiatedVersion directly:
	// the bug was that the dispatch did not consult it, so a test that calls
	// the helper itself passes with the bug fully present.
	if got := h.newDispatch("probe").GetProtocolVersion(); got != older {
		t.Errorf("the harness dispatches at %d to an engine that speaks only %d; "+
			"a real plane would negotiate down and this engine works in production", got, older)
	}
}

// The other side: an engine that speaks the current version is dispatched at
// it, so negotiating down is not a blanket downgrade.
func TestTheHarnessDispatchesAtTheCurrentVersionWhenTheEngineSpeaksIt(t *testing.T) {
	h := &harness{}
	h.regs = append(h.regs, &dholev1.EngineRegistration{
		EngineId:         "current",
		ProtocolVersions: []uint32{wire.ProtocolVersion},
	})

	if got := h.newDispatch("probe").GetProtocolVersion(); got != wire.ProtocolVersion {
		t.Errorf("dispatched at %d to an engine speaking %d", got, wire.ProtocolVersion)
	}
}

// Before any registration there is nothing to negotiate with. The registration
// case has already failed by then and every later case is skipped, so this only
// has to be a defined value rather than a panic.
func TestTheHarnessHasAVersionBeforeAnyEngineRegisters(t *testing.T) {
	h := &harness{}
	if got := h.newDispatch("probe").GetProtocolVersion(); got == 0 {
		t.Error("the harness has no dispatch version before a registration arrives")
	}
}
