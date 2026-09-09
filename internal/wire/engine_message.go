package wire

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// This file is the framing of every engine-to-plane message, and the reason it
// exists is that a bare payload cannot say what it is.
//
// An EngineHeartbeat decodes cleanly as an EngineRegistration — both begin
// with engine_id, and protobuf cannot tell a packed `repeated uint32` from a
// `repeated message` on the wire. A control plane that guessed from content
// registered engines advertising no platform and no capabilities, which made
// every step unschedulable with nothing in any log saying why. Dispatching on
// the SUBJECT fixed the symptom, but a subject is a routing decision: it can
// be forwarded, bridged, renamed or mapped by an account import, and the type
// of a message must not depend on how it was delivered.
//
// EngineMessage puts the type in the bytes. See docs/wire-contract.md,
// "Message framing", which is the contract a third-party engine is written
// against.

// FrameRegistration puts a registration in the frame engines publish it in.
func FrameRegistration(reg *dholev1.EngineRegistration) *dholev1.EngineMessage {
	return &dholev1.EngineMessage{
		Body: &dholev1.EngineMessage_Registration{Registration: reg},
	}
}

// FrameHeartbeat puts a heartbeat in the frame engines publish it in.
func FrameHeartbeat(beat *dholev1.EngineHeartbeat) *dholev1.EngineMessage {
	return &dholev1.EngineMessage{
		Body: &dholev1.EngineMessage_Heartbeat{Heartbeat: beat},
	}
}

// DecodeEngineMessage recovers an engine-to-plane message from its bytes.
//
// A frame with NO body is not an error and not a corrupt message: it is an
// engine that speaks the earlier framing and published a bare payload. The
// frame's field numbers sit above every number either payload uses precisely
// so that case is unambiguous, and the caller falls back to the subject for
// those engines — which is what "the control plane must accept engines
// speaking version N and N-1" requires.
func DecodeEngineMessage(data []byte) (*dholev1.EngineMessage, error) {
	msg := &dholev1.EngineMessage{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return nil, fmt.Errorf("wire: decoding engine message: %w", err)
	}
	return msg, nil
}
