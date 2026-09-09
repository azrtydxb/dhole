package wire_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/wire"
)

// TestABareHeartbeatDecodesAsARegistration is the defect, written down. It is
// not a regression guard — it is the evidence that the type of an engine
// message CANNOT be recovered from bare bytes, which is why the frame below
// has to exist.
//
// Both messages begin with engine_id, and protobuf cannot tell a packed
// `repeated uint32` from a `repeated message` on the wire, so a heartbeat's
// bytes are a valid registration. A plane guessing by content registered
// engines with no platform and no capabilities, and every step after that was
// unschedulable with nothing saying why.
func TestABareHeartbeatDecodesAsARegistration(t *testing.T) {
	beat := &dholev1.EngineHeartbeat{
		EngineId: "engine-1",
		InFlight: []*dholev1.InFlight{{RunId: "run-1", StepId: "build", Attempt: 1}},
	}
	raw, err := proto.Marshal(beat)
	require.NoError(t, err)

	mistaken := &dholev1.EngineRegistration{}
	require.NoError(t, proto.Unmarshal(raw, mistaken),
		"a heartbeat's bytes are a valid registration: the type is not in them")
	require.Equal(t, "engine-1", mistaken.GetEngineId())
}

// TestAFramedMessageCarriesItsOwnType is the fix. The frame puts the type in
// the bytes, where it travels with the message whatever subject carries it —
// a subject is a routing decision, and routing can be forwarded, bridged or
// renamed.
func TestAFramedMessageCarriesItsOwnType(t *testing.T) {
	beat := &dholev1.EngineHeartbeat{
		EngineId: "engine-1",
		InFlight: []*dholev1.InFlight{{RunId: "run-1", StepId: "build", Attempt: 1}},
	}
	raw, err := proto.Marshal(wire.FrameHeartbeat(beat))
	require.NoError(t, err)

	framed, err := wire.DecodeEngineMessage(raw)
	require.NoError(t, err)
	require.Nil(t, framed.GetRegistration(), "a heartbeat must never present as a registration")
	require.NotNil(t, framed.GetHeartbeat())
	require.Equal(t, "engine-1", framed.GetHeartbeat().GetEngineId())
	require.Len(t, framed.GetHeartbeat().GetInFlight(), 1)

	reg := &dholev1.EngineRegistration{
		EngineId: "engine-1", Os: "linux", Arch: "arm64", Slots: 4,
		ProtocolVersions: []uint32{1},
		Capabilities:     []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
	}
	raw, err = proto.Marshal(wire.FrameRegistration(reg))
	require.NoError(t, err)

	framed, err = wire.DecodeEngineMessage(raw)
	require.NoError(t, err)
	require.Nil(t, framed.GetHeartbeat())
	require.NotNil(t, framed.GetRegistration())
	require.Equal(t, "arm64", framed.GetRegistration().GetArch(),
		"the platform is what the scheduler matches on; losing it makes every step unschedulable")
}

// TestABarePayloadFramesWithNoBody is what makes the frame safe to introduce
// into a live fleet. An engine written against the earlier framing publishes a
// bare payload; that has to be recognisable as such rather than parsed as a
// framed message with a garbled body, which is why the frame's field numbers
// sit above every number either payload uses.
func TestABarePayloadFramesWithNoBody(t *testing.T) {
	for name, msg := range map[string]proto.Message{
		"registration": &dholev1.EngineRegistration{EngineId: "engine-1", Os: "linux", Arch: "amd64"},
		"heartbeat": &dholev1.EngineHeartbeat{EngineId: "engine-1",
			InFlight: []*dholev1.InFlight{{RunId: "run-1", StepId: "build"}}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := proto.Marshal(msg)
			require.NoError(t, err)

			framed, err := wire.DecodeEngineMessage(raw)
			require.NoError(t, err)
			require.Nil(t, framed.GetRegistration())
			require.Nil(t, framed.GetHeartbeat())
		})
	}
}

// TestAFramedMessageIsNotSILENTLYReadableAsABarePayload closes the other
// direction. An older control plane reading a framed registration must get
// something it refuses — an engine with no id — rather than a plausible
// instance built from the wrong bytes.
func TestAFramedMessageIsNotSilentlyReadableAsABarePayload(t *testing.T) {
	raw, err := proto.Marshal(wire.FrameRegistration(
		&dholev1.EngineRegistration{EngineId: "engine-1", Os: "linux", Arch: "amd64"}))
	require.NoError(t, err)

	bare := &dholev1.EngineRegistration{}
	require.NoError(t, proto.Unmarshal(raw, bare))
	require.Empty(t, bare.GetEngineId(),
		"a framed message read as a bare one must be empty, and refused, not plausible")
}
