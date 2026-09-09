package defstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// State is where a revision sits in the approval machine: draft -> reviewed ->
// active. The values are stored verbatim, so they are a persistence contract:
// add new ones, never rename an existing one.
type State string

// The states a revision passes through. At most one revision of a pipeline is
// active at a time; a revision that was active and has been superseded steps
// back to reviewed, keeping the approver who signed it off.
const (
	StateDraft    State = "draft"
	StateReviewed State = "reviewed"
	StateActive   State = "active"
)

// Revision is one immutable definition of a pipeline. ID is derived from
// ContentHash, so the identity a run pins is the content itself: two saves of
// identical bytes are the same revision, and no edit can reach a run that
// already started.
type Revision struct {
	ID          string
	PipelineID  string
	ContentHash string
	State       State
	Lockfile    map[string]string
	Author      string
	Approver    string
}

// hashDomain separates this hash from any other sha256 in the system, so a
// digest can never be mistaken for one taken over different bytes for a
// different purpose. It names the message type as well, because the hash is
// meaningful only for that type.
const hashDomain = "dhole.v1.Pipeline\x00"

// ContentHash is the revision identity of a definition: the hex sha256 over
// its canonical encoding.
//
// Protobuf is not canonical by default. Two encodings of an equal message can
// differ through map-entry ordering, which the runtime deliberately randomises,
// and through unknown fields, which proto.Unmarshal retains verbatim so a
// message that has passed through a build older than the peer that produced it
// carries bytes this build cannot even name. Either would make the hash depend
// on the process that computed it rather than on the definition — and a hash
// that moves between processes turns "which revision ran?" back into a
// question nobody can answer. So the encoding here is pinned twice: unknown
// fields are stripped from a clone, and the marshal is deterministic.
func ContentHash(p *dholev1.Pipeline) string {
	sum := sha256.Sum256(canonicalBytes(p))
	return hex.EncodeToString(sum[:])
}

// canonicalBytes is the encoding ContentHash is taken over.
func canonicalBytes(p *dholev1.Pipeline) []byte {
	clone, ok := proto.Clone(p).(*dholev1.Pipeline)
	if !ok {
		// proto.Clone returns the concrete type it was given; this cannot
		// happen, and hashing a partial encoding would be worse than a panic.
		panic("defstore: proto.Clone returned a different message type")
	}
	stripUnknown(clone.ProtoReflect())

	// Deterministic is what makes map fields emit in a fixed order. The
	// Pipeline message has none today; the guarantee has to already be here
	// when one is added, because by then every stored hash depends on it.
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(clone)
	if err != nil {
		// A Pipeline held in memory has no way to fail encoding, and a
		// silently truncated hash would collide across definitions.
		panic(fmt.Sprintf("defstore: marshal pipeline for hashing: %v", err))
	}
	return append([]byte(hashDomain), encoded...)
}

// stripUnknown clears the unknown fields of m and of every message reachable
// from it, so bytes a newer peer sent cannot change this build's hash.
func stripUnknown(m protoreflect.Message) {
	m.SetUnknown(nil)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					stripUnknown(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if fd.Message() != nil {
				list := v.List()
				for i := range list.Len() {
					stripUnknown(list.Get(i).Message())
				}
			}
		case fd.Message() != nil:
			stripUnknown(v.Message())
		}
		return true
	})
}
