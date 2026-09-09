package mirror

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// yamlMarshal is how a definition is rendered.
//
// UseProtoNames keeps the exported keys the same as the schema's, so a reader
// of the mirror and a reader of the .proto see one vocabulary. Unpopulated
// fields are left out: an export full of empty strings and UNSPECIFIED enums
// is unreadable, and every omitted value round-trips back to the same zero.
// Nothing beyond the message's own fields is written, so no backend-specific
// bookkeeping — layout, approval state, approver — can leak into a document
// that is supposed to import losslessly into another backend (ADR 0008).
var yamlMarshal = protojson.MarshalOptions{UseProtoNames: true}

// yamlUnmarshal is deliberately strict. A mirror is an export, so anything in
// the document this build cannot name is a hand edit or a version skew, and
// both are better reported than silently dropped — dropping is how an import
// loses exactly the data it was meant to carry.
var yamlUnmarshal = protojson.UnmarshalOptions{}

// ToYAML renders a pipeline definition as the YAML document the mirror holds.
//
// The output is stable for equal input: protojson deliberately varies its own
// whitespace, but the JSON is reparsed into sorted mappings on the way to
// YAML, so an unchanged definition produces an unchanged file and therefore no
// empty commit.
func ToYAML(p *dholev1.Pipeline) ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("mirror: encode definition: nil pipeline")
	}
	encoded, err := yamlMarshal.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("mirror: encode definition %s: %w", p.GetId(), err)
	}
	out, err := yaml.JSONToYAML(encoded)
	if err != nil {
		return nil, fmt.Errorf("mirror: encode definition %s: %w", p.GetId(), err)
	}
	return out, nil
}

// FromYAML parses a definition document back into a pipeline.
//
// It is the import half of the lossless round trip, not a read path for the
// running system: what runs comes from the definition store, never from the
// mirror.
func FromYAML(b []byte) (*dholev1.Pipeline, error) {
	encoded, err := yaml.YAMLToJSON(b)
	if err != nil {
		return nil, fmt.Errorf("mirror: decode definition: %w", err)
	}
	p := &dholev1.Pipeline{}
	if err := yamlUnmarshal.Unmarshal(encoded, p); err != nil {
		return nil, fmt.Errorf("mirror: decode definition: %w", err)
	}
	return p, nil
}
