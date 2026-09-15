package policy

import (
	_ "embed"
	"errors"
	"fmt"

	"sigs.k8s.io/yaml"
)

// Document is a tier policy as somebody writes it down: the file `dhole policy
// test --policy` evaluates and `dhole serve --policy` runs. One reader serves
// both, so what an author tests at their desk is exactly what the plane loads.
type Document struct {
	Revision string         `json:"revision"`
	Rules    []DocumentRule `json:"rules"`
	// Defaults are facts a `dhole policy test` case need not restate. The
	// plane ignores them: it decides on the facts of the real dispatch.
	Defaults *DocumentDefaults `json:"defaults,omitempty"`
}

// DocumentRule is one rule as written.
type DocumentRule struct {
	ID         string `json:"id"`
	Expression string `json:"expression"`
	Reason     string `json:"reason"`
}

// DocumentDefaults is what a document may fix for its test cases.
type DocumentDefaults struct {
	Upstream string `json:"upstream,omitempty"`
}

// ParseDocument reads a policy document. It is strict about the shape — a
// misspelt key is refused rather than read as an absent one, because an absent
// `rules` is a tier that denies everything — and leaves the rules themselves to
// TierPolicy, which compiles them.
func ParseDocument(raw []byte) (Document, error) {
	var doc Document
	if err := yaml.UnmarshalStrict(raw, &doc); err != nil {
		return Document{}, fmt.Errorf("policy: parse document: %w", err)
	}
	return doc, nil
}

// TierPolicy is the document as the engine evaluates it, compiled once to
// prove it can be.
//
// A document with no rules is refused. The engine denies an empty rule set,
// so accepting one would start a plane that refuses every step — which is
// never what somebody who wrote an empty file meant, and "permit everything"
// has to be said (see default.yaml) rather than left to an absence.
func (d Document) TierPolicy() (TierPolicy, error) {
	if len(d.Rules) == 0 {
		return TierPolicy{}, errors.New(
			"policy: the document has no rules, and a tier with no rules refuses everything; " +
				"to permit everything, write a rule that says so")
	}
	p := TierPolicy{Revision: d.Revision, Rules: make([]Rule, 0, len(d.Rules))}
	for _, r := range d.Rules {
		p.Rules = append(p.Rules, Rule(r))
	}
	if err := Validate(p); err != nil {
		return TierPolicy{}, err
	}
	return p, nil
}

//go:embed default.yaml
var defaultDocument []byte

// DefaultDocument is the built-in dispatch policy exactly as the binary ships
// it, comments included: what `dhole policy default` prints and what an
// operator copies to tighten (ADR 0032).
func DefaultDocument() []byte {
	return append([]byte(nil), defaultDocument...)
}

// Default is DefaultDocument as the engine evaluates it. It panics if the
// embedded document does not compile, which a test holds it to: the binary
// would otherwise ship a default that denies every step.
func Default() TierPolicy {
	doc, err := ParseDocument(defaultDocument)
	if err != nil {
		panic(err)
	}
	p, err := doc.TierPolicy()
	if err != nil {
		panic(err)
	}
	return p
}
