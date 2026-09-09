// Command typecheck-oracle answers, for a list of pipelines given as JSON on
// stdin, exactly what internal/dag.TypeCheck answers.
//
// It exists so that src/canvas/edges.ts can be tested against the REAL type
// checker rather than against somebody's memory of it. The canvas has to
// refuse a bad edge in the browser, at drop time, without a round trip (ADR
// 0001), which means the rule is implemented twice - and two implementations
// of one rule drift unless something fails when they disagree. That something
// is src/canvas/edges.agreement.test.ts, and this program is how it reaches
// the Go side.
//
// It is deliberately thin. It decodes, it calls dag.TypeCheck, it encodes.
// Anything it did beyond that would be a third implementation to keep in step.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"google.golang.org/protobuf/encoding/protojson"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
)

// diagnostic is dag.Diagnostic in the shape the TypeScript side compares
// against. Line and Col are omitted: a pipeline that arrived as JSON carries
// no source positions, and the canvas has none to show.
type diagnostic struct {
	StepID   string `json:"stepId"`
	PortName string `json:"portName"`
	Message  string `json:"message"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "typecheck-oracle: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var cases []json.RawMessage
	if err := json.NewDecoder(os.Stdin).Decode(&cases); err != nil {
		return fmt.Errorf("reading the case list: %w", err)
	}

	// A slice per case, never nil: a JSON null and an empty list would read
	// the same on the other side, and "no diagnostics" is the answer most
	// worth being unambiguous about.
	answers := make([][]diagnostic, 0, len(cases))
	for i, raw := range cases {
		var pipeline dholev1.Pipeline
		if err := protojson.Unmarshal(raw, &pipeline); err != nil {
			return fmt.Errorf("case %d is not a dhole.v1.Pipeline: %w", i, err)
		}
		found := []diagnostic{}
		for _, d := range dag.TypeCheck(&pipeline) {
			found = append(found, diagnostic{
				StepID:   d.StepID,
				PortName: d.PortName,
				Message:  d.Message,
			})
		}
		answers = append(answers, found)
	}

	return json.NewEncoder(os.Stdout).Encode(answers)
}
