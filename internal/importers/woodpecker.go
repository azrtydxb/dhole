package importers

import (
	"context"
	"encoding/json"
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// NewWoodpecker returns an importer for a Woodpecker pipeline.
//
// Woodpecker is the purest form of the problem ADR 0001 exists to solve: the
// steps declare no dependencies at all, they simply run in file order and all
// mutate the same `/woodpecker/src` volume. Nothing in the document says that
// `build` reads what `restore-deps` downloaded — the volume says it, at run
// time, invisibly. The import turns that order into edges between declared
// ports, which is the only form the derived DAG and the cache can see.
func NewWoodpecker() Importer { return woodpecker{} }

type woodpecker struct{}

type woodpeckerStep struct {
	Name        string          `json:"name"`
	Image       string          `json:"image"`
	Commands    json.RawMessage `json:"commands"`
	Command     json.RawMessage `json:"command"`
	Settings    json.RawMessage `json:"settings"`
	Secrets     json.RawMessage `json:"secrets"`
	Environment json.RawMessage `json:"environment"`
	When        json.RawMessage `json:"when"`
	DependsOn   json.RawMessage `json:"depends_on"`
	Group       string          `json:"group"`
	Detach      bool            `json:"detach"`
	Failure     string          `json:"failure"`
	Privileged  bool            `json:"privileged"`
	Volumes     json.RawMessage `json:"volumes"`
}

func (woodpecker) Import(ctx context.Context, src []byte) (*dholev1.Pipeline, Report, error) {
	var rep Report
	if err := ctx.Err(); err != nil {
		return nil, rep, err
	}
	doc, err := decodeYAMLDocument("woodpecker", src)
	if err != nil {
		return nil, rep, err
	}

	raw, ok := doc["steps"]
	if !ok {
		raw = doc["pipeline"]
	}
	if len(raw) == 0 {
		return nil, rep, fmt.Errorf("importers: woodpecker: the document defines no steps: key")
	}
	var steps []woodpeckerStep
	if err := json.Unmarshal(raw, &steps); err != nil {
		// The mapping form carries its order in the file, and no YAML decoder
		// hands that order back. Reordering steps that pass state through a
		// shared volume silently produces a different pipeline, so this is an
		// error rather than a guess.
		var mapping map[string]woodpeckerStep
		if json.Unmarshal(raw, &mapping) == nil {
			return nil, rep, fmt.Errorf(
				"importers: woodpecker: steps: is a mapping, whose order cannot be recovered and whose order is the pipeline; convert it to a list first")
		}
		return nil, rep, fmt.Errorf("importers: woodpecker: cannot parse %q: %w", "steps", err)
	}
	if len(steps) == 0 {
		return nil, rep, fmt.Errorf("importers: woodpecker: the document defines no steps: key")
	}

	for key, message := range map[string]string{
		"services": "services: is dropped; no side-car container is started",
		"when":     "the top-level when: describes triggers; the pipeline is imported unconditionally",
		"matrix":   "matrix: is dropped; the pipeline is imported once, not once per combination",
		"clone":    "clone: is dropped; declare the checkout as a step with a declared output",
		"depends_on": "the top-level depends_on: is dropped; it names other pipelines, " +
			"which are imported separately",
	} {
		if _, ok := doc[key]; ok {
			rep.unsupported("woodpecker: %s", message)
		}
	}

	p := &dholev1.Pipeline{Id: "woodpecker"}
	taken := make(map[string]bool, len(steps))
	byName := make(map[string]*dholev1.Step, len(steps))
	ids := make([]string, 0, len(steps))
	for i, st := range steps {
		name := st.Name
		if name == "" {
			name = fmt.Sprintf("step-%d", i+1)
			rep.warn("woodpecker: step %d has no name; it is imported as %q", i+1, name)
		}
		commands, err := woodpeckerCommands(name, st)
		if err != nil {
			return nil, rep, err
		}
		effect := scriptEffectClass(commands)
		ref := commandRef(commands)
		if len(commands) == 0 {
			// A plugin step: the behaviour lives in the image and the
			// settings, neither of which this package can read. It is
			// imported, never dropped, and never claimed to be safe to
			// repeat.
			rep.unsupported("woodpecker: step %q is a plugin (%s) configured by settings:; "+
				"its behaviour is not imported", name, st.Image)
			ref = "plugin:" + mustJSON(map[string]any{
				"image":    st.Image,
				"settings": json.RawMessage(st.Settings),
			})
		}
		id := uniqueID(taken, slug(name))
		s := newWorkspaceStep(id, name, ref, effect)
		woodpeckerReportStep(name, st, &rep)
		p.Steps = append(p.Steps, s)
		byName[name] = s
		ids = append(ids, id)
	}

	for i, st := range steps {
		consumer := p.GetSteps()[i]
		if len(st.DependsOn) > 0 {
			depends, ok := stringList(st.DependsOn)
			if !ok {
				return nil, rep, fmt.Errorf("importers: woodpecker: cannot parse %q: depends_on is not a list of step names",
					consumer.GetName())
			}
			for _, dep := range depends {
				producer, known := byName[dep]
				if !known {
					return nil, rep, fmt.Errorf("importers: woodpecker: step %q depends on step %q, which the document does not define",
						consumer.GetName(), dep)
				}
				linkWorkspace(p, consumer, producer.GetId())
			}
			continue
		}
		// No depends_on: the step reads what the step before it left in the
		// shared volume. That is the whole of Woodpecker's ordering, and
		// losing it loses the pipeline.
		if i > 0 {
			linkWorkspace(p, consumer, ids[i-1])
		}
	}
	return p, rep, nil
}

// woodpeckerCommands reads the two spellings of a step's script.
func woodpeckerCommands(name string, st woodpeckerStep) ([]string, error) {
	raw := st.Commands
	if len(raw) == 0 {
		raw = st.Command
	}
	commands, ok := stringList(raw)
	if !ok {
		return nil, fmt.Errorf("importers: woodpecker: cannot parse %q: commands is not a list of commands", name)
	}
	return commands, nil
}

func woodpeckerReportStep(name string, st woodpeckerStep, rep *Report) {
	if len(st.When) > 0 {
		rep.unsupported("woodpecker: step %q: when: is dropped; the step is imported unconditionally", name)
	}
	if len(st.Secrets) > 0 {
		rep.unsupported("woodpecker: step %q: secrets: is dropped; the step redeems no secret reference", name)
	}
	if st.Detach {
		rep.unsupported("woodpecker: step %q: detach: is dropped; the step runs to completion like any other", name)
	}
	if st.Failure != "" {
		rep.unsupported("woodpecker: step %q: failure: %s is dropped; a failing step fails the run", name, st.Failure)
	}
	if st.Privileged {
		rep.unsupported("woodpecker: step %q: privileged: is dropped; declare the capability on the step instead", name)
	}
	if len(st.Volumes) > 0 {
		rep.unsupported("woodpecker: step %q: volumes: is dropped; a host mount is ambient state a cache key cannot see", name)
	}
	if st.Group != "" {
		rep.warn("woodpecker: step %q: group %q is dropped; parallelism follows from the edges instead", name, st.Group)
	}
	if len(st.Environment) > 0 {
		rep.warn("woodpecker: step %q: environment: is not carried; declare the values on the step instead", name)
	}
	if st.Image != "" {
		rep.warn("woodpecker: step %q: image %q names the environment; the step keeps its commands but not the image",
			name, st.Image)
	}
}
