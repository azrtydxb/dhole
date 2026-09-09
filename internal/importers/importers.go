// Package importers translates the pipeline formats of other systems into
// Dhole pipelines.
//
// Two rules make an import honest, and both exist because the alternative is
// a pipeline that looks like the original and behaves differently.
//
// Nothing is dropped in silence. A construct this package cannot express
// becomes an entry in the Report naming it, and the job that carried it still
// becomes a step. An import that quietly omits a job produces a pipeline that
// looks complete and does less than the source, which the operator discovers
// in production.
//
// Nothing is more permissive than the source. A step this package cannot
// prove pure is emitted as EFFECT_CLASS_AT_MOST_ONCE, so it is never cached
// and never auto-retried (ADR 0002). Guessing "probably idempotent" is how an
// import turns a deploy that ran once into one that runs three times.
//
// The hard part is the shared workspace. GitLab, GitHub Actions and
// Woodpecker all pass state between steps through an implicit directory that
// nothing declares. Dhole has no such thing on purpose: a step whose inputs
// are "the filesystem, mutated by unspecified predecessors" cannot be cached,
// parallelised or reproduced (ADR 0001). So the implicit flow is translated
// into explicit ports — each step publishes its workspace on a declared
// output, and each consumer declares one input per predecessor, wired by an
// edge. The derived DAG then says exactly what the source meant, and the
// cache key can see everything the step reads.
package importers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"sigs.k8s.io/yaml"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Importer turns one source document into a pipeline and a report of what
// could not be carried across.
type Importer interface {
	// Import parses src. It returns an error naming what could not be parsed
	// rather than a partial pipeline: a half-imported pipeline is worse than
	// none, because it looks like the import worked.
	Import(ctx context.Context, src []byte) (*dholev1.Pipeline, Report, error)
}

// Report is everything the import could not carry across faithfully.
//
// Unsupported names a construct whose behaviour is gone: the pipeline runs
// without it. Warnings names a construct that was translated approximately,
// where the result still does what the source did.
type Report struct {
	Unsupported []string
	Warnings    []string
}

func (r *Report) unsupported(format string, args ...any) {
	r.Unsupported = append(r.Unsupported, fmt.Sprintf(format, args...))
}

func (r *Report) warn(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

const (
	// workspaceOutput is the port on which a step publishes the working
	// directory the source format would have left on a shared volume.
	workspaceOutput = "workspace"
	// workspaceMediaType is advisory, like every blob media type; it exists
	// so an operator reading the imported definition can tell what the port
	// carries.
	workspaceMediaType = "application/x-dhole-workspace+tar"
)

// workspaceInput names the input port on which a step receives one specific
// predecessor's workspace. One port per predecessor rather than one merged
// port, because that is what makes the source of each byte visible in the
// definition and in the cache key.
func workspaceInput(fromStep string) string {
	return "workspace_from_" + fromStep
}

func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: workspaceMediaType}},
		},
	}
}

// linkWorkspace makes one implicit "step B sees what step A left behind"
// relationship explicit: an input port on the consumer and the edge that
// fills it. Adding the port and the edge together is the point — an edge to a
// port nothing declares does not type-check, and a declared port with no edge
// carries nothing.
func linkWorkspace(p *dholev1.Pipeline, consumer *dholev1.Step, producerID string) {
	name := workspaceInput(producerID)
	for _, in := range consumer.GetInputs() {
		if in.GetName() == name {
			return
		}
	}
	consumer.Inputs = append(consumer.Inputs, blobPort(name))
	p.Edges = append(p.Edges, &dholev1.Edge{
		FromStep: producerID,
		FromPort: workspaceOutput,
		ToStep:   consumer.GetId(),
		ToPort:   name,
	})
}

// newWorkspaceStep builds a step in the only sandbox scope that carries no
// hidden state. Anything wider is the shared workspace under another name:
// state the cache key cannot see.
func newWorkspaceStep(id, name, pluginRef string, effect dholev1.EffectClass) *dholev1.Step {
	return &dholev1.Step{
		Id:          id,
		Name:        name,
		PluginRef:   pluginRef,
		EffectClass: effect,
		Outputs:     []*dholev1.Port{blobPort(workspaceOutput)},
		LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
	}
}

// commandRef renders a shell script as the placeholder plugin reference the
// rest of the tree already uses for a command step (see
// testdata/pipelines/two-step.yaml). The script is preserved verbatim so a
// reader of the imported definition sees what the source job ran.
func commandRef(script []string) string {
	joined := strings.Join(script, "\n")
	return fmt.Sprintf("command:%s", mustJSON(map[string]any{
		"args": []string{"/bin/sh", "-c", joined},
	}))
}

// slug turns a source identifier — a job name, a node label — into a step id.
// Source formats allow characters an id should not carry (`build:binary`,
// `Upload binary`), and two different names must never collapse onto one id,
// which uniqueIDs below enforces.
func slug(s string) string {
	var b strings.Builder
	lastDash := true
	for _, r := range strings.ToLower(s) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// uniqueID keeps ids distinct when two source names slug to the same thing.
// Collapsing them would silently merge two jobs into one, which is a dropped
// job wearing a disguise.
func uniqueID(taken map[string]bool, base string) string {
	if base == "" {
		base = "step"
	}
	id := base
	for n := 2; taken[id]; n++ {
		id = fmt.Sprintf("%s-%d", base, n)
	}
	taken[id] = true
	return id
}

// decodeYAMLDocument parses a source document into a top-level mapping.
//
// Every error names the system whose importer was asked to read the file:
// "this is not valid YAML" is useless to someone who fed a Woodpecker file to
// the GitLab importer by mistake.
func decodeYAMLDocument(system string, src []byte) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(src)) == 0 {
		return nil, fmt.Errorf("importers: %s: the source document is empty", system)
	}
	encoded, err := yaml.YAMLToJSON(src)
	if err != nil {
		return nil, fmt.Errorf("importers: %s: parse YAML: %w", system, err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &doc); err != nil {
		return nil, fmt.Errorf("importers: %s: the document is not a mapping of keys: %w", system, err)
	}
	return doc, nil
}

// decodeInto is the strict-enough decode of one field of a document, with the
// field named in the error so a malformed file says which key is wrong.
func decodeInto(system, field string, raw json.RawMessage, v any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("importers: %s: cannot parse %q: %w", system, field, err)
	}
	return nil
}

// stringList reads the "a string, or a list of strings" shape all three CI
// formats use for scripts, needs and tags.
func stringList(raw json.RawMessage) ([]string, bool) {
	if len(raw) == 0 {
		return nil, true
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		return []string{one}, true
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many, true
	}
	// A list of nested lists is legal in GitLab's `script`; flatten it rather
	// than refusing a file GitLab itself accepts.
	var nested []json.RawMessage
	if err := json.Unmarshal(raw, &nested); err != nil {
		return nil, false
	}
	var out []string
	for _, item := range nested {
		part, ok := stringList(item)
		if !ok {
			return nil, false
		}
		out = append(out, part...)
	}
	return out, true
}
