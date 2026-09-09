package importers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// NewN8N returns an importer for an n8n workflow export.
//
// n8n is the one source format that already declares its data flow: nodes are
// wired by connections, and what travels along a connection is a list of JSON
// items. So the translation is close to exact — and the ports must carry a
// structured type rather than an opaque blob, because a blob would type-check
// against anything and the editor could no longer reject a bad wire before
// the run.
func NewN8N() Importer { return n8n{} }

type n8n struct{}

// n8nItemsSchema is the schema id every n8n connection carries: a list of
// items, each with a `json` object and optional binary attachments. Two ports
// are compatible when their schema ids match, so naming it once is what makes
// an imported n8n workflow type-check at all.
const n8nItemsSchema = "https://schemas.dhole.dev/n8n/items.v1.json"

// pureN8NNodes are the node types whose whole effect is the items they emit.
// Everything else — every node that calls an API, sends a message, writes to
// a database, or waits for the outside world — is at-most-once, because n8n
// carries no idempotency key this package could honour.
var pureN8NNodes = map[string]bool{
	"n8n-nodes-base.aggregate":  true,
	"n8n-nodes-base.dateTime":   true,
	"n8n-nodes-base.filter":     true,
	"n8n-nodes-base.if":         true,
	"n8n-nodes-base.itemLists":  true,
	"n8n-nodes-base.limit":      true,
	"n8n-nodes-base.merge":      true,
	"n8n-nodes-base.noOp":       true,
	"n8n-nodes-base.renameKeys": true,
	"n8n-nodes-base.set":        true,
	"n8n-nodes-base.sort":       true,
	"n8n-nodes-base.splitOut":   true,
	"n8n-nodes-base.summarize":  true,
	"n8n-nodes-base.switch":     true,
}

type n8nNode struct {
	Name             string          `json:"name"`
	Type             string          `json:"type"`
	Disabled         bool            `json:"disabled"`
	RetryOnFail      bool            `json:"retryOnFail"`
	ContinueOnFail   bool            `json:"continueOnFail"`
	OnError          string          `json:"onError"`
	AlwaysOutputData bool            `json:"alwaysOutputData"`
	Parameters       json.RawMessage `json:"parameters"`
	Credentials      json.RawMessage `json:"credentials"`
}

type n8nConnection struct {
	Node  string `json:"node"`
	Type  string `json:"type"`
	Index int    `json:"index"`
}

type n8nExport struct {
	Name  string    `json:"name"`
	Nodes []n8nNode `json:"nodes"`
	// Connections is source node -> output type -> output index -> targets.
	Connections map[string]map[string][][]n8nConnection `json:"connections"`
	PinData     map[string]json.RawMessage              `json:"pinData"`
}

func (n8n) Import(ctx context.Context, src []byte) (*dholev1.Pipeline, Report, error) {
	var rep Report
	if err := ctx.Err(); err != nil {
		return nil, rep, err
	}
	if len(bytes.TrimSpace(src)) == 0 {
		return nil, rep, fmt.Errorf("importers: n8n: the source document is empty")
	}
	var export n8nExport
	if err := json.Unmarshal(src, &export); err != nil {
		return nil, rep, fmt.Errorf("importers: n8n: parse JSON: %w", err)
	}
	if len(export.Nodes) == 0 {
		return nil, rep, fmt.Errorf("importers: n8n: the export defines no node")
	}
	if len(export.PinData) > 0 {
		rep.unsupported("n8n: pinData is dropped; a run reads its real input, not the data pinned in the editor")
	}

	p := &dholev1.Pipeline{Id: n8nPipelineID(export.Name)}
	taken := make(map[string]bool, len(export.Nodes))
	byName := make(map[string]*dholev1.Step, len(export.Nodes))
	for _, node := range export.Nodes {
		if node.Name == "" {
			return nil, rep, fmt.Errorf("importers: n8n: a node has no name, so nothing can connect to it")
		}
		if _, duplicate := byName[node.Name]; duplicate {
			return nil, rep, fmt.Errorf("importers: n8n: two nodes are both named %q", node.Name)
		}
		s := &dholev1.Step{
			Id:          uniqueID(taken, slug(node.Name)),
			Name:        node.Name,
			PluginRef:   "n8n:" + mustJSON(map[string]any{"type": node.Type, "parameters": json.RawMessage(node.Parameters)}),
			EffectClass: n8nEffect(node),
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{n8nPort("main_0")},
		}
		if !n8nIsTrigger(node.Type) {
			s.Inputs = []*dholev1.Port{n8nPort("main_0")}
		}
		n8nReportNode(node, &rep)
		p.Steps = append(p.Steps, s)
		byName[node.Name] = s
	}

	sources := make([]string, 0, len(export.Connections))
	for name := range export.Connections {
		sources = append(sources, name)
	}
	sort.Strings(sources)

	for _, source := range sources {
		from, known := byName[source]
		if !known {
			return nil, rep, fmt.Errorf("importers: n8n: connections name node %q, which the export does not define", source)
		}
		outputs := export.Connections[source]
		kinds := make([]string, 0, len(outputs))
		for kind := range outputs {
			kinds = append(kinds, kind)
		}
		sort.Strings(kinds)

		for _, kind := range kinds {
			if kind != "main" {
				// An ai_tool or ai_memory connection is a different kind of
				// wire; naming it is better than pretending it was data.
				rep.unsupported("n8n: node %q has %q connections, which are not imported", source, kind)
				continue
			}
			for index, targets := range outputs[kind] {
				fromPort := fmt.Sprintf("main_%d", index)
				for _, target := range targets {
					to, known := byName[target.Node]
					if !known {
						return nil, rep, fmt.Errorf(
							"importers: n8n: node %q connects to node %q, which the export does not define",
							source, target.Node)
					}
					toPort := fmt.Sprintf("main_%d", target.Index)
					n8nEnsurePort(&from.Outputs, fromPort)
					n8nEnsurePort(&to.Inputs, toPort)
					p.Edges = append(p.Edges, &dholev1.Edge{
						FromStep: from.GetId(), FromPort: fromPort,
						ToStep: to.GetId(), ToPort: toPort,
					})
				}
			}
		}
	}
	return p, rep, nil
}

func n8nPipelineID(name string) string {
	if id := slug(name); id != "" {
		return id
	}
	return "n8n-workflow"
}

func n8nPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Structured{
				Structured: &dholev1.StructType{SchemaId: n8nItemsSchema},
			},
		},
	}
}

func n8nEnsurePort(ports *[]*dholev1.Port, name string) {
	for _, p := range *ports {
		if p.GetName() == name {
			return
		}
	}
	*ports = append(*ports, n8nPort(name))
}

// n8nIsTrigger reports whether a node starts a workflow rather than
// transforming what reached it. A trigger has no input to declare.
func n8nIsTrigger(nodeType string) bool {
	lower := strings.ToLower(nodeType)
	return strings.Contains(lower, "trigger") ||
		strings.HasSuffix(lower, ".webhook") ||
		strings.HasSuffix(lower, ".cron") ||
		strings.HasSuffix(lower, ".interval") ||
		strings.HasSuffix(lower, ".start")
}

// n8nEffect: a node is pure only when its type is on the list. A trigger
// never is — it observes the outside world, and re-running one is not free.
func n8nEffect(node n8nNode) dholev1.EffectClass {
	if !n8nIsTrigger(node.Type) && pureN8NNodes[node.Type] {
		return dholev1.EffectClass_EFFECT_CLASS_PURE
	}
	return dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
}

func n8nReportNode(node n8nNode, rep *Report) {
	if node.RetryOnFail {
		rep.unsupported("n8n: node %q sets retryOnFail; retry follows from the effect class instead", node.Name)
	}
	if node.ContinueOnFail || node.OnError != "" {
		rep.unsupported("n8n: node %q continues on error; a failing step fails the run", node.Name)
	}
	if node.Disabled {
		rep.unsupported("n8n: node %q is disabled in the export; it is imported as a step that runs", node.Name)
	}
	if len(node.Credentials) > 0 {
		rep.unsupported("n8n: node %q uses stored credentials; the step redeems no secret reference", node.Name)
	}
	if bytes.Contains(node.Parameters, []byte("={{")) {
		rep.warn("n8n: node %q uses n8n expressions, which are kept verbatim and not evaluated", node.Name)
	}
}
