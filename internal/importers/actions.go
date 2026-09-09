package importers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// NewActions returns an importer for a GitHub Actions workflow.
//
// Actions hides two different shared workspaces. Inside a job the steps run
// one after another in the runner's working directory, and every step after
// `actions/checkout` depends on it without saying so. Between jobs the
// directory is not shared at all — `needs` plus the artifact actions are what
// carries state. Both become explicit ports here: steps chain to their
// predecessor in the job, and a job's first step reads the last step of each
// job it needs.
func NewActions() Importer { return actions{} }

type actions struct{}

// pureActions are the actions whose effect is confined to the runner's
// working directory. Everything else — every action that uploads, publishes,
// deploys or comments — falls to at-most-once, because a marketplace action
// is a program this package has never seen.
var pureActions = map[string]bool{
	"actions/checkout":      true,
	"actions/setup-go":      true,
	"actions/setup-node":    true,
	"actions/setup-python":  true,
	"actions/setup-java":    true,
	"actions/setup-dotnet":  true,
	"actions/cache":         true,
	"actions/cache/restore": true,
}

type actionsStep struct {
	Name             string          `json:"name"`
	Uses             string          `json:"uses"`
	Run              string          `json:"run"`
	If               string          `json:"if"`
	Shell            string          `json:"shell"`
	WorkingDirectory string          `json:"working-directory"`
	ContinueOnError  json.RawMessage `json:"continue-on-error"`
	Env              json.RawMessage `json:"env"`
	With             json.RawMessage `json:"with"`
}

type actionsJob struct {
	Name        string          `json:"name"`
	Needs       json.RawMessage `json:"needs"`
	Steps       []actionsStep   `json:"steps"`
	If          string          `json:"if"`
	Strategy    json.RawMessage `json:"strategy"`
	Uses        string          `json:"uses"`
	Container   json.RawMessage `json:"container"`
	Services    json.RawMessage `json:"services"`
	Environment json.RawMessage `json:"environment"`
	Outputs     json.RawMessage `json:"outputs"`
	Secrets     json.RawMessage `json:"secrets"`
}

func (actions) Import(ctx context.Context, src []byte) (*dholev1.Pipeline, Report, error) {
	var rep Report
	if err := ctx.Err(); err != nil {
		return nil, rep, err
	}
	doc, err := decodeYAMLDocument("actions", src)
	if err != nil {
		return nil, rep, err
	}
	var jobs map[string]actionsJob
	if err := decodeInto("actions", "jobs", doc["jobs"], &jobs); err != nil {
		return nil, rep, err
	}
	if len(jobs) == 0 {
		return nil, rep, fmt.Errorf("importers: actions: the workflow defines no job")
	}
	if _, ok := doc["on"]; ok {
		rep.warn("actions: on: describes triggers; import the trigger separately, the pipeline itself is unconditional")
	}
	if _, ok := doc["concurrency"]; ok {
		rep.unsupported("actions: concurrency: is dropped; no run is cancelled by another")
	}

	// Jobs are a mapping, so the file's order is not preserved by any YAML
	// decoder and is not meaningful in Actions either: `needs` is the whole
	// ordering. Sorting by name makes the import deterministic instead.
	names := make([]string, 0, len(jobs))
	for name := range jobs {
		names = append(names, name)
	}
	sort.Strings(names)

	p := &dholev1.Pipeline{Id: actionsPipelineID(doc)}
	taken := make(map[string]bool, len(jobs))
	first := make(map[string]*dholev1.Step, len(jobs))
	last := make(map[string]*dholev1.Step, len(jobs))

	for _, name := range names {
		job := jobs[name]
		actionsReportJob(name, job, &rep)

		steps := job.Steps
		if len(steps) == 0 {
			// A job with no steps is either a reusable-workflow call or a
			// malformed job. Either way it becomes a step, because the one
			// thing an import must never do is quietly lose a job.
			what := "no steps"
			if job.Uses != "" {
				what = "a reusable workflow, " + job.Uses
			}
			rep.unsupported("actions: job %q calls %s; the step is imported with nothing to run", name, what)
			steps = []actionsStep{{Name: name}}
		}

		var previous *dholev1.Step
		for i, st := range steps {
			id := uniqueID(taken, fmt.Sprintf("%s-%d-%s", slug(name), i+1, actionsStepSlug(st)))
			s := newWorkspaceStep(id, actionsStepName(st), actionsStepRef(st), actionsStepEffect(st))
			actionsReportStep(name, id, st, &rep)
			p.Steps = append(p.Steps, s)
			if previous != nil {
				// The runner's working directory, made visible.
				linkWorkspace(p, s, previous.GetId())
			}
			previous = s
			if i == 0 {
				first[name] = s
			}
			last[name] = s
		}
	}

	for _, name := range names {
		needs, ok := stringList(jobs[name].Needs)
		if !ok {
			return nil, rep, fmt.Errorf("importers: actions: cannot parse %q: needs is not a job name or a list of them", name)
		}
		for _, need := range needs {
			producer, known := last[need]
			if !known {
				return nil, rep, fmt.Errorf("importers: actions: job %q needs job %q, which the workflow does not define",
					name, need)
			}
			linkWorkspace(p, first[name], producer.GetId())
		}
	}
	return p, rep, nil
}

func actionsPipelineID(doc map[string]json.RawMessage) string {
	var name string
	if err := json.Unmarshal(doc["name"], &name); err == nil {
		if id := slug(name); id != "" {
			return id
		}
	}
	return "github-actions"
}

// actionsStepSlug names a step for its id: its own name when it has one, the
// action's last path element when it uses one, and `run` otherwise.
func actionsStepSlug(st actionsStep) string {
	if s := slug(st.Name); s != "" {
		return s
	}
	if st.Uses != "" {
		ref := st.Uses
		if at := strings.Index(ref, "@"); at >= 0 {
			ref = ref[:at]
		}
		parts := strings.Split(ref, "/")
		if s := slug(parts[len(parts)-1]); s != "" {
			return s
		}
	}
	return "run"
}

func actionsStepName(st actionsStep) string {
	if st.Name != "" {
		return st.Name
	}
	if st.Uses != "" {
		return st.Uses
	}
	return "run"
}

func actionsStepRef(st actionsStep) string {
	if st.Uses != "" {
		return "action:" + mustJSON(map[string]any{"uses": st.Uses, "with": json.RawMessage(st.With)})
	}
	return commandRef(strings.Split(st.Run, "\n"))
}

// actionsStepEffect: a `uses:` step is pure only when the action is on the
// list someone vouched for, and a `run:` step only when every command in it
// is. An action nobody listed may do anything at all.
func actionsStepEffect(st actionsStep) dholev1.EffectClass {
	if st.Uses != "" {
		ref := st.Uses
		if at := strings.Index(ref, "@"); at >= 0 {
			ref = ref[:at]
		}
		if pureActions[ref] {
			return dholev1.EffectClass_EFFECT_CLASS_PURE
		}
		return dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	}
	if strings.TrimSpace(st.Run) == "" {
		return dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	}
	return scriptEffectClass(strings.Split(st.Run, "\n"))
}

func actionsReportJob(name string, job actionsJob, rep *Report) {
	if len(job.Strategy) > 0 {
		rep.unsupported("actions: job %q: strategy.matrix is dropped; the job is imported once, not once per combination", name)
	}
	if job.If != "" {
		rep.unsupported("actions: job %q: if: is dropped; the job is imported unconditionally", name)
	}
	if len(job.Services) > 0 {
		rep.unsupported("actions: job %q: services: is dropped; no side-car container is started", name)
	}
	if len(job.Environment) > 0 {
		rep.unsupported("actions: job %q: environment: is dropped; the deployment is not tracked", name)
	}
	if len(job.Outputs) > 0 {
		rep.unsupported("actions: job %q: outputs: is dropped; declare a typed output port instead", name)
	}
	if len(job.Secrets) > 0 {
		rep.unsupported("actions: job %q: secrets: is dropped; the step redeems no secret reference", name)
	}
	if len(job.Container) > 0 {
		rep.warn("actions: job %q: container: names the environment; the steps keep their commands but not the image", name)
	}
}

func actionsReportStep(job, id string, st actionsStep, rep *Report) {
	if st.If != "" {
		rep.unsupported("actions: job %q step %q: if: is dropped; the step is imported unconditionally", job, id)
	}
	if len(st.ContinueOnError) > 0 {
		rep.unsupported("actions: job %q step %q: continue-on-error is dropped; a failing step fails the run", job, id)
	}
	if st.WorkingDirectory != "" {
		rep.warn("actions: job %q step %q: working-directory %q applies to the whole workspace port", job, id, st.WorkingDirectory)
	}
	if len(st.Env) > 0 {
		rep.warn("actions: job %q step %q: env: is not carried; declare the values on the step instead", job, id)
	}
	if strings.Contains(st.Run, "${{") {
		rep.warn("actions: job %q step %q: the ${{ }} expressions are not evaluated", job, id)
	}
}
