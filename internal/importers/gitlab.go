package importers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// NewGitLab returns an importer for `.gitlab-ci.yml`.
//
// GitLab's model is stages plus one shared workspace: a job sees whatever the
// previous stage left in the working directory, and `artifacts:`/`cache:`
// decide which parts of that directory survive. None of it is declared per
// job, so the translation makes it declared — every job publishes its
// workspace and names the jobs it reads.
func NewGitLab() Importer { return gitLab{} }

type gitLab struct{}

// gitlabGlobalKeys are the top-level keys that configure the file rather than
// define a job. Everything else at the top level is a job, which is why the
// list has to be complete: mistaking a job for configuration drops it.
var gitlabGlobalKeys = map[string]bool{
	"after_script":  true,
	"before_script": true,
	"cache":         true,
	"default":       true,
	"image":         true,
	"include":       true,
	"services":      true,
	"stages":        true,
	"types":         true,
	"variables":     true,
	"workflow":      true,
}

// gitlabDefaultStages is the stage list GitLab uses when the file names none.
var gitlabDefaultStages = []string{".pre", "build", "test", "deploy", ".post"}

type gitlabJob struct {
	name   string
	id     string
	stage  string
	script []string
	needs  []string
	// hasNeeds distinguishes "needs: []" — which in GitLab means "start
	// immediately" — from an absent key, which means "wait for the previous
	// stage". Collapsing the two changes when the job runs.
	hasNeeds bool
}

func (gitLab) Import(ctx context.Context, src []byte) (*dholev1.Pipeline, Report, error) {
	var rep Report
	if err := ctx.Err(); err != nil {
		return nil, rep, err
	}
	doc, err := decodeYAMLDocument("gitlab", src)
	if err != nil {
		return nil, rep, err
	}

	for key, message := range map[string]string{
		"include":  "the top-level include: is not resolved; the included files are not imported",
		"cache":    "the top-level cache: is dropped; Dhole caches by content, not by a key you invent",
		"workflow": "the top-level workflow:rules is dropped; the pipeline is imported unconditionally",
		"services": "the top-level services: is dropped; no side-car container is started",
	} {
		if _, ok := doc[key]; ok {
			rep.unsupported("gitlab: %s", message)
		}
	}

	stages, err := gitlabStages(doc)
	if err != nil {
		return nil, rep, err
	}
	before, after, err := gitlabDefaultScripts(doc, &rep)
	if err != nil {
		return nil, rep, err
	}

	jobs, err := gitlabJobs(doc, stages, before, after, &rep)
	if err != nil {
		return nil, rep, err
	}
	if len(jobs) == 0 {
		return nil, rep, fmt.Errorf("importers: gitlab: the document defines no job")
	}

	p := &dholev1.Pipeline{Id: "gitlab-ci"}
	byID := make(map[string]*dholev1.Step, len(jobs))
	for _, j := range jobs {
		s := newWorkspaceStep(j.id, j.name, commandRef(j.script), scriptEffectClass(j.script))
		byID[j.id] = s
		p.Steps = append(p.Steps, s)
	}

	// Stage ordering is a real dependency, not decoration: a job with no
	// `needs` waits for the whole preceding stage, and an importer that reads
	// only `needs` turns a staged pipeline into a free-for-all where the
	// deploy runs beside the tests.
	byStage := make(map[string][]*gitlabJob, len(stages))
	for i := range jobs {
		byStage[jobs[i].stage] = append(byStage[jobs[i].stage], &jobs[i])
	}
	for i := range jobs {
		j := &jobs[i]
		deps := j.needs
		if !j.hasNeeds {
			deps = gitlabPreviousStageJobs(stages, byStage, j.stage)
		}
		for _, dep := range deps {
			linkWorkspace(p, byID[j.id], dep)
		}
	}
	return p, rep, nil
}

func gitlabStages(doc map[string]json.RawMessage) ([]string, error) {
	raw, ok := doc["stages"]
	if !ok {
		raw = doc["types"]
	}
	if len(raw) == 0 {
		return gitlabDefaultStages, nil
	}
	stages, ok := stringList(raw)
	if !ok {
		return nil, fmt.Errorf("importers: gitlab: cannot parse %q: it is not a list of stage names", "stages")
	}
	return stages, nil
}

// gitlabDefaultScripts returns the before/after scripts every job inherits.
// They are folded into each job's script rather than dropped, because a job
// whose setup vanished is a job that fails at run time.
func gitlabDefaultScripts(doc map[string]json.RawMessage, rep *Report) (before, after []string, err error) {
	sources := []json.RawMessage{doc["before_script"], doc["after_script"]}
	if raw, ok := doc["default"]; ok {
		var def map[string]json.RawMessage
		if err := decodeInto("gitlab", "default", raw, &def); err != nil {
			return nil, nil, err
		}
		if b, ok := def["before_script"]; ok {
			sources[0] = b
		}
		if a, ok := def["after_script"]; ok {
			sources[1] = a
		}
		if _, ok := def["image"]; ok {
			rep.warn("gitlab: default:image names the environment; the step keeps the commands but not the image")
		}
	}
	before, ok := stringList(sources[0])
	if !ok {
		return nil, nil, fmt.Errorf("importers: gitlab: cannot parse %q: it is not a list of commands", "before_script")
	}
	after, ok = stringList(sources[1])
	if !ok {
		return nil, nil, fmt.Errorf("importers: gitlab: cannot parse %q: it is not a list of commands", "after_script")
	}
	return before, after, nil
}

// gitlabJobConstructs are the per-job keys whose behaviour does not survive
// the import. Each one is reported against the job that used it.
var gitlabJobConstructs = []struct {
	key     string
	message string
}{
	{"only", "only: is dropped; the job is imported unconditionally"},
	{"except", "except: is dropped; the job is imported unconditionally"},
	{"when", "when: is dropped; the job no longer waits for a manual trigger or a failure"},
	{"parallel", "parallel: is dropped; the job is imported once, not as a matrix"},
	{"trigger", "trigger: is dropped; the downstream pipeline is not started"},
	{"extends", "extends: is not resolved; the inherited keys are not imported"},
	{"services", "services: is dropped; no side-car container is started"},
	{"cache", "cache: is dropped; Dhole caches by content, not by a key you invent"},
	{"retry", "retry: is dropped; the effect class decides what may be retried"},
	{"environment", "environment: is dropped; the deployment is not tracked"},
	{"allow_failure", "allow_failure: is dropped; a failing step fails the run"},
	{"secrets", "secrets: is dropped; the step redeems no secret reference"},
}

func gitlabJobs(doc map[string]json.RawMessage, stages, before, after []string, rep *Report) ([]gitlabJob, error) {
	names := make([]string, 0, len(doc))
	for name := range doc {
		names = append(names, name)
	}
	sort.Strings(names)

	stageIndex := make(map[string]int, len(stages))
	for i, s := range stages {
		stageIndex[s] = i
	}

	taken := make(map[string]bool, len(names))
	var jobs []gitlabJob
	for _, name := range names {
		if gitlabGlobalKeys[name] {
			continue
		}
		if strings.HasPrefix(name, ".") {
			rep.warn("gitlab: %q is a hidden template, not a job; it is not imported", name)
			continue
		}
		var body map[string]json.RawMessage
		if err := decodeInto("gitlab", name, doc[name], &body); err != nil {
			return nil, err
		}

		job := gitlabJob{name: name, stage: "test"}
		if raw, ok := body["stage"]; ok {
			if err := decodeInto("gitlab", name+".stage", raw, &job.stage); err != nil {
				return nil, err
			}
		}
		if _, known := stageIndex[job.stage]; !known {
			return nil, fmt.Errorf("importers: gitlab: job %q names stage %q, which the stages list does not define",
				name, job.stage)
		}

		script, ok := stringList(body["script"])
		if !ok {
			return nil, fmt.Errorf("importers: gitlab: cannot parse %q: script is not a list of commands", name)
		}
		if len(script) == 0 {
			// The job is still imported — dropping it is the one thing this
			// package must never do — but it can prove nothing about a job
			// whose commands it never saw.
			rep.unsupported("gitlab: job %q has no script:; the step is imported with nothing to run", name)
		}
		jobBefore, ok := stringList(body["before_script"])
		if !ok {
			return nil, fmt.Errorf("importers: gitlab: cannot parse %q: before_script is not a list of commands", name)
		}
		jobAfter, ok := stringList(body["after_script"])
		if !ok {
			return nil, fmt.Errorf("importers: gitlab: cannot parse %q: after_script is not a list of commands", name)
		}
		if len(jobBefore) > 0 || len(jobAfter) > 0 || len(before) > 0 || len(after) > 0 {
			rep.warn("gitlab: job %q keeps its before_script/after_script as part of one step", name)
		}
		job.script = concat(before, jobBefore, script, jobAfter, after)

		if raw, ok := body["needs"]; ok {
			job.hasNeeds = true
			needs, err := gitlabNeeds(name, raw)
			if err != nil {
				return nil, err
			}
			job.needs = needs
		}
		if _, ok := body["artifacts"]; ok {
			rep.warn("gitlab: job %q declares artifacts:paths; they are carried by the workspace port instead", name)
		}
		if _, ok := body["dependencies"]; ok {
			rep.warn("gitlab: job %q declares dependencies:; the artifact flow is carried by the workspace ports", name)
		}
		gitlabReportRules(name, body["rules"], rep)
		for _, c := range gitlabJobConstructs {
			if _, ok := body[c.key]; ok {
				rep.unsupported("gitlab: job %q: %s", name, c.message)
			}
		}
		jobs = append(jobs, job)
	}

	sort.SliceStable(jobs, func(i, j int) bool {
		if a, b := stageIndex[jobs[i].stage], stageIndex[jobs[j].stage]; a != b {
			return a < b
		}
		return jobs[i].name < jobs[j].name
	})
	for i := range jobs {
		jobs[i].id = uniqueID(taken, slug(jobs[i].name))
	}
	// `needs` names jobs, and the steps carry ids; resolve after every id
	// exists so the order of the file cannot matter.
	byName := make(map[string]string, len(jobs))
	for _, j := range jobs {
		byName[j.name] = j.id
	}
	for i := range jobs {
		for k, need := range jobs[i].needs {
			id, ok := byName[need]
			if !ok {
				return nil, fmt.Errorf("importers: gitlab: job %q needs job %q, which the document does not define",
					jobs[i].name, need)
			}
			jobs[i].needs[k] = id
		}
	}
	return jobs, nil
}

// gitlabNeeds reads both spellings: a list of job names, and a list of
// mappings with a `job:` key (the form that carries `artifacts:` or
// `optional:`).
func gitlabNeeds(job string, raw json.RawMessage) ([]string, error) {
	if names, ok := stringList(raw); ok {
		return names, nil
	}
	var entries []struct {
		Job string `json:"job"`
	}
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("importers: gitlab: cannot parse %q: needs is neither a list of job names nor of job mappings: %w",
			job, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Job == "" {
			return nil, fmt.Errorf("importers: gitlab: cannot parse %q: a needs entry names no job", job)
		}
		names = append(names, e.Job)
	}
	return names, nil
}

// gitlabReportRules names the rule clauses that decide at run time whether a
// job exists. Dhole has no equivalent, so the job is imported to run always —
// which is more, never less, than the source did, and the report says so.
func gitlabReportRules(job string, raw json.RawMessage, rep *Report) {
	if len(raw) == 0 {
		return
	}
	var rules []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rules); err != nil {
		rep.unsupported("gitlab: job %q: rules: is dropped; the job is imported unconditionally", job)
		return
	}
	clauses := map[string]bool{}
	for _, rule := range rules {
		for key := range rule {
			clauses[key] = true
		}
	}
	keys := make([]string, 0, len(clauses))
	for key := range clauses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		rep.unsupported("gitlab: job %q: rules:%s is not supported; the condition is dropped and the job always runs",
			job, key)
	}
}

// gitlabPreviousStageJobs returns the jobs of the nearest earlier stage that
// has any, which is what a job without `needs` actually waits for.
func gitlabPreviousStageJobs(stages []string, byStage map[string][]*gitlabJob, stage string) []string {
	index := -1
	for i, s := range stages {
		if s == stage {
			index = i
			break
		}
	}
	for i := index - 1; i >= 0; i-- {
		if jobs := byStage[stages[i]]; len(jobs) > 0 {
			ids := make([]string, 0, len(jobs))
			for _, j := range jobs {
				ids = append(ids, j.id)
			}
			sort.Strings(ids)
			return ids
		}
	}
	return nil
}

func concat(lists ...[]string) []string {
	var out []string
	for _, l := range lists {
		out = append(out, l...)
	}
	return out
}
