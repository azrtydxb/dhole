package scheduler

import (
	"encoding/json"
	"fmt"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// CommandScheme is a plugin reference whose opaque part IS the command, as
// JSON: `command:{"args":["/bin/sh","-c","..."],"env":{"K":"V"}}`.
//
// It is a placeholder, and a deliberately visible one. A JobDispatch must carry
// the argument vector and the environment the step runs with — an engine never
// calls back to ask what to run (docs/wire-contract.md) — but nothing in the
// tree yet turns an `oci://` or `cas://` plugin reference into those two
// fields: the resolver (Task 32) pins a reference to a digest and fetches its
// bytes, and lockfile resolution at save (Task 35) records the pin, but no
// step-type manifest yet says what command the artifact runs. Until one does,
// a definition that wants to run something has to say so itself.
//
// When plugin resolution arrives it replaces the whole of this file: the same
// two fields will come from the resolved plugin's manifest rather than from the
// reference text, and a definition carrying `command:` becomes a definition
// naming a plugin that does not exist.
const CommandScheme = "command:"

// commandSpec is the opaque part of a `command:` reference.
type commandSpec struct {
	Args []string          `json:"args"`
	Env  map[string]string `json:"env,omitempty"`
}

// commandFor resolves what a step runs and with which environment.
//
// A reference in any other scheme yields nothing rather than an error: the
// dispatch then carries no command, and the ENGINE refuses it with a status
// saying so. That is the right place for the refusal — it is reported against
// the step, in the run's log, instead of failing the whole Advance and leaving
// the run silently stuck.
func commandFor(step *dholev1.Step) ([]string, map[string]string, error) {
	ref := step.GetPluginRef()
	if !strings.HasPrefix(ref, CommandScheme) {
		return nil, nil, nil
	}
	var spec commandSpec
	if err := json.Unmarshal([]byte(strings.TrimPrefix(ref, CommandScheme)), &spec); err != nil {
		return nil, nil, fmt.Errorf("scheduler: step %q: decoding %s reference: %w",
			step.GetId(), CommandScheme, err)
	}
	if len(spec.Args) == 0 {
		return nil, nil, fmt.Errorf("scheduler: step %q: %s reference names no args", step.GetId(), CommandScheme)
	}
	return spec.Args, spec.Env, nil
}
