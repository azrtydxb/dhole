package importers

import (
	"encoding/json"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// The effect class is a proof obligation, not a guess. Nothing here tries to
// decide whether a command "looks safe": a command is pure only when it
// appears in a table someone wrote down deliberately, and everything else —
// an unknown binary, a shell substitution, a `make` target nobody vouched for
// — falls to at-most-once, which is never cached and never auto-retried.
//
// Erring the other way is the expensive direction. A side-effecting step
// mislabelled pure is cached and skipped, or retried into a second deploy;
// a pure step mislabelled at-most-once merely runs more often than it needed
// to (ADR 0002).

// pureCommand describes what a command may do and still be provably pure.
// A nil subcommands set means every invocation of the command qualifies.
type pureCommand struct {
	subcommands map[string]bool
}

func subs(names ...string) pureCommand {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return pureCommand{subcommands: m}
}

// pureCommands is deliberately short. Every entry is a command whose effect
// is confined to the working directory it was handed; adding one is a claim
// that running it twice with the same inputs is indistinguishable from
// running it once.
var pureCommands = map[string]pureCommand{
	"cat":           {},
	"cp":            {},
	"echo":          {},
	"gofmt":         {},
	"ls":            {},
	"mkdir":         {},
	"printf":        {},
	"pwd":           {},
	"true":          {},
	"pytest":        {},
	"tsc":           {},
	"eslint":        {},
	"go":            subs("build", "env", "fmt", "list", "mod", "test", "version", "vet"),
	"golangci-lint": subs("run", "version"),
	// `make deploy` is exactly the case a naive importer gets wrong: the
	// command looks like a build, and the target is what decides.
	"make":   subs("all", "build", "check", "fmt", "lint", "test", "vet"),
	"npm":    subs("ci", "install", "test"),
	"yarn":   subs("install"),
	"cargo":  subs("build", "check", "clippy", "fmt", "test"),
	"mvn":    subs("compile", "package", "test", "verify"),
	"gradle": subs("build", "check", "test"),
}

// scriptEffectClass classifies a whole script: pure only when every command
// in every line is provably pure, and an empty script proves nothing.
func scriptEffectClass(lines []string) dholev1.EffectClass {
	if len(lines) == 0 {
		return dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	}
	for _, line := range lines {
		if !lineIsProvablyPure(line) {
			return dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
		}
	}
	return dholev1.EffectClass_EFFECT_CLASS_PURE
}

// commandSeparators are the shell operators that start a new command inside
// one line. Splitting on them stops `go test ./... && curl https://deploy`
// from being judged by its first word alone.
var commandSeparators = strings.NewReplacer("&&", "\n", "||", "\n", ";", "\n", "|", "\n")

func lineIsProvablyPure(line string) bool {
	// A substitution can run anything at all, and its expansion is not in the
	// text we are reading, so nothing about it can be proven here. The same
	// goes for a workflow expression interpolated by the source system.
	if strings.Contains(line, "$(") || strings.Contains(line, "`") || strings.Contains(line, "${{") {
		return false
	}
	for _, cmd := range strings.Split(commandSeparators.Replace(line), "\n") {
		if !commandIsProvablyPure(cmd) {
			return false
		}
	}
	return true
}

func commandIsProvablyPure(cmd string) bool {
	fields := strings.Fields(cmd)
	// Leading VAR=value assignments belong to the command that follows.
	for len(fields) > 0 && strings.Contains(fields[0], "=") && !strings.HasPrefix(fields[0], "-") {
		fields = fields[1:]
	}
	if len(fields) == 0 {
		return true
	}
	rule, known := pureCommands[fields[0]]
	if !known {
		return false
	}
	if rule.subcommands == nil {
		return true
	}
	for _, arg := range fields[1:] {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		return rule.subcommands[arg]
	}
	// A command that takes a subcommand, invoked without one, is not the
	// invocation the table vouched for.
	return false
}

// mustJSON encodes a plugin reference payload. The inputs are strings this
// package built, so encoding cannot fail; the empty object keeps a step
// reference well-formed if that ever stops being true.
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
