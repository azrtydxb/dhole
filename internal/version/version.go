// Package version reports the identity of this build: the release version
// and the commit it was made from. Both are stamped by the linker in a
// released binary and derived from the embedded build info otherwise, so a
// binary can always name itself in a bug report.
package version

import (
	"runtime/debug"
	"sync"
)

// Stamped by the linker via -X at build time; see the Makefile's build target.
var (
	version string
	commit  string
)

const unknown = "unknown"

// Version returns the release version of this build.
func Version() string {
	resolve()
	return version
}

// Commit returns the git commit this build was made from.
func Commit() string {
	resolve()
	return commit
}

var once sync.Once

// resolve fills in whatever the linker did not stamp from the build info Go
// embeds in every binary, so an unstamped build (a `go test` binary, a
// `go install` from source) still identifies itself.
func resolve() {
	once.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			fallback()
			return
		}
		if version == "" {
			version = info.Main.Version
		}
		if commit == "" {
			for _, s := range info.Settings {
				if s.Key == "vcs.revision" {
					commit = s.Value
					break
				}
			}
		}
		fallback()
	})
}

func fallback() {
	if version == "" {
		version = unknown
	}
	if commit == "" {
		commit = unknown
	}
}
