package main

import (
	"strings"
	"testing"

	"github.com/azrtydxb/dhole/internal/executor/process"
)

func TestTheDefaultExecutorIsTheProcessBackend(t *testing.T) {
	t.Setenv("DHOLE_EXECUTOR", "")

	got, err := chooseExecutor()
	if err != nil {
		t.Fatalf("chooseExecutor with no DHOLE_EXECUTOR: %v", err)
	}
	if got.Kind() != process.Kind {
		t.Fatalf("default executor is %q, want %q", got.Kind(), process.Kind)
	}
}

// An unknown name must not quietly become the process backend: a step that
// asked for a sandbox would then run on the host that was meant to be
// protected from it.
func TestAnUnknownExecutorNameIsRefusedRatherThanDefaulted(t *testing.T) {
	t.Setenv("DHOLE_EXECUTOR", "prcoess")

	got, err := chooseExecutor()
	if err == nil {
		t.Fatalf("chooseExecutor accepted a misspelt backend and returned %q", got.Kind())
	}
	for _, kind := range executorKinds {
		if !strings.Contains(err.Error(), kind) {
			t.Errorf("the error does not name the %q backend, so it cannot be acted on: %v", kind, err)
		}
	}
}

// The guard on the list itself. A kind advertised in the error message but
// missing from the switch would tell an operator to set a value that is then
// refused.
func TestEveryAdvertisedExecutorKindCanBeChosen(t *testing.T) {
	for _, kind := range executorKinds {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("DHOLE_EXECUTOR", kind)
			// Backends that need a cluster fail to build here, and that is
			// fine — what must not happen is the switch rejecting the name.
			_, err := chooseExecutor()
			if err != nil && strings.Contains(err.Error(), "DHOLE_EXECUTOR must be one of") {
				t.Fatalf("%q is advertised but not handled: %v", kind, err)
			}
		})
	}
}
