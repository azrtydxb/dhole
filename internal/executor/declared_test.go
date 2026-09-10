package executor_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// The case this exists for: a process backend runs against whatever its host
// carries and correctly reports no stable identity, so a fleet of engines built
// from ONE immutable image caches nothing — the hosts are identical and nothing
// in the executor can see that. Only the operator can say so.
func TestADeclaredEnvironmentGivesABackendThatHasNoneAnIdentity(t *testing.T) {
	bare := process.New()
	if _, err := bare.EnvironmentIdentity(); !errors.Is(err, executor.ErrNoStableIdentity) {
		t.Fatalf("the process backend now claims an identity of its own: %v", err)
	}

	declared, err := executor.WithDeclaredEnvironment(bare, "sha256:image-digest")
	if err != nil {
		t.Fatal(err)
	}
	got, err := declared.EnvironmentIdentity()
	if err != nil {
		t.Fatalf("a declared environment still reported an error: %v", err)
	}
	if got != "sha256:image-digest" {
		t.Errorf("declared identity is %q", got)
	}
}

// A blank declaration is refused rather than treated as "no identity". The two
// mean opposite things to an operator — "I did not configure this" and "I
// configured this to nothing" — and silently accepting the second turns a typo
// into a cache that never works, with nothing said about why.
func TestABlankDeclarationIsRefused(t *testing.T) {
	for _, blank := range []string{"", "   ", "\t"} {
		if _, err := executor.WithDeclaredEnvironment(process.New(), blank); err == nil {
			t.Errorf("a blank declaration %q was accepted", blank)
		}
	}
}

// Naming an environment says what steps run IN, never what they may do. A
// wrapper that widened capabilities would turn a cache-key declaration into a
// privilege grant.
func TestADeclarationChangesNothingButTheIdentity(t *testing.T) {
	bare := process.New()
	declared, err := executor.WithDeclaredEnvironment(bare, "sha256:x")
	if err != nil {
		t.Fatal(err)
	}

	if declared.Kind() != bare.Kind() {
		t.Errorf("the backend now calls itself %q instead of %q", declared.Kind(), bare.Kind())
	}
	if len(declared.Capabilities()) != len(bare.Capabilities()) {
		t.Errorf("declaring an environment changed the capability set: %v vs %v",
			declared.Capabilities(), bare.Capabilities())
	}
}

func TestDeclaringAnEnvironmentForNoBackendIsAnError(t *testing.T) {
	_, err := executor.WithDeclaredEnvironment(nil, "sha256:x")
	if err == nil || !strings.Contains(err.Error(), "backend") {
		t.Errorf("a nil backend was accepted or the error does not say so: %v", err)
	}
}

// TestADeclaredEnvironmentIsAlsoWhatItsSandboxesReport. The declaration is the
// answer for the whole backend, and the cache key is hashed against what the
// SANDBOX names. A wrapper that changed only the registration would have the
// engine announce one environment and its sandboxes name none — the one place
// a declared identity has to hold is the one it would not have reached.
func TestADeclaredEnvironmentIsAlsoWhatItsSandboxesReport(t *testing.T) {
	declared, err := executor.WithDeclaredEnvironment(process.New(), "sha256:image-digest")
	if err != nil {
		t.Fatal(err)
	}
	sb, err := declared.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sb.Release(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("releasing the sandbox: %v", err)
		}
	})

	got, err := sb.EnvironmentIdentity()
	if err != nil {
		t.Fatalf("a sandbox of a declared backend still reported an error: %v", err)
	}
	if got != "sha256:image-digest" {
		t.Errorf("sandbox identity is %q, not the declared one", got)
	}
}
