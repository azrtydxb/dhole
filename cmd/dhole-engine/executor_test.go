package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/azrtydxb/dhole/internal/executor/kubernetes"
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

// A sandbox limit the operator believes is in force and is not is worse than
// no limit at all: they stop looking for the cause of a starved node. So a
// value that does not parse stops the engine, and says which variable.
func TestAMalformedSandboxQuantityIsAStartupErrorNamingTheVariable(t *testing.T) {
	for _, key := range sandboxResourceVars {
		// A negative quantity parses, and means nothing a pod can carry.
		for _, bad := range []string{"two cores", "-1"} {
			t.Run(key+"="+bad, func(t *testing.T) {
				t.Setenv("DHOLE_EXECUTOR", kubernetes.Kind)
				t.Setenv(key, bad)

				_, err := chooseExecutor()
				require.Error(t, err, "%s=%q was accepted", key, bad)
				require.Contains(t, err.Error(), key, "the error does not name the variable to fix")
			})
		}
	}
}

func TestSandboxResourcesAreReadFromTheEnvironment(t *testing.T) {
	t.Setenv("DHOLE_SANDBOX_CPU_REQUEST", "500m")
	t.Setenv("DHOLE_SANDBOX_CPU_LIMIT", "2")
	t.Setenv("DHOLE_SANDBOX_MEMORY_REQUEST", "256Mi")
	t.Setenv("DHOLE_SANDBOX_MEMORY_LIMIT", "1Gi")

	cfg, err := kubernetesConfig()
	require.NoError(t, err)
	got := cfg.Resources
	require.True(t, got.Requests.Cpu().Equal(resource.MustParse("500m")), "cpu request: %s", got.Requests.Cpu())
	require.True(t, got.Limits.Cpu().Equal(resource.MustParse("2")), "cpu limit: %s", got.Limits.Cpu())
	require.True(t, got.Requests.Memory().Equal(resource.MustParse("256Mi")), "memory request: %s", got.Requests.Memory())
	require.True(t, got.Limits.Memory().Equal(resource.MustParse("1Gi")), "memory limit: %s", got.Limits.Memory())
}

// Unset variables must leave the pod unsized, exactly as before they existed.
func TestUnsetSandboxResourcesConfigureNothing(t *testing.T) {
	for _, key := range sandboxResourceVars {
		t.Setenv(key, "")
	}
	got, err := sandboxResources()
	require.NoError(t, err)
	require.Empty(t, got.Requests)
	require.Empty(t, got.Limits)
}

// The API server refuses a pod whose request exceeds its limit, so an engine
// that started with one would fail EVERY step at acquire, far from the typo.
func TestASandboxRequestAboveItsLimitIsAStartupError(t *testing.T) {
	t.Setenv("DHOLE_SANDBOX_MEMORY_REQUEST", "2Gi")
	t.Setenv("DHOLE_SANDBOX_MEMORY_LIMIT", "1Gi")

	_, err := sandboxResources()
	require.Error(t, err)
	require.Contains(t, err.Error(), "DHOLE_SANDBOX_MEMORY_REQUEST")
	require.Contains(t, err.Error(), "DHOLE_SANDBOX_MEMORY_LIMIT")
}
