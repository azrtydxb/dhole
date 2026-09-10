package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/kubernetes"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// chooseExecutor builds the backend named by DHOLE_EXECUTOR.
//
// The default is deliberately the process backend and not the most capable
// one: it needs nothing from its host, so an engine started with three
// variables set works. Every other backend costs something to reach — a
// cluster, a service account — and a default that reaches for it would fail
// on the laptop this engine is also meant to run on.
//
// An unknown name is an error rather than a fallback to process. Falling back
// would run a step on the host that asked for a sandbox, which is the one
// mistake this function must not make: a typo in a trust boundary reads as
// isolation right up until it does not hold.
func chooseExecutor() (executor.Executor, error) {
	kind := envOr("DHOLE_EXECUTOR", process.Kind)
	switch kind {
	case process.Kind:
		return process.New(), nil
	case kubernetes.Kind:
		return kubernetes.New(kubernetes.Config{
			Kubeconfig:     os.Getenv("DHOLE_KUBECONFIG"),
			Namespace:      os.Getenv("DHOLE_SANDBOX_NAMESPACE"),
			ServiceAccount: os.Getenv("DHOLE_SANDBOX_SERVICE_ACCOUNT"),
		})
	default:
		return nil, fmt.Errorf("DHOLE_EXECUTOR must be one of %s, got %q",
			strings.Join(executorKinds, ", "), kind)
	}
}

// executorKinds is what chooseExecutor accepts, in the order an error lists
// them. It is a variable beside the switch rather than derived from it because
// there is no registry to derive it from, and a name here that the switch does
// not handle is caught by TestEveryAdvertisedExecutorKindCanBeChosen.
var executorKinds = []string{process.Kind, kubernetes.Kind}
