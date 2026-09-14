package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/kubernetes"
	"github.com/azrtydxb/dhole/internal/executor/process"
	"github.com/azrtydxb/dhole/internal/executor/vm"
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
	backend, err := chooseBackend()
	if err != nil {
		return nil, err
	}
	// An operator whose hosts really are built from one image can say so; the
	// process backend cannot discover that for itself and correctly refuses to
	// guess. Declaring it wrongly serves one host's results as another's, so
	// it is opt-in and never inferred (internal/executor.WithDeclaredEnvironment).
	if id := strings.TrimSpace(os.Getenv("DHOLE_ENVIRONMENT_IDENTITY")); id != "" {
		return executor.WithDeclaredEnvironment(backend, id)
	}
	return backend, nil
}

func chooseBackend() (executor.Executor, error) {
	kind := envOr("DHOLE_EXECUTOR", process.Kind)
	switch kind {
	case process.Kind:
		return process.New(), nil
	case kubernetes.Kind:
		cfg, err := kubernetesConfig()
		if err != nil {
			return nil, err
		}
		return kubernetes.New(cfg)
	case vm.Kind:
		return vm.New(vm.Config{
			Backend:     vm.Backend(os.Getenv("DHOLE_VM_BACKEND")),
			BinaryPath:  os.Getenv("DHOLE_VM_HYPERVISOR"),
			KernelImage: os.Getenv("DHOLE_VM_KERNEL"),
			RootfsImage: os.Getenv("DHOLE_VM_ROOTFS"),
			SnapshotDir: os.Getenv("DHOLE_VM_SNAPSHOT_DIR"),
			VCPUs:       envInt("DHOLE_VM_VCPUS"),
			MemMiB:      envInt("DHOLE_VM_MEM_MIB"),
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
var executorKinds = []string{process.Kind, kubernetes.Kind, vm.Kind}

// kubernetesConfig reads the kubernetes backend's settings from the
// environment. It is separate from New so what the engine was told can be
// checked without a cluster to build a client against.
func kubernetesConfig() (kubernetes.Config, error) {
	resources, err := sandboxResources()
	if err != nil {
		return kubernetes.Config{}, err
	}
	return kubernetes.Config{
		Kubeconfig:     os.Getenv("DHOLE_KUBECONFIG"),
		Namespace:      os.Getenv("DHOLE_SANDBOX_NAMESPACE"),
		ServiceAccount: os.Getenv("DHOLE_SANDBOX_SERVICE_ACCOUNT"),
		Resources:      resources,
	}, nil
}

// The variables sizing a kubernetes sandbox pod's step container. They are
// per engine, which in the chart is per tier (engines[].sandbox.resources):
// sizing a sandbox is the operator's capacity decision, not a pipeline
// author's — see kubernetes.Config.Resources for why, and for the per-step
// request that would be the natural extension.
const (
	envSandboxCPURequest    = "DHOLE_SANDBOX_CPU_REQUEST"
	envSandboxCPULimit      = "DHOLE_SANDBOX_CPU_LIMIT"
	envSandboxMemoryRequest = "DHOLE_SANDBOX_MEMORY_REQUEST"
	envSandboxMemoryLimit   = "DHOLE_SANDBOX_MEMORY_LIMIT"
)

// sandboxResourceVars lists them for tests and error messages.
var sandboxResourceVars = []string{
	envSandboxCPURequest, envSandboxCPULimit, envSandboxMemoryRequest, envSandboxMemoryLimit,
}

// sandboxResources reads the sandbox sizing from the environment. Unset
// variables configure nothing, and the pod stays unsized as it always was.
//
// Unlike envInt below, a value that does not parse is an ERROR naming the
// variable, and so is a negative one or a request above its limit. The
// difference is what a wrong guess costs: a VM that boots with one vcpu is
// slower and says so, but a limit the operator believes is in force and is not
// leaves a build free to take the node — the 36-second starvation this setting
// exists to prevent — while everyone looks for the cause somewhere else. A
// request above its limit is refused by the API server on every pod, so
// accepting one would move the failure from startup to every step.
func sandboxResources() (corev1.ResourceRequirements, error) {
	var out corev1.ResourceRequirements
	quantities := map[string]resource.Quantity{}
	for _, key := range sandboxResourceVars {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			continue
		}
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return out, fmt.Errorf("%s=%q is not a Kubernetes quantity (like 500m, 2 or 1Gi): %w", key, raw, err)
		}
		if q.Sign() < 0 {
			return out, fmt.Errorf("%s=%q is negative", key, raw)
		}
		quantities[key] = q
	}
	set := func(list *corev1.ResourceList, name corev1.ResourceName, key string) {
		if q, ok := quantities[key]; ok {
			if *list == nil {
				*list = corev1.ResourceList{}
			}
			(*list)[name] = q
		}
	}
	set(&out.Requests, corev1.ResourceCPU, envSandboxCPURequest)
	set(&out.Limits, corev1.ResourceCPU, envSandboxCPULimit)
	set(&out.Requests, corev1.ResourceMemory, envSandboxMemoryRequest)
	set(&out.Limits, corev1.ResourceMemory, envSandboxMemoryLimit)

	for _, pair := range [][2]string{
		{envSandboxCPURequest, envSandboxCPULimit},
		{envSandboxMemoryRequest, envSandboxMemoryLimit},
	} {
		req, hasReq := quantities[pair[0]]
		limit, hasLimit := quantities[pair[1]]
		if hasReq && hasLimit && req.Cmp(limit) > 0 {
			return corev1.ResourceRequirements{}, fmt.Errorf(
				"%s=%s exceeds %s=%s: the API server refuses such a pod, so every step would fail to acquire a sandbox",
				pair[0], req.String(), pair[1], limit.String())
		}
	}
	return out, nil
}

// envInt reads a numeric setting, treating anything unparseable as unset.
//
// Unset is the right reading of nonsense here because these are sizes with
// sane defaults: an engine that refused to start over DHOLE_VM_VCPUS=two would
// take a fleet down for a typo, where one that boots a one-vcpu guest is
// merely slower and says so in its registration.
func envInt(key string) int {
	n, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil {
		return 0
	}
	return n
}
