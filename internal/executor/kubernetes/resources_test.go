package kubernetes

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// These tests build pod specs without a cluster: what a sandbox pod asks the
// scheduler for is decided entirely in podSpec, before anything is sent.

// TestConfiguredSandboxResourcesReachTheStepContainer. A sandbox pod with no
// limits takes the whole node: on kw a `go build` starved the engine beside it
// for 36 seconds, past its lease, and the run thrashed.
func TestConfiguredSandboxResourcesReachTheStepContainer(t *testing.T) {
	e := &Executor{cfg: Config{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}}}

	got := e.podSpec("").Containers[0].Resources
	require.True(t, got.Requests.Cpu().Equal(resource.MustParse("500m")), "cpu request: %s", got.Requests.Cpu())
	require.True(t, got.Requests.Memory().Equal(resource.MustParse("256Mi")), "memory request: %s", got.Requests.Memory())
	require.True(t, got.Limits.Cpu().Equal(resource.MustParse("2")), "cpu limit: %s", got.Limits.Cpu())
	require.True(t, got.Limits.Memory().Equal(resource.MustParse("1Gi")), "memory limit: %s", got.Limits.Memory())
}

// TestUnconfiguredSandboxResourcesLeaveThePodUnsized. Nothing configured must
// mean exactly what it meant before the setting existed — no invented limit
// that OOM-kills a step on a cluster whose operator never asked for one.
func TestUnconfiguredSandboxResourcesLeaveThePodUnsized(t *testing.T) {
	got := (&Executor{}).podSpec("").Containers[0].Resources
	require.Empty(t, got.Requests, "an unconfigured executor requested resources")
	require.Empty(t, got.Limits, "an unconfigured executor set limits")
}

// TestSandboxResourcesOverrideOnlyTheKeysTheyName. A pod template may already
// size its container; the operator's setting wins where it speaks and leaves
// the template's other values alone. The template itself is never mutated —
// it is shared by every sandbox this executor makes.
func TestSandboxResourcesOverrideOnlyTheKeysTheyName(t *testing.T) {
	template := &corev1.PodSpec{Containers: []corev1.Container{{
		Name:  "step",
		Image: "busybox:1.36",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
		},
	}}}
	e := &Executor{cfg: Config{
		PodTemplate: template,
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")},
		},
	}}

	got := e.podSpec("").Containers[0].Resources
	require.True(t, got.Limits.Cpu().Equal(resource.MustParse("3")), "cpu limit: %s", got.Limits.Cpu())
	require.True(t, got.Requests.Memory().Equal(resource.MustParse("64Mi")), "the template's memory request was lost")
	require.True(t, template.Containers[0].Resources.Limits.Cpu().Equal(resource.MustParse("1")),
		"podSpec mutated the shared pod template")
}
