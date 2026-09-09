package kubernetes_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
	k8sexec "github.com/azrtydxb/dhole/internal/executor/kubernetes"
)

// testNamespace is created and destroyed by TestMain. It is deliberately not
// any namespace the rest of the project uses: these tests delete every pod
// they find in it.
const testNamespace = "dhole-exec-test"

// executorContract is the conformance suite every executor backend must pass.
func executorContract(t *testing.T, e executor.Executor) {
	t.Helper()
	executortest.Contract(t, e)
}

// kubeconfig returns the path to a live cluster's kubeconfig, or skips: these
// tests are worthless against a fake API server, because a fake never runs a
// pod, so nothing they assert about exec, eviction or cleanup would be real.
func kubeconfig(t *testing.T) string {
	t.Helper()
	path := os.Getenv("DHOLE_TEST_KUBECONFIG")
	if path == "" {
		t.Skip("DHOLE_TEST_KUBECONFIG is unset: point it at a kubeconfig for a live cluster to run the Kubernetes executor tests")
	}
	return path
}

// clientFor builds a client-go clientset straight from the kubeconfig, so the
// assertions about what is actually in the cluster do not go through the code
// under test.
func clientFor(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig(t))
	require.NoError(t, err)
	cs, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	return cs
}

func newExecutor(t *testing.T, mutate func(*k8sexec.Config)) executor.Executor {
	t.Helper()
	cfg := k8sexec.Config{Kubeconfig: kubeconfig(t), Namespace: testNamespace}
	if mutate != nil {
		mutate(&cfg)
	}
	e, err := k8sexec.New(cfg)
	require.NoError(t, err)
	return e
}

// waitForEmptyNamespace makes the pod counts below unambiguous: a pod another
// test released a moment ago may still be terminating, and a count taken while
// it lingers proves nothing about the pod this test created.
func waitForEmptyNamespace(t *testing.T, cs *kubernetes.Clientset) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(podsIn(t, cs)) == 0
	}, 120*time.Second, time.Second, "the test namespace still holds pods from an earlier test")
}

func podsIn(t *testing.T, cs *kubernetes.Clientset) []corev1.Pod {
	t.Helper()
	list, err := cs.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return list.Items
}

// TestMain creates and destroys the test namespace. Nothing else in the
// cluster is touched.
func TestMain(m *testing.M) {
	path := os.Getenv("DHOLE_TEST_KUBECONFIG")
	if path == "" {
		os.Exit(m.Run())
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		panic(err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	nsAPI := cs.CoreV1().Namespaces()
	_ = nsAPI.Delete(ctx, testNamespace, *metav1.NewDeleteOptions(0))
	for range 120 {
		if _, err := nsAPI.Get(ctx, testNamespace, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			break
		}
		time.Sleep(time.Second)
	}
	if _, err := nsAPI.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	}, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = nsAPI.Delete(ctx, testNamespace, *metav1.NewDeleteOptions(0))
	os.Exit(code)
}

// TestKubernetesExecutorContract holds the Kubernetes engine to exactly the
// contract the process engine passes. ADR 0006's claim is that the interface
// was not shaped around any one backend; this is where that is tested.
func TestKubernetesExecutorContract(t *testing.T) {
	executorContract(t, newExecutor(t, nil))
}

// TestPodPerStepLeaseIsDeletedOnRelease. A step-scoped sandbox that leaks its
// pod fills the cluster one build at a time, and nothing in a passing build
// would ever show it.
func TestPodPerStepLeaseIsDeletedOnRelease(t *testing.T) {
	cs := clientFor(t)
	e := newExecutor(t, nil)

	waitForEmptyNamespace(t, cs)
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)
	code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "true"}})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
	require.Len(t, podsIn(t, cs), 1, "the sandbox should have created exactly one pod")

	require.NoError(t, sb.Release(t.Context()))
	require.Eventually(t, func() bool {
		return len(podsIn(t, cs)) == 0
	}, 90*time.Second, time.Second, "the step sandbox's pod is still in the cluster after Release")
}

// TestPipelineLeaseReusesOnePodAcrossSteps. A lease scope that quietly created
// a fresh pod per Exec would be decorative: it would cost the same as a step
// lease while still marking every step non-cacheable (internal/cache.Eligible).
func TestPipelineLeaseReusesOnePodAcrossSteps(t *testing.T) {
	e := newExecutor(t, nil)
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeasePipeline})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })

	// The hostname of a pod's container is the pod name, and the boot id of
	// the sandbox filesystem differs per pod: writing in one Exec and reading
	// it back in the next proves the second ran in the same container, not
	// merely in an identically named one.
	var first strings.Builder
	code, err := sb.Exec(t.Context(), executor.Cmd{
		Args:   []string{"sh", "-c", "hostname > /tmp/witness; hostname"},
		Stdout: &first,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)

	var second strings.Builder
	code, err = sb.Exec(t.Context(), executor.Cmd{
		Args:   []string{"sh", "-c", "cat /tmp/witness; hostname"},
		Stdout: &second,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)

	name := strings.TrimSpace(first.String())
	require.NotEmpty(t, name)
	lines := strings.Fields(second.String())
	require.Len(t, lines, 2, "the second exec must see the file the first wrote: %q", second.String())
	require.Equal(t, []string{name, name}, lines, "the second Exec ran in a different pod")
}

// TestPodEvictionSurfacesAsStepFailureNotSuccess. A pod that disappears
// mid-exec — evicted, preempted, node drained — must never look like a step
// that passed, or the build is silently wrong and the result may be cached.
func TestPodEvictionSurfacesAsStepFailureNotSuccess(t *testing.T) {
	cs := clientFor(t)
	e := newExecutor(t, nil)
	waitForEmptyNamespace(t, cs)
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })

	type result struct {
		code int32
		err  error
	}
	done := make(chan result, 1)
	go func() {
		code, err := sb.Exec(context.WithoutCancel(t.Context()), executor.Cmd{
			Args: []string{"sh", "-c", "sleep 120; exit 0"},
		})
		done <- result{code, err}
	}()

	// Give the exec time to attach before the pod goes away.
	time.Sleep(3 * time.Second)
	pods := podsIn(t, cs)
	require.Len(t, pods, 1)
	require.NoError(t, cs.CoreV1().Pods(testNamespace).Delete(
		t.Context(), pods[0].Name, *metav1.NewDeleteOptions(0)))

	select {
	case got := <-done:
		require.Error(t, got.err, "a pod that vanished mid-exec is not a step that succeeded")
		require.Contains(t, strings.ToLower(got.err.Error()), "evict",
			"the failure must name eviction so an operator can tell it from a real test failure")
		require.NotEqual(t, int32(0), got.code, "an evicted step must not report exit 0")
	case <-time.After(90 * time.Second):
		t.Fatal("Exec did not return after its pod was deleted")
	}
}

// TestSignalTerminatesLongExecAndTheProcessActuallyStops. The contract times
// the return; this also proves the process tree is gone from the container
// afterwards, so a backend that merely abandons the exec stream fails here.
func TestSignalTerminatesLongExecAndTheProcessActuallyStops(t *testing.T) {
	e := newExecutor(t, nil)
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeasePipeline})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })

	done := make(chan int32, 1)
	go func() {
		code, _ := sb.Exec(context.WithoutCancel(t.Context()), executor.Cmd{
			Args: []string{"sh", "-c", "sleep 300; exit 0"},
		})
		done <- code
	}()

	require.Eventually(t, func() bool {
		if err := sb.Signal(t.Context(), executor.SIGTERM); err != nil {
			return false
		}
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 30*time.Second, 500*time.Millisecond, "SIGTERM did not stop the command")

	var ps strings.Builder
	code, err := sb.Exec(t.Context(), executor.Cmd{
		Args:   []string{"sh", "-c", "ps -o args"},
		Stdout: &ps,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
	require.NotContains(t, ps.String(), "sleep 300", "the signalled process is still running in the pod")
}

// TestCapabilitiesAreHonest. A backend that claims what its pod template does
// not provide gets scheduled work it cannot run — and, worse, work whose
// isolation assumptions are wrong.
func TestCapabilitiesAreHonest(t *testing.T) {
	plain := newExecutor(t, nil)
	require.Equal(t, "kubernetes", plain.Kind())
	require.Contains(t, plain.Capabilities(), dholev1.Capability_CAPABILITY_NETWORK)
	require.NotContains(t, plain.Capabilities(), dholev1.Capability_CAPABILITY_PRIVILEGED,
		"a default pod template grants no privilege")
	require.NotContains(t, plain.Capabilities(), dholev1.Capability_CAPABILITY_HOST_MOUNT,
		"a default pod template mounts nothing from the host")

	privileged := newExecutor(t, func(c *k8sexec.Config) {
		yes := true
		c.PodTemplate = &corev1.PodSpec{Containers: []corev1.Container{{
			Name:            "step",
			Image:           "busybox:1.36",
			SecurityContext: &corev1.SecurityContext{Privileged: &yes},
		}}}
	})
	require.Contains(t, privileged.Capabilities(), dholev1.Capability_CAPABILITY_PRIVILEGED,
		"a template that really does run privileged must say so")

	hostMount := newExecutor(t, func(c *k8sexec.Config) {
		c.PodTemplate = &corev1.PodSpec{
			Containers: []corev1.Container{{Name: "step", Image: "busybox:1.36"}},
			Volumes: []corev1.Volume{{
				Name:         "host",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp"}},
			}},
		}
	})
	require.Contains(t, hostMount.Capabilities(), dholev1.Capability_CAPABILITY_HOST_MOUNT)
}

// TestEnvironmentIdentityIsADigestNotATag. A tag is not an identity: the same
// tag resolves to different bytes over time, so caching against one serves
// results built in an environment that no longer exists.
func TestEnvironmentIdentityIsADigestNotATag(t *testing.T) {
	id, err := newExecutor(t, nil).EnvironmentIdentity()
	require.NoError(t, err)
	require.Contains(t, id, "@sha256:", "the identity must be a content digest")
	require.NotContains(t, id, ":1.36", "a mutable tag must not appear in a cache key")
}

// TestEnvironmentIdentityWithoutAResolvableImageIsNoStableIdentity. Honesty in
// the other direction: when nothing can be digested, say so rather than
// inventing a key.
func TestEnvironmentIdentityWithoutAResolvableImageIsNoStableIdentity(t *testing.T) {
	e := newExecutor(t, func(c *k8sexec.Config) {
		c.PodTemplate = &corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "step",
			Image: "dhole.invalid/nothing/here:v1",
		}}}
	})
	id, err := e.EnvironmentIdentity()
	require.Empty(t, id)
	require.ErrorIs(t, err, executor.ErrNoStableIdentity)
}

// TestReleaseIsIdempotentAndSurvivesAnAlreadyDeletedPod. Cleanup paths run
// unconditionally, often after the thing they clean up is already gone; a
// Release that errors there turns a finished run into a failed one.
func TestReleaseIsIdempotentAndSurvivesAnAlreadyDeletedPod(t *testing.T) {
	cs := clientFor(t)
	e := newExecutor(t, nil)
	waitForEmptyNamespace(t, cs)
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)

	pods := podsIn(t, cs)
	require.Len(t, pods, 1)
	require.NoError(t, cs.CoreV1().Pods(testNamespace).Delete(
		t.Context(), pods[0].Name, *metav1.NewDeleteOptions(0)))
	require.Eventually(t, func() bool {
		_, err := cs.CoreV1().Pods(testNamespace).Get(t.Context(), pods[0].Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, 90*time.Second, time.Second)

	require.NoError(t, sb.Release(t.Context()), "releasing a sandbox whose pod is already gone is not an error")
	require.NoError(t, sb.Release(t.Context()), "Release is idempotent")
}
